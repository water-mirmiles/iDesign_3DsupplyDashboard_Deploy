import fse from 'fs-extra';
import path from 'path';
import XLSX from 'xlsx';

function isDateDirName(name) {
  return /^\d{4}-\d{2}-\d{2}$/.test(name);
}

function normalize(s) {
  return String(s ?? '').trim();
}

function normalizeLower(s) {
  return normalize(s).toLowerCase();
}

function stripExt(fileName) {
  return fileName.replace(/\.[^.]+$/, '');
}

function isTruthyActiveStatus(v) {
  const s = normalizeLower(v);
  return s === '生效' || s === 'active' || s === '有效' || s === 'enabled' || s === 'true' || s === '1';
}

function safeJsonRead(filePath) {
  try {
    if (!fse.existsSync(filePath)) return null;
    return fse.readJsonSync(filePath);
  } catch {
    return null;
  }
}

function pickMappingArray(mappingConfig) {
  // Expected shapes:
  // { savedAt, mapping: { latestDir, mapping: MappingEntry[] } }
  // or { latestDir, mapping: MappingEntry[] }
  // or legacy/test
  const root = mappingConfig?.mapping ?? mappingConfig;
  const arr = root?.mapping;
  if (Array.isArray(arr)) return arr;
  return null;
}

function parseConcatPipeToken(physicalColumn) {
  const raw = String(physicalColumn || '');
  if (!raw.startsWith('CONCAT|')) return null;
  const rest = raw.slice('CONCAT|'.length);
  const parts = rest.split('||').map((s) => s.trim()).filter(Boolean);
  return parts.length >= 2 ? parts : null;
}

function partDescriptorFromToken(token) {
  const t = normalize(token);
  if (!t) return null;
  if (t.startsWith('CHAIN|')) {
    const joinPath = t.slice('CHAIN|'.length).split('->').map((x) => normalize(x)).filter(Boolean);
    const head = joinPath[0] || '';
    const dot = head.indexOf('.');
    const tbl = dot > 0 ? head.slice(0, dot) : '';
    const fld = dot > 0 ? head.slice(dot + 1) : head;
    return { physicalColumn: t, sourceField: fld, sourceTable: tbl, joinPath };
  }
  if (t.includes('@')) {
    const at = t.lastIndexOf('@');
    const col = t.slice(0, at);
    const file = t.slice(at + 1);
    return { physicalColumn: col, sourceField: col, sourceTable: file, joinPath: null };
  }
  return { physicalColumn: t, sourceField: t, sourceTable: '', joinPath: null };
}

function normalizeMappingPart(p) {
  if (!p || typeof p !== 'object') return null;
  const joinPath = Array.isArray(p.joinPath) ? p.joinPath.map((x) => normalize(x)).filter(Boolean) : null;
  let physicalColumn = normalize(p.physicalColumn);
  const sourceField = normalize(p.sourceField);
  const sourceTable = normalize(p.sourceTable);
  if (joinPath && joinPath.length >= 2) {
    physicalColumn = `CHAIN|${joinPath.join('->')}`;
  }
  if (!physicalColumn && sourceField) physicalColumn = sourceField;
  if (!physicalColumn) return null;
  return { physicalColumn, sourceField: sourceField || physicalColumn, sourceTable, joinPath };
}

export function buildStandardMap(mappingArr) {
  const out = new Map();
  for (const it of mappingArr || []) {
    const standardKey = normalize(it?.standardKey);
    if (!standardKey) continue;

    const op = normalizeLower(it?.operator);
    if (op === 'concat' && Array.isArray(it?.parts) && it.parts.length >= 2) {
      const parts = it.parts.map(normalizeMappingPart).filter(Boolean);
      if (parts.length >= 2) {
        out.set(standardKey, { mode: 'concat', parts });
        continue;
      }
    }

    let physicalColumn = normalize(it?.physicalColumn);
    const pipeParts = parseConcatPipeToken(physicalColumn);
    if (pipeParts) {
      const parts = pipeParts.map((tok) => partDescriptorFromToken(tok)).filter(Boolean);
      if (parts.length >= 2) {
        out.set(standardKey, { mode: 'concat', parts });
        continue;
      }
    }

    const joinPath = Array.isArray(it?.joinPath) ? it.joinPath.map((p) => normalize(p)).filter(Boolean) : null;
    if (joinPath && joinPath.length >= 2) {
      physicalColumn = `CHAIN|${joinPath.join('->')}`;
    } else if (physicalColumn && physicalColumn.startsWith('CHAIN|')) {
      // keep CHAIN token from saved config
    }
    const sourceField = normalize(it?.sourceField);
    if (!physicalColumn && sourceField && (!joinPath || joinPath.length < 2)) {
      physicalColumn = sourceField;
    }
    const sourceTable = normalize(it?.sourceTable);
    if (!physicalColumn) continue;
    out.set(standardKey, { mode: 'simple', physicalColumn, sourceTable, sourceField: sourceField || physicalColumn, joinPath });
  }
  return out;
}

function resolvePartValue(part, mainRow, mainTableName, tablesMap) {
  if (!part) return '';
  const token = normalize(part.physicalColumn);
  const chain = parseChainToken(token);
  if (chain) return resolveChainValue({ mainRow, mainTableName, chainNodes: chain, tablesMap });
  const col = normalize(part.sourceField) || token;
  return normalize(mainRow?.[col]);
}

function resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackCol) {
  if (!entry) {
    return fallbackCol ? normalize(mainRow?.[fallbackCol]) : '';
  }
  if (entry.mode === 'concat' && Array.isArray(entry.parts)) {
    return entry.parts.map((p) => resolvePartValue(p, mainRow, mainTableName, tablesMap)).join('');
  }
  return resolvePartValue(entry, mainRow, mainTableName, tablesMap);
}

/** 主表行上用于 guess / 直连读取的列名（CHAIN 取首段字段） */
function columnNameForMainRow(entry) {
  if (!entry || entry.mode === 'concat') return '';
  const pc = entry.physicalColumn || '';
  const chain = parseChainToken(pc);
  if (chain && chain.length) return normalize(chain[0].field);
  if (pc.startsWith('CHAIN|')) return '';
  return normalize(entry.sourceField) || normalize(entry.physicalColumn);
}

function readSheetRows(fullPath) {
  const wb = XLSX.readFile(fullPath, { cellText: false, cellDates: true });
  const sheetName = wb.SheetNames?.[0];
  if (!sheetName) return { headers: [], rows: [] };
  const sheet = wb.Sheets[sheetName];
  if (!sheet) return { headers: [], rows: [] };
  const headerRows = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: '' });
  const headers = Array.isArray(headerRows?.[0]) ? headerRows[0].map((h) => normalize(h)).filter(Boolean) : [];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: 1 });
  return { headers, rows };
}

async function listFiles(dir) {
  const entries = await fse.readdir(dir, { withFileTypes: true });
  return entries.filter((e) => e.isFile()).map((e) => e.name);
}

async function getDateDirsSorted(dataTablesDir) {
  const entries = await fse.readdir(dataTablesDir, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory() && isDateDirName(e.name))
    .map((e) => e.name)
    .sort();
}

async function loadAssetsBaseNames(dir) {
  try {
    const files = await listFiles(dir);
    return new Set(files.map(stripExt).map(normalize).filter(Boolean));
  } catch {
    return new Set();
  }
}

function guessStyleMainTable(xlsxTables, requiredCols) {
  // requiredCols: array of physical column names that must exist in same table
  // Pick the table with max matches (and all required if possible)
  let best = null;
  for (const t of xlsxTables) {
    const cols = new Set(t.headers.map(normalizeLower));
    const matches = requiredCols.filter((c) => cols.has(normalizeLower(c))).length;
    const hasAll = matches === requiredCols.length;
    if (!best) best = { ...t, matches, hasAll };
    else if (hasAll && !best.hasAll) best = { ...t, matches, hasAll };
    else if (hasAll === best.hasAll && matches > best.matches) best = { ...t, matches, hasAll };
  }
  return best;
}

function computeBrandCoverage(activeRows, brandKey, has3dAnyKey) {
  const map = new Map();
  for (const r of activeRows) {
    const brand = normalize(r?.[brandKey] ?? 'Unknown') || 'Unknown';
    const linked = Boolean(r?.[has3dAnyKey]);
    const cur = map.get(brand) || { brand, linked: 0, unlinked: 0 };
    if (linked) cur.linked += 1;
    else cur.unlinked += 1;
    map.set(brand, cur);
  }
  return Array.from(map.values()).sort((a, b) => (b.linked + b.unlinked) - (a.linked + a.unlinked));
}

function computeDelta(current, previous) {
  const delta = current - previous;
  const pct = previous > 0 ? (delta / previous) * 100 : null;
  return { delta, pct };
}

function parseChainToken(token) {
  // token: "CHAIN|tableA.field->tableB.id->tableB.code"
  const raw = String(token || '');
  if (!raw.startsWith('CHAIN|')) return null;
  const rest = raw.slice('CHAIN|'.length);
  const parts = rest.split('->').map((x) => x.trim()).filter(Boolean);
  const nodes = parts
    .map((p) => {
      const idx = p.lastIndexOf('.');
      if (idx <= 0) return null;
      return { table: p.slice(0, idx), field: p.slice(idx + 1) };
    })
    .filter(Boolean);
  return nodes.length >= 2 ? nodes : null;
}

function buildTableNameIndex(fileName) {
  // 20260415_ods_xxx_df.xlsx -> ods_xxx_df
  const base = path.basename(fileName);
  const noExt = base.replace(/\.(xlsx|xls)$/i, '');
  return noExt.replace(/^\d{8}_/, '');
}

function buildTablesMap(xlsxTables) {
  const map = new Map();
  for (const t of xlsxTables) {
    const tableName = buildTableNameIndex(t.fileName);
    map.set(tableName, t);
  }
  return map;
}

function resolveChainValue({ mainRow, mainTableName, chainNodes, tablesMap }) {
  // Interpret minimal pattern:
  // node0: mainTable.field (or any table.field but we read from mainRow)
  // node1: lookupTable.keyField
  // node2: lookupTable.valueField (same table as node1)
  // For longer nodes, we treat them as repeated lookups:
  // [main.field, t1.id, t1.code, t2.id, t2.code, ...] where (t1.id -> t1.code) produces next lookup value
  if (!chainNodes?.length) return '';
  const first = chainNodes[0];
  const startField = first.field;
  let currentValue = normalize(mainRow?.[startField]);
  if (!currentValue) return '';

  for (let i = 1; i < chainNodes.length; i += 2) {
    const keyNode = chainNodes[i];
    const valNode = chainNodes[i + 1];
    // CHAIN 必须成对出现：key -> value；若缺失，说明链路不完整（常见于只到 id 层），直接判失败
    if (!keyNode || !valNode) return '';
    // 若 AI 给出了跨表但缺少中间列（如 master.fk->t1.id->t2.code），无法确定性执行 join，直接判失败
    if (keyNode.table !== valNode.table) return '';

    const t = tablesMap.get(keyNode.table);
    if (!t) return '';
    const rows = t.rows || [];
    const keyField = keyNode.field;
    const valueField = valNode.field;

    const hit = rows.find((r) => normalize(r?.[keyField]) === currentValue);
    if (!hit) return '';
    currentValue = normalize(hit?.[valueField]);
    if (!currentValue) return '';
  }
  return currentValue;
}

async function aggregateForDateRoot({ storageRoot, dateDirName, standardMap }) {
  const dataTablesDir = path.join(storageRoot, 'data_tables');
  const dateDir = dateDirName ? path.join(dataTablesDir, dateDirName) : dataTablesDir;

  const excelFiles = (await listFiles(dateDir)).filter((n) => ['.xlsx', '.xls'].includes(path.extname(n).toLowerCase()));
  const xlsxTables = excelFiles.map((fileName) => {
    const fullPath = path.join(dateDir, fileName);
    const { headers, rows } = readSheetRows(fullPath);
    return { fileName, fullPath, headers, rows };
  });
  const tablesMap = buildTablesMap(xlsxTables);

  const styleCol = columnNameForMainRow(standardMap.get('styleCode'));
  const brandCol = columnNameForMainRow(standardMap.get('brand'));
  const statusCol =
    columnNameForMainRow(standardMap.get('status')) || columnNameForMainRow(standardMap.get('data_status'));
  const lastCol = columnNameForMainRow(standardMap.get('lastCode'));
  const soleCol = columnNameForMainRow(standardMap.get('soleCode'));
  const colorCol = columnNameForMainRow(standardMap.get('colorCode'));
  const materialCol = columnNameForMainRow(standardMap.get('materialCode'));

  const required = [styleCol, brandCol, statusCol].filter(Boolean);
  const main = required.length ? guessStyleMainTable(xlsxTables, required) : null;

  if (!main) {
    return {
      dateDirName,
      kpis: {
        activeStyles: 0,
        matchedLasts: 0,
        matchedSoles: 0,
        lastCoverage: 0,
        soleCoverage: 0,
        stylesWithAny3D: 0,
        any3DCoveragePercent: 0,
      },
      brandCoverage: [],
      inventory: [],
      meta: { mainTable: null, reason: 'No main table matched required columns. Check mapping_config.json.' },
    };
  }

  const lastsSet = await loadAssetsBaseNames(path.join(storageRoot, 'assets', 'lasts'));
  const solesSet = await loadAssetsBaseNames(path.join(storageRoot, 'assets', 'soles'));

  const mainTableName = buildTableNameIndex(main.fileName);
  const inventory = [];
  for (let i = 0; i < main.rows.length; i++) {
    const row = main.rows[i] || {};
    const statusVal = statusCol ? row?.[statusCol] : '';
    const isActive = isTruthyActiveStatus(statusVal);
    if (!isActive) continue;

    const lastCodeVal = resolveStandardMappedValue(standardMap.get('lastCode'), row, mainTableName, tablesMap, lastCol);
    const soleCodeVal = resolveStandardMappedValue(standardMap.get('soleCode'), row, mainTableName, tablesMap, soleCol);
    const colorCodeVal = resolveStandardMappedValue(standardMap.get('colorCode'), row, mainTableName, tablesMap, colorCol);
    const materialCodeVal = resolveStandardMappedValue(
      standardMap.get('materialCode'),
      row,
      mainTableName,
      tablesMap,
      materialCol
    );

    const has3DLast = lastCodeVal ? lastsSet.has(stripExt(lastCodeVal)) || lastsSet.has(lastCodeVal) : false;
    const has3DSole = soleCodeVal ? solesSet.has(stripExt(soleCodeVal)) || solesSet.has(soleCodeVal) : false;
    const has3DAny = has3DLast || has3DSole;

    inventory.push({
      id: `${dateDirName || 'latest'}-${main.fileName}-${i + 1}`,
      style_wms: styleCol ? normalize(row?.[styleCol]) : '',
      brand: brandCol ? normalize(row?.[brandCol]) : '',
      colorCode: colorCodeVal,
      materialCode: materialCodeVal,
      lastCode: lastCodeVal || undefined,
      lastStatus: has3DLast ? 'matched' : 'missing',
      soleCode: soleCodeVal || undefined,
      soleStatus: has3DSole ? 'matched' : 'missing',
      data_status: 'active',
      lastUpdated: dateDirName || '',
      updatedBy: 'Storage',
      sourceTable: main.fileName,
      __has3DAny: has3DAny,
      __has3DLast: has3DLast,
      __has3DSole: has3DSole,
    });
  }

  const activeStyles = inventory.length;
  const matchedLasts = inventory.filter((x) => x.__has3DLast).length;
  const matchedSoles = inventory.filter((x) => x.__has3DSole).length;
  const stylesWithAny3D = inventory.filter((x) => x.__has3DAny).length;
  const lastCoverage = activeStyles > 0 ? Math.round((matchedLasts / activeStyles) * 100) : 0;
  const soleCoverage = activeStyles > 0 ? Math.round((matchedSoles / activeStyles) * 100) : 0;
  const any3DCoveragePercent = activeStyles > 0 ? Math.round((stylesWithAny3D / activeStyles) * 100) : 0;

  const brandCoverage = computeBrandCoverage(inventory, 'brand', '__has3DAny');

  // cleanup private fields for API
  const inventoryOut = inventory.map(({ __has3DAny, __has3DLast, __has3DSole, ...rest }) => rest);

  return {
    dateDirName,
    kpis: {
      activeStyles,
      matchedLasts,
      matchedSoles,
      lastCoverage,
      soleCoverage,
      stylesWithAny3D,
      any3DCoveragePercent,
    },
    brandCoverage,
    inventory: inventoryOut,
    meta: { mainTable: main.fileName, requiredCols: required },
  };
}

/**
 * 全量聚合：读取 mapping_config、最新快照目录下全部 XLSX（多表 Join/CHAIN/CONCAT）、对齐 3D 资产文件名。
 * 供「认证并应用到看板」触发；结果由调用方写入 final_dashboard_data.json。
 */
export async function processAllData({ storageRoot }) {
  // eslint-disable-next-line no-console
  console.log('[Engine] 正在合成款号数据...');
  const t0 = Date.now();
  const agg = await aggregateProjectData({ storageRoot });
  const latest = agg.latest;
  const kpis = latest.kpis;

  const payload = {
    generatedAt: new Date().toISOString(),
    dates: agg.dates,
    mapping: agg.mapping,
    meta: latest.meta,
    kpis: {
      activeStyles: kpis.activeStyles,
      matched3DLasts: kpis.matchedLasts,
      matched3DSoles: kpis.matchedSoles,
      lastCoverage: kpis.lastCoverage,
      soleCoverage: kpis.soleCoverage,
      stylesWithAny3D: kpis.stylesWithAny3D,
      any3DCoveragePercent: kpis.any3DCoveragePercent,
      deltaActiveStyles: agg.deltas.activeStyles.delta,
      deltaMatched3DLasts: agg.deltas.matchedLasts.delta,
      deltaMatched3DSoles: agg.deltas.matchedSoles.delta,
    },
    brandCoverage: latest.brandCoverage,
    inventory: latest.inventory,
  };

  // eslint-disable-next-line no-console
  console.log(
    `[Engine] 合成完成，耗时 ${Date.now() - t0}ms；生效款 ${kpis.activeStyles}，任一3D命中 ${kpis.stylesWithAny3D}（${kpis.any3DCoveragePercent}%）`
  );
  return payload;
}

export async function persistFinalDashboardData(storageRoot, payload) {
  const outPath = path.join(storageRoot, 'final_dashboard_data.json');
  await fse.writeJson(outPath, payload, { spaces: 2 });
  return outPath;
}

/**
 * 在任意目录（如 storage/sandbox）上跑与 aggregateForDateRoot 相同的解析逻辑，取第一条生效行用于沙盒校验。
 */
export async function resolveFirstActiveRowFromFolder({ folderPath, standardMap }) {
  const excelFiles = (await listFiles(folderPath)).filter((n) => ['.xlsx', '.xls'].includes(path.extname(n).toLowerCase()));
  if (!excelFiles.length) {
    return { ok: false, error: 'sandbox 目录下没有 XLSX 文件', row: null, mainTable: null };
  }
  const xlsxTables = excelFiles.map((fileName) => {
    const fullPath = path.join(folderPath, fileName);
    const { headers, rows } = readSheetRows(fullPath);
    return { fileName, fullPath, headers, rows };
  });
  const tablesMap = buildTablesMap(xlsxTables);

  const styleCol = columnNameForMainRow(standardMap.get('styleCode'));
  const brandCol = columnNameForMainRow(standardMap.get('brand'));
  const statusCol =
    columnNameForMainRow(standardMap.get('status')) || columnNameForMainRow(standardMap.get('data_status'));
  const lastCol = columnNameForMainRow(standardMap.get('lastCode'));
  const soleCol = columnNameForMainRow(standardMap.get('soleCode'));
  const colorCol = columnNameForMainRow(standardMap.get('colorCode'));
  const materialCol = columnNameForMainRow(standardMap.get('materialCode'));

  const required = [styleCol, brandCol, statusCol].filter(Boolean);
  const main = required.length ? guessStyleMainTable(xlsxTables, required) : null;
  if (!main) {
    return { ok: false, error: '无法识别主表（需映射款号/品牌/状态且沙盒表头匹配）', row: null, mainTable: null };
  }

  const mainTableName = buildTableNameIndex(main.fileName);
  const tryRow = (row) => {
    const lastCodeVal = resolveStandardMappedValue(standardMap.get('lastCode'), row, mainTableName, tablesMap, lastCol);
    const soleCodeVal = resolveStandardMappedValue(standardMap.get('soleCode'), row, mainTableName, tablesMap, soleCol);
    const colorCodeVal = resolveStandardMappedValue(standardMap.get('colorCode'), row, mainTableName, tablesMap, colorCol);
    const materialCodeVal = resolveStandardMappedValue(
      standardMap.get('materialCode'),
      row,
      mainTableName,
      tablesMap,
      materialCol
    );

    return {
      style_wms: styleCol ? normalize(row?.[styleCol]) : '',
      brand: brandCol ? normalize(row?.[brandCol]) : '',
      lastCode: lastCodeVal,
      soleCode: soleCodeVal,
      colorCode: colorCodeVal,
      materialCode: materialCodeVal,
      status: statusCol ? normalize(String(row?.[statusCol] ?? '')) : '',
    };
  };

  for (let i = 0; i < main.rows.length; i++) {
    const row = main.rows[i] || {};
    const statusVal = statusCol ? row?.[statusCol] : '';
    const isActive = isTruthyActiveStatus(statusVal);
    if (!isActive) continue;

    return {
      ok: true,
      mainTable: main.fileName,
      row: tryRow(row),
      usedFallbackRow: false,
    };
  }

  // 沙盒样本可能没有“生效”状态：退回第一行仅用于维度联通性验证
  if (main.rows.length > 0) {
    const row = main.rows[0] || {};
    return {
      ok: true,
      mainTable: main.fileName,
      row: tryRow(row),
      usedFallbackRow: true,
      warning: '未找到生效行，已使用首行做沙盒校验（生产仍以生效行为准）',
    };
  }

  return { ok: false, error: '沙盒主表无数据行', row: null, mainTable: main?.fileName || null };
}

export async function aggregateProjectData({ storageRoot }) {
  const mappingConfigPath = path.join(storageRoot, 'mapping_config.json');
  const mappingConfig = safeJsonRead(mappingConfigPath);
  const mappingArr = pickMappingArray(mappingConfig);
  const standardMap = buildStandardMap(mappingArr || []);

  const dataTablesDir = path.join(storageRoot, 'data_tables');
  const dateDirs = await getDateDirsSorted(dataTablesDir);
  const latest = dateDirs.length ? dateDirs[dateDirs.length - 1] : '';
  const prev = dateDirs.length >= 2 ? dateDirs[dateDirs.length - 2] : '';

  const latestAgg = await aggregateForDateRoot({ storageRoot, dateDirName: latest, standardMap });
  const prevAgg = prev ? await aggregateForDateRoot({ storageRoot, dateDirName: prev, standardMap }) : null;

  const deltaActive = prevAgg ? computeDelta(latestAgg.kpis.activeStyles, prevAgg.kpis.activeStyles) : { delta: 0, pct: null };
  const deltaLastMatched = prevAgg ? computeDelta(latestAgg.kpis.matchedLasts, prevAgg.kpis.matchedLasts) : { delta: 0, pct: null };
  const deltaSoleMatched = prevAgg ? computeDelta(latestAgg.kpis.matchedSoles, prevAgg.kpis.matchedSoles) : { delta: 0, pct: null };

  return {
    dates: { latest, prev },
    mapping: {
      hasConfig: Boolean(mappingArr && mappingArr.length),
      configPath: mappingConfigPath,
    },
    latest: latestAgg,
    prev: prevAgg,
    deltas: {
      activeStyles: deltaActive,
      matchedLasts: deltaLastMatched,
      matchedSoles: deltaSoleMatched,
    },
  };
}

