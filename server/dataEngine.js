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

export function isTruthyActiveStatus(v) {
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

/** 比较单元格与查找键：trim、数字/字符串宽松相等 */
export function valuesEqualForJoin(a, b) {
  if (a === b) return true;
  const sa = a == null ? '' : String(a).trim();
  const sb = b == null ? '' : String(b).trim();
  if (sa === sb) return true;
  if (sa === '' || sb === '') return false;
  const na = Number(sa);
  const nb = Number(sb);
  if (Number.isFinite(na) && Number.isFinite(nb) && na === nb) return true;
  return false;
}

function engineJoinTrace(enabled, message) {
  if (!enabled) return;
  // eslint-disable-next-line no-console
  console.log(`[Engine] ${message}`);
}

/** 按表头宽松匹配列名（trim / 大小写），从行上取值 */
function getRowFieldLoose(row, fieldName, headers) {
  if (!row || fieldName == null) return undefined;
  const want = normalize(fieldName);
  if (!want) return undefined;
  if (Array.isArray(headers) && headers.length) {
    const h = headers.find((x) => normalizeLower(x) === normalizeLower(want));
    if (h != null && Object.prototype.hasOwnProperty.call(row, h)) return row[h];
  }
  if (Object.prototype.hasOwnProperty.call(row, want)) return row[want];
  const hitKey = Object.keys(row).find((k) => normalizeLower(k) === normalizeLower(want));
  return hitKey != null ? row[hitKey] : undefined;
}

function parseTableFieldToken(tok) {
  const t = normalize(tok);
  const idx = t.lastIndexOf('.');
  if (idx <= 0) return null;
  return { table: t.slice(0, idx), field: t.slice(idx + 1) };
}

function parseLegacyChainNodesFromToken(token) {
  const raw = String(token || '');
  if (!raw.startsWith('CHAIN|')) return null;
  const rest = raw.slice('CHAIN|'.length);
  const parts = rest.split('->').map((x) => x.trim()).filter(Boolean);
  const nodes = parts.map(parseTableFieldToken).filter(Boolean);
  return nodes.length >= 2 ? nodes : null;
}

/**
 * 将旧版 CHAIN 节点序列转为结构化 JoinPath：
 * [n0, n1, n2, n3, n4, ...] => hop(n0→n1), hop(n1.table 上取 n2 字段 → n3), …, terminal(n_{k-1}.table, n_k.field)
 */
function legacyChainNodesToStructuredPath(nodes) {
  if (!nodes?.length || nodes.length < 3 || nodes.length % 2 === 0) return null;
  const out = [];
  const lastPairStart = nodes.length - 2;
  for (let i = 0; i < lastPairStart; i += 2) {
    const src = nodes[i];
    const tgt = nodes[i + 1];
    if (!src?.table || !src?.field || !tgt?.table || !tgt?.field) return null;
    if (i === 0) {
      out.push({
        sourceTable: normalize(src.table),
        sourceField: normalize(src.field),
        targetTable: normalize(tgt.table),
        targetField: normalize(tgt.field),
      });
    } else {
      const prevValNode = nodes[i];
      out.push({
        sourceTable: normalize(nodes[i - 1].table),
        sourceField: normalize(prevValNode.field),
        targetTable: normalize(tgt.table),
        targetField: normalize(tgt.field),
      });
    }
  }
  const keyN = nodes[lastPairStart];
  const valN = nodes[lastPairStart + 1];
  if (!keyN?.table || !valN?.field) return null;
  out.push({
    targetTable: normalize(keyN.table),
    valueField: normalize(valN.field),
  });
  return out;
}

function stringArrayJoinPathToStructured(pathArr) {
  const nodes = pathArr.map(parseTableFieldToken).filter(Boolean);
  return legacyChainNodesToStructuredPath(nodes);
}

function isHopSegment(seg) {
  if (!seg || typeof seg !== 'object') return false;
  return (
    normalize(seg.sourceTable) &&
    normalize(seg.sourceField) &&
    normalize(seg.targetTable) &&
    normalize(seg.targetField) &&
    !normalize(seg.valueField)
  );
}

function isTerminalSegment(seg) {
  if (!seg || typeof seg !== 'object') return false;
  return normalize(seg.targetTable) && normalize(seg.valueField) && !normalize(seg.targetField);
}

function normalizeStructuredJoinPath(arr) {
  if (!Array.isArray(arr) || arr.length < 2) return null;
  const out = [];
  for (let i = 0; i < arr.length; i++) {
    const s = arr[i];
    if (!s || typeof s !== 'object') return null;
    if (i === arr.length - 1) {
      if (!isTerminalSegment(s)) return null;
      out.push({
        targetTable: normalize(s.targetTable),
        valueField: normalize(s.valueField),
      });
    } else {
      if (!isHopSegment(s)) return null;
      out.push({
        sourceTable: normalize(s.sourceTable),
        sourceField: normalize(s.sourceField),
        targetTable: normalize(s.targetTable),
        targetField: normalize(s.targetField),
      });
    }
  }
  return out;
}

/**
 * 从配置项解析标准 JoinPath（对象数组）；支持旧 string[] 与 CHAIN| 仅用于读入迁移。
 */
export function parseJoinPathFromConfig({ joinPath, physicalColumn }) {
  if (Array.isArray(joinPath) && joinPath.length) {
    if (typeof joinPath[0] === 'object' && joinPath[0] !== null) {
      return normalizeStructuredJoinPath(joinPath);
    }
    if (typeof joinPath[0] === 'string') {
      return stringArrayJoinPathToStructured(joinPath.map((x) => normalize(x)).filter(Boolean));
    }
  }
  const pc = normalize(physicalColumn);
  if (pc.startsWith('CHAIN|')) {
    const nodes = parseLegacyChainNodesFromToken(pc);
    return legacyChainNodesToStructuredPath(nodes);
  }
  return null;
}

/**
 * 万能递归路径解析：主表行出发，按 hop 在维表中逐层定位行，终端段读取 valueField。
 * @param {{ trace?: boolean, traceLabel?: string }} [opts] — traceLabel 用于首跳「字段: xxx」日志
 */
export function resolveRecursiveValue({ mainRow, mainTableName, joinPath, tablesMap, trace = false, traceLabel = '' }) {
  const path = Array.isArray(joinPath) ? normalizeStructuredJoinPath(joinPath) : null;
  if (!path?.length) return '';

  let currentRow = mainRow;
  let currentTable = normalize(mainTableName);
  const mainKey = normalize(mainTableName);

  for (let i = 0; i < path.length; i++) {
    const seg = path[i];
    const last = i === path.length - 1;

    if (last) {
      if (!isTerminalSegment(seg)) return '';
      const tt = normalize(seg.targetTable);
      const vf = normalize(seg.valueField);
      if (normalize(currentTable) !== tt) {
        engineJoinTrace(trace, `终端段表名不匹配：当前 ${currentTable}，期望 ${tt}`);
        return '';
      }
      const termTbl = tablesMap.get(tt);
      const headers = termTbl?.headers || [];
      engineJoinTrace(trace, `找到匹配行，准备提取 ${vf} 的值...`);
      const v = getRowFieldLoose(currentRow, vf, headers);
      const out = v == null ? '' : normalize(v);
      if (out !== '') engineJoinTrace(trace, `提取成功: ${out}`);
      else engineJoinTrace(trace, `提取失败: 字段 ${vf} 为空或不存在`);
      return out;
    }

    if (!isHopSegment(seg)) return '';
    const st = normalize(seg.sourceTable);
    const sf = normalize(seg.sourceField);
    const tt = normalize(seg.targetTable);
    const tf = normalize(seg.targetField);

    if (i > 0 && normalize(currentTable) !== st) {
      engineJoinTrace(trace, `Join 中断：当前表 ${currentTable} 与 hop.sourceTable ${st} 不一致`);
      return '';
    }
    if (i === 0 && st && currentTable && st !== currentTable) {
      // 兼容历史 CHAIN：首段表名与主表 Excel 逻辑名不一致时仍从主表行读 sourceField
    }

    const srcHeaders = i === 0 ? tablesMap.get(mainKey)?.headers || [] : tablesMap.get(st)?.headers || [];
    const rawKey = getRowFieldLoose(currentRow, sf, srcHeaders);
    if (rawKey == null || normalize(rawKey) === '') {
      engineJoinTrace(trace, `起点值为空，字段 ${sf}（表 ${i === 0 ? mainKey : st}）`);
      return '';
    }

    const fieldTag = i === 0 && traceLabel ? traceLabel : sf;
    engineJoinTrace(trace, `起点值: ${rawKey} (字段: ${fieldTag})`);

    const tbl = tablesMap.get(tt);
    if (!tbl?.rows?.length) {
      engineJoinTrace(trace, `维表 ${tt} 无数据行`);
      return '';
    }

    engineJoinTrace(trace, `正在表 ${tt} 中寻找 ${tf} = ${rawKey} 的行...`);
    const hit = tbl.rows.find((r) => valuesEqualForJoin(getRowFieldLoose(r, tf, tbl.headers), rawKey));
    if (!hit) {
      engineJoinTrace(trace, `未在表 ${tt} 中找到 ${tf} = ${rawKey} 的行`);
      return '';
    }

    engineJoinTrace(trace, `已在表 ${tt} 中命中行，继续 Join 链路`);
    currentRow = hit;
    currentTable = tt;
  }
  return '';
}

function partDescriptorFromToken(token, mainTableHint = '') {
  const t = normalize(token);
  if (!t) return null;
  if (t.startsWith('CHAIN|')) {
    const structured = parseJoinPathFromConfig({ joinPath: null, physicalColumn: t });
    const nodes = parseLegacyChainNodesFromToken(t);
    const head = nodes?.[0];
    const fld = head ? normalize(head.field) : '';
    const tbl = head ? normalize(head.table) : '';
    return {
      physicalColumn: '',
      sourceField: fld,
      sourceTable: tbl || mainTableHint,
      joinPath: structured || undefined,
    };
  }
  if (t.includes('@')) {
    const at = t.lastIndexOf('@');
    const col = t.slice(0, at);
    const file = t.slice(at + 1);
    return { physicalColumn: col, sourceField: col, sourceTable: file, joinPath: undefined };
  }
  return { physicalColumn: t, sourceField: t, sourceTable: '', joinPath: undefined };
}

function normalizeMappingPart(p, mainTableHint = '') {
  if (!p || typeof p !== 'object') return null;
  const structured =
    parseJoinPathFromConfig({ joinPath: p.joinPath, physicalColumn: p.physicalColumn }) || undefined;
  let physicalColumn = normalize(p.physicalColumn);
  if (physicalColumn.startsWith('CHAIN|')) physicalColumn = '';
  const sourceField = normalize(p.sourceField);
  const sourceTable = normalize(p.sourceTable);
  const firstHopField = structured?.[0] && typeof structured[0] === 'object' ? normalize(structured[0].sourceField) : '';
  if (!physicalColumn && sourceField) physicalColumn = sourceField;
  if (!physicalColumn && firstHopField) physicalColumn = firstHopField;
  if (!structured && !physicalColumn) return null;
  return {
    physicalColumn,
    sourceField: sourceField || physicalColumn || firstHopField,
    sourceTable,
    joinPath: structured,
  };
}

export function buildStandardMap(mappingArr) {
  const out = new Map();
  for (const it of mappingArr || []) {
    const standardKey = normalize(it?.standardKey);
    if (!standardKey) continue;

    const op = normalizeLower(it?.operator);
    if (op === 'concat' && Array.isArray(it?.parts) && it.parts.length >= 2) {
      const parts = it.parts.map((pt) => normalizeMappingPart(pt, '')).filter(Boolean);
      if (parts.length >= 2) {
        out.set(standardKey, { standardKey, mode: 'concat', parts });
        continue;
      }
    }

    let physicalColumn = normalize(it?.physicalColumn);
    const pipeParts = parseConcatPipeToken(physicalColumn);
    if (pipeParts) {
      const parts = pipeParts.map((tok) => partDescriptorFromToken(tok)).filter(Boolean);
      if (parts.length >= 2) {
        out.set(standardKey, { standardKey, mode: 'concat', parts });
        continue;
      }
    }

    const structured = parseJoinPathFromConfig({ joinPath: it?.joinPath, physicalColumn });
    if (physicalColumn.startsWith('CHAIN|')) physicalColumn = '';
    const sourceField = normalize(it?.sourceField);
    if (!physicalColumn && sourceField && !structured) {
      physicalColumn = sourceField;
    }
    const sourceTable = normalize(it?.sourceTable);
    if (!physicalColumn && !structured) continue;

    const firstHopField = structured?.[0] && typeof structured[0] === 'object' ? normalize(structured[0].sourceField) : '';
    out.set(standardKey, {
      standardKey,
      mode: 'simple',
      physicalColumn: physicalColumn || sourceField || firstHopField || '',
      sourceTable,
      sourceField: sourceField || physicalColumn || firstHopField || '',
      joinPath: structured || undefined,
    });
  }
  return out;
}

/** 单列 / CONCAT 子段解析：CHAIN、结构化 joinPath、主表直连、col@维表 启发式跨表 */
function resolveScalarMappingPart(part, mainRow, mainTableName, tablesMap, standardKeyHint, options = {}) {
  if (!part) return '';
  const trace = Boolean(options.trace);
  const lbl = normalize(options.traceLabel) || normalize(standardKeyHint);
  const recOpts = { trace, traceLabel: lbl };
  if (Array.isArray(part.joinPath) && part.joinPath.length >= 2) {
    if (typeof part.joinPath[0] === 'object' && part.joinPath[0] !== null) {
      return resolveRecursiveValue({ mainRow, mainTableName, joinPath: part.joinPath, tablesMap, ...recOpts });
    }
    const pathObj = parseJoinPathFromConfig({ joinPath: part.joinPath });
    if (pathObj?.length) return resolveRecursiveValue({ mainRow, mainTableName, joinPath: pathObj, tablesMap, ...recOpts });
  }
  const token = normalize(part.physicalColumn);
  if (token.startsWith('CHAIN|')) {
    const path = parseJoinPathFromConfig({ joinPath: null, physicalColumn: token });
    if (path) return resolveRecursiveValue({ mainRow, mainTableName, joinPath: path, tablesMap, ...recOpts });
  }
  const col = normalize(part.sourceField) || token;
  const fileRef = normalize(part.sourceTable);
  if (col && fileRef && isSpreadsheetFileRef(fileRef) && tablesMap?.size) {
    const dimLogical = buildTableNameIndex(fileRef);
    if (normalize(dimLogical) !== normalize(mainTableName)) {
      const cross = resolveHeuristicDimLookup({
        mainRow,
        mainTableName,
        dimLogical,
        valueCol: col,
        tablesMap,
        standardKey: standardKeyHint,
        trace,
        traceLabel: lbl,
      });
      if (cross !== '') return cross;
    }
  }
  const mainTbl = tablesMap.get(normalize(mainTableName));
  const direct = getRowFieldLoose(mainRow, col, mainTbl?.headers);
  return direct == null ? '' : normalize(direct);
}

function resolvePartValue(part, mainRow, mainTableName, tablesMap, standardKeyHint = '', options = {}) {
  return resolveScalarMappingPart(part, mainRow, mainTableName, tablesMap, standardKeyHint, options);
}

function resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackCol, options = {}) {
  const trace = Boolean(options.trace);
  const traceLabel = normalize(options.traceLabel);
  const sk = entry ? normalize(entry.standardKey) : '';
  const ro = { trace, traceLabel: traceLabel || sk };
  if (!entry) {
    if (!fallbackCol) return '';
    const mainTbl = tablesMap.get(normalize(mainTableName));
    const v = getRowFieldLoose(mainRow, fallbackCol, mainTbl?.headers);
    return v == null ? '' : normalize(v);
  }
  if (entry.mode === 'concat' && Array.isArray(entry.parts)) {
    return entry.parts.map((p) => resolveScalarMappingPart(p, mainRow, mainTableName, tablesMap, sk, ro)).join('');
  }
  if (entry.mode === 'simple' && Array.isArray(entry.joinPath) && entry.joinPath.length >= 2) {
    if (typeof entry.joinPath[0] === 'object' && entry.joinPath[0] !== null) {
      return resolveRecursiveValue({ mainRow, mainTableName, joinPath: entry.joinPath, tablesMap, ...ro });
    }
    const pathObj = parseJoinPathFromConfig({ joinPath: entry.joinPath });
    if (pathObj?.length) return resolveRecursiveValue({ mainRow, mainTableName, joinPath: pathObj, tablesMap, ...ro });
  }
  return resolveScalarMappingPart(
    {
      physicalColumn: entry.physicalColumn,
      sourceField: entry.sourceField,
      sourceTable: entry.sourceTable,
      joinPath: entry.joinPath,
    },
    mainRow,
    mainTableName,
    tablesMap,
    sk,
    ro
  );
}

function columnNameForMainRow(entry) {
  if (!entry || entry.mode === 'concat') return '';
  const jp = entry.joinPath;
  if (Array.isArray(jp) && jp.length && jp[0] && typeof jp[0] === 'object') {
    const sf = normalize(jp[0].sourceField);
    if (sf) return sf;
  }
  const pc = normalize(entry.physicalColumn);
  if (pc.startsWith('CHAIN|')) {
    const nodes = parseLegacyChainNodesFromToken(pc);
    if (nodes?.[0]?.field) return normalize(nodes[0].field);
  }
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

function buildTableNameIndex(fileName) {
  const base = path.basename(fileName);
  const noExt = base.replace(/\.(xlsx|xls)$/i, '');
  return noExt.replace(/^\d{8}_/, '');
}

function isSpreadsheetFileRef(s) {
  const x = normalize(s);
  return /\.xlsx$/i.test(x) || /\.xls$/i.test(x);
}

function getXlsxTableByLogicalName(logicalName, tablesMap) {
  const want = normalize(logicalName);
  if (!want || !(tablesMap instanceof Map)) return null;
  if (tablesMap.has(want)) return tablesMap.get(want);
  for (const [k, t] of tablesMap) {
    if (normalize(k) === want) return t;
    if (t?.fileName && buildTableNameIndex(t.fileName) === want) return t;
  }
  return null;
}

/** 主表行上用于关联维表的外键列猜测（含 brand→brand） */
function fkTryOrder(standardKey) {
  const sk = normalize(standardKey);
  const o = [];
  if (sk === 'brand') o.push('brand', 'brand_id', 'base_brand_id', 'wms_brand_id');
  if (sk === 'lastCode') o.push('associated_last', 'associated_last_type', 'last_id', 'last_type_id');
  if (sk === 'soleCode') o.push('associated_sole_info', 'sole_id', 'associated_sole');
  if (sk === 'colorCode') o.push('initial_sample_color_id', 'color_id');
  if (sk === 'materialCode') o.push('material_id', 'main_material');
  if (sk) o.push(`${sk}_id`, sk);
  return [...new Set(o.filter(Boolean))];
}

function guessFkColumnFromMainRow(mainRow, standardKey) {
  if (!mainRow || typeof mainRow !== 'object') return '';
  const keys = Object.keys(mainRow);
  const keyLower = new Map(keys.map((k) => [normalizeLower(k), k]));
  for (const cand of fkTryOrder(standardKey)) {
    const k = keyLower.get(normalizeLower(cand));
    if (k != null && mainRow[k] != null && normalize(mainRow[k]) !== '') return k;
  }
  return '';
}

function resolveHeaderAlias(headers, col) {
  if (!col || !Array.isArray(headers)) return '';
  const want = normalizeLower(col);
  const ex = headers.find((h) => normalizeLower(h) === want);
  return ex ? normalize(ex) : '';
}

function guessDimKeyColumnOnHeaders(headers) {
  const idCol = resolveHeaderAlias(headers, 'id');
  if (idCol) return idCol;
  const h0 = headers?.[0];
  return h0 ? normalize(h0) : 'id';
}

/**
 * col@维表.xlsx 且无 CHAIN：用主表外键在维表按主键行匹配，取目标列（启发式）。
 */
function resolveHeuristicDimLookup({
  mainRow,
  mainTableName,
  dimLogical,
  valueCol,
  tablesMap,
  standardKey,
  trace = false,
  traceLabel = '',
}) {
  const dim = getXlsxTableByLogicalName(dimLogical, tablesMap);
  if (!dim?.rows?.length) return '';
  if (normalize(dimLogical) === normalize(mainTableName)) return '';

  const mainTbl = tablesMap.get(normalize(mainTableName));
  const mainHeaders = mainTbl?.headers || [];

  const fkCol = guessFkColumnFromMainRow(mainRow, standardKey);
  if (!fkCol) return '';
  const fkVal = getRowFieldLoose(mainRow, fkCol, mainHeaders);
  if (fkVal == null || normalize(fkVal) === '') return '';

  const headers = dim.headers || [];
  const keyCol = guessDimKeyColumnOnHeaders(headers);
  const tag = traceLabel || fkCol;
  engineJoinTrace(trace, `起点值: ${fkVal} (字段: ${tag})`);
  engineJoinTrace(trace, `正在表 ${dimLogical} 中寻找 ${keyCol} = ${fkVal} 的行...`);
  const hit = dim.rows.find((r) => valuesEqualForJoin(getRowFieldLoose(r, keyCol, headers), fkVal));
  if (!hit) {
    engineJoinTrace(trace, `未在表 ${dimLogical} 中找到 ${keyCol} = ${fkVal} 的行`);
    return '';
  }

  engineJoinTrace(trace, `找到匹配行，准备提取 ${valueCol} 的值...`);
  const v = getRowFieldLoose(hit, valueCol, headers);
  const out = v == null ? '' : normalize(v);
  if (out !== '') engineJoinTrace(trace, `提取成功: ${out}`);
  else engineJoinTrace(trace, `提取失败: 字段 ${valueCol} 为空或不存在`);
  return out;
}

function buildTablesMap(xlsxTables) {
  const map = new Map();
  for (const t of xlsxTables) {
    const tableName = buildTableNameIndex(t.fileName);
    map.set(tableName, t);
  }
  return map;
}

/** 读取目录下全部 Excel，构建与聚合引擎一致的 tablesMap */
export async function loadExcelFolderAsTablesMap(folderPath) {
  const excelFiles = (await listFiles(folderPath)).filter((n) =>
    ['.xlsx', '.xls'].includes(path.extname(n).toLowerCase())
  );
  if (!excelFiles.length) {
    return { xlsxTables: [], tablesMap: new Map() };
  }
  const xlsxTables = excelFiles.map((fileName) => {
    const fullPath = path.join(folderPath, fileName);
    const { headers, rows } = readSheetRows(fullPath);
    return { fileName, fullPath, headers, rows };
  });
  return { xlsxTables, tablesMap: buildTablesMap(xlsxTables) };
}

/** 根据 AI/heuristic 的 smartSuggestions 推断主表文件（需含款号/品牌/状态列） */
export function inferMainTableFromSmartSuggestions(xlsxTables, smartSuggestions) {
  const byKey = new Map();
  for (const s of smartSuggestions || []) {
    const k = normalize(s?.standardKey);
    const f = normalize(s?.sourceField);
    if (k && f) byKey.set(k, f);
  }
  const styleCol = byKey.get('styleCode') || '';
  const brandCol = byKey.get('brand') || '';
  const statusCol = byKey.get('status') || byKey.get('data_status') || '';
  const required = [styleCol, brandCol, statusCol].filter(Boolean);
  if (!required.length) return null;
  return guessStyleMainTable(xlsxTables, required);
}

/**
 * 用最新 XLSX 与黄金值回测 joinPathSuggestions；仅通过 strict 字符串相等的路径带 valid:true 返回。
 */
export async function validateJoinPathSuggestionsWithGoldenXlsx({
  joinPathSuggestions,
  goldenByStandardKey,
  folderPath,
  smartSuggestions,
}) {
  const list = Array.isArray(joinPathSuggestions) ? joinPathSuggestions : [];
  const goldMap =
    goldenByStandardKey instanceof Map ? goldenByStandardKey : new Map(Object.entries(goldenByStandardKey || {}));
  const { xlsxTables, tablesMap } = await loadExcelFolderAsTablesMap(folderPath);
  if (!list.length) return [];

  const main = inferMainTableFromSmartSuggestions(xlsxTables, smartSuggestions || []);
  if (!main || !tablesMap.size) {
    // eslint-disable-next-line no-console
    console.warn(
      '[Validator] 无法回测：无 XLSX 或无法根据 smartSuggestions 识别主表，已拒绝全部 joinPathSuggestions'
    );
    return [];
  }

  const mainTableName = buildTableNameIndex(main.fileName);
  const byKey = new Map();
  for (const s of smartSuggestions || []) {
    const k = normalize(s?.standardKey);
    const f = normalize(s?.sourceField);
    if (k && f) byKey.set(k, f);
  }
  const statusCol = byKey.get('status') || byKey.get('data_status') || '';

  const validated = [];
  for (const jp of list) {
    const targetStandardKey = typeof jp?.targetStandardKey === 'string' ? jp.targetStandardKey.trim() : '';
    const pathArr = Array.isArray(jp?.path) ? jp.path.map((p) => String(p).trim()).filter(Boolean) : [];
    if (!targetStandardKey || pathArr.length < 2) continue;

    const goldenRaw = goldMap.get(targetStandardKey);
    const golden = goldenRaw != null ? String(goldenRaw).trim() : '';
    if (!golden) {
      // eslint-disable-next-line no-console
      console.warn(`[Validator] 无黄金样本，跳过回测并丢弃 joinPathSuggestions：${targetStandardKey}`);
      continue;
    }

    const structured = parseJoinPathFromConfig({ joinPath: pathArr });
    if (!structured?.length) {
      // eslint-disable-next-line no-console
      console.warn(`[Validator] 路径无法解析为 DSL：${targetStandardKey}`, pathArr);
      continue;
    }

    const rowsCandidate = main.rows || [];
    const tryRows = statusCol
      ? rowsCandidate.filter((r) => isTruthyActiveStatus(r?.[statusCol]))
      : rowsCandidate;
    const rowsToScan = tryRows.length ? tryRows : rowsCandidate;

    let lastResolved = '';
    let passed = false;
    for (const row of rowsToScan) {
      const resolved = resolveRecursiveValue({
        mainRow: row,
        mainTableName,
        joinPath: structured,
        tablesMap,
      });
      lastResolved = resolved;
      if (resolved != null && String(resolved).trim() === golden) {
        passed = true;
        break;
      }
    }

    if (passed) {
      validated.push({
        targetStandardKey,
        path: pathArr,
        valid: true,
      });
    } else {
      const shown = lastResolved != null ? String(lastResolved).trim() : '';
      // eslint-disable-next-line no-console
      console.warn(`[Validator] AI 路径校验失败：预期 ${golden}，实际得到 ${shown}`);
    }
  }
  return validated;
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

    const brandVal = resolveStandardMappedValue(standardMap.get('brand'), row, mainTableName, tablesMap, brandCol);

    inventory.push({
      id: `${dateDirName || 'latest'}-${main.fileName}-${i + 1}`,
      style_wms: styleCol ? normalize(row?.[styleCol]) : '',
      brand: brandVal,
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
 * 全量聚合：读取 mapping_config、最新快照目录下全部 XLSX（多表 Join / CONCAT）、对齐 3D 资产文件名。
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
export async function resolveFirstActiveRowFromFolder({ folderPath, standardMap, traceJoins = false }) {
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
  const traceOpts = (label) => (traceJoins ? { trace: true, traceLabel: label } : {});
  const tryRow = (row) => {
    const brandVal = resolveStandardMappedValue(
      standardMap.get('brand'),
      row,
      mainTableName,
      tablesMap,
      brandCol,
      traceOpts('brand')
    );
    const lastCodeVal = resolveStandardMappedValue(
      standardMap.get('lastCode'),
      row,
      mainTableName,
      tablesMap,
      lastCol,
      traceOpts('lastCode')
    );
    const soleCodeVal = resolveStandardMappedValue(
      standardMap.get('soleCode'),
      row,
      mainTableName,
      tablesMap,
      soleCol,
      traceOpts('soleCode')
    );
    const colorCodeVal = resolveStandardMappedValue(
      standardMap.get('colorCode'),
      row,
      mainTableName,
      tablesMap,
      colorCol,
      traceOpts('colorCode')
    );
    const materialCodeVal = resolveStandardMappedValue(
      standardMap.get('materialCode'),
      row,
      mainTableName,
      tablesMap,
      materialCol,
      traceOpts('materialCode')
    );

    return {
      style_wms: styleCol ? normalize(row?.[styleCol]) : '',
      brand: brandVal,
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
