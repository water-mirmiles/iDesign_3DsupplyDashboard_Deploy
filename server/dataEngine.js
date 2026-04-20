import fse from 'fs-extra';
import path from 'path';
import XLSX from 'xlsx';
import { fileURLToPath } from 'url';
import { getLogicalTableName } from './utils.js';

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
  return (
    s === '生效' ||
    s === '有效' ||
    s === '启用' ||
    s === 'active' ||
    s === 'enabled' ||
    s === 'effective' ||
    s === 'true' ||
    s === '1'
  );
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

/** 比较单元格与查找键：强制 trim + toLowerCase，消除 Excel 数字/字符串形态差异 */
export function valuesEqualForJoin(a, b) {
  const norm = (v) => String(v ?? '').trim().toLowerCase();
  const sa = norm(a);
  const sb = norm(b);
  if (!sa || !sb) return false;
  if (sa === sb) return true;

  // JSON 数组成员包含：支持从表字段存 ["SBOX26008M", ...] 或被转义成 \" 的字符串
  // 例：link_product_number='[\"SBOX26008M\",\"...\"]'，应视为包含匹配
  const looksJsonArray = (s) => {
    const t = String(s || '').trim();
    return t.startsWith('[') && t.endsWith(']');
  };
  const tryParseJsonArray = (raw) => {
    const t0 = String(raw ?? '').trim();
    if (!looksJsonArray(t0)) return null;
    const candidates = [];
    candidates.push(t0);
    // 处理常见转义：\" -> "，\\ -> \
    candidates.push(t0.replace(/\\"/g, '"').replace(/\\\\/g, '\\'));
    // 处理“被额外包了一层引号”的情况：'"[\"A\"]"'
    if ((t0.startsWith('"') && t0.endsWith('"')) || (t0.startsWith("'") && t0.endsWith("'"))) {
      candidates.push(t0.slice(1, -1));
      candidates.push(t0.slice(1, -1).replace(/\\"/g, '"').replace(/\\\\/g, '\\'));
    }
    for (const c of candidates) {
      try {
        const parsed = JSON.parse(c);
        if (Array.isArray(parsed)) return parsed;
      } catch {
        // ignore
      }
    }
    return null;
  };

  // member-of：若单元格值是 JSON 数组，则判断查找键是否在其中
  const arr = tryParseJsonArray(a);
  if (arr && arr.length) {
    const set = new Set(arr.map((x) => norm(x)).filter(Boolean));
    if (set.has(sb)) return true;
  }
  return false;
}

function engineLogPath() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  return path.join(__dirname, 'storage', 'engine.log');
}

function engineAuditLogPath() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  return path.join(__dirname, 'storage', 'engine_audit.log');
}

function aiTraceLogPath() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  return path.join(__dirname, 'storage', 'ai_trace.log');
}

function appendEngineLogSync(line) {
  try {
    const p = engineLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.appendFileSync(p, `${String(line ?? '')}\n`, { encoding: 'utf8' });
  } catch {
    // ignore: file logging should never break engine runtime
  }
}

function appendEngineAuditLogSync(line) {
  try {
    const p = engineAuditLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.appendFileSync(p, `${String(line ?? '')}\n`, { encoding: 'utf8' });
  } catch {
    // ignore
  }
}

function appendAiTraceLogSync(line) {
  try {
    const p = aiTraceLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.appendFileSync(p, `${String(line ?? '')}\n`, { encoding: 'utf8' });
  } catch {
    // ignore
  }
}

export function clearEngineLogSync() {
  try {
    const p = engineLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.writeFileSync(p, '', { encoding: 'utf8' });
  } catch {
    // ignore
  }
}

export function clearEngineAuditLogSync() {
  try {
    const p = engineAuditLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.writeFileSync(p, '', { encoding: 'utf8' });
  } catch {
    // ignore
  }
}

export function clearAiTraceLogSync() {
  try {
    const p = aiTraceLogPath();
    fse.ensureDirSync(path.dirname(p));
    fse.writeFileSync(p, '', { encoding: 'utf8' });
  } catch {
    // ignore
  }
}

export function aiTraceLineSync(line) {
  const out = String(line ?? '');
  if (!out) return;
  appendAiTraceLogSync(out);
}

export function auditLineSync(line) {
  const out = String(line ?? '');
  if (!out) return;
  appendEngineAuditLogSync(out);
}

function engineJoinTrace(enabled, message) {
  if (!enabled) return;
  // eslint-disable-next-line no-console
  console.log(`[Engine] ${message}`);
}

function traceJoin(enabled, message) {
  if (!enabled) return;
  const line = `[Trace] ${message}`;
  // eslint-disable-next-line no-console
  console.log(line);
  appendEngineLogSync(line);
  appendEngineAuditLogSync(line);
  appendAiTraceLogSync(line);
}

function traceLine(line) {
  const out = String(line ?? '');
  if (!out) return;
  // eslint-disable-next-line no-console
  console.log(out);
  if (out.startsWith('[Trace]')) {
    appendEngineLogSync(out);
    appendEngineAuditLogSync(out);
    appendAiTraceLogSync(out);
  }
}

function setJoinDiag(diagnosticsMap, key, msg) {
  if (!diagnosticsMap || !key) return;
  if (!diagnosticsMap.has(key)) diagnosticsMap.set(key, msg);
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
 * @param {{ trace?: boolean, traceLabel?: string, standardKey?: string, diagnosticsMap?: Map<string,string> }} [opts]
 */
export function resolveRecursiveValue({
  mainRow,
  mainTableName,
  joinPath,
  tablesMap,
  trace = false,
  traceLabel = '',
  standardKey = '',
  diagnosticsMap = null,
}) {
  const path = Array.isArray(joinPath) ? normalizeStructuredJoinPath(joinPath) : null;
  const sk = normalize(standardKey) || normalize(traceLabel);

  if (!path?.length) {
    setJoinDiag(diagnosticsMap, sk, 'Invalid Or Empty Join Path');
    return '';
  }

  if (trace) traceJoin(true, `正在处理维度: ${sk || 'join'}`);

  let currentRow = mainRow;
  let currentTable = normalize(mainTableName);
  const mainKey = normalize(mainTableName);
  let hopStep = 0;

  for (let i = 0; i < path.length; i++) {
    const seg = path[i];
    const last = i === path.length - 1;

    if (last) {
      if (!isTerminalSegment(seg)) return '';
      const tt = normalize(seg.targetTable);
      const vf = normalize(seg.valueField);
      if (normalize(currentTable) !== tt) {
        setJoinDiag(diagnosticsMap, sk, `Join Path Table Mismatch (expected ${tt}, got ${currentTable})`);
        traceJoin(trace, `匹配结果: 失败 (终端表不一致: 当前 ${currentTable}，期望 ${tt})`);
        return '';
      }
      const termTbl = getTableFromMap(tt, tablesMap);
      const headers = termTbl?.headers || [];
      const v = getRowFieldLoose(currentRow, vf, headers);
      const out = v == null ? '' : normalize(v);
      traceJoin(trace, `最终提取列 ${vf} 的值: ${out === '' ? '(空)' : out}`);
      if (out === '') setJoinDiag(diagnosticsMap, sk, `Join Terminal Empty: ${tt}.${vf}`);
      return out;
    }

    if (!isHopSegment(seg)) return '';
    const st = normalize(seg.sourceTable);
    const sf = normalize(seg.sourceField);
    const tt = normalize(seg.targetTable);
    const tf = normalize(seg.targetField);

    if (i > 0 && normalize(currentTable) !== st) {
      setJoinDiag(diagnosticsMap, sk, `Join Path Broken: current table ${currentTable} !== hop.sourceTable ${st}`);
      traceJoin(trace, `匹配结果: 失败 (链路表名不一致)`);
      return '';
    }
    if (i === 0 && st && currentTable && st !== currentTable) {
      // 兼容历史 CHAIN：首段表名与主表 Excel 逻辑名不一致时仍从主表行读 sourceField
    }

    const srcHeaders = i === 0 ? tablesMap.get(mainKey)?.headers || [] : tablesMap.get(st)?.headers || [];
    const rawKey = getRowFieldLoose(currentRow, sf, srcHeaders);
    if (rawKey == null || normalize(rawKey) === '') {
      setJoinDiag(diagnosticsMap, sk, `Join Source Empty: ${i === 0 ? mainKey : st}.${sf}`);
      traceJoin(trace, `匹配结果: 失败 (源字段无值: ${sf}=${rawKey == null ? 'undefined/null' : JSON.stringify(String(rawKey))})`);
      return '';
    }

    if (i === 0 && trace) traceJoin(true, `起始值: ${rawKey} (来自主表)`);

    hopStep += 1;
    const tbl = getTableFromMap(tt, tablesMap);
    if (!tbl?.rows?.length) {
      setJoinDiag(diagnosticsMap, sk, `Table Not Found: ${tt}`);
      traceJoin(trace, `步骤 ${hopStep}: 在表 ${tt} 中匹配 ${tf} === ${rawKey}`);
      traceJoin(trace, `匹配结果: 失败 (未找到行)`);
      return '';
    }

    traceJoin(trace, `步骤 ${hopStep}: 在表 ${tt} 中匹配 ${tf} === ${rawKey}`);
    const hit = tbl.rows.find((r) => valuesEqualForJoin(getRowFieldLoose(r, tf, tbl.headers), rawKey));
    traceJoin(trace, `匹配结果: ${hit ? '成功' : '失败 (未找到行)'}`);
    if (!hit) {
      setJoinDiag(diagnosticsMap, sk, `Join Match Failed: ${tt}.${tf}=${rawKey}`);
      return '';
    }

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
  // 防御：[object Object] —— physicalColumn/sourceField/sourceTable 必须是字符串
  let physicalColumn = typeof p.physicalColumn === 'string' ? normalize(p.physicalColumn) : '';
  if (physicalColumn.startsWith('CHAIN|')) physicalColumn = '';
  const sourceField = typeof p.sourceField === 'string' ? normalize(p.sourceField) : '';
  const sourceTable = typeof p.sourceTable === 'string' ? normalize(p.sourceTable) : '';
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
  // 兼容：若上游误传 string，按 physicalColumn token 处理（避免 "[object Object]"）
  if (typeof part === 'string') {
    return resolveScalarMappingPart({ physicalColumn: part, sourceField: '', sourceTable: '', joinPath: undefined }, mainRow, mainTableName, tablesMap, standardKeyHint, options);
  }
  const trace = Boolean(options.trace);
  const lbl = normalize(options.traceLabel) || normalize(standardKeyHint);
  const sk = normalize(options.standardKey) || lbl;
  const diagnosticsMap = options.diagnosticsMap;
  const recOpts = { trace, traceLabel: lbl, standardKey: sk, diagnosticsMap };
  if (Array.isArray(part.joinPath) && part.joinPath.length >= 2) {
    if (typeof part.joinPath[0] === 'object' && part.joinPath[0] !== null) {
      return resolveRecursiveValue({ mainRow, mainTableName, joinPath: part.joinPath, tablesMap, ...recOpts });
    }
    const pathObj = parseJoinPathFromConfig({ joinPath: part.joinPath });
    if (pathObj?.length) return resolveRecursiveValue({ mainRow, mainTableName, joinPath: pathObj, tablesMap, ...recOpts });
  }
  const token = typeof part.physicalColumn === 'string' ? normalize(part.physicalColumn) : '';
  if (token.startsWith('CHAIN|')) {
    const path = parseJoinPathFromConfig({ joinPath: null, physicalColumn: token });
    if (path) return resolveRecursiveValue({ mainRow, mainTableName, joinPath: path, tablesMap, ...recOpts });
  }
  const col = (typeof part.sourceField === 'string' ? normalize(part.sourceField) : '') || token;
  const fileRef = typeof part.sourceTable === 'string' ? normalize(part.sourceTable) : '';
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
        diagnosticsMap,
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
  const diagnosticsMap = options.diagnosticsMap;
  const ro = { trace, traceLabel: traceLabel || sk, standardKey: sk, diagnosticsMap };
  if (!entry) {
    if (!fallbackCol) return '';
    const mainTbl = tablesMap.get(normalize(mainTableName));
    const v = getRowFieldLoose(mainRow, fallbackCol, mainTbl?.headers);
    return v == null ? '' : normalize(v);
  }
  if (entry.mode === 'concat' && Array.isArray(entry.parts)) {
    if (sk === 'materialCode' && entry.parts.length >= 2) {
      const partVals = entry.parts.map((p, idx) => {
        const v = resolveScalarMappingPart(p, mainRow, mainTableName, tablesMap, sk, ro);
        traceLine(
          `[Trace][materialCode CONCAT / LT04] part[${idx}] => ${JSON.stringify(p)} | resolved="${v}"`
        );
        return v;
      });
      const out = partVals.join('');
      // 专项审计：材质 LT04 拼接必须有可核查证据
      if (partVals.length >= 2) {
        const p0 = String(partVals[0] || '').trim();
        const p1 = String(partVals[1] || '').trim();
        if (p0 === 'LT' && p1 === '04') {
          auditLineSync('[Concat] 发现父级 LT，发现子级 04，成功拼装为 LT04');
        }
      }
      return out;
    }
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
  const grid = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: '' });

  const normHeader = (v) => String(v ?? '').trim().toLowerCase();
  const rowCharScore = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return 0;
    let s = 0;
    for (const c of arr) {
      const t = String(c ?? '').trim();
      s += t.length;
    }
    return s;
  };
  const rowAllEmpty = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return true;
    return arr.every((c) => String(c ?? '').trim() === '');
  };

  // 精准定位表头：若第一行为空，向下扫描选“字符总量最高”的那一行作为真实表头
  let headerRowIndex = 0;
  let bestScore = -1;
  const scanN = Math.min(Array.isArray(grid) ? grid.length : 0, 50);
  for (let i = 0; i < scanN; i++) {
    const r = grid[i];
    if (rowAllEmpty(r)) continue;
    const score = rowCharScore(r);
    if (score > bestScore) {
      bestScore = score;
      headerRowIndex = i;
    }
  }

  const rawHeaderRow = Array.isArray(grid?.[headerRowIndex]) ? grid[headerRowIndex] : [];
  const used = new Map();
  const headers = rawHeaderRow.map((h, idx) => {
    let name = normHeader(h);
    if (!name) name = `col_${idx + 1}`;
    const k = name;
    const seen = used.get(k) || 0;
    used.set(k, seen + 1);
    if (seen > 0) name = `${name}_${seen + 1}`;
    return name;
  });

  // 数据行从表头下一行开始
  let rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: headerRowIndex + 1 });

  const styleKey = headers.includes('style_wms') ? 'style_wms' : '';
  const normalizeCellForRow = (v) => {
    const s = String(v ?? '');
    const t = s.trim();
    // JSON 字符串自动脱壳：形如 ["SBOX26008M"] -> ["SBOX26008M"] (Array)
    if (t.startsWith('["')) {
      const candidates = [t, t.replace(/\\"/g, '"').replace(/\\\\/g, '\\')];
      for (const c of candidates) {
        try {
          const parsed = JSON.parse(c);
          if (Array.isArray(parsed)) return parsed;
        } catch {
          // ignore
        }
      }
    }
    return t;
  };

  rows = (Array.isArray(rows) ? rows : [])
    .map((r) => {
      const out = {};
      for (const [k, v] of Object.entries(r || {})) out[String(k)] = normalizeCellForRow(v);
      return out;
    })
    // 过滤脏数据：全空行跳过；若存在款号列 style_wms 且为空则跳过
    .filter((r) => {
      const vals = Object.values(r || {});
      const allEmpty = vals.every((v) => {
        if (Array.isArray(v)) return v.length === 0;
        return String(v ?? '').trim() === '';
      });
      if (allEmpty) return false;
      if (styleKey && String(r?.[styleKey] ?? '').trim() === '') return false;
      return true;
    });

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
  return normalize(getLogicalTableName(fileName));
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
    if (t?.fileName && normalize(getLogicalTableName(t.fileName)) === want) return t;
  }
  return null;
}

function getTableFromMap(tableKey, tablesMap) {
  const k = normalize(tableKey);
  if (!k || !(tablesMap instanceof Map)) return null;
  return tablesMap.get(k) || getXlsxTableByLogicalName(k, tablesMap);
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
  diagnosticsMap = null,
}) {
  const sk = normalize(standardKey) || normalize(traceLabel);
  const dim = getXlsxTableByLogicalName(dimLogical, tablesMap);
  if (!dim?.rows?.length) {
    setJoinDiag(diagnosticsMap, sk, `Table Not Found: ${dimLogical}`);
    return '';
  }
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
    setJoinDiag(diagnosticsMap, sk, `Join Match Failed: ${dimLogical}.${keyCol}=${fkVal}`);
    return '';
  }

  engineJoinTrace(trace, `找到匹配行，准备提取 ${valueCol} 的值...`);
  const v = getRowFieldLoose(hit, valueCol, headers);
  const out = v == null ? '' : normalize(v);
  if (out !== '') engineJoinTrace(trace, `提取成功: ${out}`);
  else {
    engineJoinTrace(trace, `提取失败: 字段 ${valueCol} 为空或不存在`);
    setJoinDiag(diagnosticsMap, sk, `Join Terminal Empty: ${dimLogical}.${valueCol}`);
  }
  return out;
}

function buildTablesMap(xlsxTables) {
  const map = new Map();
  for (const t of xlsxTables) {
    const logical = normalize(getLogicalTableName(t.fileName));
    if (!logical) continue;
    map.set(logical, t);
  }
  return map;
}

/** 读取目录下全部 Excel，构建与聚合引擎一致的 tablesMap（预览前强制调用以保证最新磁盘内容） */
export async function readAllFilesFromFolder(folderPath) {
  return loadExcelFolderAsTablesMap(folderPath);
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
    // eslint-disable-next-line no-console
    console.log(`[Excel] 加载表: ${fileName}`);
    // eslint-disable-next-line no-console
    console.log(`[Excel] 识别到表头: ${(headers || []).slice(0, 5).join(', ')}...`);
    // eslint-disable-next-line no-console
    console.log(`[Excel] 数据行数: ${(rows || []).length} 行`);
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
  targetStyle = '',
  masterTableLogicalName = '',
}) {
  const list = Array.isArray(joinPathSuggestions) ? joinPathSuggestions : [];
  const goldMap =
    goldenByStandardKey instanceof Map ? goldenByStandardKey : new Map(Object.entries(goldenByStandardKey || {}));
  const { xlsxTables, tablesMap } = await loadExcelFolderAsTablesMap(folderPath);
  if (!list.length) return [];

  // ===========================
  // 唯一合法三步走：强制锚定主表 + 款号行
  // 1) 主表：ods_pdm_pdm_product_info_df
  // 2) 寻行：仅允许 style_wms == targetStyle（例如 SBOX26008M）
  // 3) 外键 -> 维表 -> 业务列：只能按 joinPath 执行
  // ===========================
  const MAIN_LOGICAL = 'ods_pdm_pdm_product_info_df';
  const main =
    xlsxTables?.length ? xlsxTables.find((t) => normalize(getLogicalTableName(t?.fileName || '')) === MAIN_LOGICAL) || null : null;
  if (!main || !tablesMap.size) {
    // eslint-disable-next-line no-console
    console.warn(
      '[Validator] 无法回测：无 XLSX 或无法根据 smartSuggestions 识别主表，已拒绝全部 joinPathSuggestions'
    );
    return [];
  }

  const mainTableName = buildTableNameIndex(main.fileName);
  const styleCol = 'style_wms';
  const statusCol = '';

  const validated = [];
  for (const jp of list) {
    const targetStandardKey = typeof jp?.targetStandardKey === 'string' ? jp.targetStandardKey.trim() : '';
    const legacyPathArr = Array.isArray(jp?.path) ? jp.path.map((p) => String(p).trim()).filter(Boolean) : [];
    const structuredJoinPath = Array.isArray(jp?.joinPath) ? jp.joinPath : null;
    const pathArr = legacyPathArr.length >= 2 ? legacyPathArr : [];
    if (!targetStandardKey) continue;

    const goldenRaw = goldMap.get(targetStandardKey);
    const golden = goldenRaw != null ? String(goldenRaw).trim() : '';
    if (!golden) {
      // eslint-disable-next-line no-console
      console.warn(`[Validator] 无黄金样本，跳过回测并丢弃 joinPathSuggestions：${targetStandardKey}`);
      continue;
    }

    const structured =
      (structuredJoinPath && structuredJoinPath.length >= 2 ? parseJoinPathFromConfig({ joinPath: structuredJoinPath }) : null) ||
      (pathArr.length >= 2 ? parseJoinPathFromConfig({ joinPath: pathArr }) : null);
    if (!structured?.length) {
      // eslint-disable-next-line no-console
      console.warn(`[Validator] 路径无法解析为 DSL：${targetStandardKey}`, structuredJoinPath || pathArr);
      continue;
    }

    const tStyle = String(targetStyle || '').trim();
    const eqLoose = (a, b) => String(a ?? '').trim().toUpperCase() === String(b ?? '').trim().toUpperCase();
    const rowsCandidate = main.rows || [];
    const anchoredRow = tStyle ? rowsCandidate.find((r) => eqLoose(r?.[styleCol], tStyle)) || null : null;
    if (!anchoredRow) {
      aiTraceLineSync(`[FAIL] 无法在主表 ods_pdm_pdm_product_info_df 的 style_wms 列找到 ${tStyle || '(空)'} `);
      continue;
    }

    let resolved = resolveRecursiveValue({
      mainRow: anchoredRow,
      mainTableName,
      joinPath: structured,
      tablesMap,
      // 让 engine.log / engine_audit.log 有完整链路证据
      trace: true,
      traceLabel: targetStandardKey,
      standardKey: targetStandardKey,
    });
    const shown = resolved != null ? String(resolved).trim() : '';
    let passed = shown === golden;

    // ===========================
    // 强制纠偏（仍遵守三步走：锚定主表行 -> 取主表外键 -> 跨表取业务列）
    // 仅在 AI joinPath 未通过黄金值时启用
    // ===========================
    if (!passed) {
      const tryAlt = (pathTokens) => {
        const p = parseJoinPathFromConfig({ joinPath: pathTokens });
        if (!p?.length) return { ok: false, value: '' };
        const v = resolveRecursiveValue({
          mainRow: anchoredRow,
          mainTableName,
          joinPath: p,
          tablesMap,
          trace: true,
          traceLabel: `${targetStandardKey}/alt`,
          standardKey: targetStandardKey,
        });
        return { ok: true, value: String(v ?? '').trim() };
      };

      // colorCode：优先用主表 initial_sample_color_id -> base_color_df.id -> code
      if (normalizeLower(targetStandardKey) === 'colorcode') {
        const alt1 = tryAlt([
          'ods_pdm_pdm_product_info_df.initial_sample_color_id',
          'ods_pdm_pdm_base_color_df.id',
          'ods_pdm_pdm_base_color_df.code',
        ]);
        if (alt1.ok && alt1.value === golden) {
          resolved = alt1.value;
          passed = true;
          validated.push({
            targetStandardKey,
            path: [
              'ods_pdm_pdm_product_info_df.initial_sample_color_id',
              'ods_pdm_pdm_base_color_df.id',
              'ods_pdm_pdm_base_color_df.code',
            ],
            valid: true,
          });
          continue;
        }
      }

      // soleCode：尝试 associated_sole_info -> base_mold_df.id -> code，再尝试 associated_heel_info -> base_heel_df.id -> code
      if (normalizeLower(targetStandardKey) === 'solecode') {
        const altMold = tryAlt([
          'ods_pdm_pdm_product_info_df.associated_sole_info',
          'ods_pdm_pdm_base_mold_df.id',
          'ods_pdm_pdm_base_mold_df.code',
        ]);
        if (altMold.ok && altMold.value === golden) {
          resolved = altMold.value;
          passed = true;
          validated.push({
            targetStandardKey,
            path: [
              'ods_pdm_pdm_product_info_df.associated_sole_info',
              'ods_pdm_pdm_base_mold_df.id',
              'ods_pdm_pdm_base_mold_df.code',
            ],
            valid: true,
          });
          continue;
        }
        const altHeel = tryAlt([
          'ods_pdm_pdm_product_info_df.associated_heel_info',
          'ods_pdm_pdm_base_heel_df.id',
          'ods_pdm_pdm_base_heel_df.code',
        ]);
        if (altHeel.ok && altHeel.value === golden) {
          resolved = altHeel.value;
          passed = true;
          validated.push({
            targetStandardKey,
            path: [
              'ods_pdm_pdm_product_info_df.associated_heel_info',
              'ods_pdm_pdm_base_heel_df.id',
              'ods_pdm_pdm_base_heel_df.code',
            ],
            valid: true,
          });
          continue;
        }
      }

      // status：若主表 data_status 本身已等于 golden，则不允许走字典 join（直接视为“无需 join”）
      if (normalizeLower(targetStandardKey) === 'status') {
        const mainStatus = String(anchoredRow?.data_status ?? '').trim();
        if (mainStatus && mainStatus === golden) {
          auditLineSync(`[Validator][Status] direct match on main.data_status=${JSON.stringify(mainStatus)}; ignore joinPathSuggestion`);
          // 这里不推 synthetic joinPath（避免自连接误命中别行）；让前端用 smartSuggestion/直连映射解决
        }
      }
    }

    if (passed) {
      validated.push({
        targetStandardKey,
        path: pathArr.length >= 2 ? pathArr : undefined,
        joinPath: structuredJoinPath && structuredJoinPath.length ? structuredJoinPath : undefined,
        valid: true,
      });
    } else {
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

function expectsDimensionJoin(entry) {
  if (!entry) return false;
  if (entry.mode === 'concat') return true;
  if (Array.isArray(entry.joinPath) && entry.joinPath.length >= 2) return true;
  if (normalize(entry.physicalColumn).startsWith('CHAIN|')) return true;
  const fileRef = normalize(entry.sourceTable);
  if (fileRef && isSpreadsheetFileRef(fileRef)) return true;
  return false;
}

/** 预览模式：禁止把未解析的外键当业务值返回，并合并引擎诊断信息 */
function previewSanitizeField(fieldKey, entry, value, row, mainTableName, tablesMap, fallbackCol, diagnosticsMap) {
  if (!diagnosticsMap || !entry) return value;
  const sk = normalize(fieldKey);
  const existing = diagnosticsMap.get(sk);
  if (existing) return '';
  if (!expectsDimensionJoin(entry)) return value;

  const mainHeaders = tablesMap.get(normalize(mainTableName))?.headers || [];
  if (value === '' || value == null) {
    setJoinDiag(diagnosticsMap, sk, 'Join Match Failed');
    return '';
  }
  if (fallbackCol) {
    const raw = getRowFieldLoose(row, fallbackCol, mainHeaders);
    if (raw != null && normalize(value) === normalize(raw) && normalize(raw) !== '') {
      setJoinDiag(diagnosticsMap, sk, 'Join Likely Unresolved: value equals source FK column');
      return '';
    }
  }
  return value;
}

/**
 * 在任意目录（如 storage/sandbox）上跑与 aggregateForDateRoot 相同的解析逻辑，取第一条生效行用于沙盒校验。
 * @param {{ preload?: { xlsxTables: unknown[], tablesMap: Map }, previewStrict?: boolean }} [opts]
 */
export async function resolveFirstActiveRowFromFolder({
  folderPath,
  standardMap,
  traceJoins = false,
  preload = null,
  previewStrict = false,
  targetStyle = '',
  masterTableLogicalName = '',
}) {
  const { xlsxTables, tablesMap } = preload ?? (await readAllFilesFromFolder(folderPath));
  if (!xlsxTables?.length) {
    return { ok: false, error: 'sandbox 目录下没有 XLSX 文件', row: null, mainTable: null };
  }

  const targetStyleNorm = String(targetStyle || '').trim().toUpperCase();
  const eqLoose = (a, b) => String(a ?? '').trim().toUpperCase() === String(b ?? '').trim().toUpperCase();
  const rowContainsTarget = (row, target) => {
    if (!row || !target) return false;
    for (const v of Object.values(row)) {
      if (eqLoose(v, target)) return true;
    }
    return false;
  };

  // 唯一合法锚定列：style_wms（禁止推断/暴力全列搜）
  const styleCol = 'style_wms';
  const brandCol = columnNameForMainRow(standardMap.get('brand'));
  const statusCol =
    columnNameForMainRow(standardMap.get('status')) || columnNameForMainRow(standardMap.get('data_status'));
  const lastCol = columnNameForMainRow(standardMap.get('lastCode'));
  const soleCol = columnNameForMainRow(standardMap.get('soleCode'));
  const colorCol = columnNameForMainRow(standardMap.get('colorCode'));
  const materialCol = columnNameForMainRow(standardMap.get('materialCode'));

  // 唯一合法主表：ods_pdm_pdm_product_info_df（禁止按表头/全表猜）
  const MAIN_LOGICAL = 'ods_pdm_pdm_product_info_df';
  const main =
    xlsxTables.find((t) => normalize(getLogicalTableName(t?.fileName || '')) === MAIN_LOGICAL) ||
    (masterTableLogicalName
      ? xlsxTables.find((t) => normalize(getLogicalTableName(t?.fileName || '')) === normalize(masterTableLogicalName)) || null
      : null);
  if (!main) {
    return { ok: false, error: '无法识别主表（必须存在 ods_pdm_pdm_product_info_df.xlsx）', row: null, mainTable: null };
  }

  const mainTableName = buildTableNameIndex(main.fileName);
  // 强制对账：逻辑表名 -> 物理文件路径
  // eslint-disable-next-line no-console
  console.log('[Debug] 正在查找逻辑表:', mainTableName, '对应的物理文件是:', main.fullPath || main.fileName);
  const diagnosticsMap = previewStrict ? new Map() : null;
  let effectiveStyleCol = styleCol;
  const findColumnContainingTarget = (row, target) => {
    if (!row || !target) return '';
    for (const [k, v] of Object.entries(row)) {
      if (eqLoose(v, target)) return String(k || '');
    }
    return '';
  };
  const traceOpts = (label) => ({
    trace: traceJoins,
    traceLabel: label,
    standardKey: label,
    ...(previewStrict ? { diagnosticsMap } : {}),
  });
  const tryRow = (row) => {
    const brandRaw = resolveStandardMappedValue(
      standardMap.get('brand'),
      row,
      mainTableName,
      tablesMap,
      brandCol,
      traceOpts('brand')
    );
    const lastRaw = resolveStandardMappedValue(
      standardMap.get('lastCode'),
      row,
      mainTableName,
      tablesMap,
      lastCol,
      traceOpts('lastCode')
    );
    const soleRaw = resolveStandardMappedValue(
      standardMap.get('soleCode'),
      row,
      mainTableName,
      tablesMap,
      soleCol,
      traceOpts('soleCode')
    );
    const colorRaw = resolveStandardMappedValue(
      standardMap.get('colorCode'),
      row,
      mainTableName,
      tablesMap,
      colorCol,
      traceOpts('colorCode')
    );
    const materialRaw = resolveStandardMappedValue(
      standardMap.get('materialCode'),
      row,
      mainTableName,
      tablesMap,
      materialCol,
      traceOpts('materialCode')
    );

    const brandVal = previewSanitizeField('brand', standardMap.get('brand'), brandRaw, row, mainTableName, tablesMap, brandCol, diagnosticsMap);
    const lastCodeVal = previewSanitizeField(
      'lastCode',
      standardMap.get('lastCode'),
      lastRaw,
      row,
      mainTableName,
      tablesMap,
      lastCol,
      diagnosticsMap
    );
    const soleCodeVal = previewSanitizeField(
      'soleCode',
      standardMap.get('soleCode'),
      soleRaw,
      row,
      mainTableName,
      tablesMap,
      soleCol,
      diagnosticsMap
    );
    const colorCodeVal = previewSanitizeField(
      'colorCode',
      standardMap.get('colorCode'),
      colorRaw,
      row,
      mainTableName,
      tablesMap,
      colorCol,
      diagnosticsMap
    );
    const materialCodeVal = previewSanitizeField(
      'materialCode',
      standardMap.get('materialCode'),
      materialRaw,
      row,
      mainTableName,
      tablesMap,
      materialCol,
      diagnosticsMap
    );

    return {
      style_wms: effectiveStyleCol ? normalize(row?.[effectiveStyleCol]) : '',
      brand: brandVal,
      lastCode: lastCodeVal,
      soleCode: soleCodeVal,
      colorCode: colorCodeVal,
      materialCode: materialCodeVal,
      status: statusCol ? normalize(String(row?.[statusCol] ?? '')) : '',
    };
  };

  const packOk = (row, usedFallbackRow, warning) => {
    const outRow = tryRow(row);
    const previewFieldErrors = diagnosticsMap?.size ? Object.fromEntries(diagnosticsMap) : undefined;
    return {
      ok: true,
      mainTable: main.fileName,
      row: outRow,
      usedFallbackRow,
      ...(warning ? { warning } : {}),
      ...(previewFieldErrors ? { previewFieldErrors } : {}),
    };
  };

  // 唯一合法寻行：只允许 style_wms == targetStyle（例如 SBOX26008M）
  if (targetStyleNorm) {
    const hit = (main.rows || []).find((r) => eqLoose(r?.[styleCol], targetStyleNorm)) || null;
    if (!hit) {
      aiTraceLineSync(`[FAIL] 无法在主表 ods_pdm_pdm_product_info_df 的 style_wms 列找到 ${targetStyleNorm}`);
      return {
        ok: false,
        error: `[FAIL] 无法在主表 ods_pdm_pdm_product_info_df 的 style_wms 列找到 ${targetStyleNorm}`,
        row: null,
        mainTable: main?.fileName || null,
      };
    }
    return packOk(hit, false);
  }

  for (let i = 0; i < main.rows.length; i++) {
    const row = main.rows[i] || {};
    const statusVal = statusCol ? row?.[statusCol] : '';
    const isActive = isTruthyActiveStatus(statusVal);
    if (!isActive) continue;

    return packOk(row, false);
  }

  // 不再使用“首行兜底”，避免逻辑崩盘/误导
  return { ok: false, error: '未找到生效行', row: null, mainTable: main?.fileName || null };
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
