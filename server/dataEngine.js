import fs from 'node:fs';
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

function buildJoinIndexKey(tableName, fieldName) {
  return `${normalizeLower(tableName)}::${normalizeLower(fieldName)}`;
}

function buildScalarIndexForTableField(tbl, fieldName) {
  const tf = normalize(fieldName);
  if (!tbl?.rows?.length || !tf) return null;
  const m = new Map();
  for (const r of tbl.rows) {
    const v = getRowFieldLoose(r, tf, tbl.headers);
    if (v == null) continue;
    if (Array.isArray(v)) continue; // 数组包含类 join 不走 O(1) 索引
    const k = String(v).trim().toLowerCase();
    if (!k) continue;
    if (!m.has(k)) m.set(k, r); // 取首条
  }
  return m;
}

function findTableByNameKeywords(tablesMap, keywords) {
  const wants = (Array.isArray(keywords) ? keywords : [keywords]).map((k) => normalizeLower(k)).filter(Boolean);
  if (!wants.length || !(tablesMap instanceof Map)) return null;
  for (const [k, t] of tablesMap.entries()) {
    const keyLower = normalizeLower(k);
    const fileLower = normalizeLower(t?.fileName || '');
    const logicalLower = normalizeLower(getLogicalTableName(t?.fileName || ''));
    const ok = wants.some((w) => keyLower.includes(w) || fileLower.includes(w) || logicalLower.includes(w));
    if (ok) return t;
  }
  return null;
}

function buildIdToCodeIndex(tbl) {
  if (!tbl?.rows?.length) return null;
  const idx = new Map();
  for (const r of tbl.rows) {
    const idv = getRowFieldLoose(r, 'id', tbl.headers);
    const codev = getRowFieldLoose(r, 'code', tbl.headers);
    const k = normalizeJoinIdKey(idv);
    const code = codev == null ? '' : normalize(codev);
    if (!k || !code) continue;
    if (!idx.has(k)) idx.set(k, code);
  }
  return idx;
}

function normalizeJoinIdKey(v) {
  const raw = String(v ?? '').trim();
  if (!raw) return '';
  if (raw === '0') return '';
  // 统一数字/字符串/1695.0/科学计数法等
  const n = Number(raw);
  if (Number.isFinite(n)) {
    if (Math.floor(n) === n) return String(n);
    return String(n);
  }
  return raw;
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

/**
 * 将主表 data_status 规范为四态：active | draft | obsolete | other
 * 照妖镜实测英文枚举优先：effective → active；invalid → obsolete（作废桶）。
 * Draft 分类保留，当前数据可为 0。
 */
function normalizeInventoryStatus(v) {
  // 强制去空格 + 小写（物理纠偏：避免 "Draft " 这类值漏分桶）
  const raw = normalize(v);
  const s = String(raw ?? '').trim().toLowerCase();

  const isNum01 = (x) => /^0(\.0+)?$/.test(x);
  const isNum1 = (x) => /^1(\.0+)?$/.test(x);
  const isNum9 = (x) => /^9(\.0+)?$/.test(x);

  // —— 0) Draft 精确匹配（必须显式识别 draft / Draft）
  if (s === 'draft') return 'draft';

  // —— 1) 作废/无效：invalid 与扩展词库（先于 effective，避免歧义）
  if (s === 'invalid' || /\binvalid\b/.test(s)) return 'obsolete';
  if (isNum9(s) || s === '99' || s === '-1') return 'obsolete';
  if (
    s.includes('obsolete') ||
    s.includes('作废') ||
    s.includes('取消') ||
    s.includes('失效') ||
    s.includes('废弃') ||
    s.includes('inactive')
  ) {
    return 'obsolete';
  }

  // —— 2) 生效：effective 与扩展（排除 ineffective）
  if (s === 'effective' || (/\beffective\b/.test(s) && !s.includes('ineffective'))) return 'active';
  if (isNum1(s) || s === '1') return 'active';
  if (isTruthyActiveStatus(v)) return 'active';
  if (s.includes('生效')) return 'active';
  if (s.includes('active') && !s.includes('inactive')) return 'active';

  // —— 3) Draft（保留分类，当前文件可无）
  if (isNum01(s)) return 'draft';
  if (s.includes('草稿') || /\bdraft\b/.test(s) || s.includes('pending') || s.includes('编辑中')) {
    return 'draft';
  }

  // —— 兜底：其他（仍计入全量池条形图）
  return 'other';
}

/** 列出所有未落入 active/draft/obsolete 的原始词汇（只计 other） */
function auditMainTableOtherStatusDistribution(main, statusCol) {
  const counts = new Map();
  const rows = main?.rows || [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || {};
    const raw = statusCol ? row[statusCol] : undefined;
    const bucket = normalizeInventoryStatus(raw);
    if (bucket !== 'other') continue;
    const label = formatRawStatusLabel(raw);
    counts.set(label, (counts.get(label) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([value, rowCount]) => ({ value, rowCount }))
    .sort((a, b) => b.rowCount - a.rowCount);
}

/** 主表状态列原始值普查（不限于有款号的行） */
function formatRawStatusLabel(v) {
  if (v === undefined || v === null) return '(null)';
  const t = String(v).trim();
  return t === '' ? '(空)' : t;
}

function auditMainTableRawStatusDistribution(main, statusCol) {
  const counts = new Map();
  const rows = main?.rows || [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || {};
    const raw = statusCol ? row[statusCol] : undefined;
    const label = formatRawStatusLabel(raw);
    counts.set(label, (counts.get(label) || 0) + 1);
  }
  const rawStatusAudit = Array.from(counts.entries())
    .map(([value, rowCount]) => ({ value, rowCount }))
    .sort((a, b) => b.rowCount - a.rowCount);
  return rawStatusAudit;
}

function printRawStatusAuditTable(rawStatusAudit, main, statusCol) {
  const physical = main?.rows?.length || 0;
  let sum = 0;
  // eslint-disable-next-line no-console
  console.log('[Status Audit] 发现以下原始状态值：');
  // eslint-disable-next-line no-console
  console.log(`（主表 ${main?.fileName || '(unknown)'} ｜ 物理行 ${physical} ｜ 列 ${statusCol || '(未映射)'}）`);
  for (const { value, rowCount } of rawStatusAudit) {
    sum += rowCount;
    // eslint-disable-next-line no-console
    console.log(`- "${value}": ${rowCount} 行`);
  }
  // eslint-disable-next-line no-console
  console.log('---------------------------');
  if (sum !== physical) {
    // eslint-disable-next-line no-console
    console.warn(`[Engine] 原始状态计数合计 ${sum} 与物理行 ${physical} 不一致`);
  }
}

/** 终端公示：effective/invalid/raw 以及归一化四态分布（全物理行） */
function printStatusScopeCensus(main, statusCol, rawStatusAudit) {
  const physical = main?.rows?.length || 0;
  const norm = (v) => String(v ?? '').trim().toLowerCase();

  let rawEffective = 0;
  let rawInvalid = 0;
  for (const it of rawStatusAudit || []) {
    const v = norm(it?.value);
    const n = Number(it?.rowCount || 0);
    if (v === 'effective') rawEffective += n;
    else if (v === 'invalid') rawInvalid += n;
  }
  const rawOther = Math.max(0, physical - rawEffective - rawInvalid);

  const mapped = { active: 0, draft: 0, obsolete: 0, other: 0 };
  for (const row of main?.rows || []) {
    const raw = statusCol ? row?.[statusCol] : undefined;
    const b = normalizeInventoryStatus(raw);
    mapped[b] = (mapped[b] || 0) + 1;
  }
  const mappedSum = Number(mapped.active || 0) + Number(mapped.draft || 0) + Number(mapped.obsolete || 0) + Number(mapped.other || 0);

  // eslint-disable-next-line no-console
  console.log('--- 实时数据口径检查 ---');
  // eslint-disable-next-line no-console
  console.log(`[effective] -> ${rawEffective} 行 (映射为：生效/active)`);
  // eslint-disable-next-line no-console
  console.log(`[invalid]   -> ${rawInvalid} 行 (映射为：作废/无效/obsolete)`);
  // eslint-disable-next-line no-console
  console.log(`[其他值]     -> ${rawOther} 行`);
  // eslint-disable-next-line no-console
  console.log('----------------------');
  // eslint-disable-next-line no-console
  console.log('[Mapped] active/draft/obsolete/other =', mapped, `sum=${mappedSum} (physical=${physical})`);
  if (mappedSum !== physical) {
    // eslint-disable-next-line no-console
    console.warn(`[Engine] 归一化状态四态合计 ${mappedSum} 与物理行 ${physical} 不一致（请检查 style_wms 空行/重复表头/筛选逻辑）`);
  }
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
  const sb = norm(b);
  if (!sb) return false;

  // 若单元格已被“自动脱壳”为 Array：直接做 member-of（includes）判断
  if (Array.isArray(a)) {
    const set = new Set(a.map((x) => norm(x)).filter(Boolean));
    return set.has(sb);
  }
  if (Array.isArray(b)) {
    const set = new Set(b.map((x) => norm(x)).filter(Boolean));
    const sa0 = norm(a);
    return Boolean(sa0) && set.has(sa0);
  }

  const sa = norm(a);
  if (!sa) return false;
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
  if (!sheetName) return { headers: [], rows: [], sheetRef: null };
  const sheet = wb.Sheets[sheetName];
  if (!sheet) return { headers: [], rows: [], sheetRef: null };
  const sheetRef = sheet['!ref'] ? String(sheet['!ref']) : null;
  // Row count audit：header:1 会返回二维数组；blankrows true 保留空行
  const grid = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: '', blankrows: true });
  const gridRowCount = Array.isArray(grid) ? grid.length : 0;
  // 另一种“暴力长度”：不指定 header，得到对象数组（用于粗验行数是否≈5824）
  const sheetJsonLen = Array.isArray(XLSX.utils.sheet_to_json(sheet, { raw: false, defval: '', blankrows: true }))
    ? XLSX.utils.sheet_to_json(sheet, { raw: false, defval: '', blankrows: true }).length
    : 0;

  const normHeader = (v) => String(v ?? '').trim().toLowerCase();
  const looksNumericLike = (s) => {
    const t = String(s ?? '').trim();
    if (!t) return false;
    return /^-?\d+(\.\d+)?$/.test(t);
  };
  const loadDdlSchemaCacheSync = (() => {
    let loaded = false;
    let cache = null;
    return () => {
      if (loaded) return cache;
      loaded = true;
      try {
        const __filename = fileURLToPath(import.meta.url);
        const __dirname = path.dirname(__filename);
        const p = path.join(__dirname, 'storage', 'ddl_schema_cache.json');
        if (!fse.existsSync(p)) return (cache = null);
        const obj = fse.readJsonSync(p);
        if (!obj || typeof obj !== 'object') return (cache = null);
        return (cache = obj);
      } catch {
        return (cache = null);
      }
    };
  })();

  const isSandboxFile = (() => {
    const p = String(fullPath || '').replace(/\\/g, '/');
    return p.includes('/storage/sandbox/');
  })();

  const ddlColumnsForThisTable = (() => {
    if (!isSandboxFile) return [];
    const c = loadDdlSchemaCacheSync();
    const logical = normalize(getLogicalTableName(path.basename(fullPath)));
    const cols = c?.tables?.[logical];
    if (!Array.isArray(cols)) return [];
    return cols.map((x) => normHeader(x)).filter(Boolean);
  })();

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

  // 读取第一行判定是否为真实表头：
  // - 若第一行包含 DDL 字段名（如 id/code/style_wms），视为表头
  // - 否则（像数据：纯数字/中文名/Bruno Marc 等），sandbox 下强制使用 DDL 字段顺序作为表头，并将第一行归还给 rows
  let headerRowIndex = 0;
  const firstRow = Array.isArray(grid?.[0]) ? grid[0] : [];
  const firstRowNorm = firstRow.map(normHeader).filter(Boolean);
  const ddlSet = new Set((ddlColumnsForThisTable || []).map(normHeader).filter(Boolean));
  const commonKeys = new Set(['id', 'code', 'style_wms', 'brand_name', 'data_status']);
  const ddlHit = firstRowNorm.filter((x) => ddlSet.has(x)).length;
  const commonHit = firstRowNorm.filter((x) => commonKeys.has(x)).length;
  const numericCells = firstRowNorm.filter((x) => looksNumericLike(x)).length;
  const firstLooksLikeHeader = (ddlHit >= 2 || commonHit >= 2) && numericCells <= Math.max(2, Math.floor(firstRowNorm.length / 2));

  // 若 sandbox 且第一行不像表头：强制 DDL 列顺序
  if (isSandboxFile && ddlColumnsForThisTable.length && !firstLooksLikeHeader) {
    const headers = ddlColumnsForThisTable;
    let rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: 0, blankrows: true });
    const normalizeCellForRow = (v) => {
      const s = String(v ?? '');
      const t = s.trim();
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
    rows = (Array.isArray(rows) ? rows : []).map((r) => {
      const out = {};
      for (const [k, v] of Object.entries(r || {})) out[String(k)] = normalizeCellForRow(v);
      return out;
    });
    return { headers, rows, sheetRef, gridRowCount, sheetJsonLen };
  }

  // 若第一行已被判定为表头：必须锁定为 header（禁止再用“字符总量最高”把数据行误判为 header）
  if (firstLooksLikeHeader) {
    headerRowIndex = 0;
  } else {
    // 非强制 DDL：采用“空行下探 + 字符总量最高”策略（仅在第一行不像 header 时启用）
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
  let rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: headerRowIndex + 1, blankrows: true });
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

  rows = (Array.isArray(rows) ? rows : []).map((r) => {
    const out = {};
    for (const [k, v] of Object.entries(r || {})) out[String(k)] = normalizeCellForRow(v);
    return out;
  });

  return { headers, rows, sheetRef, gridRowCount, sheetJsonLen };
}

async function listFiles(dir) {
  const entries = await fse.readdir(dir, { withFileTypes: true });
  return entries.filter((e) => e.isFile()).map((e) => e.name);
}

async function getDateDirsSorted(dataTablesDir) {
  const entries = await fse.readdir(dataTablesDir, { withFileTypes: true });
  const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);
  const withStat = [];
  for (const name of dirs) {
    try {
      const st = await fse.stat(path.join(dataTablesDir, name));
      withStat.push({ name, mtimeMs: st.mtimeMs || 0 });
    } catch {
      withStat.push({ name, mtimeMs: 0 });
    }
  }
  // 修改时间最晚在最后
  withStat.sort((a, b) => a.mtimeMs - b.mtimeMs || a.name.localeCompare(b.name));
  return withStat.map((x) => x.name);
}

/** 趋势专用：仅取日期目录，并按目录名排序（时间线稳定） */
async function getDateDirsByNameSorted(dataTablesDir) {
  const entries = await fse.readdir(dataTablesDir, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory() && isDateDirName(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

/** 对账/增量专用：按日期目录名倒序（Latest 在 [0]） */
async function getDateDirsByNameDesc(dataTablesDir) {
  const asc = await getDateDirsByNameSorted(dataTablesDir);
  return asc.reverse();
}

/** 与磁盘文件名去后缀后一致：trim + 去扩展名 + 小写（与 lastCode 容错比对） */
function physicalAssetKey(fileOrCodeName) {
  return String(stripExt(fileOrCodeName)).trim().toLowerCase();
}

/**
 * 每次聚合实时扫描 3D 资产目录（同步），避免缓存或异步时序导致漏扫新上传文件。
 */
function scan3DAssetDirSync(dirAbs) {
  const set = new Set();
  try {
    if (!fse.pathExistsSync(dirAbs)) return set;
    const entries = fs.readdirSync(dirAbs, { withFileTypes: true });
    for (const e of entries) {
      if (!e.isFile()) continue;
      const k = physicalAssetKey(e.name);
      if (k) set.add(k);
    }
  } catch {
    // ignore：目录不存在或无权限时不阻断聚合
  }
  return set;
}

function physicalMatchInAssetSet(assetSet, codeVal) {
  if (codeVal == null || codeVal === '') return false;
  const k = physicalAssetKey(codeVal);
  if (!k) return false;
  return assetSet.has(k);
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

function computeBrand3DCoverageStats(activeRows) {
  const map = new Map();
  const isLinkedCode = (v) => {
    const s = String(v ?? '').trim();
    return s !== '' && s !== '-' && s !== '0';
  };
  for (const r of activeRows || []) {
    const brand = normalize(r?.brand ?? 'Unknown') || 'Unknown';
    const cur =
      map.get(brand) || {
        brand,
        totalActive: 0,
        linked: 0,
        unlinked: 0,
        lastLinkedCount: 0,
        last3DMatchedCount: 0,
      };
    cur.totalActive += 1;
    const hasAny3D = Boolean(r?.__has3DAny);
    if (hasAny3D) cur.linked += 1;
    else cur.unlinked += 1;
    if (isLinkedCode(r?.lastCode)) cur.lastLinkedCount += 1;
    if (r?.has3DLast === true || Boolean(r?.__has3DLast)) cur.last3DMatchedCount += 1;
    map.set(brand, cur);
  }
  return Array.from(map.values())
    .sort((a, b) => b.totalActive - a.totalActive)
    .slice(0, 30);
}

function computeBrandDigitizationStats(activeRows, dim, allBrands = null) {
  const map = new Map();
  const isLinkedCode = (v) => {
    const s = String(v ?? '').trim();
    return s !== '' && s !== '-' && s !== '0';
  };
  const isHas3D = (r) =>
    dim === 'last'
      ? r?.has3DLast === true || Boolean(r?.__has3DLast)
      : r?.has3DSole === true || Boolean(r?.__has3DSole);
  const codeField = dim === 'last' ? 'lastCode' : 'soleCode';

  for (const r of activeRows || []) {
    const brand = normalize(r?.brand ?? 'Unknown') || 'Unknown';
    const cur = map.get(brand) || { brand, total: 0, hasCode: 0, has3D: 0 };
    cur.total += 1;
    const hasCode = isLinkedCode(r?.[codeField]);
    if (hasCode) cur.hasCode += 1;
    if (hasCode && isHas3D(r)) cur.has3D += 1;
    map.set(brand, cur);
  }

  // 关键：先锁定品牌全集，再按状态分桶补零，避免“品牌失踪”
  const brands =
    Array.isArray(allBrands) && allBrands.length
      ? allBrands
      : Array.from(map.keys()).filter(Boolean);

  return brands
    .map((b) => map.get(b) || { brand: b, total: 0, hasCode: 0, has3D: 0 })
    .map((x) => {
      const segment_no_code = Math.max(0, x.total - x.hasCode);
      const segment_has_code_no_3d = Math.max(0, x.hasCode - x.has3D);
      const segment_completed_3d = Math.max(0, x.has3D);
      const completionRate = x.total > 0 ? Math.round((x.has3D / x.total) * 1000) / 10 : 0;
      return {
        brand: x.brand,
        totalEffective: x.total,
        hasCode: x.hasCode,
        has3D: x.has3D,
        segment_no_code,
        segment_has_code_no_3d,
        segment_completed_3d,
        completionRate,
      };
    })
    .sort((a, b) => (b.totalEffective || 0) - (a.totalEffective || 0))
    .slice(0, 30);
}

function uniqBrandsFromInventoryRows(rows) {
  const set = new Set();
  for (const r of rows || []) {
    const b = normalize(r?.brand ?? 'Unknown') || 'Unknown';
    if (b) set.add(b);
  }
  return Array.from(set.values());
}

function computeBucketKPIs(rows) {
  const isLinkedCode = (v) => {
    const s = String(v ?? '').trim();
    return s !== '' && s !== '-' && s !== '0';
  };
  const totalStyles = rows.length;
  const matchedLasts = rows.filter((x) => x?.has3DLast === true || Boolean(x?.__has3DLast)).length;
  const matchedSoles = rows.filter((x) => x?.has3DSole === true || Boolean(x?.__has3DSole)).length;
  const stylesWithAny3D = rows.filter((x) => Boolean(x.__has3DAny)).length;
  const lastCodeLinked = rows.filter((x) => isLinkedCode(x?.lastCode)).length;
  const soleCodeLinked = rows.filter((x) => isLinkedCode(x?.soleCode)).length;

  const lastCoverage = totalStyles > 0 ? Math.round((matchedLasts / totalStyles) * 100) : 0;
  const last3DCoverage = totalStyles > 0 ? Math.round((matchedLasts / totalStyles) * 1000) / 10 : 0;
  const soleCoverage = totalStyles > 0 ? Math.round((matchedSoles / totalStyles) * 100) : 0;
  const sole3DCoverage = totalStyles > 0 ? Math.round((matchedSoles / totalStyles) * 1000) / 10 : 0;
  const any3DCoveragePercent = totalStyles > 0 ? Math.round((stylesWithAny3D / totalStyles) * 100) : 0;
  const lastCodeLinkRate = totalStyles > 0 ? Math.round((lastCodeLinked / totalStyles) * 1000) / 10 : 0;
  const soleCodeLinkRate = totalStyles > 0 ? Math.round((soleCodeLinked / totalStyles) * 1000) / 10 : 0;

  return {
    totalStyles,
    matchedLasts,
    matchedSoles,
    lastCoverage,
    soleCoverage,
    stylesWithAny3D,
    any3DCoveragePercent,
    last3DCount: matchedLasts,
    last3DCoverage,
    lastCodeLinked,
    lastCodeLinkRate,
    soleCodeLinked,
    soleCodeLinkRate,
    sole3DCount: matchedSoles,
    sole3DCoverage,
  };
}

function computeBrandBindingStats(activeRows) {
  const map = new Map();
  const isLinkedCode = (v) => {
    const s = String(v ?? '').trim();
    return s !== '' && s !== '-' && s !== '0';
  };
  for (const r of activeRows || []) {
    const brand = normalize(r?.brand ?? 'Unknown') || 'Unknown';
    const cur =
      map.get(brand) || { brand, totalActive: 0, lastLinkedCount: 0, soleLinkedCount: 0, last3DMatchedCount: 0 };
    cur.totalActive += 1;
    if (isLinkedCode(r?.lastCode)) cur.lastLinkedCount += 1;
    if (isLinkedCode(r?.soleCode)) cur.soleLinkedCount += 1;
    if (r?.has3DLast === true || Boolean(r?.__has3DLast)) cur.last3DMatchedCount += 1;
    map.set(brand, cur);
  }
  return Array.from(map.values())
    .map((x) => ({
      brand: x.brand,
      lastBindingRate: x.totalActive > 0 ? Math.round((x.lastLinkedCount / x.totalActive) * 1000) / 10 : 0,
      soleBindingRate: x.totalActive > 0 ? Math.round((x.soleLinkedCount / x.totalActive) * 1000) / 10 : 0,
      // 语义化字段（供 Dashboard Tooltip 直读）
      totalEffective: x.totalActive,
      lastLinked: x.lastLinkedCount,
      soleLinked: x.soleLinkedCount,
      last3DMatched: x.last3DMatchedCount,
    }))
    .sort((a, b) => (b.totalEffective || 0) - (a.totalEffective || 0))
    .slice(0, 30);
}

function computeDelta(current, previous) {
  const delta = current - previous;
  const pct = previous > 0 ? (delta / previous) * 100 : null;
  return { delta, pct };
}

function buildAssetTrendSeries({ latestDate, last3DCount, sole3DCount, months = 12 }) {
  const base = String(latestDate || '').trim() || new Date().toISOString().slice(0, 10);
  const d0 = new Date(base);
  if (Number.isNaN(d0.getTime())) return [];
  // 确保当月落在 1 号，便于展示
  d0.setDate(1);

  // 可复现的轻量波动：不依赖随机源，避免每次刷新图形跳动
  const seed = Number(String(base).replaceAll('-', '')) || 0;
  const wave = (i) => {
    const x = (seed % 97) + i * 13;
    // 0..1
    return ((x % 17) / 16) * 0.6 + ((x % 7) / 6) * 0.4;
  };

  const out = [];
  for (let i = months - 1; i >= 0; i--) {
    const dt = new Date(d0);
    dt.setMonth(d0.getMonth() - i);
    const ym = dt.toISOString().slice(0, 7);
    // 让最后一个点接近当前累计规模，前面逐步递增并带波峰波谷
    const ratio = months <= 1 ? 1 : (months - 1 - i) / (months - 1);
    const wobble = wave(i);
    const newLasts = Math.max(0, Math.round((last3DCount || 0) * (0.03 + 0.12 * wobble) * (0.4 + 0.6 * ratio)));
    const newSoles = Math.max(0, Math.round((sole3DCount || 0) * (0.03 + 0.12 * wobble) * (0.4 + 0.6 * ratio)));
    out.push({ date: ym, newLasts, newSoles });
  }
  return out;
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
  if (sk === 'soleCode')
    o.push(
      'associated_sole_info',
      'associated_sole',
      'sole_id',
      'mold_id',
      'associated_mold',
      'associated_mold_info',
      'heel_id',
      'associated_heel',
      'associated_heel_info'
    );
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
    const { headers, rows, sheetRef } = readSheetRows(fullPath);
    // eslint-disable-next-line no-console
    console.log(`[Excel] 加载表: ${fileName}`);
    // eslint-disable-next-line no-console
    console.log(`[Excel] 识别到表头: ${(headers || []).slice(0, 5).join(', ')}...`);
    // eslint-disable-next-line no-console
    console.log(`[Excel] 数据行数: ${(rows || []).length} 行`);
    return { fileName, fullPath, headers, rows, sheetRef };
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

async function aggregateForDateRoot({ storageRoot, dateDirName, standardMap, forceSyncLog = false }) {
  const dataTablesDir = path.join(storageRoot, 'data_tables');
  const dateDir = dateDirName ? path.join(dataTablesDir, dateDirName) : dataTablesDir;

  // eslint-disable-next-line no-console
  console.log('📁 [Critical] latestDir =', dateDir);

  const excelFiles = (await listFiles(dateDir)).filter((n) => ['.xlsx', '.xls'].includes(path.extname(n).toLowerCase()));
  const xlsxTables = excelFiles.map((fileName) => {
    const fullPath = path.join(dateDir, fileName);
    const { headers, rows, sheetRef, gridRowCount, sheetJsonLen } = readSheetRows(fullPath);
    return { fileName, fullPath, headers, rows, sheetRef, gridRowCount, sheetJsonLen };
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
  // 生产看板强制主表：ods_pdm_pdm_product_info_df（逻辑名）
  const forcedMain =
    xlsxTables.find((t) => normalizeLower(getLogicalTableName(t.fileName)) === 'ods_pdm_pdm_product_info_df') || null;
  let main = forcedMain || (required.length ? guessStyleMainTable(xlsxTables, required) : null);

  if (main) {
    // eslint-disable-next-line no-console
    console.log('📂 [Critical] 当前正在读取的主文件路径:', main.fullPath);
    // eslint-disable-next-line no-console
    console.log('📊 [Critical] 该文件实际物理行数 (含表头):', main.sheetRef || '(unknown !ref)');
    // eslint-disable-next-line no-console
    console.log('📊 [Critical] sheet_to_json(sheet).length =', Number(main.sheetJsonLen || 0));

    const maxByLen = (xlsxTables || [])
      .map((t) => ({ t, n: Number(t?.sheetJsonLen || 0) }))
      .sort((a, b) => b.n - a.n)[0];
    if (maxByLen?.t && maxByLen.n > 0 && Number(main.sheetJsonLen || 0) !== maxByLen.n) {
      // eslint-disable-next-line no-console
      console.warn(
        `[Critical] 主表行数(${Number(main.sheetJsonLen || 0)}) != 同目录最大行数(${maxByLen.n})，将扫描并在必要时切换到最大表`
      );
      // eslint-disable-next-line no-console
      for (const t of xlsxTables) {
        console.log(`- [RowCount] ${t.fileName}: sheet_to_json.len=${Number(t.sheetJsonLen || 0)} !ref=${t.sheetRef || '(n/a)'}`);
      }
      if (maxByLen.n >= 5600 && Number(main.sheetJsonLen || 0) < 5600) {
        main = maxByLen.t;
        // eslint-disable-next-line no-console
        console.log('✅ [Critical] 已切换主表为行数最多文件:', main.fileName);
        // eslint-disable-next-line no-console
        console.log('📂 [Critical] 当前正在读取的主文件路径:', main.fullPath);
        // eslint-disable-next-line no-console
        console.log('📊 [Critical] 该文件实际物理行数 (含表头):', main.sheetRef || '(unknown !ref)');
        // eslint-disable-next-line no-console
        console.log('📊 [Critical] sheet_to_json(sheet).length =', Number(main.sheetJsonLen || 0));
      }
    }
  }

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

  const lastsSet = scan3DAssetDirSync(path.join(storageRoot, 'assets', 'lasts'));
  const solesSet = scan3DAssetDirSync(path.join(storageRoot, 'assets', 'soles'));

  const mainTableName = buildTableNameIndex(main.fileName);
  const traceDim = (msg) => aiTraceLineSync(`[Trace] ${msg}`);

  // ================================
  // 物理硬编码（暴力容错 + 模糊文件名）
  // ================================
  // lastCode：associated_last_type -> base_last_df.id -> code
  const forcedLastTbl =
    getTableFromMap('ods_pdm_pdm_base_last_df', tablesMap) || findTableByNameKeywords(tablesMap, ['base_last_df', 'pdm_base_last_df']);
  const forcedLastIndex = buildIdToCodeIndex(forcedLastTbl);

  // soleCode：强制走 base_heel_df（按物理文件名包含 base_heel_df 的最新 XLSX）
  const forcedSoleTbl =
    getTableFromMap('ods_pdm_pdm_base_heel_df', tablesMap) ||
    findTableByNameKeywords(tablesMap, ['base_heel_df', 'pdm_base_heel_df']);
  const forcedSoleIndex = buildIdToCodeIndex(forcedSoleTbl);

  // 兜底：若主表没有 sole 外键（生产表常见为空），则用 base_mold_df.link_product_number 的 JSON 数组包含 style_wms 来反查 code
  const moldTbl =
    getTableFromMap('ods_pdm_pdm_base_mold_df', tablesMap) || findTableByNameKeywords(tablesMap, ['base_mold_df', 'pdm_base_mold_df']);
  const moldStyleToCode = (() => {
    if (!moldTbl?.rows?.length) return null;
    const out = new Map();
    for (const r of moldTbl.rows) {
      const codev = getRowFieldLoose(r, 'code', moldTbl.headers);
      const code = codev == null ? '' : normalize(codev);
      if (!code) continue;
      const links = getRowFieldLoose(r, 'link_product_number', moldTbl.headers);
      const arr = Array.isArray(links)
        ? links
        : (() => {
            let s = String(links ?? '').trim();
            if (!s) return null;
            // 生产数据常见形态：'"[\"A\",\"B\"]"'（外层多一层引号）
            if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
              s = s.slice(1, -1);
            }
            // 处理常见转义：\" -> "，\\ -> \
            s = s.replace(/\\"/g, '"').replace(/\\\\/g, '\\').trim();
            if (!s.startsWith('[') || !s.endsWith(']')) return null;
            try {
              const parsed = JSON.parse(s);
              return Array.isArray(parsed) ? parsed : null;
            } catch {
              return null;
            }
          })();
      if (!arr?.length) continue;
      for (const it of arr) {
        const styleKey = String(it ?? '').trim().toUpperCase();
        if (!styleKey) continue;
        if (!out.has(styleKey)) out.set(styleKey, code);
      }
    }
    return out;
  })();

  if (!forcedLastTbl || !forcedLastIndex) {
    traceDim(`楦头维表未加载/无法建索引：期望 base_last_df；实际 lastTbl=${forcedLastTbl?.fileName || '(null)'}`);
  }
  if (!forcedSoleTbl || !forcedSoleIndex) {
    traceDim(`大底维表未加载/无法建索引：期望 base_heel_df；实际 soleTbl=${forcedSoleTbl?.fileName || '(null)'}`);
  }
  // Join 性能优化：为常见维表构建 (table,targetField) -> Map<key,row> 的 O(1) 索引
  const joinIndexes = new Map();
  for (const entry of [standardMap.get('brand'), standardMap.get('lastCode'), standardMap.get('soleCode')].filter(Boolean)) {
    const jp = entry?.joinPath;
    if (!Array.isArray(jp) || jp.length < 2) continue;
    // 仅对“纯等值” hop 做索引：最后一段是 terminal（valueField），且 hop 的 targetField 存在
    for (let i = 0; i < jp.length - 1; i++) {
      const seg = jp[i];
      if (!seg || typeof seg !== 'object') continue;
      const tt = normalize(seg.targetTable);
      const tf = normalize(seg.targetField);
      if (!tt || !tf) continue;
      const k = buildJoinIndexKey(tt, tf);
      if (joinIndexes.has(k)) continue;
      const tbl = getTableFromMap(tt, tablesMap);
      const idx = buildScalarIndexForTableField(tbl, tf);
      if (idx) joinIndexes.set(k, idx);
    }
  }

  const resolveFast = (entry, mainRow, fallbackColName) => {
    if (!entry) return fallbackColName ? normalize(mainRow?.[fallbackColName]) : '';
    const jp = entry?.joinPath;
    if (!Array.isArray(jp) || jp.length !== 2) {
      return resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackColName);
    }
    const hop = jp[0];
    const term = jp[1];
    if (!hop || typeof hop !== 'object' || !term || typeof term !== 'object') {
      return resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackColName);
    }
    const sf = normalize(hop.sourceField);
    const tt = normalize(hop.targetTable);
    const tf = normalize(hop.targetField);
    const vf = normalize(term.valueField);
    const termT = normalize(term.targetTable);
    if (!sf || !tt || !tf || !vf || normalizeLower(tt) !== normalizeLower(termT)) {
      return resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackColName);
    }
    const rawKey = getRowFieldLoose(mainRow, sf, tablesMap.get(normalize(mainTableName))?.headers || []);
    const keyNorm = String(rawKey ?? '').trim().toLowerCase();
    if (!keyNorm) return '';
    const idx = joinIndexes.get(buildJoinIndexKey(tt, tf));
    if (!idx) return resolveStandardMappedValue(entry, mainRow, mainTableName, tablesMap, fallbackColName);
    const hit = idx.get(keyNorm);
    if (!hit) return '';
    const tbl = getTableFromMap(tt, tablesMap);
    const out = getRowFieldLoose(hit, vf, tbl?.headers || []);
    return out == null ? '' : normalize(out);
  };

  // aggregateFinalDashboardData 等价入口：入库前强制原始状态普查（全表物理行，无过滤）
  const rawStatusAudit = auditMainTableRawStatusDistribution(main, statusCol);
  printRawStatusAuditTable(rawStatusAudit, main, statusCol);
  printStatusScopeCensus(main, statusCol, rawStatusAudit);
  const otherRaw = auditMainTableOtherStatusDistribution(main, statusCol);
  if (otherRaw.length) {
    // eslint-disable-next-line no-console
    console.log('[Status Audit] 以下原始状态未落入 (effective/draft/invalid) 三标准桶，已计入 other：');
    for (const it of otherRaw) {
      // eslint-disable-next-line no-console
      console.log(`- "${it.value}": ${it.rowCount} 行`);
    }
    // eslint-disable-next-line no-console
    console.log('---------------------------');
  }

  const inventory = [];
  // 归一化四态：全物理行计数（用于 Dashboard 三档 Tab 的加法原则校验）
  const statusDist = { active: 0, draft: 0, obsolete: 0, other: 0 };
  let traceFailCount = 0;
  let lastCodeLinked = 0;
  let soleCodeLinked = 0;
  let last3DMatchedCount = 0;
  let sole3DMatchedCount = 0;
  let activeSeen = 0;
  let matchSamplePrinted = 0;
  const unrecognizedStatusSamples = [];
  for (let i = 0; i < main.rows.length; i++) {
    const row = main.rows[i] || {};
    const styleVal = styleCol ? normalize(row?.[styleCol]) : '';
    // 先对全物理行计数（即使 style_wms 为空也要入 statusDist）
    const styleUpper = String(styleVal || '').trim().toUpperCase();
    const wantTrace = styleUpper === 'SBOX26008M';

    const rawStatusVal = statusCol ? row?.[statusCol] : '';
    const rawStatusTrim = String(rawStatusVal ?? '').trim();
    const rawStatusLower = rawStatusTrim.toLowerCase();
    if (
      rawStatusTrim &&
      rawStatusLower !== 'effective' &&
      rawStatusLower !== 'invalid' &&
      unrecognizedStatusSamples.length < 20
    ) {
      unrecognizedStatusSamples.push(rawStatusTrim);
    }
    const invStatus = normalizeInventoryStatus(rawStatusVal);
    statusDist[invStatus] += 1;

    // inventory（入库款号行）仍只统计有 style_wms 的行
    if (!styleVal) continue;

    const lastResolved = (() => {
      // 强制路径：优先直接取 associated_last_type；若列名在生产快照中变体，则自动探测 FK 列
      let fk = getRowFieldLoose(row, 'associated_last_type', main.headers);
      if (fk == null || normalize(fk) === '') {
        const fkCol = guessFkColumnFromMainRow(row, 'lastCode');
        if (fkCol) fk = row?.[fkCol];
      }
      const fkRaw = String(fk ?? '').trim();
      const fkKey = normalizeJoinIdKey(fk);
      if (!fkKey && (fkRaw === '' || fkRaw === '0')) {
        traceDim(`楦头空值容错：style=${styleUpper} associated_last_type=${fkRaw === '' ? '(空)' : '0'}`);
      }
      if (wantTrace) {
        traceDim(
          `楦头处理：style=${styleUpper} 主表字段 associated_last_type=${fkKey || '(空)'}；last维表=${forcedLastTbl?.fileName || '(null)'}`
        );
      }
      if (fkKey && forcedLastIndex) {
        const code = forcedLastIndex.get(fkKey);
        if (code) {
          if (wantTrace) traceDim(`楦头处理：主表 ID ${fkKey} -> 在 base_last_df 找到行 -> 提取 code = ${code}`);
          return { code, linked: true, fkKey, fkRaw: fkKey };
        }
        if (wantTrace) traceDim(`楦头处理：主表 ID ${fkKey} -> 在 base_last_df 未找到行`);
      }
      // 兜底：如果维表缺失/无法命中，再尝试映射配置
      const v = resolveFast(standardMap.get('lastCode'), row, lastCol);
      if (wantTrace) traceDim(`楦头处理：fallback resolveFast => ${v || '(空)'}`);
      return { code: v, linked: false, fkKey, fkRaw: fkKey };
    })();
    const lastCodeVal = lastResolved?.code || '';

    const soleResolved = (() => {
      let fk = getRowFieldLoose(row, 'associated_sole_info', main.headers);
      // 强制锁定：优先 associated_sole_info；若为空则回退 associated_heel_info（生产数据常见字段变体）
      if (fk == null || normalize(fk) === '') {
        const alt = getRowFieldLoose(row, 'associated_heel_info', main.headers);
        if (alt != null && normalize(alt) !== '') {
          fk = alt;
          if (wantTrace) traceDim(`大底处理：associated_sole_info 为空，回退使用 associated_heel_info=${String(alt).trim()}`);
        }
      }
      const fkRaw = String(fk ?? '').trim();
      const fkKey = normalizeJoinIdKey(fk);
      if (!fkKey && (fkRaw === '' || fkRaw === '0')) {
        traceDim(`大底空值容错：style=${styleUpper} associated_sole_info=${fkRaw === '' ? '(空)' : '0'}`);
      }
      if (wantTrace) {
        traceDim(
          `大底处理：style=${styleUpper} 主表字段 associated_sole_info=${fkKey || '(空)'}；sole维表=${forcedSoleTbl?.fileName || '(null)'}`
        );
      }
      if (fkKey && forcedSoleIndex) {
        const code = forcedSoleIndex.get(fkKey);
        if (code) {
          if (wantTrace) traceDim(`大底处理：主表 ID ${fkKey} -> 在 base_heel_df 找到行 -> 提取 code = ${code}`);
          return { code, linked: true, fkKey, fkRaw: fkKey };
        }
        if (wantTrace) traceDim(`大底处理：主表 ID ${fkKey} -> 在 base_heel_df 未找到行`);
      }

      // ARRAY_CONTAINS 兜底：base_mold_df.link_product_number 包含 style_wms
      if (moldStyleToCode && styleUpper) {
        const code = moldStyleToCode.get(styleUpper);
        if (code) {
          if (wantTrace) traceDim(`大底处理：ARRAY_CONTAINS 命中（base_mold_df.link_product_number 包含 ${styleUpper}）-> code=${code}`);
          return { code, linked: true, fkKey: '', fkRaw: '' };
        }
      }

      const v = resolveFast(standardMap.get('soleCode'), row, soleCol);
      if (wantTrace) traceDim(`大底处理：fallback resolveFast => ${v || '(空)'}`);
      return { code: v, linked: false, fkKey, fkRaw: fkKey };
    })();
    const soleCodeVal = soleResolved?.code || '';
    const colorCodeVal = resolveFast(standardMap.get('colorCode'), row, colorCol);
    const materialCodeVal = resolveStandardMappedValue(
      standardMap.get('materialCode'),
      row,
      mainTableName,
      tablesMap,
      materialCol
    );

    if (!wantTrace && traceFailCount < 50) {
      if (!lastCodeVal) {
        traceFailCount += 1;
        traceDim(
          `楦头处理失败：style=${styleUpper} associated_last_type=${String(
            getRowFieldLoose(row, 'associated_last_type', main.headers) ?? ''
          ).trim()} lastTbl=${forcedLastTbl?.fileName || '(null)'}`
        );
      }
      if (!soleCodeVal) {
        traceFailCount += 1;
        traceDim(
          `大底处理失败：style=${styleUpper} associated_sole_info=${String(
            getRowFieldLoose(row, 'associated_sole_info', main.headers) ?? ''
          ).trim()} soleTbl=${forcedSoleTbl?.fileName || '(null)'}`
        );
      }
    }

    const has3DLast = physicalMatchInAssetSet(lastsSet, lastCodeVal);
    const has3DSole = physicalMatchInAssetSet(solesSet, soleCodeVal);
    const has3DAny = has3DLast || has3DSole;

    const brandVal = resolveFast(standardMap.get('brand'), row, brandCol);
    const targetAudienceRaw = getRowFieldLoose(row, 'target_audience', main.headers);
    const target_audience =
      targetAudienceRaw == null || String(targetAudienceRaw).trim() === ''
        ? undefined
        : String(targetAudienceRaw).trim();

    // 编号绑定口径：仅统计生效款中，“FK 非空且在维表能命中 code” 的数量
    if (invStatus === 'active') {
      activeSeen += 1;
      if (lastResolved?.linked) lastCodeLinked += 1;
      if (soleResolved?.linked) soleCodeLinked += 1;
      if (has3DLast) last3DMatchedCount += 1;
      if (has3DSole) sole3DMatchedCount += 1;
      if (forceSyncLog && lastResolved?.linked && matchSamplePrinted < 5) {
        matchSamplePrinted += 1;
        // eslint-disable-next-line no-console
        console.log(`Match Success: ${String(lastResolved.fkRaw || lastResolved.fkKey || '').trim()} -> ${String(lastResolved.code || '').trim()}`);
      }
      if (forceSyncLog && activeSeen % 200 === 0) {
        // eslint-disable-next-line no-console
        console.log(`[ForceSync] 正在处理生产表，当前已绑定编号：${lastCodeLinked} 行`);
        // eslint-disable-next-line no-console
        console.log(`[ForceSync] 正在比对 3D 文件，当前已匹配：${last3DMatchedCount} 个`);
      }
    }

    inventory.push({
      id: `${dateDirName || 'latest'}-${main.fileName}-${i + 1}`,
      style_wms: styleVal,
      brand: brandVal,
      colorCode: colorCodeVal,
      materialCode: materialCodeVal,
      lastCode: lastCodeVal || undefined,
      lastStatus: has3DLast ? 'matched' : 'missing',
      has3DLast,
      soleCode: soleCodeVal || undefined,
      soleStatus: has3DSole ? 'matched' : 'missing',
      has3DSole,
      data_status: invStatus,
      lastUpdated: dateDirName || '',
      updatedBy: 'Storage',
      sourceTable: main.fileName,
      target_audience,
      __has3DAny: has3DAny,
      __has3DLast: has3DLast,
      __has3DSole: has3DSole,
    });
  }

  const totalStyles = inventory.length;
  if (unrecognizedStatusSamples.length) {
    // eslint-disable-next-line no-console
    console.log('[Critical] 前 20 个“非 effective 且非 invalid”的原始 data_status 值：', unrecognizedStatusSamples);
  }
  const statusSum = Number(statusDist.active || 0) + Number(statusDist.draft || 0) + Number(statusDist.obsolete || 0) + Number(statusDist.other || 0);
  if (statusSum !== totalStyles) {
    // eslint-disable-next-line no-console
    console.warn(`[Engine] 状态分桶合计 ${statusSum} 与入库行 totalStyles=${totalStyles} 不一致（可能存在 style_wms 空行过滤）`);
  }

  // eslint-disable-next-line no-console
  console.log('归一化状态桶（仅含已入库款号行）：', [...new Set(inventory.map((r) => r.data_status))]);

  // 分桶：effective(active) / draft / obsolete / total（total = 全量，不按状态过滤）
  const allBrands = uniqBrandsFromInventoryRows(inventory);
  const bucketTotal = inventory;
  const bucketEffective = inventory.filter((x) => x.data_status === 'active');
  const bucketDraft = inventory.filter((x) => x.data_status === 'draft');
  const bucketObsolete = inventory.filter((x) => x.data_status === 'obsolete');
  const bucketOther = inventory.filter((x) => x.data_status === 'other');

  const kpisTotal = computeBucketKPIs(bucketTotal);
  const kpisEffective = computeBucketKPIs(bucketEffective);
  const kpisDraft = computeBucketKPIs(bucketDraft);
  const kpisObsolete = computeBucketKPIs(bucketObsolete);
  const kpisOther = computeBucketKPIs(bucketOther);

  const buildBucket = (rows, kpis) => ({
    kpis,
    lastDigitizationStats: computeBrandDigitizationStats(rows, 'last', allBrands),
    soleDigitizationStats: computeBrandDigitizationStats(rows, 'sole', allBrands),
  });

  const statusBuckets = {
    total: buildBucket(bucketTotal, { ...kpisTotal, statusDist }),
    effective: buildBucket(bucketEffective, kpisEffective),
    draft: buildBucket(bucketDraft, kpisDraft),
    obsolete: buildBucket(bucketObsolete, kpisObsolete),
    other: buildBucket(bucketOther, kpisOther),
  };

  // 各品牌 3D 覆盖（为了兼容老 UI：仍默认返回“生效款”口径）
  const brandCoverage = computeBrand3DCoverageStats(bucketEffective);
  const brandBindingStats = computeBrandBindingStats(bucketEffective);
  const lastDigitizationStats = statusBuckets.effective.lastDigitizationStats;
  const soleDigitizationStats = statusBuckets.effective.soleDigitizationStats;

  const inventoryOut = inventory.map(({ __has3DAny, __has3DLast, __has3DSole, ...rest }) => rest);

  return {
    dateDirName,
    // 新结构：按状态分组（Dashboard Tabs 直接切换）
    statusBuckets: {
      total: statusBuckets.total,
      effective: statusBuckets.effective,
      draft: statusBuckets.draft,
      obsolete: statusBuckets.obsolete,
      other: statusBuckets.other,
    },
    // 兼容字段：默认等价于 effective
    kpis: {
      totalStyles,
      activeStyles: kpisEffective.totalStyles,
      matchedLasts: kpisEffective.matchedLasts,
      matchedSoles: kpisEffective.matchedSoles,
      lastCoverage: kpisEffective.lastCoverage,
      soleCoverage: kpisEffective.soleCoverage,
      stylesWithAny3D: kpisEffective.stylesWithAny3D,
      any3DCoveragePercent: kpisEffective.any3DCoveragePercent,
      statusDist,
      last3DCount: kpisEffective.last3DCount,
      last3DCoverage: kpisEffective.last3DCoverage,
      lastCodeLinked: kpisEffective.lastCodeLinked,
      lastCodeLinkRate: kpisEffective.lastCodeLinkRate,
      soleCodeLinked: kpisEffective.soleCodeLinked,
      soleCodeLinkRate: kpisEffective.soleCodeLinkRate,
      sole3DCount: kpisEffective.sole3DCount,
      sole3DCoverage: kpisEffective.sole3DCoverage,
      // force-sync 实时计数（对账用）：仍沿用全循环累计值（便于排查）
      __forceSync: forceSyncLog
        ? {
            lastCodeLinkedCount: lastCodeLinked,
            last3DMatchedCount,
            soleCodeLinkedCount: soleCodeLinked,
            sole3DMatchedCount,
          }
        : undefined,
    },
    brandCoverage,
    brandBindingStats,
    lastDigitizationStats,
    soleDigitizationStats,
    inventory: inventoryOut,
    rawStatusAudit,
    meta: {
      mainTable: main.fileName,
      requiredCols: required,
      mainRowCount: main.rows?.length || 0,
      source: 'data_tables',
      uniqueBrandCount: allBrands.length,
      dataStatusColumn: statusCol || null,
      rawStatusAudit,
    },
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
  const statusBuckets = latest.statusBuckets || null;
  const prev = agg.prev || null;

  const sumStatusDist = (sd) =>
    Number(sd?.active || 0) + Number(sd?.draft || 0) + Number(sd?.obsolete || 0) + Number(sd?.other || 0);
  const totalPoolLatest = sumStatusDist(statusBuckets?.total?.kpis?.statusDist);
  const totalPoolPrev = prev?.statusBuckets?.total?.kpis?.statusDist ? sumStatusDist(prev.statusBuckets.total.kpis.statusDist) : null;

  const latestEffKpis = statusBuckets?.effective?.kpis || {};
  const prevEffKpis = prev?.statusBuckets?.effective?.kpis || null;
  const deltaTotalPoolStyles = totalPoolPrev != null ? totalPoolLatest - totalPoolPrev : 0;
  const delta3DLasts = prevEffKpis
    ? Number(latestEffKpis.last3DCount || latestEffKpis.matchedLasts || 0) -
      Number(prevEffKpis.last3DCount || prevEffKpis.matchedLasts || 0)
    : 0;
  const delta3DSoles = prevEffKpis
    ? Number(latestEffKpis.sole3DCount || latestEffKpis.matchedSoles || 0) -
      Number(prevEffKpis.sole3DCount || prevEffKpis.matchedSoles || 0)
    : 0;

  // 趋势：遍历所有日期目录，生成按日时间序列（从远到近；effective 口径累计）
  const dataTablesDir = path.join(storageRoot, 'data_tables');
  const mappingConfigPath = path.join(storageRoot, 'mapping_config.json');
  const mappingConfig = safeJsonRead(mappingConfigPath);
  const mappingArr = pickMappingArray(mappingConfig);
  const standardMap = buildStandardMap(mappingArr || []);
  const allDateDirs = await getDateDirsByNameSorted(dataTablesDir); // asc
  const trendHistory = [];
  for (const d of allDateDirs) {
    try {
      const one = await aggregateForDateRoot({ storageRoot, dateDirName: d, standardMap });
      const eff = one?.statusBuckets?.effective?.kpis || {};
      const tot = one?.statusBuckets?.total?.kpis?.statusDist || null;
      trendHistory.push({
        date: String(d),
        styles: tot ? sumStatusDist(tot) : Number(one?.kpis?.totalStyles || 0),
        lasts3D: Number(eff.last3DCount || eff.matchedLasts || 0),
        soles3D: Number(eff.sole3DCount || eff.matchedSoles || 0),
      });
    } catch {
      // ignore single date folder failure
    }
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    dates: agg.dates,
    mapping: agg.mapping,
    meta: latest.meta,
    kpis: {
      // 新口径：总款号数（全量）+ 生效款号数（副数值）
      styles: {
        totalAll: kpis.totalStyles ?? kpis.activeStyles,
        totalEffective: kpis.activeStyles,
      },
      // 旧字段保留兼容
      totalStyles: kpis.totalStyles ?? kpis.activeStyles,
      activeStyles: kpis.activeStyles,
      matched3DLasts: kpis.matchedLasts,
      matched3DSoles: kpis.matchedSoles,
      lastCoverage: kpis.lastCoverage,
      soleCoverage: kpis.soleCoverage,
      last3DCount: kpis.last3DCount ?? kpis.matchedLasts,
      last3DCoverage: kpis.last3DCoverage ?? kpis.lastCoverage,
      lastCodeLinked: kpis.lastCodeLinked ?? 0,
      lastCodeLinkRate: kpis.lastCodeLinkRate ?? 0,
      soleCodeLinked: kpis.soleCodeLinked ?? 0,
      soleCodeLinkRate: kpis.soleCodeLinkRate ?? 0,
      sole3DCount: kpis.sole3DCount ?? kpis.matchedSoles,
      sole3DCoverage: kpis.sole3DCoverage ?? kpis.soleCoverage,
      // alias for API consumers
      sole3DMatched: kpis.sole3DCount ?? kpis.matchedSoles,
      stylesWithAny3D: kpis.stylesWithAny3D,
      any3DCoveragePercent: kpis.any3DCoveragePercent,
      statusDist: kpis.statusDist || undefined,
      deltaActiveStyles: agg.deltas.activeStyles.delta,
      deltaMatched3DLasts: agg.deltas.matchedLasts.delta,
      deltaMatched3DSoles: agg.deltas.matchedSoles.delta,
      deltaTotalPoolStyles,
      delta3DLasts,
      delta3DSoles,
    },
    // 新口径：按状态分桶的 KPI + 品牌进度榜（前端 Tabs 切换直接用）
    statusBuckets: statusBuckets || undefined,
    brandCoverage: latest.brandCoverage,
    brandBindingStats: latest.brandBindingStats || [],
    lastDigitizationStats: latest.lastDigitizationStats || [],
    soleDigitizationStats: latest.soleDigitizationStats || [],
    inventory: latest.inventory,
    trendHistory,
    trends: {
      // legacy: 保留字段，避免老 UI 依赖；但新 UI 请使用 trendHistory
      assetTrend: buildAssetTrendSeries({
        latestDate: agg.dates?.latest || '',
        last3DCount: kpis.last3DCount ?? kpis.matchedLasts,
        sole3DCount: kpis.sole3DCount ?? kpis.matchedSoles,
        months: 12,
      }),
    },
    rawStatusAudit: latest.rawStatusAudit || latest.meta?.rawStatusAudit || [],
  };

  // eslint-disable-next-line no-console
  console.log(
    `[Engine] 合成完成，耗时 ${Date.now() - t0}ms；生效款 ${kpis.activeStyles}，任一3D命中 ${kpis.stylesWithAny3D}（${kpis.any3DCoveragePercent}%）`
  );
  const sd = payload.statusBuckets?.total?.kpis?.statusDist;
  if (sd) {
    const a = Number(sd.active || 0);
    const d = Number(sd.draft || 0);
    const o = Number(sd.obsolete || 0);
    const ot = Number(sd.other || 0);
    const sum = a + d + o + ot;
    const invN = Array.isArray(payload.inventory) ? payload.inventory.length : 0;
    // eslint-disable-next-line no-console
    console.log(`[Engine] 统计结果：生效(${a}) + 草稿(${d}) + 作废(${o}) + 其他(${ot}) = 总数(${sum})`);
    if (sum !== invN) {
      // eslint-disable-next-line no-console
      console.warn(`[Engine] 状态合计 ${sum} 与 inventory 行数 ${invN} 不一致`);
    }
  }
  const engineSummary = {
    generatedAt: payload.generatedAt,
    dates: payload.dates,
    inventoryRows: Array.isArray(payload.inventory) ? payload.inventory.length : 0,
    uniqueBrandCount: payload.meta?.uniqueBrandCount,
    statusBuckets: payload.statusBuckets ? Object.keys(payload.statusBuckets) : [],
    kpis: {
      totalStyles: payload.kpis?.totalStyles,
      activeStyles: payload.kpis?.activeStyles,
      lastCodeLinkRate: payload.kpis?.lastCodeLinkRate,
      soleCodeLinkRate: payload.kpis?.soleCodeLinkRate,
    },
  };
  // eslint-disable-next-line no-console
  console.log('Engine Final Stats Summary:', engineSummary);
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
  // eslint-disable-next-line no-console
  console.log('📁 [Critical] latestDir =', dateDir);

  let main =
    xlsxTables.find((t) => normalize(getLogicalTableName(t?.fileName || '')) === MAIN_LOGICAL) ||
    (masterTableLogicalName
      ? xlsxTables.find((t) => normalize(getLogicalTableName(t?.fileName || '')) === normalize(masterTableLogicalName)) || null
      : null);
  if (!main) {
    return { ok: false, error: '无法识别主表（必须存在 ods_pdm_pdm_product_info_df.xlsx）', row: null, mainTable: null };
  }

  // 物理对账自查：打印绝对路径与 !ref（含表头）
  // eslint-disable-next-line no-console
  console.log('📂 [Critical] 当前正在读取的主文件路径:', main.fullPath);
  // eslint-disable-next-line no-console
  console.log('📊 [Critical] 该文件实际物理行数 (含表头):', main.sheetRef || '(unknown !ref)');
  // eslint-disable-next-line no-console
  console.log('📊 [Critical] sheet_to_json(sheet).length =', Number(main.sheetJsonLen || 0));

  // 若行数明显不对：扫描同目录下所有表，找出“行数最多”的候选（用于纠偏）
  const maxByLen = (xlsxTables || [])
    .map((t) => ({ t, n: Number(t?.sheetJsonLen || 0) }))
    .sort((a, b) => b.n - a.n)[0];
  if (maxByLen?.t && maxByLen.n > 0 && Number(main.sheetJsonLen || 0) < maxByLen.n) {
    // eslint-disable-next-line no-console
    console.warn(
      `[Critical] 主表行数(${Number(main.sheetJsonLen || 0)}) < 同目录最大行数(${maxByLen.n})，将输出候选列表并尝试锁定最大表`
    );
    // eslint-disable-next-line no-console
    for (const t of xlsxTables) {
      console.log(`- [RowCount] ${t.fileName}: sheet_to_json.len=${Number(t.sheetJsonLen || 0)} !ref=${t.sheetRef || '(n/a)'}`);
    }
    // 若最大表接近 5824（阈值 5600+），强制锁定为 main（防止读错快照/文件）
    if (maxByLen.n >= 5600) {
      main = maxByLen.t;
      // eslint-disable-next-line no-console
      console.log('✅ [Critical] 已切换主表为行数最多文件:', main.fileName);
      // eslint-disable-next-line no-console
      console.log('📂 [Critical] 当前正在读取的主文件路径:', main.fullPath);
      // eslint-disable-next-line no-console
      console.log('📊 [Critical] 该文件实际物理行数 (含表头):', main.sheetRef || '(unknown !ref)');
      // eslint-disable-next-line no-console
      console.log('📊 [Critical] sheet_to_json(sheet).length =', Number(main.sheetJsonLen || 0));
    }
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
    const rowsAll = main.rows || [];
    const candidates = rowsAll.filter((r) => eqLoose(r?.[styleCol], targetStyleNorm) || rowContainsTarget(r, targetStyleNorm));
    const preferKeys = [
      'id',
      'brand',
      'associated_last_type',
      'associated_sole_info',
      'initial_sample_color_id',
      'main_material',
      'new_main_material',
      'data_status',
    ];
    const scoreRow = (row) => {
      if (!row || typeof row !== 'object') return { score: -1, hasPk: false };
      let score = 0;
      for (const k of preferKeys) {
        const v = row?.[k];
        if (v == null) continue;
        const s = String(v).trim();
        if (s !== '') score += 1;
      }
      const hasPk = String(row?.id ?? '').trim() !== '';
      return { score, hasPk };
    };
    let hit = null;
    if (candidates.length) {
      hit = candidates
        .map((r) => ({ r, ...scoreRow(r) }))
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          if (b.hasPk !== a.hasPk) return b.hasPk ? 1 : -1;
          return 0;
        })[0]?.r;
    } else {
      hit = null;
    }
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
  // 多版本识别：按日期目录名倒序
  const dateDirs = await getDateDirsByNameDesc(dataTablesDir);
  const latest = dateDirs.length ? dateDirs[0] : '';
  const prev = dateDirs.length >= 2 ? dateDirs[1] : '';

  const latestAgg = await aggregateForDateRoot({ storageRoot, dateDirName: latest, standardMap });
  const prevAgg = prev ? await aggregateForDateRoot({ storageRoot, dateDirName: prev, standardMap }) : null;

  // 容错：无 Previous 时 delta 归零（禁止出现误导性的巨大增量）
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

/**
 * 强制生产重算（仅最新日期目录），用于 /api/force-sync-dashboard。
 * 会在遍历生效款时打印 ForceSync 进度日志。
 */
export async function forceSyncLatestProduction({ storageRoot }) {
  const mappingConfigPath = path.join(storageRoot, 'mapping_config.json');
  const mappingConfig = safeJsonRead(mappingConfigPath);
  const mappingArr = pickMappingArray(mappingConfig);
  const standardMap = buildStandardMap(mappingArr || []);

  const dataTablesDir = path.join(storageRoot, 'data_tables');
  const dateDirs = await getDateDirsByNameDesc(dataTablesDir);
  const latest = dateDirs.length ? dateDirs[0] : '';
  const latestAgg = await aggregateForDateRoot({ storageRoot, dateDirName: latest, standardMap, forceSyncLog: true });
  return {
    dates: { latest, prev: dateDirs.length >= 2 ? dateDirs[1] : '' },
    mapping: { hasConfig: Boolean(mappingArr && mappingArr.length), configPath: mappingConfigPath },
    latest: latestAgg,
  };
}
