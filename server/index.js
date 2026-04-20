import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import fs from 'node:fs';
import fse from 'fs-extra';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import XLSX from 'xlsx';
import { GoogleGenerativeAI } from '@google/generative-ai';
import {
  aggregateProjectData,
  buildStandardMap,
  auditLineSync,
  aiTraceLineSync,
  clearAiTraceLogSync,
  clearEngineAuditLogSync,
  clearEngineLogSync,
  inferMainTableFromSmartSuggestions,
  isTruthyActiveStatus,
  parseJoinPathFromConfig,
  persistFinalDashboardData,
  processAllData,
  readAllFilesFromFolder,
  resolveRecursiveValue,
  resolveFirstActiveRowFromFolder,
  validateJoinPathSuggestionsWithGoldenXlsx,
} from './dataEngine.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- ENV LOADER (absolute path) ---
// ESM 环境下无法把 require(...) 放在 import 之前；这里使用绝对路径强制加载 server/.env
const ENV_PATH = path.join(__dirname, '.env');
const envStat = (() => {
  try {
    const exists = fse.existsSync(ENV_PATH);
    const bytes = exists ? fse.readFileSync(ENV_PATH).length : 0;
    return { exists, bytes };
  } catch {
    return { exists: false, bytes: 0 };
  }
})();
const dotenvResult = dotenv.config({ path: ENV_PATH, debug: true });

const app = express();

app.use(cors());
// 17 表 DDL + 草稿 JSON 体积较大，放宽上限避免 413
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

/** 与 package.json 中 `node server/index.js`（cwd=仓库根）对齐；若在 server/ 内直接 node index.js 则 cwd 为 server */
function resolveProjectServerStorageDir() {
  const cwd = process.cwd();
  if (path.basename(cwd) === 'server') {
    return path.join(cwd, 'storage');
  }
  return path.join(cwd, 'server', 'storage');
}

/** 物理路径：等价于仓库根下 `server/storage`（见 resolveProjectServerStorageDir） */
const DRAFT_STORAGE_DIR = resolveProjectServerStorageDir();
const SCHEMA_DRAFT_PATH = path.join(DRAFT_STORAGE_DIR, 'schema_draft.json');

const STORAGE_ROOT = path.join(__dirname, 'storage');
const DIRS = {
  dataTables: path.join(STORAGE_ROOT, 'data_tables'),
  lasts: path.join(STORAGE_ROOT, 'assets', 'lasts'),
  soles: path.join(STORAGE_ROOT, 'assets', 'soles'),
  sandbox: path.join(STORAGE_ROOT, 'sandbox'),
};
const MAPPING_CONFIG_PATH = path.join(STORAGE_ROOT, 'mapping_config.json');
const FINAL_DASHBOARD_PATH = path.join(STORAGE_ROOT, 'final_dashboard_data.json');

async function readFinalDashboardSnapshot() {
  try {
    if (!(await fse.pathExists(FINAL_DASHBOARD_PATH))) return null;
    const data = await fse.readJson(FINAL_DASHBOARD_PATH);
    if (!data || typeof data !== 'object' || !data.kpis) return null;
    return data;
  } catch {
    return null;
  }
}

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const genAI = GEMINI_API_KEY ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;
// NOTE: This API key's available models are discoverable via /api/debug/gemini-models
// Tier 1（New Users 权限限制）热修：锁定确权可用模型（不带 models/ 前缀，禁止使用 -exp 实验模型）
const MODEL_PRIORITY = ['gemini-1.5-flash', 'gemini-2.0-flash', 'gemini-pro-latest'];
// 环境变量强制指定：若设置 GEMINI_MODEL，则仅使用该模型，跳过优先级列表
const GEMINI_FORCED_MODEL = (process.env.GEMINI_MODEL || '').trim();
const GEMINI_DISCOVERY_URL = 'https://generativelanguage.googleapis.com/v1beta/models';
const GEMINI_V1_MODELS_URL = 'https://generativelanguage.googleapis.com/v1/models';
const GEMINI_V1_GENERATE_URL = 'https://generativelanguage.googleapis.com/v1/models';

let currentAIStatus = {
  status: 'idle',
  message: '',
  model: '',
  updatedAt: Date.now(),
  parsedCount: 0,
  totalCount: 0,
  parsedTables: [],
  // 串行维度建模：阶段性产出给前端轮询消费
  partialRevision: 0,
  partial: {
    smartSuggestions: [],
    joinPathSuggestions: [],
    concatMappingSuggestions: [],
    // 记录每个维度的状态（success/fail/attempts/lastError）
    dims: {},
  },
};
function setAIStatus(patch) {
  currentAIStatus = {
    ...currentAIStatus,
    ...patch,
    updatedAt: Date.now(),
  };
}

/** Step2 黄金样本：支持按标准维度 + 多段拼接（兼容旧版单行 table/field/value） */
function normalizeGoldenSamplesForAi(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const x of raw) {
    if (x && Array.isArray(x.segments) && x.segments.length) {
      const standardKey = typeof x.standardKey === 'string' ? x.standardKey.trim() : '';
      const segments = x.segments
        .map((s) => ({
          tableName: typeof s?.tableName === 'string' ? s.tableName.trim() : '',
          fieldName: typeof s?.fieldName === 'string' ? s.fieldName.trim() : '',
          value: typeof s?.value === 'string' ? String(s.value) : String(s?.value ?? ''),
        }))
        .filter((s) => s.tableName && s.fieldName);
      if (segments.length) out.push({ standardKey, segments });
      continue;
    }
    const tableName = typeof x?.tableName === 'string' ? x.tableName.trim() : '';
    const fieldName = typeof x?.fieldName === 'string' ? x.fieldName.trim() : '';
    const value = typeof x?.value === 'string' ? String(x.value) : String(x?.value ?? '');
    if (tableName && fieldName) out.push({ standardKey: '', segments: [{ tableName, fieldName, value }] });
  }
  return out;
}

/**
 * Step2 按维度分组：targetGoal + rows[].notes + segments
 * body.goldenByDimension: Record<standardKey, { targetGoal, rows: [{ id?, notes, segments }] }>
 */
function normalizeGoldenByDimensionForAi(raw) {
  const out = [];
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return out;
  for (const [key, grp] of Object.entries(raw)) {
    const standardKey = String(key || '').trim();
    if (!standardKey) continue;
    const targetGoal = typeof grp?.targetGoal === 'string' ? String(grp.targetGoal).trim() : '';
    const rowsRaw = Array.isArray(grp?.rows) ? grp.rows : [];
    const rows = [];
    for (const r of rowsRaw) {
      const notes = typeof r?.notes === 'string' ? String(r.notes).trim() : '';
      const segments = Array.isArray(r?.segments)
        ? r.segments
            .map((s) => ({
              tableName: typeof s?.tableName === 'string' ? s.tableName.trim() : '',
              fieldName: typeof s?.fieldName === 'string' ? s.fieldName.trim() : '',
              value: typeof s?.value === 'string' ? String(s.value) : String(s?.value ?? ''),
            }))
            .filter((s) => s.tableName && s.fieldName)
        : [];
      if (!segments.length) continue;
      rows.push({ notes, segments });
    }
    if (!targetGoal && !rows.length) continue;
    out.push({ standardKey, targetGoal, rows });
  }
  return out;
}

/** 兼容旧 Prompt 统计：扁平化为「每组一行」 */
function expandGoldenBlocksToFlatSamples(blocks) {
  const flat = [];
  for (const b of blocks || []) {
    for (const row of b.rows || []) {
      if (row.segments?.length) flat.push({ standardKey: b.standardKey, segments: row.segments });
    }
  }
  return flat;
}

/** 黄金期望值 Map：Step2 样本 + reference 缺省字段（供 Prompt 与 XLSX 回测共用） */
function buildGoldenExpectedMap({ goldenBlocks, goldenSamples, reference }) {
  const out = new Map();
  if (goldenBlocks && goldenBlocks.length) {
    for (const b of goldenBlocks) {
      const sk = String(b.standardKey || '').trim();
      if (!sk) continue;
      const goal = String(b.targetGoal || '').trim();
      if (goal) {
        out.set(sk, goal);
        continue;
      }
      const rows = Array.isArray(b.rows) ? b.rows : [];
      const joined = rows
        .map((r) => (Array.isArray(r?.segments) ? r.segments.map((s) => String(s?.value ?? '')).join('') : ''))
        .join('');
      if (joined.trim()) out.set(sk, joined.trim());
    }
  } else if (goldenSamples && goldenSamples.length) {
    for (const g of goldenSamples) {
      const sk = String(g.standardKey || '').trim();
      if (!sk) continue;
      const joined = Array.isArray(g.segments) ? g.segments.map((s) => String(s?.value ?? '')).join('') : '';
      if (joined.trim() && !out.has(sk)) out.set(sk, joined.trim());
    }
  }
  const refKeys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
  if (reference && typeof reference === 'object') {
    for (const k of refKeys) {
      const v = reference[k];
      if (v != null && String(v).trim() && !out.has(k)) out.set(k, String(v).trim());
    }
  }
  return out;
}

/** joinPath 最后一节的字段名（小写），用于拒绝楦/底以 .id 结尾的错误路径 */
function joinPathLastFieldLower(pathArr) {
  const last = pathArr && pathArr.length ? pathArr[pathArr.length - 1] : '';
  const s = String(last);
  const dot = s.lastIndexOf('.');
  return dot > 0 ? s.slice(dot + 1).trim().toLowerCase() : '';
}

/**
 * AI 新 DSL：中间段 { sourceTable, sourceField, targetTable, targetField }，末段 { targetTable, valueField }。
 * 转为与引擎/前端兼容的 table.field 链。
 */
function structuredJoinPathToLegacyStringPath(segments) {
  const norm = (x) => String(x ?? '').trim();
  if (!Array.isArray(segments) || segments.length < 2) return null;
  const out = [];
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i];
    if (!seg || typeof seg !== 'object') return null;
    const st = norm(seg.sourceTable);
    const sf = norm(seg.sourceField);
    const tt = norm(seg.targetTable);
    const tf = norm(seg.targetField);
    if (!st || !sf || !tt || !tf) return null;
    if (norm(seg.valueField)) return null;
    if (i === 0) out.push(`${st}.${sf}`);
    else out.push(`${st}.${sf}`);
    out.push(`${tt}.${tf}`);
  }
  const last = segments[segments.length - 1];
  if (!last || typeof last !== 'object') return null;
  const tt = norm(last.targetTable);
  const vf = norm(last.valueField);
  if (!tt || !vf) return null;
  if (norm(last.targetField) || norm(last.sourceTable) || norm(last.sourceField)) return null;
  out.push(`${tt}.${vf}`);
  return out.length >= 2 ? out : null;
}

/** AI 输出：结构化 joinPath 数组或旧版 string[]（table.field） */
function normalizeAiJoinPathToStringPath(raw) {
  if (!Array.isArray(raw) || !raw.length) return [];
  if (typeof raw[0] === 'object' && raw[0] !== null) {
    return structuredJoinPathToLegacyStringPath(raw) || [];
  }
  return raw.map((p) => String(p).trim()).filter(Boolean);
}

function isModelNotFoundError(e) {
  const status = getHttpStatusFromGeminiError(e);
  if (status === 404) return true;
  const msg = String(e?.message || '').toLowerCase();
  return msg.includes('404') || (msg.includes('model') && msg.includes('not found'));
}

function getGenerativeModelCompat(modelName) {
  // SDK 有时要求带 models/ 前缀，有时要求裸名；这里做兼容兜底
  const name = String(modelName || '').trim();
  try {
    return genAI.getGenerativeModel({ model: name });
  } catch (e) {
    const withPrefix = name.startsWith('models/') ? name : `models/${name}`;
    // eslint-disable-next-line no-console
    console.warn('[gemini] getGenerativeModel fallback to prefixed name:', withPrefix);
    return genAI.getGenerativeModel({ model: withPrefix });
  }
}

function normalizeModelName(name) {
  return String(name || '').replace(/^models\//, '').trim();
}

async function listAvailableModelsV1() {
  if (!GEMINI_API_KEY) return [];
  const url = `${GEMINI_V1_MODELS_URL}?key=${encodeURIComponent(GEMINI_API_KEY)}`;
  try {
    const resp = await fetch(url);
    const json = await resp.json().catch(() => null);
    const models = Array.isArray(json?.models) ? json.models : [];
    return models
      .filter((m) => Array.isArray(m?.supportedGenerationMethods) && m.supportedGenerationMethods.includes('generateContent'))
      .map((m) => normalizeModelName(m?.name))
      .filter(Boolean);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[models] listAvailableModelsV1 failed', e instanceof Error ? e.message : String(e));
    return [];
  }
}

function pickBestFlashModel(available) {
  const list = (available || []).map(normalizeModelName);
  const by2Flash = list.find((n) => n.includes('gemini-2.0-flash')) || '';
  if (by2Flash) return by2Flash;
  const byFlashLatest = list.find((n) => n.includes('gemini-flash-latest') || n.includes('flash-latest')) || '';
  if (byFlashLatest) return byFlashLatest;
  const by15Flash = list.find((n) => n.includes('gemini-1.5-flash') || n.includes('1.5-flash')) || '';
  if (by15Flash) return by15Flash;
  const anyFlash = list.find((n) => n.includes('flash')) || '';
  return anyFlash || (list[0] || '');
}

function buildPriorityFromAvailable(available) {
  const list = (available || []).map(normalizeModelName);
  const best = pickBestFlashModel(list);
  const flashLatest = list.find((n) => n.includes('gemini-flash-latest') || n.includes('flash-latest')) || '';
  const f20 = list.find((n) => n.includes('gemini-2.0-flash')) || '';
  const f15 = list.find((n) => n.includes('gemini-1.5-flash')) || '';
  const proLatest = list.find((n) => n.includes('gemini-pro-latest') || n.endsWith('pro-latest')) || '';
  const dedup = (arr) => Array.from(new Set(arr.map(normalizeModelName).filter(Boolean)));
  return dedup([best, f20, flashLatest, f15, proLatest]);
}

async function generateContentV1({ modelName, prompt }) {
  if (!GEMINI_API_KEY) throw new Error('GEMINI_API_KEY is not configured');
  const cleanModel = normalizeModelName(modelName);
  const url = `${GEMINI_V1_GENERATE_URL}/${encodeURIComponent(cleanModel)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
  const body = {
    contents: [{ role: 'user', parts: [{ text: String(prompt || '') }] }],
  };
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) {
    if (resp.status === 404) {
      // eslint-disable-next-line no-console
      console.error('[gemini][404] generateContent URL:', url);
    }
    const err = new Error(text || `Gemini generateContent failed (HTTP ${resp.status})`);
    err.status = resp.status;
    throw err;
  }
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    return { text: String(text || '') };
  }
  const out =
    json?.candidates?.[0]?.content?.parts?.map((p) => p?.text).filter(Boolean).join('') ||
    json?.candidates?.[0]?.content?.parts?.[0]?.text ||
    '';
  return { text: String(out || '') };
}

function sanitizeSqlForAi(sqlText) {
  let s = String(sqlText || '');
  // 移除常见“表属性杂质”，降低 token 与模型负载（保留字段与 COMMENT）
  s = s.replace(/^\s*ENGINE\s*=\s*.*$/gmi, '');
  s = s.replace(/^\s*ROW\s+FORMAT\s+.*$/gmi, '');
  s = s.replace(/^\s*STORED\s+AS\s+.*$/gmi, '');
  s = s.replace(/^\s*LOCATION\s+.*$/gmi, '');
  s = s.replace(/^\s*TBLPROPERTIES\s*\([\s\S]*?\)\s*;?\s*$/gmi, '');
  s = s.replace(/^\s*PROPERTIES\s*\([\s\S]*?\)\s*;?\s*$/gmi, '');
  // 去掉列定义结束后的一长串 options（只保留到第一个右括号为止）
  s = s.replace(/\)\s*ENGINE[\s\S]*?;(\s*)$/gmi, ');\n');
  s = s.replace(/\)\s*COMMENT\s+['"][\s\S]*?['"]\s*;(\s*)$/gmi, ');\n');
  // 连续空行压缩
  s = s.replace(/\n{3,}/g, '\n\n');
  return s.trim();
}

function parseSingleTableLocal(ddlText) {
  const ddl = String(ddlText || '');
  const tableName = extractTableNameFromDdl(ddl) || '';
  const columns = [];
  const columnCommentRe = /^\s*([`"]?)([a-zA-Z_][\w]*)\1\s+[^,]*?\bcomment\b\s+(['"])(.*?)\3/i;
  const columnRe = /^\s*([`"]?)([a-zA-Z_][\w]*)\1\s+[\w]+/i;

  // 仅解析括号内字段定义，避免把 CREATE/ TABLE 误当字段
  const start = ddl.indexOf('(');
  const end = ddl.lastIndexOf(')');
  const body = start >= 0 && end > start ? ddl.slice(start + 1, end) : ddl;

  // 按逗号切分（简单版，足以覆盖常见 DDL）
  const parts = body.split(',').map((p) => p.trim()).filter(Boolean);
  for (const part of parts) {
    if (!part) continue;
    if (/^(primary|unique|key|index|constraint)\b/i.test(part)) continue;
    const mCC = columnCommentRe.exec(part);
    if (mCC) {
      columns.push({ name: mCC[2], comment: (mCC[4] || '').trim() });
      continue;
    }
    const mC = columnRe.exec(part);
    if (mC) columns.push({ name: mC[2], comment: '' });
  }

  // 去重：comment 优先
  const byName = new Map();
  for (const c of columns) {
    const k = String(c.name || '').toLowerCase();
    const prev = byName.get(k);
    if (!prev) byName.set(k, c);
    else if (!prev.comment && c.comment) byName.set(k, c);
  }

  return { tableName: tableName || '', columns: Array.from(byName.values()).filter((c) => c?.name) };
}

function stripJsonFences(text) {
  const s = String(text || '').trim();
  return s
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/```$/i, '')
    .trim();
}

function normalizeLower(s) {
  return String(s || '').trim().toLowerCase();
}

function heuristicInferSuggestions({ tables }) {
  const outSuggestions = [];
  const outJoinPaths = [];

  const all = (tables || []).map((t) => ({
    tableName: String(t?.tableName || ''),
    tableKey: normalizeLower(t?.tableName || ''),
    columns: Array.isArray(t?.columns) ? t.columns : [],
  }));

  const findTableBy = (pred) => all.find(pred);
  const findColIn = (t, pred) => (t?.columns || []).find(pred);

  const product =
    findTableBy((t) => t.columns.some((c) => normalizeLower(c?.name).includes('style') || normalizeLower(c?.comment).includes('款号'))) ||
    findTableBy((t) => t.tableKey.includes('product')) ||
    all[0];

  const productName = product?.tableName || '';
  const styleCol =
    findColIn(product, (c) => normalizeLower(c?.name).includes('style') || normalizeLower(c?.comment).includes('款号'))?.name ||
    '';
  if (productName && styleCol) {
    outSuggestions.push({ standardKey: 'styleCode', sourceTable: productName, sourceField: styleCol });
  }

  const brandCol = findColIn(product, (c) => normalizeLower(c?.name).includes('brand') || normalizeLower(c?.comment).includes('品牌'))?.name || '';
  if (productName && brandCol) outSuggestions.push({ standardKey: 'brand', sourceTable: productName, sourceField: brandCol });

  const statusCol = findColIn(product, (c) => normalizeLower(c?.name).includes('status') || normalizeLower(c?.comment).includes('状态'))?.name || '';
  if (productName && statusCol) outSuggestions.push({ standardKey: 'status', sourceTable: productName, sourceField: statusCol });

  const baseLast =
    findTableBy((t) => t.tableKey.includes('last') && t.columns.some((c) => normalizeLower(c?.name) === 'code' || normalizeLower(c?.comment).includes('楦'))) ||
    findTableBy((t) => t.tableKey.includes('last'));
  const baseSole =
    findTableBy((t) => t.tableKey.includes('sole') && t.columns.some((c) => normalizeLower(c?.name) === 'code' || normalizeLower(c?.comment).includes('底'))) ||
    findTableBy((t) => t.tableKey.includes('sole'));
  const baseColor =
    findTableBy((t) => t.tableKey.includes('color') && t.columns.some((c) => normalizeLower(c?.name) === 'code' || normalizeLower(c?.name) === 'name')) ||
    findTableBy((t) => t.tableKey.includes('color'));
  const baseMaterial =
    findTableBy((t) => t.tableKey.includes('material') && t.columns.some((c) => normalizeLower(c?.name) === 'code' || normalizeLower(c?.name) === 'name')) ||
    findTableBy((t) => t.tableKey.includes('material'));

  const baseBrand =
    findTableBy(
      (t) =>
        t.tableKey.includes('brand') &&
        (t.tableKey.includes('base') || t.tableKey.includes('wms')) &&
        t.columns.some((c) => normalizeLower(c?.name) === 'id' || normalizeLower(c?.comment).includes('id'))
    ) || findTableBy((t) => t.tableKey.includes('brand') && t.columns.some((c) => normalizeLower(c?.name) === 'id'));

  const pickIdLike = (kw) =>
    findColIn(product, (c) => normalizeLower(c?.name).includes(kw) && (normalizeLower(c?.name).includes('id') || normalizeLower(c?.comment).includes('id'))) ||
    findColIn(product, (c) => normalizeLower(c?.comment).includes(kw) && normalizeLower(c?.comment).includes('id'));

  const pickCodeLike = (t) =>
    findColIn(t, (c) => normalizeLower(c?.name) === 'code' || normalizeLower(c?.comment).includes('编号')) ||
    findColIn(t, (c) => normalizeLower(c?.name) === 'name') ||
    (t?.columns || [])[0];

  const idCol = (t) => findColIn(t, (c) => normalizeLower(c?.name) === 'id')?.name || 'id';

  const lastId = pickIdLike('last') || pickIdLike('楦') || findColIn(product, (c) => normalizeLower(c?.name).includes('associated_last')) || null;
  if (productName && lastId?.name && baseLast?.tableName) {
    const code = pickCodeLike(baseLast);
    outJoinPaths.push({
      targetStandardKey: 'lastCode',
      path: [`${productName}.${lastId.name}`, `${baseLast.tableName}.${idCol(baseLast)}`, `${baseLast.tableName}.${code?.name || 'code'}`],
    });
  }

  const soleId = pickIdLike('sole') || pickIdLike('底') || null;
  if (productName && soleId?.name && baseSole?.tableName) {
    const code = pickCodeLike(baseSole);
    outJoinPaths.push({
      targetStandardKey: 'soleCode',
      path: [`${productName}.${soleId.name}`, `${baseSole.tableName}.${idCol(baseSole)}`, `${baseSole.tableName}.${code?.name || 'code'}`],
    });
  }

  const colorId = pickIdLike('color') || pickIdLike('颜色') || null;
  if (productName && colorId?.name && baseColor?.tableName) {
    const code = pickCodeLike(baseColor);
    outJoinPaths.push({
      targetStandardKey: 'colorCode',
      path: [`${productName}.${colorId.name}`, `${baseColor.tableName}.${idCol(baseColor)}`, `${baseColor.tableName}.${code?.name || 'code'}`],
    });
  }

  const materialId = pickIdLike('material') || pickIdLike('材质') || null;
  if (productName && materialId?.name && baseMaterial?.tableName) {
    const code = pickCodeLike(baseMaterial);
    outJoinPaths.push({
      targetStandardKey: 'materialCode',
      path: [`${productName}.${materialId.name}`, `${baseMaterial.tableName}.${idCol(baseMaterial)}`, `${baseMaterial.tableName}.${code?.name || 'name'}`],
    });
  }

  if (productName && brandCol && baseBrand?.tableName) {
    const brandNameCol =
      findColIn(baseBrand, (c) => normalizeLower(c?.name) === 'brand_name') ||
      findColIn(baseBrand, (c) => normalizeLower(c?.name) === 'name') ||
      findColIn(baseBrand, (c) => normalizeLower(c?.name).includes('brand') && normalizeLower(c?.name).includes('name')) ||
      findColIn(baseBrand, (c) => normalizeLower(c?.comment).includes('品牌') && !normalizeLower(c?.name).includes('id'));
    const bname = brandNameCol?.name || 'brand_name';
    outJoinPaths.push({
      targetStandardKey: 'brand',
      path: [`${productName}.${brandCol}`, `${baseBrand.tableName}.${idCol(baseBrand)}`, `${baseBrand.tableName}.${bname}`],
    });
  }

  return { smartSuggestions: outSuggestions, joinPathSuggestions: outJoinPaths };
}

async function generateWithModelList({ modelNames, prompt, retries = 3 }) {
  let lastErr = null;
  for (const modelName of modelNames) {
    try {
      setAIStatus({ status: 'running', model: modelName, message: `正在尝试模型: ${modelName}...` });
      // 指数退避：1s,2s,4s（仅对 503/429/500）
      let attemptErr = null;
      for (let attempt = 1; attempt <= retries; attempt++) {
        try {
          const model = genAI.getGenerativeModel({ model: normalizeModelName(modelName) }); // 强制不带 models/ 前缀
          const result = await model.generateContent(prompt);
          const text = result?.response?.text?.() || '';
          return { ok: true, text, modelName };
        } catch (e) {
          attemptErr = e;
          const status = getHttpStatusFromGeminiError(e);
          const is404 = status === 404 || String(e?.message || '').includes('404');
          const transient = isTransientStatus(status);
          if (is404) break; // 404 直接换下一个模型
          if (attempt < retries && transient) {
            const delay = 1000 * Math.pow(2, attempt - 1);
            // eslint-disable-next-line no-console
            console.warn(`[Retry] 模型 ${modelName} 忙碌/异常（HTTP ${status}），${delay / 1000}s 后重试...`);
            await sleepWithCountdown(delay, { modelName, prefix: '[Retry] 模型忙碌，' });
            continue;
          }
          break;
        }
      }
      lastErr = attemptErr;
      const status = getHttpStatusFromGeminiError(attemptErr);
      const is404 = status === 404 || String(attemptErr?.message || '').includes('404');
      const is503 = status === 503 || String(attemptErr?.message || '').includes('503');
      if (is404 || is503) {
        // eslint-disable-next-line no-console
        console.warn(`[Fallback] 模型 ${modelName} 不可用（HTTP ${status}），尝试下一个模型...`);
        continue;
      }
      throw attemptErr;
    } catch (e) {
      lastErr = e;
    }
  }
  return { ok: false, error: lastErr };
}

function getHttpStatusFromGeminiError(error) {
  const maybeStatus =
    error?.status ??
    error?.response?.status ??
    error?.response?.statusCode ??
    error?.cause?.status ??
    error?.cause?.response?.status;
  if (typeof maybeStatus === 'number') return maybeStatus;
  const msg = String(error?.message || '');
  const m = msg.match(/\b(4\d{2}|5\d{2})\b/);
  return m ? Number(m[1]) : null;
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

async function sleepWithCountdown(ms, { modelName, prefix }) {
  const totalSec = Math.max(1, Math.ceil(ms / 1000));
  for (let s = totalSec; s >= 1; s--) {
    setAIStatus({
      status: 'retrying',
      model: modelName,
      message: `${prefix}${s}秒后自动重试...`,
    });
    await sleep(1000);
  }
}

async function generateContentWithFallback({ prompt }) {
  if (!genAI) throw new Error('GEMINI_API_KEY is not configured');
  const models = (GEMINI_FORCED_MODEL ? [GEMINI_FORCED_MODEL] : MODEL_PRIORITY).map(normalizeModelName);
  let lastError = null;

  setAIStatus({ status: 'running', model: models[0] || '', message: '开始解析，准备请求模型...' });

  for (let i = 0; i < models.length; i++) {
    const modelName = models[i];
    const nextModel = models[i + 1];
    try {
      setAIStatus({ status: 'running', model: modelName, message: `正在尝试模型: ${modelName}...` });
      const model = getGenerativeModelCompat(modelName);
      const result = await model.generateContent(prompt);
      const text = result?.response?.text?.() || '';
      setAIStatus({ status: 'done', model: modelName, message: '解析完成' });
      return { text, modelName };
    } catch (e) {
      lastError = e;
      const status = getHttpStatusFromGeminiError(e);
      const transient = status === 503 || status === 429 || status === 500;
      const notFound = isModelNotFoundError(e);

      // 503: 指数级退避（先重试当前模型 1 次，再考虑降级）
      if (status === 503) {
        try {
          // eslint-disable-next-line no-console
          console.warn('[Retry] 模型忙碌，2秒后自动重试...', { model: modelName });
          await sleepWithCountdown(2000, { modelName, prefix: '[Retry] 模型忙碌，' });
          const modelRetry = getGenerativeModelCompat(modelName);
          const resultRetry = await modelRetry.generateContent(prompt);
          const textRetry = resultRetry?.response?.text?.() || '';
          setAIStatus({ status: 'done', model: modelName, message: '解析完成' });
          return { text: textRetry, modelName };
        } catch (eRetry) {
          lastError = eRetry;
          // 仍失败则继续走降级逻辑（如果还有下一个模型）
          setAIStatus({ status: 'running', model: modelName, message: `重试失败（503），准备降级到下一个模型...` });
        }
      }

      // 若本次失败是“模型找不到”，再尝试一次带 models/ 前缀（有些 SDK/模型名要求）
      if (notFound) {
        const altName = String(modelName || '').startsWith('models/') ? String(modelName || '').replace(/^models\//, '') : `models/${modelName}`;
        try {
          // eslint-disable-next-line no-console
          console.warn(`[Fallback] 模型 ${modelName} 返回 404，尝试别名 ${altName}...`);
          setAIStatus({ status: 'running', model: modelName, message: `[Fallback] 模型 ${modelName} 返回 404，尝试别名 ${altName}...` });
          const altModel = getGenerativeModelCompat(altName);
          const result = await altModel.generateContent(prompt);
          const text = result?.response?.text?.() || '';
          setAIStatus({ status: 'done', model: altName, message: '解析完成' });
          return { text, modelName: altName };
        } catch (e2) {
          lastError = e2;
        }
      }

      if (notFound && nextModel) {
        // eslint-disable-next-line no-console
        console.warn(`[Fallback] 模型 ${modelName} 返回 404，正在尝试备用模型 ${nextModel}...`);
        setAIStatus({ status: 'fallback', model: modelName, message: `[Fallback] 模型 ${modelName} 返回 404，正在尝试备用模型 ${nextModel}...` });
        await sleep(50);
        continue;
      }

      if (transient && nextModel) {
        // eslint-disable-next-line no-console
        console.warn(`[Fallback] 模型 ${modelName} 忙碌/异常（HTTP ${status}），正在尝试模型 ${nextModel}...`);
        setAIStatus({ status: 'fallback', model: modelName, message: `[Fallback] 模型 ${modelName} 忙碌/异常（HTTP ${status}），正在尝试模型 ${nextModel}...` });
        // 小间隔，避免瞬时抖动造成连续失败
        await sleep(200);
        continue;
      }

      // 非可降级错误，或者已经无模型可用：直接抛出
      setAIStatus({ status: 'error', model: modelName, message: `解析失败（HTTP ${status || 'unknown'}）：${String(e?.message || '')}` });
      throw e;
    }
  }

  setAIStatus({ status: 'error', model: '', message: '所有模型均失败' });
  throw lastError || new Error('All Gemini models failed');
}

async function listGeminiModels() {
  if (!GEMINI_API_KEY) return null;
  try {
    const resp = await fetch(`${GEMINI_DISCOVERY_URL}?key=${encodeURIComponent(GEMINI_API_KEY)}`);
    const text = await resp.text();
    try {
      return { status: resp.status, json: JSON.parse(text) };
    } catch {
      return { status: resp.status, text };
    }
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function listAvailableModels() {
  // Prefer v1; fallback to v1beta if needed
  const v1 = await listAvailableModelsV1();
  if (v1.length) return v1.map((n) => `models/${normalizeModelName(n)}`);
  if (!GEMINI_API_KEY) return [];
  try {
    const resp = await fetch(`${GEMINI_DISCOVERY_URL}?key=${encodeURIComponent(GEMINI_API_KEY)}`);
    const json = await resp.json().catch(() => null);
    const models = Array.isArray(json?.models) ? json.models : [];
    const usable = models
      .filter((m) => Array.isArray(m?.supportedGenerationMethods) && m.supportedGenerationMethods.includes('generateContent'))
      .map((m) => `models/${normalizeModelName(m?.name)}`)
      .filter(Boolean);
    return usable;
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[models] listAvailableModels failed', e instanceof Error ? e.message : String(e));
    return [];
  }
}

// NOTE: 模型优先级已锁定，不再在启动时自动改写，以避免线上抖动与不可控切换

async function ensureStorageDirs() {
  await fse.ensureDir(DIRS.dataTables);
  await fse.ensureDir(DIRS.lasts);
  await fse.ensureDir(DIRS.soles);
  await fse.ensureDir(DIRS.sandbox);
}

function extractJsonArray(text) {
  if (!text) return null;
  const trimmed = stripJsonFences(String(text));
  try {
    const v = JSON.parse(trimmed);
    if (Array.isArray(v)) return v;
  } catch {
    // ignore
  }
  const m = /\[[\s\S]*\]/.exec(trimmed);
  if (!m) return null;
  try {
    const v = JSON.parse(m[0]);
    if (Array.isArray(v)) return v;
  } catch {
    return null;
  }
  return null;
}

function extractJsonObject(text) {
  if (!text) return null;
  const trimmed = stripJsonFences(String(text));
  try {
    const v = JSON.parse(trimmed);
    if (v && typeof v === 'object' && !Array.isArray(v)) return v;
  } catch {
    // ignore
  }
  const m = /\{[\s\S]*\}/.exec(trimmed);
  if (!m) return null;
  try {
    const v = JSON.parse(m[0]);
    if (v && typeof v === 'object' && !Array.isArray(v)) return v;
  } catch {
    return null;
  }
  return null;
}

function splitCreateTableStatements(sqlText) {
  const src = String(sqlText || '');
  const re = /create\s+table\b[\s\S]*?(?:;\s*|$)/gi;
  const out = [];
  let m;
  while ((m = re.exec(src)) !== null) {
    const chunk = String(m[0] || '').trim();
    if (chunk) out.push(chunk);
  }
  return out;
}

function extractTableNameFromDdl(ddlText) {
  const m = /create\s+table\s+([`"]?)([\w.]+)\1/i.exec(String(ddlText || ''));
  return m?.[2] ? String(m[2]).trim() : '';
}

function isTransientStatus(status) {
  return status === 503 || status === 429 || status === 500;
}

// NOTE: 单表解析重试已并入 generateWithModelList（按模型列表 + 1/2/4s 退避）
function isDateDirName(name) {
  return /^\d{4}-\d{2}-\d{2}$/.test(name);
}

async function getLatestDataTablesDir() {
  await ensureStorageDirs();
  const entries = await fse.readdir(DIRS.dataTables, { withFileTypes: true });
  const dateDirs = entries
    .filter((e) => e.isDirectory() && isDateDirName(e.name))
    .map((e) => e.name)
    .sort();

  if (dateDirs.length === 0) return DIRS.dataTables;
  return path.join(DIRS.dataTables, dateDirs[dateDirs.length - 1]);
}

function isExcelFileName(name) {
  const ext = path.extname(name).toLowerCase();
  return ext === '.xlsx' || ext === '.xls';
}

function readFirstSheet(workbook) {
  const sheetName = workbook.SheetNames?.[0];
  if (!sheetName) return null;
  return workbook.Sheets[sheetName] || null;
}

function normalizeHeaderCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v).trim();
  return s;
}

function getSheetHeaders(sheet) {
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: '' });
  const headerRow = Array.isArray(rows?.[0]) ? rows[0] : [];
  return headerRow.map(normalizeHeaderCell).filter(Boolean);
}

function getSheetColumnSamples(sheet, headers, sampleCount = 3) {
  // 使用 range:1 跳过第一行表头
  const data = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: 1 });
  const firstN = data.slice(0, sampleCount);

  const samples = {};
  for (const h of headers) samples[h] = [];
  for (const row of firstN) {
    for (const h of headers) {
      samples[h].push(row?.[h] ?? '');
    }
  }
  return samples;
}

async function listFileNames(dirPath) {
  try {
    const entries = await fse.readdir(dirPath, { withFileTypes: true });
    return entries.filter((e) => e.isFile()).map((e) => e.name).sort();
  } catch {
    return [];
  }
}

let cachedAssets = { lasts: [], soles: [], files: [] };

async function refreshAssetsCache() {
  const [lasts, soles] = await Promise.all([listFileNames(DIRS.lasts), listFileNames(DIRS.soles)]);
  cachedAssets = {
    lasts,
    soles,
    files: [...lasts, ...soles],
  };
  return cachedAssets;
}

const upload = multer({
  storage: multer.diskStorage({
    destination: async (_req, file, cb) => {
      try {
        const ext = path.extname(file.originalname).toLowerCase();
        const baseName = path.basename(file.originalname);

        const xlsxExts = new Set(['.xlsx', '.xls']);
        const lastExts = new Set(['.obj', '.stl', '.3dm']);
        const soleExts = new Set(['.obj', '.stl', '.3dm']);

        if (xlsxExts.has(ext)) {
          const m = /^(\d{4})(\d{2})(\d{2})_/.exec(baseName);
          if (m) {
            const dateDir = `${m[1]}-${m[2]}-${m[3]}`;
            const targetDir = path.join(DIRS.dataTables, dateDir);
            await fse.ensureDir(targetDir);
            return cb(null, targetDir);
          }
          return cb(null, DIRS.dataTables);
        }

        // PoC：last/sole 均允许 .obj/.stl/.3dm，先按命名规则粗分，否则归档到 lasts
        // 命名包含 "SOLE" 认为是 soles；包含 "LST" 认为是 lasts；都不包含则默认 lasts
        if (lastExts.has(ext) || soleExts.has(ext)) {
          const upper = baseName.toUpperCase();
          if (upper.includes('SOLE')) return cb(null, DIRS.soles);
          if (upper.includes('LST') || upper.includes('LAST')) return cb(null, DIRS.lasts);
          return cb(null, DIRS.lasts);
        }

        return cb(new Error(`Unsupported file extension: ${ext}`), null);
      } catch (e) {
        return cb(e, null);
      }
    },
    filename: (_req, file, cb) => {
      cb(null, file.originalname);
    },
  }),
  limits: { fileSize: 1024 * 1024 * 1024 }, // 1GB (quick PoC)
});

const sandboxUpload = multer({
  storage: multer.diskStorage({
    destination: async (_req, _file, cb) => {
      try {
        await fse.ensureDir(DIRS.sandbox);
        cb(null, DIRS.sandbox);
      } catch (e) {
        cb(e, null);
      }
    },
    filename: (_req, file, cb) => {
      const safe = String(file.originalname || 'sample.xlsx').replace(/[^\w.\-]+/g, '_');
      cb(null, `${Date.now()}_${safe}`);
    },
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
});

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

// 本地快速解析：从多表 DDL 中提取表名 + 字段名列表（供前端动态样本配置下拉框使用）
app.post('/api/parse-ddl-schema', async (req, res) => {
  const sqlText = String(req.body?.sqlText || '');
  if (!sqlText.trim()) return res.status(400).json({ ok: false, error: 'sqlText is required' });
  try {
    // 快速预处理：先用正则切出 CREATE TABLE 片段，再本地解析字段（不依赖 AI）
    const ddls = splitCreateTableStatements(sqlText);
    if (!ddls.length) {
      // eslint-disable-next-line no-console
      console.warn('[Parser] 警告：未匹配到 CREATE TABLE 语句（可能 SQL 格式不规范）');
      return res.json({ ok: true, tableMap: {}, tables: [] });
    }
    const tables = ddls
      .map((ddl, idx) => {
        const parsed = parseSingleTableLocal(ddl);
        if (!parsed?.tableName) parsed.tableName = extractTableNameFromDdl(ddl) || `table_${idx + 1}`;
        return parsed;
      })
      .filter((t) => t?.tableName);
    if (tables.length < ddls.length) {
      // eslint-disable-next-line no-console
      console.warn('[Parser] 警告：部分表结构不规范（表名未解析或语句异常）', { total: ddls.length, parsed: tables.length });
    }
    const tableMap = {};
    for (const t of tables) {
      const tn = String(t?.tableName || '').trim();
      if (!tn) continue;
      tableMap[tn] = Array.isArray(t?.columns)
        ? t.columns
            .map((c) => ({
              name: typeof c?.name === 'string' ? c.name.trim() : '',
              comment: typeof c?.comment === 'string' ? c.comment.trim() : '',
            }))
            .filter((c) => c.name)
        : [];
      if (tableMap[tn].length === 0) {
        // eslint-disable-next-line no-console
        console.warn('[Parser] 警告：表字段为空或不规范', { table: tn });
      }
    }
    return res.json({ ok: true, tableMap, tables });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'parse failed' });
  }
});

// 调试：列出当前 API Key 可用模型
app.get('/api/debug/gemini-models', async (_req, res) => {
  const out = await listGeminiModels();
  res.json({ ok: true, out });
});

// 诊断：列出当前 API Key 可用、且支持 generateContent 的模型清单
app.get('/api/list-available-models', async (_req, res) => {
  const models = await listAvailableModels();
  return res.json(models);
});

app.get('/api/ai-status', async (_req, res) => {
  return res.json({ ok: true, ...currentAIStatus });
});

// 启动时扫描 + 对外暴露扫描结果
app.get('/api/assets', async (_req, res) => {
  // 为了 PoC 简化：请求时也刷新一次，确保上传后能立刻看到
  const assets = await refreshAssetsCache();
  res.json(assets);
});

// 扫描最新日期目录下的 XLSX，并读取首行表头
app.get('/api/table-headers', async (_req, res) => {
  const latestDir = await getLatestDataTablesDir();
  const entries = await fse.readdir(latestDir, { withFileTypes: true });
  const files = entries.filter((e) => e.isFile() && isExcelFileName(e.name)).map((e) => e.name).sort();

  const out = {};
  for (const fileName of files) {
    try {
      const fullPath = path.join(latestDir, fileName);
      const wb = XLSX.readFile(fullPath, { cellText: false, cellDates: true });
      const sheet = readFirstSheet(wb);
      if (!sheet) {
        out[fileName] = [];
        continue;
      }
      out[fileName] = getSheetHeaders(sheet);
    } catch {
      out[fileName] = [];
    }
  }

  res.json({ ok: true, latestDir: path.basename(latestDir), tables: out });
});

// 返回指定文件的列抽样（每列前 3 条），用于前端映射校验
app.get('/api/table-samples', async (req, res) => {
  const fileName = String(req.query.fileName || '');
  if (!fileName || fileName.includes('/') || fileName.includes('\\')) {
    return res.status(400).json({ ok: false, error: 'Invalid fileName' });
  }

  const latestDir = await getLatestDataTablesDir();
  const fullPath = path.join(latestDir, fileName);
  if (!(await fse.pathExists(fullPath))) {
    return res.status(404).json({ ok: false, error: 'File not found' });
  }
  if (!isExcelFileName(fileName)) {
    return res.status(400).json({ ok: false, error: 'Not an Excel file' });
  }

  const wb = XLSX.readFile(fullPath, { cellText: false, cellDates: true });
  const sheet = readFirstSheet(wb);
  if (!sheet) return res.json({ ok: true, fileName, headers: [], samples: {} });
  const headers = getSheetHeaders(sheet);
  const samples = getSheetColumnSamples(sheet, headers, 3);
  return res.json({ ok: true, fileName, headers, samples });
});

// 预览：根据当前 mapping（结构化 JoinPath + resolveRecursiveValue）从最新快照目录 XLSX 抽取一行真实数据
app.post('/api/preview-mapping-row', async (req, res) => {
  await ensureStorageDirs();
  try {
    const mappingArr = Array.isArray(req.body?.mapping) ? req.body.mapping : null;
    if (!mappingArr?.length) return res.status(400).json({ ok: false, error: 'mapping array required' });
    const latestDir = await getLatestDataTablesDir();
    if (!(await fse.pathExists(latestDir))) {
      return res.status(404).json({
        ok: false,
        error: 'Latest data_tables directory not found',
        code: 'DataDirMissing',
      });
    }
    const preload = await readAllFilesFromFolder(latestDir);
    if (!preload.xlsxTables?.length) {
      return res.status(404).json({
        ok: false,
        error: 'No Excel files in latest data_tables snapshot',
        code: 'NoExcelInDataDir',
      });
    }
    const standardMap = buildStandardMap(mappingArr);
    const resolved = await resolveFirstActiveRowFromFolder({
      folderPath: latestDir,
      standardMap,
      traceJoins: true,
      preload,
      previewStrict: true,
    });
    if (!resolved.ok) {
      return res.status(404).json({
        ok: false,
        error: resolved.error || 'resolve failed',
        code: 'PreviewResolveFailed',
      });
    }
    return res.json({
      ok: true,
      latestDir: path.basename(latestDir),
      mainTable: resolved.mainTable,
      usedFallbackRow: resolved.usedFallbackRow,
      warning: resolved.warning,
      row: resolved.row || {},
      previewFieldErrors: resolved.previewFieldErrors,
      engine: 'resolveRecursiveValue',
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'preview failed' });
  }
});

// Gemini AI 解析 DDL：仅返回 JSON 数组 [{"fieldName","comment"}]
app.post('/api/ai-parse-ddl', async (req, res) => {
  const sqlText = String(req.body?.sqlText || '');
  if (!sqlText.trim()) return res.status(400).json({ ok: false, error: 'sqlText is required' });
  if (!genAI) return res.status(500).json({ ok: false, error: 'GEMINI_API_KEY is not configured' });

  try {
    clearEngineLogSync();
    setAIStatus({ status: 'running', model: '', message: '开始 AI 解析 DDL...' });
    const prompt = [
      '你是一个 SQL 专家。',
      '请解析以下 DDL 语句，并仅以 JSON 数组格式返回字段信息。',
      '格式必须为：[{"fieldName":"xxx","comment":"xxx"}]。',
      '不要包含任何 Markdown 代码块标签或解释文字。',
      '',
      'DDL:',
      sqlText,
    ].join('\n');

    let text = '';
    try {
      const out = await generateContentWithFallback({ prompt });
      text = out.text;
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[gemini] ai-parse-ddl primary failed', {
        message: error?.message,
        status: error?.status,
        code: error?.code,
        response: error?.response,
        responseText: error?.response?.text,
        responseData: error?.response?.data,
      });
      throw error;
    }

    const arr = extractJsonArray(text);
    if (!arr) return res.status(500).json({ ok: false, error: 'AI output is not a valid JSON array' });

    const fields = arr
      .map((x) => ({
        fieldName: typeof x?.fieldName === 'string' ? x.fieldName.trim() : '',
        comment: typeof x?.comment === 'string' ? x.comment.trim() : '',
      }))
      .filter((x) => x.fieldName);

    setAIStatus({ status: 'done', model: currentAIStatus.model, message: 'DDL 解析完成' });
    return res.json({ ok: true, fields });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[gemini] ai-parse-ddl failed', {
      message: e?.message,
      status: e?.status,
      code: e?.code,
      response: e?.response,
      responseText: e?.response?.text,
      responseData: e?.response?.data,
    });
    setAIStatus({ status: 'error', model: currentAIStatus.model, message: e instanceof Error ? e.message : 'AI parse failed' });
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'AI parse failed' });
  }
});

// Chat-to-Config：将“业务场景描述”解析为 Step2 dimensionSamples 结构（不猜表字段，可留空供前端基于 DDL 自动补全）
app.post('/api/parse-chat-to-samples', async (req, res) => {
  const text = String(req.body?.text || req.body?.chat || '').trim();
  const masterTable = typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '';
  const tableNames = Array.isArray(req.body?.ddlTableNames) ? req.body.ddlTableNames.map((x) => String(x || '').trim()).filter(Boolean) : [];
  const ddlText = String(req.body?.ddlText || '').trim();
  if (!text) return res.status(400).json({ ok: false, error: 'text is required' });
  if (!genAI) return res.status(500).json({ ok: false, error: 'GEMINI_API_KEY is not configured' });

  const emptyDim = () => ({ targetGoal: '', rows: [] });
  const makeSegmentsRow = (value, notes = '') => ({
    id: `chat_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    notes,
    segments: [{ tableName: '', fieldName: '', value: String(value ?? '') }],
  });
  const localFallback = (raw) => {
    const s = String(raw || '');
    const out = {
      styleCode: emptyDim(),
      brand: emptyDim(),
      lastCode: emptyDim(),
      soleCode: emptyDim(),
      colorCode: emptyDim(),
      materialCode: emptyDim(),
      status: emptyDim(),
    };

    // styleCode: 常见形态 SBOX26008M / 任意大写字母+数字+字母
    const style = s.match(/\b[A-Z]{2,10}\d{3,10}[A-Z]?\b/);
    if (style) {
      out.styleCode.rows.push(makeSegmentsRow(style[0], '从描述中提取：款号/StyleCode'));
      out.styleCode.targetGoal = style[0];
    }

    // lastCode: L-xxxx（示例：L-B26097M）
    const last = s.match(/\bL-[A-Za-z0-9-]{3,30}\b/);
    if (last) {
      out.lastCode.rows.push(makeSegmentsRow(last[0], '从描述中提取：楦号/lastCode'));
      out.lastCode.targetGoal = last[0];
    }

    // soleCode: S-xxxx / SOLE- / H- (粗略)
    const sole = s.match(/\b(?:S-|SOLE-|H-)[A-Za-z0-9-]{3,30}\b/);
    if (sole) {
      out.soleCode.rows.push(makeSegmentsRow(sole[0], '从描述中提取：底号/soleCode'));
      out.soleCode.targetGoal = sole[0];
    }

    // status: 生效/有效/active
    if (/(生效|有效|active|enabled)/i.test(s)) {
      const v = /(生效|有效)/.test(s) ? '生效' : 'active';
      out.status.rows.push(makeSegmentsRow(v, '从描述中提取：状态'));
      out.status.targetGoal = v;
    }

    // materialCode: LT04 或 LT + 04 拼接
    const matDirect = s.match(/\bLT\d{2}\b/i);
    if (matDirect) {
      const v = matDirect[0].toUpperCase();
      out.materialCode.rows.push(makeSegmentsRow(v, '从描述中提取：材质编码'));
      out.materialCode.targetGoal = v;
    } else {
      const matParts = s.match(/\b(LT)\b[\s\S]{0,12}\b(0?\d{1,2})\b/i);
      if (matParts) {
        const p1 = String(matParts[1]).toUpperCase();
        const p2 = String(matParts[2]).padStart(2, '0');
        out.materialCode.rows.push(makeSegmentsRow(p1, '拼接段1：父/前缀（例如 LT）'));
        out.materialCode.rows.push(makeSegmentsRow(p2, '拼接段2：子/后缀（例如 04）'));
        out.materialCode.targetGoal = `${p1}${p2}`;
      }
    }

    // brand: 提取“品牌 X/brand X”后的文本（尽量保守，避免误判）
    const brand =
      s.match(/品牌[:：\s]*([A-Za-z][A-Za-z\s]{1,40})/i) ||
      s.match(/\bbrand[:：\s]*([A-Za-z][A-Za-z\s]{1,40})/i);
    if (brand && brand[1]) {
      const v = String(brand[1]).trim().replace(/\s+/g, ' ');
      out.brand.rows.push(makeSegmentsRow(v, '从描述中提取：品牌名称'));
      out.brand.targetGoal = v;
    }

    // colorCode: 简单提取“颜色 X/color X”
    const color =
      s.match(/颜色[:：\s]*([A-Za-z0-9-]{2,20})/i) ||
      s.match(/\bcolor[:：\s]*([A-Za-z0-9-]{2,20})/i);
    if (color && color[1]) {
      const v = String(color[1]).trim();
      out.colorCode.rows.push(makeSegmentsRow(v, '从描述中提取：颜色编码/名称'));
      out.colorCode.targetGoal = v;
    }

    return out;
  };

  const ddlTables = (() => {
    try {
      if (!ddlText) return [];
      const ddls = splitCreateTableStatements(ddlText);
      return ddls.map((ddl, idx) => {
        const t = parseSingleTableLocal(ddl);
        if (!t.tableName) t.tableName = extractTableNameFromDdl(ddl) || `table_${idx + 1}`;
        return t;
      });
    } catch {
      return [];
    }
  })();

  const ddlTableMap = (() => {
    const m = new Map();
    for (const t of ddlTables) {
      const tn = String(t?.tableName || '').trim();
      if (!tn) continue;
      const cols = Array.isArray(t?.columns) ? t.columns : [];
      m.set(
        tn,
        cols
          .map((c) => ({ name: String(c?.name || '').trim(), comment: String(c?.comment || '') }))
          .filter((c) => c.name)
      );
    }
    return m;
  })();

  const findTableContainingField = (fieldName) => {
    const want = String(fieldName || '').trim().toLowerCase();
    if (!want) return '';
    for (const [tn, cols] of ddlTableMap) {
      if (cols.some((c) => String(c.name || '').trim().toLowerCase() === want)) return tn;
    }
    return '';
  };

  const suggestFieldInTable = (tableName, standardKey) => {
    const tn = String(tableName || '').trim();
    const cols = ddlTableMap.get(tn) || [];
    const kw =
      standardKey === 'styleCode'
        ? ['style', 'style_wms', '款号', 'style_no']
        : standardKey === 'brand'
          ? ['brand_name', 'brand', '品牌', 'name']
          : standardKey === 'lastCode'
            ? ['last', '楦', 'code', 'last_code']
            : standardKey === 'soleCode'
              ? ['sole', '底', 'heel', 'code', 'sole_code']
              : standardKey === 'colorCode'
                ? ['color', '颜色', 'colour', 'code', 'color_code']
                : standardKey === 'materialCode'
                  ? ['material', '材质', 'category', 'code', 'name', 'material_code', 'category_code']
                  : standardKey === 'status'
                    ? ['status', '状态', 'data_status', '生效', '有效']
                    : [];
    let best = '';
    let bestScore = 0;
    for (const c of cols) {
      const hay = `${c.name || ''} ${c.comment || ''}`.toLowerCase();
      let score = 0;
      for (const k of kw) if (hay.includes(String(k).toLowerCase())) score += 10;
      if (standardKey.endsWith('Code') && String(c.name || '').toLowerCase() === 'code') score += 8;
      if (standardKey === 'brand' && String(c.name || '').toLowerCase() === 'brand_name') score += 12;
      if (score > bestScore) {
        bestScore = score;
        best = c.name;
      }
    }
    return best || (cols[0]?.name || '');
  };

  const fillCoordinatesFromDdl = (dimensionSamples) => {
    const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
    const explicit = [];
    const refRe = /\b([\w.]+)\.([A-Za-z_][A-Za-z0-9_]*)\b/g;
    let m;
    while ((m = refRe.exec(text)) !== null) {
      const t = String(m[1] || '').trim();
      const f = String(m[2] || '').trim();
      if (t && f) explicit.push({ tableName: t, fieldName: f });
    }
    const explicitSet = new Set(explicit.map((x) => `${x.tableName}.${x.fieldName}`));

    for (const k of keys) {
      const blk = dimensionSamples[k];
      if (!blk || !Array.isArray(blk.rows)) continue;
      for (const r of blk.rows) {
        const segs = Array.isArray(r?.segments) ? r.segments : [];
        for (const s of segs) {
          const curT = String(s.tableName || '').trim();
          const curF = String(s.fieldName || '').trim();
          if (curT && curF) continue;

          // 若用户明确写了 table.field，优先信任（按 DDL 验证存在则用）
          const pick = explicit.find((x) => {
            if (!x.tableName || !x.fieldName) return false;
            if (!ddlTableMap.size) return true;
            const cols = ddlTableMap.get(x.tableName);
            return cols ? cols.some((c) => String(c.name).toLowerCase() === x.fieldName.toLowerCase()) : false;
          });
          if (pick && !explicitSet.has(`${pick.tableName}.${pick.fieldName}`)) {
            // keep
          }
          if (pick) {
            s.tableName = pick.tableName;
            s.fieldName = pick.fieldName;
            continue;
          }

          // 若只知道 fieldName：在 DDL 中找表
          if (!curT && curF) {
            const tFound = findTableContainingField(curF);
            if (tFound) {
              s.tableName = tFound;
              continue;
            }
          }

          // 维度启发式：优先 masterTable，否则扫全库选最可能字段
          if (!curT) {
            const tPref = masterTable && ddlTableMap.has(masterTable) ? masterTable : (ddlTables[0]?.tableName || '');
            if (tPref) s.tableName = tPref;
          }
          if (s.tableName && !curF) {
            s.fieldName = suggestFieldInTable(s.tableName, k);
          }
        }
      }
    }
    return dimensionSamples;
  };

  try {
    clearEngineAuditLogSync();
    auditLineSync(`[Audit] ===== Chat-to-Config 开始 ${new Date().toISOString()} =====`);
    auditLineSync('[Audit][A] Input: conversational text');
    auditLineSync(text);

    // Chat-to-Config 优先使用 flash，降低 503 概率；失败则本地兜底
    const modelList = GEMINI_FORCED_MODEL
      ? [normalizeModelName(GEMINI_FORCED_MODEL)]
      : ['gemini-1.5-flash', 'gemini-2.0-flash', 'gemini-pro-latest'].map(normalizeModelName);
    const prompt = [
      '你是一个专业的 ETL 配置员。你的任务：把用户的大白话业务案例，提取为“动态黄金样本配置（dimensionSamples）”。',
      '',
      '标准维度（standardKey）固定为以下 7 个：',
      '- styleCode (款号)',
      '- brand (品牌)',
      '- lastCode (楦)',
      '- soleCode (底)',
      '- colorCode (颜色)',
      '- materialCode (材质)',
      '- status (状态)',
      '',
      '输出必须是严格 JSON 对象（不要 Markdown，不要解释），结构必须与前端 dimensionSamples 状态一致：',
      '{',
      '  "styleCode": { "targetGoal": "", "rows": [ { "id": "可省略", "notes": "", "segments": [ { "tableName": "", "fieldName": "", "value": "" } ] } ] },',
      '  "brand": { "targetGoal": "", "rows": [ ... ] },',
      '  ... 其余维度 ...',
      '}',
      '',
      '规则：',
      '1) 你可以把用户描述中的“期望结果”写入 targetGoal（例如材质 LT04）。',
      '2) rows 表示该维度的“样本行”。每行可带 notes（逻辑备注/线索）。',
      '3) segments 表示该行的“物理取值分段”。',
      '4) 【特别注意复合逻辑】如果用户提到“拼接/合成/父子/由X和Y组成”，请在同一维度下返回多行 rows（每一行对应一个分段），并在 notes 中清晰写明“父/子/拼接顺序”。',
      '5) tableName 与 fieldName 若用户未明确指出，必须留空字符串（不要胡编表名字段名）。前端会基于 DDL 自动检索补全。',
      '',
      masterTable ? `业务主表（供参考，不要臆造字段）：${masterTable}` : '',
      tableNames.length ? `DDL 表名清单（供参考，不要臆造）：${tableNames.join(', ')}` : '',
      '',
      '用户描述：',
      text,
    ]
      .filter(Boolean)
      .join('\n');

    auditLineSync('[Audit][A] Prompt: parse-chat-to-samples');
    auditLineSync(prompt);

    const out = await generateWithModelList({ modelNames: modelList, prompt, retries: 3 });
    auditLineSync('[Audit][B] AI Raw Output: parse-chat-to-samples');
    auditLineSync(out.ok ? String(out.text || '') : `[Audit][B] FAILED: ${String(out.error?.message || '')}`);
    if (!out.ok) {
      auditLineSync('[Audit][Fallback] Gemini failed; use local regex fallback');
      let dimensionSamples = localFallback(text);
      if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
      auditLineSync(JSON.stringify({ fallback: true, dimensionSamples }, null, 2));
      auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
      return res.json({ ok: true, dimensionSamples, fallback: true });
    }

    const obj = extractJsonObject(stripJsonFences(out.text));
    if (!obj || typeof obj !== 'object') {
      auditLineSync('[Audit][Fallback] AI JSON invalid; use local regex fallback');
      let dimensionSamples = localFallback(text);
      if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
      auditLineSync(JSON.stringify({ fallback: true, dimensionSamples }, null, 2));
      auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
      return res.json({ ok: true, dimensionSamples, fallback: true });
    }

    const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
    const normalized = {};
    for (const k of keys) {
      const blk = obj[k] && typeof obj[k] === 'object' ? obj[k] : {};
      const targetGoal = typeof blk?.targetGoal === 'string' ? String(blk.targetGoal) : '';
      const rowsRaw = Array.isArray(blk?.rows) ? blk.rows : [];
      const rows = rowsRaw
        .map((r) => {
          const notes = typeof r?.notes === 'string' ? String(r.notes) : '';
          const id = typeof r?.id === 'string' ? String(r.id) : '';
          const segs = Array.isArray(r?.segments) ? r.segments : [];
          const segments = segs
            .map((s) => ({
              tableName: typeof s?.tableName === 'string' ? String(s.tableName) : '',
              fieldName: typeof s?.fieldName === 'string' ? String(s.fieldName) : '',
              value: typeof s?.value === 'string' ? String(s.value) : String(s?.value ?? ''),
            }))
            .filter((s) => String(s.value || '').trim() !== '' || s.tableName || s.fieldName);
          return { ...(id ? { id } : {}), notes, segments: segments.length ? segments : [{ tableName: '', fieldName: '', value: '' }] };
        })
        .filter((r) => r.segments.some((s) => String(s.value || '').trim() !== ''));
      normalized[k] = { targetGoal, rows };
    }
    if (ddlText) fillCoordinatesFromDdl(normalized);

    auditLineSync(`[Audit] ===== Chat-to-Config 结束 ${new Date().toISOString()} =====`);
    return res.json({ ok: true, dimensionSamples: normalized, fallback: false });
  } catch (e) {
    auditLineSync(`[Audit] ===== Chat-to-Config 异常 ${new Date().toISOString()} =====`);
    auditLineSync(String(e instanceof Error ? e.stack || e.message : String(e)));
    // 兜底：任何异常都走本地解析，保证前端可用
    let dimensionSamples = localFallback(text);
    if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
    auditLineSync('[Audit][Fallback] Exception; use local regex fallback');
    auditLineSync(JSON.stringify({ fallback: true, error: e instanceof Error ? e.message : String(e), dimensionSamples }, null, 2));
    auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
    return res.json({ ok: true, dimensionSamples, fallback: true });
  }
});

// Gemini AI 解析多表 SQL：返回 tables + smartSuggestions
app.post('/api/ai-parse-multi-sql', async (req, res) => {
  const sqlText = String(req.body?.sqlText || '');
  const conversationalInput = String(req.body?.conversationalInput || req.body?.chat || '').trim();
  const reference = req.body?.reference || null;
  const masterTable = typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '';
  const goldenByDimRaw = req.body?.dimensionSamples ?? req.body?.goldenByDimension;
  let goldenBlocks = null;
  let goldenSamples = [];
  if (goldenByDimRaw && typeof goldenByDimRaw === 'object' && !Array.isArray(goldenByDimRaw)) {
    goldenBlocks = normalizeGoldenByDimensionForAi(goldenByDimRaw);
    goldenSamples = expandGoldenBlocksToFlatSamples(goldenBlocks);
  } else {
    goldenSamples = normalizeGoldenSamplesForAi(req.body?.goldenSamples);
  }
  const sampleRow =
    req.body?.sampleRow && typeof req.body.sampleRow === 'object' && !Array.isArray(req.body.sampleRow)
      ? req.body.sampleRow
      : null;
  const goldenExpectedMap = buildGoldenExpectedMap({ goldenBlocks, goldenSamples, reference });
  if (!sqlText.trim()) return res.status(400).json({ ok: false, error: 'sqlText is required' });
  if (!genAI) return res.status(500).json({ ok: false, error: 'GEMINI_API_KEY is not configured' });

  try {
    // 新一轮 AI 建模开始：清空上一轮引擎 Trace 日志，便于用户直接看 engine.log
    clearEngineLogSync();
    clearEngineAuditLogSync();
    clearAiTraceLogSync();
    aiTraceLineSync('[Step 1] 从对话中提取业务实体...');
    auditLineSync(`[Audit] ===== AI 逻辑建模开始 ${new Date().toISOString()} =====`);
    auditLineSync('[Audit][A] Input Audit: /api/ai-parse-multi-sql request body');
    try {
      auditLineSync(
        JSON.stringify(
          {
            masterTable,
            sqlChars: sqlText.length,
            goldenByDimension: goldenByDimRaw,
            goldenSamples: req.body?.goldenSamples,
            reference,
          },
          null,
          2
        )
      );
    } catch {
      auditLineSync('[Audit][A] Input Audit JSON stringify failed');
    }
    // 若用户传了对话文本：先解析对话为 dimensionSamples，并作为建模种子喂给后续推导
    if (conversationalInput) {
      aiTraceLineSync('[Step 1] conversationalInput detected; parsing chat-to-samples');
      try {
        const emptyDim = () => ({ targetGoal: '', rows: [] });
        const makeSegmentsRow = (value, notes = '') => ({
          id: `chat_${Date.now()}_${Math.random().toString(16).slice(2)}`,
          notes,
          segments: [{ tableName: '', fieldName: '', value: String(value ?? '') }],
        });
        const s = String(conversationalInput || '');
        const dimensionSamples = {
          styleCode: emptyDim(),
          brand: emptyDim(),
          lastCode: emptyDim(),
          soleCode: emptyDim(),
          colorCode: emptyDim(),
          materialCode: emptyDim(),
          status: emptyDim(),
        };

        // styleCode: SBOX26008M / 任意大写字母+数字+字母（保守）
        const style = s.match(/\b[A-Z]{2,10}\d{3,10}[A-Z]?\b/);
        if (style) {
          dimensionSamples.styleCode.rows.push(makeSegmentsRow(style[0], '从描述中提取：款号/StyleCode'));
          dimensionSamples.styleCode.targetGoal = style[0];
        }

        // lastCode: L-xxxx（示例：L-B26097M）
        const last = s.match(/\bL-[A-Za-z0-9-]{3,30}\b/);
        if (last) {
          dimensionSamples.lastCode.rows.push(makeSegmentsRow(last[0], '从描述中提取：楦号/lastCode'));
          dimensionSamples.lastCode.targetGoal = last[0];
        }

        // soleCode: S-xxxx / SOLE- / H- (粗略)
        const sole = s.match(/\b(?:S-|SOLE-|H-)[A-Za-z0-9-]{3,30}\b/);
        if (sole) {
          dimensionSamples.soleCode.rows.push(makeSegmentsRow(sole[0], '从描述中提取：底号/soleCode'));
          dimensionSamples.soleCode.targetGoal = sole[0];
        }

        // status: 生效/有效/active
        if (/(生效|有效|active|enabled)/i.test(s)) {
          const v = /(生效|有效)/.test(s) ? '生效' : 'active';
          dimensionSamples.status.rows.push(makeSegmentsRow(v, '从描述中提取：状态'));
          dimensionSamples.status.targetGoal = v;
        }

        // materialCode: LT04 或 LT + 04 拼接
        const matDirect = s.match(/\bLT\d{2}\b/i);
        if (matDirect) {
          const v = matDirect[0].toUpperCase();
          dimensionSamples.materialCode.rows.push(makeSegmentsRow(v, '从描述中提取：材质编码'));
          dimensionSamples.materialCode.targetGoal = v;
        } else {
          const matParts = s.match(/\b(LT)\b[\s\S]{0,12}\b(0?\d{1,2})\b/i);
          if (matParts) {
            const p1 = String(matParts[1]).toUpperCase();
            const p2 = String(matParts[2]).padStart(2, '0');
            dimensionSamples.materialCode.rows.push(makeSegmentsRow(p1, '拼接段1：父/前缀（例如 LT）'));
            dimensionSamples.materialCode.rows.push(makeSegmentsRow(p2, '拼接段2：子/后缀（例如 04）'));
            dimensionSamples.materialCode.targetGoal = `${p1}${p2}`;
          }
        }

        // brand: “品牌 X/brand X”
        const brand =
          s.match(/品牌[:：\s]*([A-Za-z][A-Za-z\s]{1,40})/i) ||
          s.match(/\bbrand[:：\s]*([A-Za-z][A-Za-z\s]{1,40})/i);
        if (brand && brand[1]) {
          const v = String(brand[1]).trim().replace(/\s+/g, ' ');
          dimensionSamples.brand.rows.push(makeSegmentsRow(v, '从描述中提取：品牌名称'));
          dimensionSamples.brand.targetGoal = v;
        }

        // colorCode: “颜色 X/color X”
        const color = s.match(/颜色[:：\s]*([A-Za-z0-9-]{2,20})/i) || s.match(/\bcolor[:：\s]*([A-Za-z0-9-]{2,20})/i);
        if (color && color[1]) {
          const v = String(color[1]).trim();
          dimensionSamples.colorCode.rows.push(makeSegmentsRow(v, '从描述中提取：颜色编码/名称'));
          dimensionSamples.colorCode.targetGoal = v;
        }

        goldenBlocks = normalizeGoldenByDimensionForAi(dimensionSamples);
        goldenSamples = expandGoldenBlocksToFlatSamples(goldenBlocks);
        aiTraceLineSync('[Step 1] chat parsed (local fallback) and injected into modeling seed');
      } catch (e) {
        aiTraceLineSync(`[Step 1] chat-to-samples exception; continue: ${e instanceof Error ? e.message : String(e)}`);
      }
    }

    aiTraceLineSync('[Step 2] 开始根据实体推导 SQL 关联路径...');

    // 1) 正则拆分每个 CREATE TABLE
    const ddls = splitCreateTableStatements(sqlText);
    if (!ddls.length) return res.status(400).json({ ok: false, error: 'No CREATE TABLE statements found' });

    const total = ddls.length;
    const parsedTables = [];
    const parsedNames = [];

    const modelList = GEMINI_FORCED_MODEL ? [normalizeModelName(GEMINI_FORCED_MODEL)] : MODEL_PRIORITY.map(normalizeModelName);
    setAIStatus({ status: 'running', model: modelList[0] || '', message: `开始分批解析：0/${total}...`, parsedCount: 0, totalCount: total, parsedTables: [] });

    const refInstruction = reference
      ? [
          '',
          '你拥有一个“正确样例对照”（Golden Record），用于帮助推断隐含外键与 join path：',
          `- 款号(styleCode) = ${String(reference?.styleCode || '')}`,
          reference?.brand ? `- 品牌(brand) = ${String(reference?.brand || '')}（若为可读名称而主表存 ID，必须经品牌基础表 join）` : '',
          reference?.lastCode ? `- 楦号(lastCode) = ${String(reference?.lastCode || '')}` : '',
          reference?.soleCode ? `- 底号(soleCode) = ${String(reference?.soleCode || '')}` : '',
          reference?.colorCode ? `- 颜色(colorCode) = ${String(reference?.colorCode || '')}` : '',
          reference?.materialCode ? `- 材质(materialCode) = ${String(reference?.materialCode || '')}` : '',
          reference?.lastId ? `- 黄金样本：主表关联的楦头 ID(lastId) = ${String(reference?.lastId || '')}` : '',
          reference?.lastAssetCode ? `- 黄金样本：资产库楦头文件名(lastAssetCode) = ${String(reference?.lastAssetCode || '')}` : '',
        ].filter(Boolean)
      : [];

    // 2) DDL 解析：强制本地解析（避免对 Gemini 的 N 次调用导致 503/超时）
    for (let idx = 0; idx < ddls.length; idx++) {
      const ddl = ddls[idx];
      const nameHint = extractTableNameFromDdl(ddl) || `table_${idx + 1}`;
      setAIStatus({
        status: 'running',
        model: modelList[0] || '',
        message: `正在解析第 ${idx + 1}/${total} 张表：${nameHint}...`,
        parsedCount: idx,
        totalCount: total,
        parsedTables: parsedNames,
      });
      const normalized = parseSingleTableLocal(ddl);
      if (!normalized.tableName) normalized.tableName = nameHint;

      parsedTables.push(normalized);
      parsedNames.push(normalized.tableName);
      setAIStatus({
        status: 'running',
        model: modelList[0] || '',
        message: `已完成：${parsedNames.length}/${total}（最近：${normalized.tableName}）`,
        parsedCount: parsedNames.length,
        totalCount: total,
        parsedTables: parsedNames,
      });
    }

    // ===========================
    // 新模式：维度串行解析（降低单次上下文，避免 503/超时）
    // ===========================
    {
      const schemaCompact = parsedTables.map((t) => ({
        tableName: t.tableName,
        columns: (t.columns || []).slice(0, 200),
      }));
      const allDdlTableNames = parsedTables.map((t) => String(t.tableName || '').trim()).filter(Boolean);
      const dimensionKeys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];

      const buildSingleDimensionPrompt = ({ dimKey, dimExpected }) => {
        const expectation = dimExpected != null ? String(dimExpected).trim() : '';
        const hintLines =
          goldenBlocks && goldenBlocks.length
            ? (() => {
                const blk = goldenBlocks.find((b) => String(b.standardKey || '').trim() === dimKey);
                if (!blk) return [];
                return [
                  '',
                  `【该维度的用户线索】standardKey=${dimKey}`,
                  blk.targetGoal ? `- Target Goal: ${JSON.stringify(String(blk.targetGoal))}` : '',
                  ...(blk.rows || []).flatMap((r, idx) => {
                    const note = r?.notes ? ` ｜ notes=${JSON.stringify(String(r.notes))}` : '';
                    const detail = (r?.segments || [])
                      .map((s) => `${s.tableName}.${s.fieldName}=${JSON.stringify(String(s.value ?? ''))}`)
                      .join(' + ');
                    return [`- 样本行 ${idx + 1}${note}: ${detail}`];
                  }),
                ].filter(Boolean);
              })()
            : (() => {
                const gs = goldenSamples.filter((g) => String(g.standardKey || '').trim() === dimKey);
                if (!gs.length) return [];
                return [
                  '',
                  `【该维度的用户线索】standardKey=${dimKey}`,
                  ...gs.slice(0, 10).map((g) => {
                    const joined = (g.segments || []).map((s) => String(s.value ?? '')).join('');
                    const detail = (g.segments || [])
                      .map((s) => `${s.tableName}.${s.fieldName}=${JSON.stringify(String(s.value ?? ''))}`)
                      .join(' + ');
                    return `- 合成≈${JSON.stringify(joined)} ｜ ${detail}`;
                  }),
                ];
              })();

        return [
          `你现在是一个数据专家/数据侦探。`,
          masterTable ? `业务主表（Master Table）是：${masterTable}` : '',
          '',
          '【任务】仅针对一个维度强制连线，输出可执行 joinPath（结构化数组）。',
          `目标维度：${dimKey}`,
          expectation ? `黄金期望值：${JSON.stringify(expectation)}` : '黄金期望值：（空；仍需输出最合理路径）',
          '',
          '【强制要求】',
          '1) 你必须输出 joinPathSuggestions 数组，且仅包含本维度的 1 条候选路径（targetStandardKey=dimKey）。',
          '2) 若 dimKey=materialCode 且用户提供多行样本/多段目标，请输出 concatMappingSuggestions（operator:"CONCAT"，parts 数量与样本行一致）。',
          '3) 路径末项必须是 { targetTable, valueField } 且 valueField 必须是业务值列（code/name/category_code/brand_name...），禁止以 id 结尾。',
          '4) 你严禁输出“未找到”。请在 DDL 中寻找最可能的 *_id / associated_* 链路（允许 0~2 中间表）。',
          '',
          '【DDL 表名清单】',
          allDdlTableNames.join(', '),
          ...hintLines,
          '',
          '输入数据（表结构精简版）：',
          JSON.stringify(schemaCompact),
          '',
          '输出严格 JSON（不要 Markdown，不要解释），形如：',
          '{ "smartSuggestions": [], "joinPathSuggestions": [{"targetStandardKey":"DIM","joinPath":[{"sourceTable":"...","sourceField":"...","targetTable":"...","targetField":"..."},{"targetTable":"...","valueField":"..."}]}], "concatMappingSuggestions":[] }',
        ]
          .filter(Boolean)
          .join('\n');
      };

      // 初始化阶段性状态：前端轮询 /api/ai-status 后可即时合并进卡片
      setAIStatus({
        status: 'running',
        model: modelList[0] || '',
        message: `开始维度串行解析：0/${dimensionKeys.length}`,
        parsedCount: 0,
        totalCount: dimensionKeys.length,
        partialRevision: (currentAIStatus.partialRevision || 0) + 1,
        partial: { smartSuggestions: [], joinPathSuggestions: [], concatMappingSuggestions: [], dims: {} },
      });

      const latestDirForBacktest = await getLatestDataTablesDir();
      let smartSuggestions = [];
      let joinPathSuggestions = [];
      let concatMappingSuggestions = [];
      const dims = {};

      const buildTableNameIndexLike = (fileName) => {
        const base = path.basename(String(fileName || ''));
        const noExt = base.replace(/\.(xlsx|xls)$/i, '');
        return noExt.replace(/^\d{8}_/, '');
      };

      const traceJoinPathOnOneRow = async ({ dimKey, pathArr, expectedValue, smartSuggestionsForMain }) => {
        try {
          const { xlsxTables, tablesMap } = await readAllFilesFromFolder(latestDirForBacktest);
          if (!tablesMap || !tablesMap.size) {
            aiTraceLineSync(`[${dimKey}] [现场 C] PATH_BROKEN: tablesMap empty`);
            return;
          }
          const main = inferMainTableFromSmartSuggestions(xlsxTables, smartSuggestionsForMain || []);
          if (!main) {
            aiTraceLineSync(`[${dimKey}] [现场 C] PATH_BROKEN: cannot infer main table from smartSuggestions`);
            return;
          }
          const mainTableName = buildTableNameIndexLike(main.fileName);
          const statusCol = (smartSuggestionsForMain || []).find((x) => String(x?.standardKey || '').trim() === 'status')?.sourceField || '';
          const rowsCandidate = main.rows || [];
          const rowsToScan = statusCol ? rowsCandidate.filter((r) => isTruthyActiveStatus(r?.[statusCol])) : rowsCandidate;
          const row = rowsToScan[0] || rowsCandidate[0] || null;
          if (!row) {
            aiTraceLineSync(`[${dimKey}] [现场 C] PATH_BROKEN: main table has no rows`);
            return;
          }
          const structured = parseJoinPathFromConfig({ joinPath: pathArr });
          if (!structured?.length) {
            aiTraceLineSync(`[${dimKey}] [现场 C] PATH_BROKEN: joinPath cannot be parsed`);
            return;
          }
          aiTraceLineSync(`[现场 C] ===== ${dimKey} Step-by-Step Execution =====`);
          const got = resolveRecursiveValue({
            mainRow: row,
            mainTableName,
            joinPath: structured,
            tablesMap,
            trace: true,
            traceLabel: dimKey,
            standardKey: dimKey,
          });
          aiTraceLineSync(`[现场 D] ${dimKey} 真实抓取值 = ${JSON.stringify(String(got || ''))}`);
          aiTraceLineSync(`[现场 D] ${dimKey} 用户期望值 = ${JSON.stringify(String(expectedValue || ''))}`);
          if (String(expectedValue || '').trim() && String(got || '').trim() === String(expectedValue || '').trim()) {
            aiTraceLineSync(`[现场 D] ${dimKey} Final Verdict: OK`);
          } else {
            aiTraceLineSync(`[现场 D] ${dimKey} Final Verdict: VALUE_MISMATCH`);
          }
        } catch (e) {
          aiTraceLineSync(`[${dimKey}] [现场 C] EXCEPTION: ${e instanceof Error ? e.message : String(e)}`);
        }
      };

      for (let di = 0; di < dimensionKeys.length; di++) {
        const dimKey = dimensionKeys[di];
        const expected = goldenExpectedMap.get(dimKey) || '';
        dims[dimKey] = { status: 'running', attempts: 0, expected: String(expected || '') };
        setAIStatus({
          status: 'running',
          model: modelList[0] || '',
          message: `正在破解 ${dimKey} 维度关联路径...`,
          parsedCount: di,
          totalCount: dimensionKeys.length,
          partialRevision: (currentAIStatus.partialRevision || 0) + 1,
          partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
        });

        let success = false;
        let lastErr = '';
        for (let attempt = 1; attempt <= 3; attempt++) {
          dims[dimKey] = { ...(dims[dimKey] || {}), status: 'running', attempts: attempt };
          setAIStatus({
            status: 'running',
            model: modelList[0] || '',
            message: `正在破解 ${dimKey}...（尝试 ${attempt}/3）`,
            parsedCount: di,
            totalCount: dimensionKeys.length,
            partialRevision: (currentAIStatus.partialRevision || 0) + 1,
            partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
          });

          const prompt = buildSingleDimensionPrompt({ dimKey, dimExpected: expected });
          auditLineSync(`[Audit][A] Prompt Snapshot (single-dim) dim=${dimKey} attempt=${attempt}`);
          auditLineSync(prompt);
          aiTraceLineSync(`[现场 A] ===== dim=${dimKey} attempt=${attempt} Prompt Audit =====`);
          aiTraceLineSync(prompt);
          const out = await generateWithModelList({ modelNames: modelList, prompt, retries: 2 });
          auditLineSync(`[Audit][B] AI Raw Output (single-dim) dim=${dimKey} attempt=${attempt}`);
          auditLineSync(out.ok ? String(out.text || '') : `[Audit][B] FAILED: ${String(out.error?.message || '')}`);
          aiTraceLineSync(`[现场 B] ===== dim=${dimKey} attempt=${attempt} Raw AI Output =====`);
          aiTraceLineSync(out.ok ? String(out.text || '') : `[AI_CALL_FAILED] ${String(out.error?.message || '')}`);
          if (!out.ok) {
            lastErr = String(out.error?.message || 'AI failed');
            continue;
          }

          const obj = extractJsonObject(stripJsonFences(out.text));
          const ssRaw = Array.isArray(obj?.smartSuggestions) ? obj.smartSuggestions : [];
          const jpRaw = Array.isArray(obj?.joinPathSuggestions) ? obj.joinPathSuggestions : [];
          const ccRaw = Array.isArray(obj?.concatMappingSuggestions) ? obj.concatMappingSuggestions : [];

          const jpOne = jpRaw
            .map((j) => {
              const targetStandardKey = typeof j?.targetStandardKey === 'string' ? j.targetStandardKey.trim() : '';
              if (targetStandardKey !== dimKey) return null;
              const rawJoin =
                Array.isArray(j?.joinPath) && j.joinPath.length ? j.joinPath : Array.isArray(j?.path) ? j.path : [];
              const pathArr = normalizeAiJoinPathToStringPath(rawJoin);
              return pathArr.length >= 2 ? { targetStandardKey, path: pathArr } : null;
            })
            .filter(Boolean);

          aiTraceLineSync('[Step 3] 物理回测验证...');
          const validated = await validateJoinPathSuggestionsWithGoldenXlsx({
            joinPathSuggestions: jpOne,
            goldenByStandardKey: goldenExpectedMap,
            folderPath: latestDirForBacktest,
            smartSuggestions: ssRaw,
          });

          if (validated && validated.length) {
            // 现场 C/D：对通过的路径跑一次逐步 trace（只取第一条）
            const v0 = validated[0];
            const rawPathArr = Array.isArray(v0?.path) ? v0.path : [];
            if (rawPathArr.length >= 2) {
              await traceJoinPathOnOneRow({ dimKey, pathArr: rawPathArr, expectedValue: expected, smartSuggestionsForMain: ssRaw });
            }
            const merged = new Map(joinPathSuggestions.map((x) => [x.targetStandardKey, x]));
            for (const v of validated) merged.set(v.targetStandardKey, v);
            joinPathSuggestions = Array.from(merged.values());
            if (dimKey === 'materialCode' && Array.isArray(ccRaw) && ccRaw.length) concatMappingSuggestions = ccRaw;
            if (Array.isArray(ssRaw) && ssRaw.length) smartSuggestions = ssRaw;

            dims[dimKey] = { ...(dims[dimKey] || {}), status: 'success' };
            setAIStatus({
              status: 'running',
              model: modelList[0] || '',
              message: `${dimKey} 维度认证成功，正在处理下一个维度...`,
              parsedCount: di + 1,
              totalCount: dimensionKeys.length,
              partialRevision: (currentAIStatus.partialRevision || 0) + 1,
              partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
            });
            success = true;
            break;
          }

          lastErr = `Join backtest failed (expected=${String(expected || '')})`;
          auditLineSync(`[Audit][D] FAIL dim=${dimKey} attempt=${attempt}: ${lastErr}`);
          aiTraceLineSync(`[现场 D] ${dimKey} Final Verdict: PATH_BROKEN / VALUE_MISMATCH (backtest failed)`);
        }

        if (!success) {
          dims[dimKey] = { ...(dims[dimKey] || {}), status: 'fail', lastError: lastErr };
          setAIStatus({
            status: 'running',
            model: modelList[0] || '',
            message: `${dimKey} 维度未能回测通过（先继续下一维度）`,
            parsedCount: di + 1,
            totalCount: dimensionKeys.length,
            partialRevision: (currentAIStatus.partialRevision || 0) + 1,
            partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
          });
        }
      }

      setAIStatus({
        status: 'done',
        model: modelList[0] || '',
        message: `维度串行解析完成：${dimensionKeys.length}/${dimensionKeys.length}`,
        parsedCount: dimensionKeys.length,
        totalCount: dimensionKeys.length,
        partialRevision: (currentAIStatus.partialRevision || 0) + 1,
        partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
      });

      auditLineSync(`[Audit] ===== AI 逻辑建模结束 ${new Date().toISOString()} =====`);
      return res.json({
        ok: true,
        tables: parsedTables,
        smartSuggestions,
        joinPathSuggestions,
        concatMappingSuggestions,
        _debug: { mode: 'dimension-serial', totalDims: dimensionKeys.length },
      });
    }

    // 3) 合并返回（保持旧前端结构兼容）
    // 4) 第二阶段：侦探式 join 链路推理（解决“AI 发现报告为空”）
    let smartSuggestions = [];
    let joinPathSuggestions = [];
    let concatMappingSuggestions = [];

    const schemaCompact = parsedTables.map((t) => ({
      tableName: t.tableName,
      columns: (t.columns || []).slice(0, 200), // 防止异常超大
    }));
    const chunkArray = (arr, size) => {
      const out = [];
      for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
      return out;
    };

    const allDdlTableNames = parsedTables.map((t) => String(t.tableName || '').trim()).filter(Boolean);
    const isNumericLike = (s) => {
      const x = String(s ?? '').trim();
      return Boolean(x) && /^[0-9]+$/.test(x);
    };
    const goldenKeysWithMultiRows =
      goldenBlocks && goldenBlocks.length
        ? goldenBlocks.filter((b) => b.rows.length > 1).map((b) => b.standardKey)
        : (() => {
            const goldenKeyCounts = new Map();
            for (const g of goldenSamples) {
              const k = String(g.standardKey || '').trim();
              if (!k) continue;
              goldenKeyCounts.set(k, (goldenKeyCounts.get(k) || 0) + 1);
            }
            return [...goldenKeyCounts.entries()].filter(([, n]) => n > 1).map(([k]) => k);
          })();

    let goldenSectionForPrompt = '';
    if (goldenBlocks && goldenBlocks.length) {
      goldenSectionForPrompt = [
        '',
        '【用户结构化线索 — 必须优先用于推理】',
        '用户不仅提供了物理字段与样本值，还提供：',
        '1) **维度目标值 (Target Goal)**：该标准维度在业务上要达成的最终值（例如材质维度目标为 LT04）。',
        '2) **逻辑备注 (Context Notes)**：每一「样本行」旁的说明（例如「这是父类」「这是子类，需拼接」）。',
        '请综合 Target Goal 与 notes，在全部 DDL 表中寻找完整 Join；若目标值由多段拼接而成，必须输出 operator: "CONCAT" 的 concatMappingSuggestions，parts 顺序与样本行顺序一致，且每段 joinPath 终点为值列（不得用 .id 作为最终展示列，见上文硬性约束）。',
        '',
        '【按标准维度分组的黄金样本】',
        ...goldenBlocks.slice(0, 24).flatMap((b) => [
          `### 维度 ${b.standardKey}`,
          `- 目标期望值 (Target Goal): ${JSON.stringify(b.targetGoal || '')}`,
          ...b.rows.map((row, idx) => {
            const note = row.notes ? ` ｜ 逻辑备注: ${JSON.stringify(row.notes)}` : '';
            const detail = row.segments
              .map((s) => `${s.tableName}.${s.fieldName}=${JSON.stringify(String(s.value ?? ''))}`)
              .join(' + ');
            return `  - 样本行 ${idx + 1}${note} ｜ 分段：${detail}`;
          }),
        ]),
        goldenBlocks.length > 24 ? `\n... 省略 ${goldenBlocks.length - 24} 个维度块` : '',
        '',
        '【复合维度】同一维度多行样本 + Target Goal 为合成值（如 LT04）时：为每一行分别推导 Join Path，concatMappingSuggestions.parts 与行数一致。',
        goldenKeysWithMultiRows.length
          ? `【多行样本维度】${goldenKeysWithMultiRows.join('、')} — CONCAT 的 parts 数量须等于该维度样本行数。`
          : '',
      ]
        .filter(Boolean)
        .join('\n');
    } else if (goldenSamples.length) {
      goldenSectionForPrompt = [
        '',
        '用户提供了“动态黄金样本”：每条绑定一个 standardKey（标准维度），内含一个或多个 (table.field = value) 分段。',
        '【复合维度 / 拼接 CONCAT — 同组多分段】若同一条样本组含多个 segments，真实取值 = 各分段 value 按顺序直接连接（无分隔符），例如 "LT"+"04" -> "LT04"。请为每个分段分别推导 Join Path，在 concatMappingSuggestions 中输出 operator: "CONCAT" 与 parts[]，每一段对应 parts 中一项且含独立 joinPath（若该段在主表上直连则可仅用 sourceTable+sourceField）。',
        goldenKeysWithMultiRows.length
          ? [
              '',
              `【复合维度 — Step2 多行样本组】以下 standardKey 在用户界面中出现了多组黄金样本（每组可含一个或多个分段），表示 CONCAT 的多段来源：${goldenKeysWithMultiRows.join('、')}。`,
              '你必须：① 为每一「样本组」分别寻找从主表可达、且终点为「值列」的 Join Path；② 在 concatMappingSuggestions 中为该 standardKey 返回 operator: "CONCAT"，parts 数组长度等于该维度的样本组数量，顺序与下方清单中该 standardKey 各组出现顺序一致；③ 最终业务上 path1 末端取值 + path2 末端取值 + … = 用户期望的合成编码（如 LT+04）。',
            ].join('\n')
          : '',
        '若合成值在单表单列中不存在，请用多段路径 + CONCAT 解释。',
        '【parent_id 父子编码】若分段值所在行 parent_id 指向另一行 id，另一分段在父行 category_code 等字段，请输出完整多跳路径（可自关联）。',
        '样本清单：',
        ...goldenSamples.slice(0, 40).map((g) => {
          const sk = g.standardKey || '(legacy 未标注)';
          const joined = g.segments.map((s) => String(s.value ?? '')).join('');
          const detail = g.segments
            .map((s) => `${s.tableName}.${s.fieldName}=${JSON.stringify(String(s.value ?? ''))}`)
            .join(' + ');
          return `- [${sk}] 合成≈ ${JSON.stringify(joined)} ｜ 分段：${detail}`;
        }),
        goldenSamples.length > 40 ? `- ... 省略 ${goldenSamples.length - 40} 条样本组` : '',
      ]
        .filter(Boolean)
        .join('\n');
    }

    const buildDetectivePrompt = ({ schemaPart, partIndex, partTotal }) => {
        return [
          `你现在是一个数据专家/数据侦探。请解析以下提供的所有 SQL DDL 表结构信息。用户共提供了 ${total} 张表。`,
          partTotal > 1 ? `为性能考虑，本次是第 ${partIndex + 1}/${partTotal} 批（每批最多 5 张表）。请在本批次内尽可能推断 Join 关系与映射。` : '',
          masterTable ? `用户已手动指定 ${masterTable} 为业务主表（Master Table）。请以该表为起点寻找到其它表的 Join Path。` : '',
          '',
          '【强制连线（指定目标）— 你的唯一任务】',
          '你不是在“猜字段”，你是在“强制连线”。用户已经在 Step2 明确选定了每个维度的终点字段（targetTable.targetField）及其期望值（targetValue）。',
          '你必须为每个维度输出一条从【主表】到【目标表.目标字段】的可执行 CHAIN 路径（结构化 joinPath）：',
          '- 起点：主表中的某个 *_id / associated_* 等外键字段',
          '- 中间：允许 0~2 个中间表转接（主表.mid_id -> 中间表.id；中间表.target_id -> 目标表.id）',
          '- 终点：必须严格落在用户指定的 targetTable.targetField（禁止改终点字段）',
          '你严禁返回“未找到”。只要 DDL 里存在任何可用关联，你必须给出最合理的一条链。',
          '判真标准：把路径执行到终点后得到的值必须与 targetValue 字符串完全一致；若你算出的仍是数字 ID，而 targetValue 是业务码/名称字符串，则路径错误，必须继续往下跳到 code/name/category_code 等业务列。',
          '',
          '【User-Defined First — 用户指定优先（绝对硬性规则）】',
          '当用户在 Step2 的黄金样本中明确选择了 tableName 与 fieldName（即某个具体的 目标表.目标字段），你严禁改选其它字段作为终点（尤其禁止把 *.id 替换为 *.code 或其它列）。',
          '',
          '【业务目标 — 值匹配优先（必读）】',
          '主表上的外键/关联列往往是「内部 ID」，它只是 Join 链上的过客，不是用户要看的最终答案。用户与黄金样本要的是「业务可读值」：例如楦号 L-B26097M、款号 SBOX…、材质编码 LT04。',
          '你必须在 SQL/DDL 中主动寻找「最后一公里」：从维表主键 id 或中间键继续延伸到 code、style_no、category_code、last_code、sole_code、material_code、number、name 等**业务列**，使整条链执行后得到的**末字段取值**能与黄金样本字符串一致。不要把路径停在纯数字 id 上，除非用户样本本身就是该数字。',
          '',
          '你的任务从“猜字段”变为“找路径”：从【主表】出发，用结构化 joinPath 描述每一跳，直到**最后一项的 valueField** 指向要展示给用户的业务值列（与 Step2 所选字段一致）。',
          '',
          '【JoinPath 输出协议 — 必须严格遵守】',
          'joinPathSuggestions 与 concatMappingSuggestions.parts[].joinPath 必须使用**结构化数组**（禁止仅用 string[] 的 table.field 链）：',
          '- 前若干项为「关联跳」，每项必须含四键：sourceTable, sourceField, targetTable, targetField（在 targetTable 中用 targetField 匹配当前键值）。',
          '- **最后一项必须为终点**，且仅含两键：targetTable, valueField（表示在当前维表行上读取的最终业务列）。',
          '- 示例单跳：[{"sourceTable":"ods_product_df","sourceField":"associated_last","targetTable":"ods_last_df","targetField":"id"},{"targetTable":"ods_last_df","valueField":"code"}]',
          '- 禁止在最后一项写 sourceField/targetField；禁止省略末项 valueField。',
          '',
          '【硬性约束 — 必须遵守，否则视为错误输出】',
          '1) 终点必须匹配「值」：末项 valueField 取到的值在形态上必须与用户黄金样本一致（可打印业务号，而非无意图的纯内部 ID）。',
          '2) 严禁「ID 终点」与「纯数字陷阱」：最后一项的 valueField 不得名为 id 或 *_id；若该列取值为纯数字而用户黄金样本含字母/横杠等业务字符，则必须改选 code/name 等业务列。除非用户样本本身即为该数字 ID。',
          '3) 路径完整性：主表外键 → 维表定位键 → … → 维表业务值列，每一跳都写入 joinPath，不得跳步。',
          '4) 表名严格对齐：smartSuggestions 与 joinPath 内所有 table 名须与「DDL 表名清单」100% 一致，禁止臆造缩写。',
          '5) 【品牌 brand — 数字主表 vs 字符串黄金样本】若黄金样本中品牌为**可读名称**（如 Bruno Marc），而主表 `brand`（或 smartSuggestions 中映射列）在业务上存的是**数值 ID**（如 1），则**必须**输出 targetStandardKey=brand 的 joinPath：主表该列 → 品牌维表（DDL 中如 ods_wms_base_brand_df 或含 base_brand / wms+brand 的表）的 id（或主键）→ **品牌展示列**（优先 brand_name，其次 name）。禁止把可执行路径的终点留在主表数字列上。结构化 joinPath 示例末项 valueField 须为 brand_name（或与 COMMENT 一致之展示列）。Legacy CHAIN 等价示例：CHAIN|ods_pdm_pdm_product_info_df.brand->ods_wms_base_brand_df.id->ods_wms_base_brand_df.brand_name（表名以 DDL 为准，勿臆造）。',
          '',
          '【强迫学习（必须照抄范式）】',
          'brand 已被证明可执行且正确：主表.brand -> 品牌表.id -> 品牌表.brand_name。',
          '你必须把它当成“模板模式”：其它维度（lastCode/soleCode/colorCode/materialCode）必须同样遵循「主表外键/关联列(可能是数字ID) -> 目标维表定位键(id/parent_id/...) -> 目标维表业务值列(code/name/...)」。',
          '如果你算出的终点仍是纯数字（例如 1695）而黄金样本是业务号（例如 L-B26097M），说明路径没有走到最后一公里，你必须继续往下跳到 code/name 等字段，直到与黄金样本字符串严格一致。',
          '',
          '【楦/底维度硬性线索（必须优先尝试）】',
          '1) 楦编号 lastCode：起点通常在主表的 associated_last_type（或 associated_last/last_id/last_type_id 等），终点必须是 ods_pdm_pdm_base_last_df.code。',
          '   - 允许中间多跳（<=3表），但最后一项必须是 { targetTable: "ods_pdm_pdm_base_last_df", valueField: "code" }。',
          '2) 大底编号 soleCode：起点通常在主表的 associated_sole_info（或 sole_id/associated_sole 等），终点必须是 ods_pdm_pdm_base_heel_df.code。',
          '   - 允许中间多跳（<=3表），但最后一项必须是 { targetTable: "ods_pdm_pdm_base_heel_df", valueField: "code" }。',
          '注意：上述表名与字段名必须与 DDL 清单一致；若 DDL 中确无该表/字段，则在 joinPathSuggestions 中明确留空并说明原因（不要编造）。',
          '',
          '【DDL 表名清单（唯一合法表名来源）】',
          allDdlTableNames.join(', '),
          '',
          goldenSectionForPrompt,
          '已知存在“黄金样本 (Golden Record)”用于验证最终链路是否正确。',
          '',
          '新增关键任务（必须完成）：',
          '1) 在用户指定的【主表】中，根据字段名与 COMMENT 语义，自动识别代表【款号(styleCode)】【品牌(brand)】【状态(status/data_status)】的物理列名。',
          '2) 你必须把上述识别结果作为 smartSuggestions 返回（standardKey=styleCode/brand/status），并以此为起点继续推导 joinPathSuggestions。',
          '注意：若主表缺少某字段，请在 smartSuggestions 中留空（不要胡编），并继续推导其它可推导的链路。',
          '',
          '任务：请务必返回严格 JSON（不要 Markdown，不要解释）。结构如下：',
          '{',
          '  "smartSuggestions": [',
          '    { "standardKey": "styleCode|brand|lastCode|soleCode|colorCode|materialCode|status", "sourceTable": "...", "sourceField": "..." }',
          '  ],',
          '  "joinPathSuggestions": [',
          '    { "targetStandardKey": "lastCode", "joinPath": [',
          '      { "sourceTable": "主表DDL名", "sourceField": "指向楦的外键列", "targetTable": "ods_xxx_last_df", "targetField": "id" },',
          '      { "targetTable": "ods_xxx_last_df", "valueField": "code" }',
          '    ] },',
          '    { "targetStandardKey": "brand", "joinPath": [',
          '      { "sourceTable": "ods_pdm_pdm_product_info_df", "sourceField": "brand", "targetTable": "ods_wms_base_brand_df", "targetField": "id" },',
          '      { "targetTable": "ods_wms_base_brand_df", "valueField": "brand_name" }',
          '    ] }',
          '  ],',
          '  "concatMappingSuggestions": [',
          '    { "standardKey": "materialCode", "operator": "CONCAT", "parts": [',
          '      { "sourceTable": "ods_xxx_material_df", "sourceField": "category_code", "joinPath": [',
          '        { "sourceTable": "主表DDL名", "sourceField": "material_id", "targetTable": "ods_xxx_material_df", "targetField": "id" },',
          '        { "targetTable": "ods_xxx_material_df", "valueField": "category_code" }',
          '      ] },',
          '      { "sourceTable": "ods_xxx_material_df", "sourceField": "category_code", "joinPath": [',
          '        { "sourceTable": "主表DDL名", "sourceField": "sub_material_ref", "targetTable": "ods_xxx_material_df", "targetField": "parent_id" },',
          '        { "targetTable": "ods_xxx_material_df", "valueField": "category_code" }',
          '      ] }',
          '    ] }',
          '  ]',
          '}',
          '',
          '说明：joinPathSuggestions 也可额外带 "path": ["a.b","c.d"] 兼容旧版，但优先且推荐只输出 joinPath 结构化数组。concatMappingSuggestions：operator 恒为 "CONCAT"；parts≥2；每 part 的 joinPath 末项必须是 { targetTable, valueField }；sourceField 填 valueField 列名以便展示。',
          '',
          '【复合维度 CONCAT（多行）— 必须读 notes】',
          '若同一 standardKey 在 Step2 中有多行样本（rows.length>1），必须返回 operator:"CONCAT" 且 parts 顺序与行顺序一致。',
          '你必须阅读每行 notes 线索：例如材质 LT04，若 notes 提示父子关系，请利用 parent_id 做自连接（Self-Join）：(Parent.category_code) + (Child.category_code)。在 parts 中分别给出父段与子段的 joinPath，并确保终点字段仍是用户指定的 category_code（或用户指定字段）。',
          '',
          '侦探式推理步骤（必须按此思路输出结构化 joinPath）：',
          '1) 哪张表承载款号（如 style_wms）？',
          '2) 主表上哪些外键指向**品牌 / 楦 / 底 / 色 / 材**维表（brand、associated_*、sole_id、material_id 等）？主表上的数值多为 ID，仅作跳板。',
          '3) 品牌：对照黄金样本若为「名称字符串」而主表为数字，必须经品牌基础表解析出 brand_name（或等价列），不得停在主表 1。',
          '4) 在维表 DDL 中定位「最后一公里」业务列：brand_name、code、category_code、last_code、name 等，使 valueField 读出后与黄金样本同形态。',
          '5) 若需 ods_pdm_pdm_base_heel_df / ods_pdm_pdm_base_material_df 等，先 id 关联再延伸到业务编码列；末项 valueField 禁止为裸 id（除非样本为数字）。',
          '6) 为每个目标维度写出完整 joinPath（含末项 valueField），确保后端可按数组逐步执行并得到目标业务值。',
          '',
          '进阶任务（Logic Sandbox）：',
          '若提供了“样本行”，你必须对比【样本表中的列值】与【DDL 字段结构】，找出将样本中的值 A 通过 Join 推导到值 B 的完整路径（允许跨最多 3 张表）。',
          '样本行是键值对：列名/表头 -> 单元格值。请优先用样本中的真实列名去匹配 DDL 中的字段名或 COMMENT。',
          '',
          '黄金样本（业务语义，用于对照）：',
          `- 款号(styleCode) = ${String(reference?.styleCode || 'SBOX26008M')}`,
          reference?.lastCode ? `- 楦号(lastCode) = ${String(reference?.lastCode || '')}` : '- 楦号(lastCode) = L-B26097M',
          reference?.soleCode ? `- 底号(soleCode) = ${String(reference?.soleCode || '')}` : '',
          reference?.colorCode ? `- 颜色(colorCode) = ${String(reference?.colorCode || '')}` : '',
          reference?.materialCode ? `- 材质(materialCode) = ${String(reference?.materialCode || '')}` : '',
          '',
          '样本行（Excel 沙盒实测，可为空）：',
          sampleRow && Object.keys(sampleRow).length ? JSON.stringify(sampleRow) : '(未提供；仅使用 Golden Record)',
          '',
          '输入数据（表结构精简版）：',
          JSON.stringify(schemaPart),
        ].join('\n');
      };

    const schemaBatches = total > 20 ? chunkArray(schemaCompact, 5) : [schemaCompact];
      const smartByKey = new Map();
      const joinByKey = new Map();
      const concatByKey = new Map();

    try {
      setAIStatus({ status: 'running', model: modelList[0] || '', message: '开始推理 Join 链路与智能建议...' });
      for (let bi = 0; bi < schemaBatches.length; bi++) {
        setAIStatus({
          status: 'running',
          model: modelList[0] || '',
          message: schemaBatches.length > 1 ? `正在推理 Join（批次 ${bi + 1}/${schemaBatches.length}）...` : '正在推理 Join...',
        });
        const detective = buildDetectivePrompt({ schemaPart: schemaBatches[bi], partIndex: bi, partTotal: schemaBatches.length });
        auditLineSync(`[Audit][A] Prompt Snapshot (detective) batch=${bi + 1}/${schemaBatches.length}`);
        auditLineSync(detective);
        const out2 = await generateWithModelList({ modelNames: modelList, prompt: detective, retries: 3 });
        auditLineSync(`[Audit][B] AI Raw Output (detective) batch=${bi + 1}/${schemaBatches.length}`);
        auditLineSync(out2.ok ? String(out2.text || '') : `[Audit][B] FAILED: ${String(out2.error?.message || '')}`);
        if (!out2.ok) continue;
        const obj2 = extractJsonObject(stripJsonFences(out2.text));
        const ss = Array.isArray(obj2?.smartSuggestions) ? obj2.smartSuggestions : [];
        const jp = Array.isArray(obj2?.joinPathSuggestions) ? obj2.joinPathSuggestions : [];

        for (const s of ss) {
          const standardKey = typeof s?.standardKey === 'string' ? s.standardKey.trim() : '';
          const sourceField = typeof s?.sourceField === 'string' ? s.sourceField.trim() : '';
          const sourceTable = typeof s?.sourceTable === 'string' ? s.sourceTable.trim() : '';
          if (!standardKey || !sourceField || !sourceTable) continue;
          if (!smartByKey.has(standardKey)) smartByKey.set(standardKey, { standardKey, sourceField, sourceTable });
        }

        for (const j of jp) {
          const targetStandardKey = typeof j?.targetStandardKey === 'string' ? j.targetStandardKey.trim() : '';
          const rawJoin =
            Array.isArray(j?.joinPath) && j.joinPath.length ? j.joinPath : Array.isArray(j?.path) ? j.path : [];
          const pathArr = normalizeAiJoinPathToStringPath(rawJoin);
          if (!targetStandardKey || pathArr.length < 2) continue;
          const expected = goldenExpectedMap.get(targetStandardKey) || '';
          const lastLower = joinPathLastFieldLower(pathArr);
          if (lastLower === 'id' || lastLower.endsWith('_id')) {
            if (!isNumericLike(expected)) continue;
          }
          if (!joinByKey.has(targetStandardKey)) joinByKey.set(targetStandardKey, { targetStandardKey, path: pathArr });
        }

        const cc = Array.isArray(obj2?.concatMappingSuggestions) ? obj2.concatMappingSuggestions : [];
        for (const c of cc) {
          const standardKey = typeof c?.standardKey === 'string' ? c.standardKey.trim() : '';
          const partsRaw = Array.isArray(c?.parts) ? c.parts : [];
          const parts = partsRaw
            .map((p) => {
              const rawJp = Array.isArray(p?.joinPath) && p.joinPath.length ? p.joinPath : undefined;
              const jpNorm = rawJp ? normalizeAiJoinPathToStringPath(rawJp) : undefined;
              return {
                sourceField: typeof p?.sourceField === 'string' ? p.sourceField.trim() : '',
                sourceTable: typeof p?.sourceTable === 'string' ? p.sourceTable.trim() : '',
                joinPath: jpNorm && jpNorm.length >= 2 ? jpNorm : undefined,
              };
            })
            .filter((p) => {
              if (!p.sourceField || !p.sourceTable) return false;
              const jpArr = p.joinPath;
              const expected = goldenExpectedMap.get(standardKey) || '';
              if (Array.isArray(jpArr) && jpArr.length >= 2) {
                const lastLower = joinPathLastFieldLower(jpArr);
                if (lastLower === 'id' || lastLower.endsWith('_id')) {
                  if (!isNumericLike(expected)) return false;
                }
              }
              return true;
            });
          if (!standardKey || parts.length < 2) continue;
          if (!concatByKey.has(standardKey)) concatByKey.set(standardKey, { standardKey, parts });
        }
      }

      smartSuggestions = Array.from(smartByKey.values());
      joinPathSuggestions = Array.from(joinByKey.values());
      concatMappingSuggestions = Array.from(concatByKey.values());
    } catch (e2) {
      // eslint-disable-next-line no-console
      console.warn('[ai-parse-multi-sql] join inference failed, return empty suggestions', e2?.message);
    }

    // 若 AI 推理为空，则本地启发式兜底（至少给出基础链路，避免“发现报告空”）
    const heuristic = heuristicInferSuggestions({ tables: parsedTables });
    if (
      (!smartSuggestions || smartSuggestions.length === 0) &&
      (!joinPathSuggestions || joinPathSuggestions.length === 0) &&
      (!concatMappingSuggestions || concatMappingSuggestions.length === 0)
    ) {
      smartSuggestions = heuristic.smartSuggestions || [];
      joinPathSuggestions = heuristic.joinPathSuggestions || [];
    }

    const latestDirForBacktest = await getLatestDataTablesDir();
    auditLineSync('[Audit][C] Join Execution Trace: start backtest on latest XLSX');
    auditLineSync(JSON.stringify({ latestDirForBacktest, joinPathCount: joinPathSuggestions.length }, null, 2));
    const validatedFirst = await validateJoinPathSuggestionsWithGoldenXlsx({
      joinPathSuggestions,
      goldenByStandardKey: goldenExpectedMap,
      folderPath: latestDirForBacktest,
      smartSuggestions,
    });
    joinPathSuggestions = validatedFirst;
    auditLineSync('[Audit][D] Validation & Retry Logic: first validation result');
    auditLineSync(JSON.stringify({ validatedCount: validatedFirst.length, validatedKeys: validatedFirst.map((x) => x.targetStandardKey) }, null, 2));

    // 若关键维度仍无法通过“物理执行+黄金值”校验，则给 AI 一次明确反馈重试机会（最多 1 次）
    const mustKeys = ['brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode'];
    const missing = mustKeys.filter((k) => {
      const expected = goldenExpectedMap.get(k) || '';
      if (!expected) return false;
      return !validatedFirst.some((x) => x.targetStandardKey === k);
    });
    if (missing.length) {
      auditLineSync('[Audit][D] FAIL detected: missing validated keys => retry once');
      auditLineSync(JSON.stringify({ missing, expected: Object.fromEntries(missing.map((k) => [k, goldenExpectedMap.get(k) || ''])) }, null, 2));
      // eslint-disable-next-line no-console
      console.warn('[ai-parse-multi-sql] joinPath backtest failed, retry once for keys:', missing);
      setAIStatus({ status: 'running', model: modelList[0] || '', message: `回测失败：${missing.join('、')}，正在要求 AI 重新强制连线...` });
      const retryFeedback = [
        '',
        '【系统回测反馈（必须修正）】',
        `下列维度的 joinPath 在最新 Excel 快照上执行后未能得到黄金值，请你换一条链路：${missing.join('、')}`,
        ...missing.map((k) => `- ${k}: 期望值 = ${JSON.stringify(String(goldenExpectedMap.get(k) || ''))}`),
        '你必须重新寻找不同的外键/中间表/终点值列，直到执行结果与黄金值严格一致。',
        '输出仍需为严格 JSON（不要解释、不要 Markdown）。',
      ].join('\n');
      auditLineSync('[Audit][D] Retry feedback sent to AI:');
      auditLineSync(retryFeedback);
      const detectiveRetry = `${buildDetectivePrompt({ schemaPart: schemaBatches[0], partIndex: 0, partTotal: schemaBatches.length })}\n${retryFeedback}`;
      auditLineSync('[Audit][A] Full Prompt (detective retry):');
      auditLineSync(detectiveRetry);
      const outRetry = await generateWithModelList({ modelNames: modelList, prompt: detectiveRetry, retries: 2 });
      auditLineSync('[Audit][B] AI Raw Output (detective retry)');
      auditLineSync(outRetry.ok ? String(outRetry.text || '') : `[Audit][B] FAILED: ${String(outRetry.error?.message || '')}`);
      if (outRetry.ok) {
        const objRetry = extractJsonObject(stripJsonFences(outRetry.text));
        const jpRetry = Array.isArray(objRetry?.joinPathSuggestions) ? objRetry.joinPathSuggestions : [];
        const joinByKeyRetry = new Map();
        for (const j of jpRetry) {
          const targetStandardKey = typeof j?.targetStandardKey === 'string' ? j.targetStandardKey.trim() : '';
          const rawJoin =
            Array.isArray(j?.joinPath) && j.joinPath.length ? j.joinPath : Array.isArray(j?.path) ? j.path : [];
          const pathArr = normalizeAiJoinPathToStringPath(rawJoin);
          if (!targetStandardKey || pathArr.length < 2) continue;
          if (!joinByKeyRetry.has(targetStandardKey)) joinByKeyRetry.set(targetStandardKey, { targetStandardKey, path: pathArr });
        }
        const validatedRetry = await validateJoinPathSuggestionsWithGoldenXlsx({
          joinPathSuggestions: Array.from(joinByKeyRetry.values()),
          goldenByStandardKey: goldenExpectedMap,
          folderPath: latestDirForBacktest,
          smartSuggestions,
        });
        auditLineSync('[Audit][D] Retry validation result');
        auditLineSync(JSON.stringify({ validatedRetryCount: validatedRetry.length, validatedRetryKeys: validatedRetry.map((x) => x.targetStandardKey) }, null, 2));
        // 合并：以通过校验的为准补齐缺失项
        const merged = new Map(validatedFirst.map((x) => [x.targetStandardKey, x]));
        for (const x of validatedRetry) merged.set(x.targetStandardKey, x);
        joinPathSuggestions = Array.from(merged.values());
      }
    }
    auditLineSync(`[Audit] ===== AI 逻辑建模结束 ${new Date().toISOString()} =====`);

    setAIStatus({ status: 'done', model: modelList[0] || '', message: `分批解析完成：${parsedNames.length}/${total}`, parsedCount: parsedNames.length, totalCount: total, parsedTables: parsedNames });
    return res.json({
      ok: true,
      tables: parsedTables,
      smartSuggestions,
      joinPathSuggestions,
      concatMappingSuggestions,
      _debug: { parsedCount: parsedNames.length, totalCount: total, heuristic },
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[gemini] ai-parse-multi-sql failed', {
      message: e?.message,
      status: e?.status,
      code: e?.code,
      response: e?.response,
      responseText: e?.response?.text,
      responseData: e?.response?.data,
    });
    setAIStatus({ status: 'error', model: currentAIStatus.model, message: e instanceof Error ? e.message : 'AI parse failed' });
    auditLineSync(`[Audit] ===== AI 逻辑建模异常 ${new Date().toISOString()} =====`);
    auditLineSync(String(e instanceof Error ? e.stack || e.message : String(e)));
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'AI parse failed' });
  }
});

// 下载/查看完整逻辑审计日志
app.get('/api/engine-audit-log', async (_req, res) => {
  await ensureStorageDirs();
  try {
    const p = path.join(__dirname, 'storage', 'engine_audit.log');
    if (!(await fse.pathExists(p))) return res.status(404).json({ ok: false, error: 'engine_audit.log not found' });
    const text = await fse.readFile(p, 'utf8');
    return res.json({ ok: true, text });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'read audit log failed' });
  }
});

// 深度审计：AI 建模全生命周期日志（Prompt/Raw/Step-by-step/Verdict）
app.get('/api/ai-trace-log', async (_req, res) => {
  await ensureStorageDirs();
  try {
    const p = path.join(__dirname, 'storage', 'ai_trace.log');
    if (!(await fse.pathExists(p))) return res.status(404).json({ ok: false, error: 'ai_trace.log not found' });
    const text = await fse.readFile(p, 'utf8');
    return res.json({ ok: true, text });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'read ai trace log failed' });
  }
});

// 保存映射配置到 storage/mapping_config.json
app.post('/api/save-mapping', async (req, res) => {
  await ensureStorageDirs();
  const mapping = req.body;
  if (!mapping || typeof mapping !== 'object') {
    return res.status(400).json({ ok: false, error: 'Invalid mapping payload' });
  }

  const payload = {
    savedAt: new Date().toISOString(),
    mappingAuthenticated: Boolean(mapping?.mappingAuthenticated),
    mapping,
  };

  await fse.writeJson(MAPPING_CONFIG_PATH, payload, { spaces: 2 });
  return res.json({ ok: true });
});

// 健康检查：验证 storage 目录可写（临时接口，便于排查草稿保存失败）
app.get('/api/test-write', (_req, res) => {
  try {
    const dir = DRAFT_STORAGE_DIR;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const testPath = path.join(dir, 'test.txt');
    fs.writeFileSync(testPath, `ok ${new Date().toISOString()}\n`, 'utf8');
    return res.json({ ok: true, path: testPath });
  } catch (error) {
    console.error('test-write 失败:', error);
    return res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// 字段映射页草稿（与已认证的 mapping_config 隔离，便于开发反复调试）
app.get('/api/load-schema-draft', async (_req, res) => {
  await ensureStorageDirs();
  try {
    if (!(await fse.pathExists(SCHEMA_DRAFT_PATH))) {
      return res.json({ ok: true, draft: {} });
    }
    const draft = await fse.readJson(SCHEMA_DRAFT_PATH);
    return res.json({ ok: true, draft: draft && typeof draft === 'object' && !Array.isArray(draft) ? draft : {} });
  } catch {
    return res.json({ ok: true, draft: {} });
  }
});

app.post('/api/save-schema-draft', async (req, res) => {
  try {
    await ensureStorageDirs();
    const draftDir = path.dirname(SCHEMA_DRAFT_PATH);
    fse.ensureDirSync(draftDir);
    const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const out = {
      savedAt: new Date().toISOString(),
      ...body,
    };

    // 绝对路径落盘，确保目录存在（避免写到错误 cwd）
    fse.ensureDirSync(path.dirname(SCHEMA_DRAFT_PATH));
    await fse.writeJson(SCHEMA_DRAFT_PATH, out, { spaces: 2 });
    return res.json({ ok: true });
  } catch (error) {
    console.error('保存草稿失败:', error);
    const message = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ ok: false, error: message });
  }
});

// 上传逻辑沙盒样本 XLSX（多文件可覆盖同目录，供 Join 验证）
app.post('/api/upload-sandbox-xlsx', sandboxUpload.array('files', 50), async (req, res) => {
  await ensureStorageDirs();
  const files = req.files;
  if (!files || !Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ ok: false, error: 'No files uploaded' });
  }
  const out = [];
  for (const f of files) {
    if (!isExcelFileName(f.originalname)) continue;
    const fullPath = path.join(f.destination, f.filename);
    try {
      const wb = XLSX.readFile(fullPath, { cellText: false, cellDates: true });
      const sheet = readFirstSheet(wb);
      if (!sheet) {
        out.push({ storedName: f.filename, originalName: f.originalname, headers: [], firstRow: {} });
        continue;
      }
      const headers = getSheetHeaders(sheet);
      const rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: 1 });
      const firstRow = rows[0] && typeof rows[0] === 'object' ? rows[0] : {};
      out.push({ storedName: f.filename, originalName: f.originalname, headers, firstRow });
    } catch (e) {
      out.push({ storedName: f.filename, originalName: f.originalname, error: e instanceof Error ? e.message : 'read failed' });
    }
  }
  return res.json({ ok: true, sandboxDir: 'storage/sandbox', files: out });
});

// 沙盒校验：对 storage/sandbox 下 XLSX 套用映射，与期望 Golden 值比对
app.post('/api/sandbox-validate-mapping', async (req, res) => {
  await ensureStorageDirs();
  const body = req.body || {};
  const mappingArr = Array.isArray(body.mapping) ? body.mapping : null;
  const expected = body.expected && typeof body.expected === 'object' ? body.expected : {};
  if (!mappingArr?.length) return res.status(400).json({ ok: false, error: 'mapping array required' });

  const standardMap = buildStandardMap(mappingArr);
  const resolved = await resolveFirstActiveRowFromFolder({ folderPath: DIRS.sandbox, standardMap });
  if (!resolved.ok) {
    return res.json({ ok: false, error: resolved.error, checks: {} });
  }

  const row = resolved.row || {};
  const norm = (v) => String(v ?? '').trim();
  const checks = {
    styleCode: { expected: norm(expected.styleCode), actual: norm(row.style_wms), pass: norm(expected.styleCode) === '' || norm(row.style_wms) === norm(expected.styleCode) },
    brand: { expected: norm(expected.brand), actual: norm(row.brand), pass: norm(expected.brand) === '' || norm(row.brand) === norm(expected.brand) },
    lastCode: { expected: norm(expected.lastCode), actual: norm(row.lastCode), pass: norm(expected.lastCode) === '' || norm(row.lastCode) === norm(expected.lastCode) },
    soleCode: { expected: norm(expected.soleCode), actual: norm(row.soleCode), pass: norm(expected.soleCode) === '' || norm(row.soleCode) === norm(expected.soleCode) },
    colorCode: { expected: norm(expected.colorCode), actual: norm(row.colorCode), pass: norm(expected.colorCode) === '' || norm(row.colorCode) === norm(expected.colorCode) },
    materialCode: { expected: norm(expected.materialCode), actual: norm(row.materialCode), pass: norm(expected.materialCode) === '' || norm(row.materialCode) === norm(expected.materialCode) },
    status: { expected: norm(expected.status), actual: norm(row.status), pass: norm(expected.status) === '' || norm(row.status) === norm(expected.status) },
  };

  const requiredKeys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
  const allPass = requiredKeys.every((k) => {
    const exp = norm(expected[k]);
    if (!exp) return true;
    return checks[k]?.pass;
  });

  return res.json({
    ok: true,
    allPass,
    mainTable: resolved.mainTable,
    usedFallbackRow: resolved.usedFallbackRow,
    warning: resolved.warning,
    resolvedRow: row,
    checks,
  });
});

app.post('/api/upload', upload.array('files', 200), async (req, res) => {
  const files = req.files;
  if (!files || !Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ ok: false, error: 'No files uploaded' });
  }

  await refreshAssetsCache();

  res.json({
    ok: true,
    uploaded: files.map((f) => ({
      originalName: f.originalname,
      storedName: f.filename,
      size: f.size,
      destination: f.destination,
    })),
  });
});

function detectCategoryFromPath(fullPath) {
  const normalized = fullPath.split(path.sep).join('/');
  if (normalized.includes('/storage/data_tables/')) return 'xlsx';
  if (normalized.includes('/storage/assets/lasts/')) return '3d_lasts';
  if (normalized.includes('/storage/assets/soles/')) return '3d_soles';
  return 'unknown';
}

async function walkFiles(dir) {
  const out = [];
  const entries = await fse.readdir(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...await walkFiles(full));
    else if (e.isFile()) out.push(full);
  }
  return out;
}

function formatDateTime(d) {
  const pad2 = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

app.get('/api/history', async (_req, res) => {
  await ensureStorageDirs();

  const [dataTableFiles, lastFiles, soleFiles] = await Promise.all([
    walkFiles(DIRS.dataTables),
    walkFiles(DIRS.lasts),
    walkFiles(DIRS.soles),
  ]);

  const allFiles = [...dataTableFiles, ...lastFiles, ...soleFiles];
  const items = [];

  for (const fullPath of allFiles) {
    const stat = await fse.stat(fullPath);
    const rel = path.relative(STORAGE_ROOT, fullPath).split(path.sep).join('/');
    const category = detectCategoryFromPath(fullPath);
    const fileName = path.basename(fullPath);

    let snapshotDate;
    if (category === 'xlsx') {
      // 1) 优先目录日期：data_tables/YYYY-MM-DD/xxx.xlsx
      const m1 = /data_tables\/(\d{4}-\d{2}-\d{2})\//.exec(rel);
      if (m1) snapshotDate = m1[1];
      // 2) 其次文件名前缀：YYYYMMDD_xxx.xlsx
      if (!snapshotDate) {
        const m2 = /^(\d{4})(\d{2})(\d{2})_/.exec(fileName);
        if (m2) snapshotDate = `${m2[1]}-${m2[2]}-${m2[3]}`;
      }
    }

    items.push({
      id: rel,
      fileName,
      relPath: rel,
      size: stat.size,
      uploadTime: formatDateTime(stat.mtime),
      snapshotDate,
      category,
    });
  }

  items.sort((a, b) => (a.uploadTime < b.uploadTime ? 1 : -1));
  res.json({ ok: true, items });
});

// 真实看板统计：优先读认证落盘的 final_dashboard_data.json，否则实时聚合
app.get('/api/dashboard-stats', async (_req, res) => {
  try {
    const snap = await readFinalDashboardSnapshot();
    if (snap) {
      return res.json({
        ok: true,
        source: 'final_dashboard_data',
        generatedAt: snap.generatedAt,
        dates: snap.dates,
        mapping: snap.mapping,
        meta: snap.meta,
        kpis: snap.kpis,
        brandCoverage: snap.brandCoverage || [],
      });
    }

    const agg = await aggregateProjectData({ storageRoot: STORAGE_ROOT });
    const latest = agg.latest;

    return res.json({
      ok: true,
      source: 'live',
      dates: agg.dates,
      mapping: agg.mapping,
      meta: latest.meta,
      kpis: {
        activeStyles: latest.kpis.activeStyles,
        matched3DLasts: latest.kpis.matchedLasts,
        matched3DSoles: latest.kpis.matchedSoles,
        lastCoverage: latest.kpis.lastCoverage,
        soleCoverage: latest.kpis.soleCoverage,
        stylesWithAny3D: latest.kpis.stylesWithAny3D,
        any3DCoveragePercent: latest.kpis.any3DCoveragePercent,
        deltaActiveStyles: agg.deltas.activeStyles.delta,
        deltaMatched3DLasts: agg.deltas.matchedLasts.delta,
        deltaMatched3DSoles: agg.deltas.matchedSoles.delta,
      },
      brandCoverage: latest.brandCoverage,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[dataEngine] dashboard-stats failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'dashboard-stats failed' });
  }
});

// 认证并应用到看板：可选 body.mappingConfig 写入 mapping_config.json → dataEngine 全量 Excel → final_dashboard_data.json
app.post('/api/certify-and-sync', async (req, res) => {
  try {
    await ensureStorageDirs();
    const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const mc = body.mappingConfig;
    if (mc && typeof mc === 'object' && !Array.isArray(mc)) {
      const filePayload = {
        savedAt: new Date().toISOString(),
        mappingAuthenticated: Boolean(mc.mappingAuthenticated),
        mapping: mc,
      };
      await fse.writeJson(MAPPING_CONFIG_PATH, filePayload, { spaces: 2 });
    }
    const payload = await processAllData({ storageRoot: STORAGE_ROOT });
    const outPath = await persistFinalDashboardData(STORAGE_ROOT, payload);
    // eslint-disable-next-line no-console
    console.log('[Engine] certify-and-sync 已写入', outPath);
    return res.json({
      ok: true,
      path: outPath,
      generatedAt: payload.generatedAt,
      kpis: payload.kpis,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[Engine] certify-and-sync failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'certify-and-sync failed' });
  }
});

// 真实款号清单：与看板一致，优先 final_dashboard_data.json
app.get('/api/inventory-real', async (_req, res) => {
  try {
    const snap = await readFinalDashboardSnapshot();
    if (snap?.inventory) {
      return res.json({
        ok: true,
        source: 'final_dashboard_data',
        generatedAt: snap.generatedAt,
        dates: snap.dates,
        mapping: snap.mapping,
        meta: snap.meta,
        items: snap.inventory,
      });
    }
    const agg = await aggregateProjectData({ storageRoot: STORAGE_ROOT });
    return res.json({
      ok: true,
      source: 'live',
      dates: agg.dates,
      mapping: agg.mapping,
      meta: agg.latest.meta,
      items: agg.latest.inventory,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[dataEngine] inventory-real failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'inventory-real failed' });
  }
});

// 统一错误处理（包含 multer 触发的错误）
// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  return res.status(400).json({ ok: false, error: err?.message || 'Server error' });
});

async function start() {
  await ensureStorageDirs();
  await refreshAssetsCache();

  const port = 3001;
  app.listen(port, () => {
    // eslint-disable-next-line no-console
    console.clear();
    // eslint-disable-next-line no-console
    console.log('[startup] Gemini preferred model:', GEMINI_FORCED_MODEL || MODEL_PRIORITY[0]);
    (async () => {
      const modelsList = await listAvailableModels(); // returns models/xxx
      // eslint-disable-next-line no-console
      console.log('✅ 你的 Key 可用的模型清单:', modelsList);
      // eslint-disable-next-line no-console
      console.log('[startup] MODEL_PRIORITY (locked):', MODEL_PRIORITY);
    })().catch(() => {});
    // eslint-disable-next-line no-console
    console.log(`[server] listening on http://localhost:${port}`);
    // eslint-disable-next-line no-console
    console.log('[env] path:', ENV_PATH);
    // eslint-disable-next-line no-console
    console.log('[env] file exists:', envStat.exists ? 'Yes' : 'No', 'bytes:', envStat.bytes);
    // eslint-disable-next-line no-console
    console.log('[env] dotenv parsed keys:', Object.keys(dotenvResult.parsed || {}).join(', ') || '(none)');
    // eslint-disable-next-line no-console
    console.log('Gemini Key Loaded:', process.env.GEMINI_API_KEY ? 'Yes' : 'No');
    // eslint-disable-next-line no-console
    console.log(`[server] assets lasts=${cachedAssets.lasts.length} soles=${cachedAssets.soles.length}`);
  });
}

start().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[server] failed to start', err);
  process.exit(1);
});

