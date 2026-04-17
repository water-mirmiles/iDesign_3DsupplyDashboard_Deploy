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
import { aggregateProjectData, buildStandardMap, resolveFirstActiveRowFromFolder } from './dataEngine.js';

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

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const genAI = GEMINI_API_KEY ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;
// NOTE: This API key's available models are discoverable via /api/debug/gemini-models
// Tier 1（New Users 权限限制）热修：锁定确权可用模型（不带 models/ 前缀）
const MODEL_PRIORITY = ['gemini-1.5-flash', 'gemini-1.5-flash-latest', 'gemini-2.0-flash-exp'];
// 环境变量强制指定：若设置 GEMINI_MODEL，则仅使用该模型，跳过优先级列表
const GEMINI_FORCED_MODEL = (process.env.GEMINI_MODEL || '').trim();
const GEMINI_DISCOVERY_URL = 'https://generativelanguage.googleapis.com/v1beta/models';
const GEMINI_V1_MODELS_URL = 'https://generativelanguage.googleapis.com/v1/models';
const GEMINI_V1_GENERATE_URL = 'https://generativelanguage.googleapis.com/v1/models';

let currentAIStatus = { status: 'idle', message: '', model: '', updatedAt: Date.now() };
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

// 预览：根据当前 mapping（含 CHAIN joinPath）从最新 XLSX 抽取一行真实数据
app.post('/api/preview-mapping-row', async (req, res) => {
  await ensureStorageDirs();
  try {
    const mappingArr = Array.isArray(req.body?.mapping) ? req.body.mapping : null;
    if (!mappingArr?.length) return res.status(400).json({ ok: false, error: 'mapping array required' });
    const latestDir = await getLatestDataTablesDir();
    const standardMap = buildStandardMap(mappingArr);
    const resolved = await resolveFirstActiveRowFromFolder({ folderPath: latestDir, standardMap });
    if (!resolved.ok) return res.json({ ok: false, error: resolved.error || 'resolve failed' });
    return res.json({
      ok: true,
      latestDir: path.basename(latestDir),
      mainTable: resolved.mainTable,
      usedFallbackRow: resolved.usedFallbackRow,
      warning: resolved.warning,
      row: resolved.row || {},
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

// Gemini AI 解析多表 SQL：返回 tables + smartSuggestions
app.post('/api/ai-parse-multi-sql', async (req, res) => {
  const sqlText = String(req.body?.sqlText || '');
  const reference = req.body?.reference || null;
  const masterTable = typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '';
  const goldenSamples = normalizeGoldenSamplesForAi(req.body?.goldenSamples);
  const sampleRow =
    req.body?.sampleRow && typeof req.body.sampleRow === 'object' && !Array.isArray(req.body.sampleRow)
      ? req.body.sampleRow
      : null;
  if (!sqlText.trim()) return res.status(400).json({ ok: false, error: 'sqlText is required' });
  if (!genAI) return res.status(500).json({ ok: false, error: 'GEMINI_API_KEY is not configured' });

  try {
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
          reference?.lastCode ? `- 楦号(lastCode) = ${String(reference?.lastCode || '')}` : '',
          reference?.soleCode ? `- 底号(soleCode) = ${String(reference?.soleCode || '')}` : '',
          reference?.colorCode ? `- 颜色(colorCode) = ${String(reference?.colorCode || '')}` : '',
          reference?.materialCode ? `- 材质(materialCode) = ${String(reference?.materialCode || '')}` : '',
          reference?.lastId ? `- 黄金样本：主表关联的楦头 ID(lastId) = ${String(reference?.lastId || '')}` : '',
          reference?.lastAssetCode ? `- 黄金样本：资产库楦头文件名(lastAssetCode) = ${String(reference?.lastAssetCode || '')}` : '',
        ].filter(Boolean)
      : [];

    // 2) 逐表调用 Gemini（每表独立重试：1s,2s,4s）
    for (let idx = 0; idx < ddls.length; idx++) {
      const ddl = ddls[idx];
      const ddlForAi = sanitizeSqlForAi(ddl);
      const nameHint = extractTableNameFromDdl(ddl) || `table_${idx + 1}`;
      setAIStatus({
        status: 'running',
        model: modelList[0] || '',
        message: `正在解析第 ${idx + 1}/${total} 张表：${nameHint}...`,
        parsedCount: idx,
        totalCount: total,
        parsedTables: parsedNames,
      });

      const prompt = [
        '你是一个 SQL DDL 专家，请只解析“单张表”的建表语句。',
        '目标：返回严格 JSON（不要输出 Markdown、不要解释）。',
        '返回格式：',
        '{ "tableName": "...", "columns": [{ "name": "...", "comment": "..." }] }',
        '规则：',
        '- tableName 必须是表名',
        '- columns 必须包含该表所有字段',
        '- comment 优先使用 COMMENT，若没有 COMMENT 则返回空字符串',
        ...refInstruction,
        '',
        'DDL:',
        ddlForAi,
      ].join('\n');

      const out = await generateWithModelList({ modelNames: modelList, prompt, retries: 3 });
      let normalized;
      if (!out.ok) {
        // 3) 终极 fallback：本地解析（不报错，保证字段目录可用）
        // eslint-disable-next-line no-console
        console.warn('[ai-parse-multi-sql] all models failed, fallback to local parser for table:', nameHint, out.error?.message);
        normalized = parseSingleTableLocal(ddl);
        if (!normalized.tableName) normalized.tableName = nameHint;
      } else {
        const obj = extractJsonObject(out.text);
        if (!obj) {
          // 模型输出不规范：本地兜底
          // eslint-disable-next-line no-console
          console.warn('[ai-parse-multi-sql] invalid JSON object, fallback to local parser for table:', nameHint);
          normalized = parseSingleTableLocal(ddl);
          if (!normalized.tableName) normalized.tableName = nameHint;
        } else {
          normalized = {
            tableName: typeof obj?.tableName === 'string' ? obj.tableName.trim() : nameHint,
            columns: Array.isArray(obj?.columns)
              ? obj.columns
                  .map((c) => ({
                    name: typeof c?.name === 'string' ? c.name.trim() : '',
                    comment: typeof c?.comment === 'string' ? c.comment.trim() : '',
                  }))
                  .filter((c) => c.name)
              : [],
          };
        }
      }

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

    // 3) 合并返回（保持旧前端结构兼容）
    // 4) 第二阶段：侦探式 join 链路推理（解决“AI 发现报告为空”）
    let smartSuggestions = [];
    let joinPathSuggestions = [];
    let concatMappingSuggestions = [];
    try {
      setAIStatus({ status: 'running', model: modelList[0] || '', message: '开始推理 Join 链路与智能建议...' });
      const schemaCompact = parsedTables.map((t) => ({
        tableName: t.tableName,
        columns: (t.columns || []).slice(0, 200), // 防止异常超大
      }));
      const chunkArray = (arr, size) => {
        const out = [];
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
        return out;
      };

      const buildDetectivePrompt = ({ schemaPart, partIndex, partTotal }) => {
        return [
          `你现在是一个数据专家/数据侦探。请解析以下提供的所有 SQL DDL 表结构信息。用户共提供了 ${total} 张表。`,
          partTotal > 1 ? `为性能考虑，本次是第 ${partIndex + 1}/${partTotal} 批（每批最多 5 张表）。请在本批次内尽可能推断 Join 关系与映射。` : '',
          masterTable ? `用户已手动指定 ${masterTable} 为业务主表（Master Table）。请以该表为起点寻找到其它表的 Join Path。` : '',
          goldenSamples.length
            ? [
                '',
                '用户提供了“动态黄金样本”：每条绑定一个 standardKey（标准维度），内含一个或多个 (table.field = value) 分段。',
                '【复合维度 / 拼接 CONCAT】若同一条样本含多个分段，通常表示该标准维度的真实取值 = 各分段 value 按顺序直接连接（默认无分隔符），例如 "LT"+"04" -> "LT04"。',
                '若合成值（如 LT04）在任意单表的单一列中都不存在完全相等的单元格，请主动尝试：是否存在两列或多行（经 Join 可达）使得其值拼接后等于该合成值。',
                '【parent_id 父子编码】若某分段值所在行的 parent_id 指向另一行的 id，而另一分段值出现在父行（或祖先行）的 category_code 等字段上，这是典型的“父子层级编码拼接”。请识别该关系并输出可达的 Join 路径（可含自关联/多跳）。',
                '你的任务：推断【主表】如何通过 Join 到达每一个分段；对需要拼接的标准维度，在 concatMappingSuggestions 中输出多个 parts（每段可有独立 joinPath）。',
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
              ].filter(Boolean).join('\n')
            : '',
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
          '    { "targetStandardKey": "lastCode|soleCode|colorCode|materialCode", "path": ["table.field", "table.id", "table.code"] }',
          '  ],',
          '  "concatMappingSuggestions": [',
          '    { "standardKey": "materialCode", "parts": [',
          '      { "sourceTable": "与DDL一致的逻辑表名", "sourceField": "列名", "joinPath": ["master.col","lookup.parent_id","lookup.category_code"] }',
          '    ] }',
          '  ]',
          '}',
          '',
          'concatMappingSuggestions 规则：仅当某 standardKey 需要多字段拼接时才输出；parts 至少 2 项；每项 sourceTable/sourceField 必填；需要跨表或自关联时用 joinPath（格式同 joinPathSuggestions.path，偶数段为 lookup.key / lookup.value 交替）。',
          '',
          '侦探式推理步骤（必须按这个思路找链路并给出 path）：',
          '1) 哪张表能找到款号（通常是 product_info 的 style_wms 或类似字段）？',
          '2) 在这张表里哪个字段可能指向楦头/大底/颜色/材质的 ID（如 associated_last_type、sole_id、initial_sample_color_id、main_material 等）？',
          '3) 哪张表能找到楦号/底号/颜色/材质（搜索所有名为 code/name/number 的字段注释与列名）？',
          '4) 这两张表如何通过 ID 关联？请输出可执行的 joinPathSuggestions.path。',
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

      for (let bi = 0; bi < schemaBatches.length; bi++) {
        setAIStatus({
          status: 'running',
          model: modelList[0] || '',
          message: schemaBatches.length > 1 ? `正在推理 Join（批次 ${bi + 1}/${schemaBatches.length}）...` : '正在推理 Join...',
        });
        const detective = buildDetectivePrompt({ schemaPart: schemaBatches[bi], partIndex: bi, partTotal: schemaBatches.length });
        const out2 = await generateWithModelList({ modelNames: modelList, prompt: detective, retries: 3 });
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
          const pathArr = Array.isArray(j?.path) ? j.path.map((p) => String(p).trim()).filter(Boolean) : [];
          if (!targetStandardKey || pathArr.length < 2) continue;
          if (!joinByKey.has(targetStandardKey)) joinByKey.set(targetStandardKey, { targetStandardKey, path: pathArr });
        }

        const cc = Array.isArray(obj2?.concatMappingSuggestions) ? obj2.concatMappingSuggestions : [];
        for (const c of cc) {
          const standardKey = typeof c?.standardKey === 'string' ? c.standardKey.trim() : '';
          const partsRaw = Array.isArray(c?.parts) ? c.parts : [];
          const parts = partsRaw
            .map((p) => ({
              sourceField: typeof p?.sourceField === 'string' ? p.sourceField.trim() : '',
              sourceTable: typeof p?.sourceTable === 'string' ? p.sourceTable.trim() : '',
              joinPath: Array.isArray(p?.joinPath) ? p.joinPath.map((x) => String(x).trim()).filter(Boolean) : undefined,
            }))
            .filter((p) => p.sourceField && p.sourceTable);
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
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'AI parse failed' });
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
    const draftDir = path.dirname(SCHEMA_DRAFT_PATH);
    if (!fs.existsSync(draftDir)) {
      fs.mkdirSync(draftDir, { recursive: true });
    }
    await ensureStorageDirs();
    const body = req.body || {};
    const ddlText = typeof body.ddlText === 'string' ? body.ddlText : '';
    const masterTable = typeof body.masterTable === 'string' ? body.masterTable : '';
    const goldenRaw = body.goldenRecord;
    const goldenRecord =
      goldenRaw && typeof goldenRaw === 'object' && !Array.isArray(goldenRaw) ? goldenRaw : {};
    const goldenSamplesRaw = body.goldenSamples;
    const goldenSamples = Array.isArray(goldenSamplesRaw)
      ? goldenSamplesRaw
          .map((x) => {
            if (x && Array.isArray(x.segments) && x.segments.length) {
              return {
                standardKey: typeof x.standardKey === 'string' ? x.standardKey : '',
                segments: x.segments.map((s) => ({
                  tableName: typeof s?.tableName === 'string' ? s.tableName : '',
                  fieldName: typeof s?.fieldName === 'string' ? s.fieldName : '',
                  value: typeof s?.value === 'string' ? s.value : '',
                })),
              };
            }
            return {
              tableName: typeof x?.tableName === 'string' ? x.tableName : '',
              fieldName: typeof x?.fieldName === 'string' ? x.fieldName : '',
              value: typeof x?.value === 'string' ? x.value : '',
            };
          })
          .filter((x) => {
            if (x.segments) return x.segments.some((s) => s.tableName || s.fieldName || s.value);
            return x.tableName || x.fieldName || x.value;
          })
      : [];

    const out = {
      savedAt: new Date().toISOString(),
      ddlText,
      goldenRecord,
      goldenSamples,
      masterTable,
    };

    if (body.sandboxMergedRow && typeof body.sandboxMergedRow === 'object' && !Array.isArray(body.sandboxMergedRow)) {
      out.sandboxMergedRow = body.sandboxMergedRow;
    }
    if (typeof body.sandboxUploadHint === 'string') {
      out.sandboxUploadHint = body.sandboxUploadHint;
    }
    if (Array.isArray(body.standardFields)) {
      out.standardFields = body.standardFields;
    }

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

// 真实看板统计（基于 mapping_config + 最新日期 XLSX + assets）
app.get('/api/dashboard-stats', async (_req, res) => {
  try {
    const agg = await aggregateProjectData({ storageRoot: STORAGE_ROOT });
    const latest = agg.latest;

    return res.json({
      ok: true,
      dates: agg.dates,
      mapping: agg.mapping,
      meta: latest.meta,
      kpis: {
        activeStyles: latest.kpis.activeStyles,
        matched3DLasts: latest.kpis.matchedLasts,
        matched3DSoles: latest.kpis.matchedSoles,
        lastCoverage: latest.kpis.lastCoverage,
        soleCoverage: latest.kpis.soleCoverage,
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

// 真实款号清单（基于 mapping_config + 最新日期 XLSX + assets）
app.get('/api/inventory-real', async (_req, res) => {
  try {
    const agg = await aggregateProjectData({ storageRoot: STORAGE_ROOT });
    return res.json({
      ok: true,
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

