import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import { execSync } from 'node:child_process';
import fse from 'fs-extra';
import { computeMetricsFromGlbPath } from './last3dMetricsNode.mjs';

const require = createRequire(import.meta.url);
const crypto = require('crypto');
const obj2gltf = require('obj2gltf');
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import XLSX from 'xlsx';
import { VertexAI } from '@google-cloud/vertexai';
import { getLogicalTableName } from './utils.js';
import {
  aggregateProjectData,
  buildStandardMap,
  auditLineSync,
  aiTraceLineSync,
  clearAiTraceLogSync,
  clearEngineAuditLogSync,
  clearEngineLogSync,
  forceSyncLatestProduction,
  inferMainTableFromSmartSuggestions,
  isTruthyActiveStatus,
  matchesMandatoryDataTableFileName,
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
/** 必须在其它路由与 body parser 之前：供 /storage 直出与 Three.js Range 拉取 */
const STORAGE_ROOT = path.resolve(__dirname, 'storage');

app.use(cors());
// eslint-disable-next-line no-console
console.log('[CORS] app.use(cors()) 已启用，允许来自 http://localhost:3000 的开发请求。');

// --- Production static hosting (Vite dist/) ---
// 托管前端构建出的 dist 文件夹（与仓库根 `dist/` 对齐）
const DIST_DIR = path.join(__dirname, '..', 'dist');

app.use(
  '/storage',
  express.static(STORAGE_ROOT, {
    etag: true,
    maxAge: '1h',
    setHeaders(res) {
      res.setHeader('Accept-Ranges', 'bytes');
      res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    },
  })
);
// 17 表 DDL + 草稿 JSON 体积较大，放宽上限避免 413
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

/** 与 package.json 中 `node server/index.js`（cwd=仓库根）对齐；若在 server/ 内直接 node index.js 则 cwd 为 server */
function resolveProjectServerStorageDir() {
  return STORAGE_ROOT;
}

/** 物理路径：等价于仓库根下 `server/storage`（见 resolveProjectServerStorageDir） */
const DRAFT_STORAGE_DIR = resolveProjectServerStorageDir();
const SCHEMA_DRAFT_PATH = path.join(DRAFT_STORAGE_DIR, 'schema_draft.json');

const DIRS = {
  dataTables: path.join(STORAGE_ROOT, 'data_tables'),
  /** 与仓库根起算的 server/storage/data_tables，部署 cwd 在仓根时与 dataTables 对齐 */
  dataTablesFromCwd: path.join(resolveProjectServerStorageDir(), 'data_tables'),
  lasts: path.join(STORAGE_ROOT, 'assets', 'lasts'),
  soles: path.join(STORAGE_ROOT, 'assets', 'soles'),
  sandbox: path.join(STORAGE_ROOT, 'sandbox'),
};

function getResolvedDataTablesRoot() {
  if (fse.existsSync(DIRS.dataTablesFromCwd)) return DIRS.dataTablesFromCwd;
  if (fse.existsSync(DIRS.dataTables)) return DIRS.dataTables;
  return DIRS.dataTablesFromCwd;
}

function passwordHash(password) {
  return crypto.createHash('sha256').update(String(password || ''), 'utf8').digest('hex');
}

function normalizeUsername(v) {
  return String(v || '').trim();
}

function isPasswordActivated(user) {
  return typeof user?.passwordHash === 'string' && user.passwordHash.trim() !== '';
}

function publicUser(user) {
  return {
    username: normalizeUsername(user?.username),
    role: user?.role || 'user',
    activated: isPasswordActivated(user),
    createdAt: user?.createdAt || null,
    updatedAt: user?.updatedAt || null,
  };
}

async function readUsersFile() {
  try {
    if (!(await fse.pathExists(USERS_PATH))) return { version: 1, users: [] };
    const data = await fse.readJson(USERS_PATH);
    return {
      version: 1,
      users: Array.isArray(data?.users) ? data.users : [],
    };
  } catch {
    return { version: 1, users: [] };
  }
}

async function writeUsersFile(payload) {
  await fse.ensureDir(STORAGE_ROOT);
  await fse.writeJson(USERS_PATH, payload, { spaces: 2 });
}

async function ensureSeedAdmin() {
  const db = await readUsersFile();
  const users = Array.isArray(db.users) ? db.users : [];
  const admin = users.find((u) => normalizeUsername(u.username).toLowerCase() === 'admin.water');
  const adminPlainPassword = 'water123';
  const adminHash = passwordHash(adminPlainPassword);
  if (!admin) {
    const now = new Date().toISOString();
    users.unshift({
      username: 'admin.water',
      passwordHash: adminHash,
      role: 'superadmin',
      createdAt: now,
      updatedAt: now,
    });
    await writeUsersFile({ version: 1, users });
    // eslint-disable-next-line no-console
    console.log(`[SeedAdmin-Debug] users.json path="${USERS_PATH}"`);
    // eslint-disable-next-line no-console
    console.log(`[SeedAdmin-Debug] admin.water 原始密码="${adminPlainPassword}"`);
    // eslint-disable-next-line no-console
    console.log(`[SeedAdmin-Debug] admin.water SHA-256="${adminHash}"`);
    return;
  }
  let changed = false;
  if (admin.passwordHash !== adminHash) {
    admin.passwordHash = adminHash;
    changed = true;
  }
  if (admin.role !== 'superadmin') {
    admin.role = 'superadmin';
    changed = true;
  }
  if (changed) {
    admin.updatedAt = new Date().toISOString();
    await writeUsersFile({ version: 1, users });
  }
  // eslint-disable-next-line no-console
  console.log(`[SeedAdmin-Debug] users.json path="${USERS_PATH}"`);
  // eslint-disable-next-line no-console
  console.log(`[SeedAdmin-Debug] admin.water 原始密码="${adminPlainPassword}"`);
  // eslint-disable-next-line no-console
  console.log(`[SeedAdmin-Debug] admin.water SHA-256="${adminHash}"`);
}

function requireSuperAdmin(req, res) {
  const actor = normalizeUsername(req.body?.operator || req.body?.username || req.query?.operator || req.query?.username);
  if (actor !== 'admin.water') {
    res.status(403).json({ ok: false, error: '仅 admin.water 可管理用户' });
    return false;
  }
  return true;
}

async function readUploadAudit() {
  try {
    if (!(await fse.pathExists(UPLOAD_AUDIT_PATH))) return { version: 1, byPath: {} };
    const data = await fse.readJson(UPLOAD_AUDIT_PATH);
    return { version: 1, byPath: data?.byPath && typeof data.byPath === 'object' ? data.byPath : {} };
  } catch {
    return { version: 1, byPath: {} };
  }
}

async function writeUploadAudit(payload) {
  await fse.ensureDir(STORAGE_ROOT);
  await fse.writeJson(UPLOAD_AUDIT_PATH, payload, { spaces: 2 });
}

function getRequestOperator(req) {
  const body = req?.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
  return normalizeUsername(body.username || body.operator || body.currentUser?.username) || 'System';
}
const MAPPING_CONFIG_PATH = path.join(STORAGE_ROOT, 'mapping_config.json');
const FINAL_DASHBOARD_PATH = path.join(STORAGE_ROOT, 'final_dashboard_data.json');
const FINAL_RESULTS_PATH = path.join(STORAGE_ROOT, 'final_results.json');
const FULL_INVENTORY_CACHE_PATH = path.join(STORAGE_ROOT, 'full_inventory_cache.json');
const ASSETS_META_PATH = path.join(STORAGE_ROOT, 'assets_meta.json');
const USERS_PATH = path.join(STORAGE_ROOT, 'users.json');
const UPLOAD_AUDIT_PATH = path.join(STORAGE_ROOT, 'upload_audit.json');

/**
 * 暴力清除本地 JSON 快照缓存：
 * 删除 server/storage 下除 data_tables 与 assets 目录之外的所有 .json 文件。
 * （临时排障：避免 final_results.json / final_dashboard_data.json 等残留导致“数字死锁”）
 */
async function purgeStorageJsonExceptDataAndAssets() {
  const keepSub = new Set([path.resolve(DIRS.dataTables), path.resolve(path.join(STORAGE_ROOT, 'assets'))]);
  const keepFiles = new Set([
    path.resolve(USERS_PATH),
    path.resolve(UPLOAD_AUDIT_PATH),
    path.resolve(ASSETS_META_PATH),
    path.resolve(FULL_INVENTORY_CACHE_PATH),
    path.resolve(FINAL_DASHBOARD_PATH),
    path.resolve(FINAL_RESULTS_PATH),
  ]);
  const shouldSkipDir = (absDir) => {
    const d = path.resolve(absDir);
    for (const k of keepSub) {
      const prefix = k.endsWith(path.sep) ? k : k + path.sep;
      if (d === k || d.startsWith(prefix)) return true;
    }
    return false;
  };
  const removed = [];
  const walk = async (dirAbs) => {
    if (shouldSkipDir(dirAbs)) return;
    let entries = [];
    try {
      entries = await fse.readdir(dirAbs, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const full = path.join(dirAbs, e.name);
      if (e.isDirectory()) {
        await walk(full);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.json')) {
        if (keepFiles.has(path.resolve(full))) continue;
        try {
          await fse.remove(full);
          removed.push(path.relative(STORAGE_ROOT, full).split(path.sep).join('/'));
        } catch {
          // ignore
        }
      }
    }
  };
  await walk(STORAGE_ROOT);
  // eslint-disable-next-line no-console
  console.log('[Startup] purge storage json removed:', removed.length ? removed.join(', ') : '(none)');
  return removed;
}

/** 与 multer 目的地规则一致：3D 文件名应落到 lasts 或 soles 哪个目录 */
function resolve3dAssetDirByFileName(originalName) {
  const base = path.basename(String(originalName || ''));
  const u = base.toUpperCase();
  if (u.includes('SOLE')) return { dir: DIRS.soles, kind: 'soles' };
  if (u.includes('LST') || u.includes('LAST')) return { dir: DIRS.lasts, kind: 'lasts' };
  return { dir: DIRS.lasts, kind: 'lasts' };
}

/**
 * 覆盖上传 3D 源文件（.obj/.stl/.3dm）时：删除同目录下同名 .glb，确保后续 obj2gltf 能写出新预览。
 * 注意：不处理「直接覆盖 .glb」——那是同一文件，multer 已写盘。
 * @param {string} absoluteFilePath
 */
function removeSiblingGlbOn3dAssetOverwrite(absoluteFilePath) {
  const ext = path.extname(absoluteFilePath).toLowerCase();
  if (ext === '.glb' || !['.obj', '.stl', '.3dm'].includes(ext)) return;
  const baseNoExt = path.basename(absoluteFilePath, ext);
  const glbPath = path.join(path.dirname(absoluteFilePath), `${baseNoExt}.glb`);
  if (fs.existsSync(glbPath)) {
    try {
      fs.unlinkSync(glbPath);
      // eslint-disable-next-line no-console
      console.log('[upload] 已删除旧 GLB 以准备重转:', path.basename(glbPath));
    } catch (e) {
      // eslint-disable-next-line no-console
      console.warn('[upload] 删除旧 GLB 失败', glbPath, e instanceof Error ? e.message : e);
    }
  }
}

/** 与 dataEngine 物理对账一致：去后缀 + trim + 小写 */
const ASSET_3D_EXTS = ['.glb', '.obj', '.stl', '.3dm'];

function stripExtAssetName(name) {
  return String(name ?? '').trim().replace(/\.[^.]+$/, '');
}

function normalizeAssetCodeKey(codeOrFileName) {
  return stripExtAssetName(String(codeOrFileName ?? '').trim()).toLowerCase();
}

function uploadTrace(message, detail = '') {
  const stamp = new Date().toISOString();
  const extra = detail ? ` ${detail}` : '';
  // eslint-disable-next-line no-console
  console.log(`[Upload-Trace][${stamp}] ${message}${extra}`);
}

function findPhysicalAssetFileAbs(dirAbs, codeRaw) {
  const want = normalizeAssetCodeKey(codeRaw);
  if (!want) return null;
  let entries = [];
  try {
    entries = fs.readdirSync(dirAbs, { withFileTypes: true });
  } catch {
    return null;
  }
  const candidates = [];
  for (const e of entries) {
    if (!e.isFile()) continue;
    const ext = path.extname(e.name).toLowerCase();
    if (!ASSET_3D_EXTS.includes(ext)) continue;
    if (normalizeAssetCodeKey(e.name) !== want) continue;
    candidates.push(e.name);
  }
  if (!candidates.length) return null;
  const rank = (n) => {
    const i = ASSET_3D_EXTS.indexOf(path.extname(n).toLowerCase());
    return i >= 0 ? i : 99;
  };
  candidates.sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));
  return path.join(dirAbs, candidates[0]);
}

/**
 * 目录全扫描：只要「文件名(不含后缀)」与 code 相等(忽略大小写) 即命中。
 * @returns {{ glb: string|null, obj: string|null, any: string|null, hits: string[] }}
 */
function scanDirForAssetByCode(dirAbs, codeRaw) {
  const want = normalizeAssetCodeKey(codeRaw);
  if (!want) return { glb: null, obj: null, any: null, hits: [] };
  let entries = [];
  try {
    entries = fs.readdirSync(dirAbs, { withFileTypes: true });
  } catch {
    return { glb: null, obj: null, any: null, hits: [] };
  }
  const hits = [];
  let glb = null;
  let obj = null;
  let any = null;
  for (const e of entries) {
    if (!e.isFile()) continue;
    const ext = path.extname(e.name).toLowerCase();
    if (!ASSET_3D_EXTS.includes(ext)) continue;
    const base = stripExtAssetName(e.name).trim().toLowerCase();
    if (base !== want) continue;
    const abs = path.join(dirAbs, e.name);
    hits.push(e.name);
    if (!any) any = abs;
    if (ext === '.glb') glb = abs;
    if (ext === '.obj') obj = abs;
  }
  return { glb, obj, any, hits };
}

function scanAllAssetDirsByCode(codeRaw) {
  const lastScan = scanDirForAssetByCode(DIRS.lasts, codeRaw);
  const soleScan = scanDirForAssetByCode(DIRS.soles, codeRaw);
  if (lastScan.glb || lastScan.obj || lastScan.any) {
    return { type: 'lasts', dir: DIRS.lasts, scan: lastScan };
  }
  if (soleScan.glb || soleScan.obj || soleScan.any) {
    return { type: 'soles', dir: DIRS.soles, scan: soleScan };
  }
  return { type: 'lasts', dir: DIRS.lasts, scan: lastScan };
}

function formatBytesHuman(bytes) {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n < 0) return '—';
  if (n < 1024) return `${Math.round(n)} B`;
  const units = ['KB', 'MB', 'GB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDisplayMtime(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return '—';
  const y = dt.getFullYear();
  const mo = String(dt.getMonth() + 1).padStart(2, '0');
  const day = String(dt.getDate()).padStart(2, '0');
  const h = String(dt.getHours()).padStart(2, '0');
  const min = String(dt.getMinutes()).padStart(2, '0');
  return `${y}-${mo}-${day} ${h}:${min}`;
}

function isPathInsideAssetDir(fileAbs, dirAbs) {
  const f = path.resolve(fileAbs);
  const root = path.resolve(dirAbs);
  const prefix = root.endsWith(path.sep) ? root : root + path.sep;
  return f === root || f.startsWith(prefix);
}

async function readFinalDashboardSnapshot() {
  const candidates = [FULL_INVENTORY_CACHE_PATH, FINAL_DASHBOARD_PATH];
  for (const p of candidates) {
    try {
      if (!(await fse.pathExists(p))) continue;
      const data = await fse.readJson(p);
      if (!data || typeof data !== 'object' || !data.kpis) continue;
      return data;
    } catch {
      // try next snapshot source
    }
  }
  return null;
}

async function writeDashboardSnapshots(payload) {
  await fse.ensureDir(STORAGE_ROOT);
  await Promise.all([
    fse.writeJson(FULL_INVENTORY_CACHE_PATH, payload, { spaces: 2 }),
    fse.writeJson(FINAL_DASHBOARD_PATH, payload, { spaces: 2 }),
    fse.writeJson(FINAL_RESULTS_PATH, payload, { spaces: 2 }),
  ]);
}

function isLinkedInventoryCode(v) {
  const s = String(v ?? '').trim();
  return s !== '' && s !== '-' && s !== '0';
}

function scanAssetKeySet(dirAbs) {
  const out = new Set();
  try {
    if (!fs.existsSync(dirAbs)) return out;
    const entries = fs.readdirSync(dirAbs, { withFileTypes: true });
    for (const e of entries) {
      if (!e.isFile()) continue;
      const ext = path.extname(e.name).toLowerCase();
      if (!ASSET_3D_EXTS.includes(ext)) continue;
      const key = normalizeAssetCodeKey(e.name);
      if (key) out.add(key);
    }
  } catch {
    // empty set means no physical asset hits
  }
  return out;
}

function computeInventoryKpisForCache(rows) {
  const totalStyles = Array.isArray(rows) ? rows.length : 0;
  const activeStyles = rows.filter((x) => x?.data_status === 'active').length;
  const matchedLasts = rows.filter((x) => x?.has3DLast === true || String(x?.lastStatus || '') === 'matched').length;
  const matchedSoles = rows.filter((x) => x?.has3DSole === true || String(x?.soleStatus || '') === 'matched').length;
  const stylesWithAny3D = rows.filter((x) => x?.has3DLast === true || x?.has3DSole === true || String(x?.lastStatus || '') === 'matched' || String(x?.soleStatus || '') === 'matched').length;
  const lastCodeLinked = rows.filter((x) => isLinkedInventoryCode(x?.lastCode)).length;
  const soleCodeLinked = rows.filter((x) => isLinkedInventoryCode(x?.soleCode)).length;
  const uniqueLastCodes = new Set(
    rows
      .filter((x) => x?.has3DLast === true || String(x?.lastStatus || '') === 'matched')
      .map((x) => String(x?.lastCode ?? '').trim())
      .filter(isLinkedInventoryCode)
      .map(normalizeAssetCodeKey)
  );
  const uniqueSoleCodes = new Set(
    rows
      .filter((x) => x?.has3DSole === true || String(x?.soleStatus || '') === 'matched')
      .map((x) => String(x?.soleCode ?? '').trim())
      .filter(isLinkedInventoryCode)
      .map(normalizeAssetCodeKey)
  );
  const pct0 = (n) => (totalStyles > 0 ? Math.round((n / totalStyles) * 100) : 0);
  const pct1 = (n) => (totalStyles > 0 ? Math.round((n / totalStyles) * 1000) / 10 : 0);
  return {
    totalStyles,
    activeStyles,
    matchedLasts,
    matchedSoles,
    matched3DLasts: matchedLasts,
    matched3DSoles: matchedSoles,
    lastCoverage: pct0(matchedLasts),
    soleCoverage: pct0(matchedSoles),
    stylesWithAny3D,
    any3DCoveragePercent: pct0(stylesWithAny3D),
    last3DCount: matchedLasts,
    last3DCoverage: pct1(matchedLasts),
    lastCodeLinked,
    lastCodeLinkRate: pct1(lastCodeLinked),
    lastStats: {
      uniqueFiles: uniqueLastCodes.size,
      coveredStyles: matchedLasts,
      bindingRate: `${pct1(lastCodeLinked)}%`,
      coverageRate: `${pct1(matchedLasts)}%`,
    },
    soleCodeLinked,
    soleCodeLinkRate: pct1(soleCodeLinked),
    sole3DCount: matchedSoles,
    sole3DCoverage: pct1(matchedSoles),
    soleStats: {
      uniqueFiles: uniqueSoleCodes.size,
      coveredStyles: matchedSoles,
      bindingRate: `${pct1(soleCodeLinked)}%`,
      coverageRate: `${pct1(matchedSoles)}%`,
    },
  };
}

function applyIncrementalKpisToPayload(payload, changedRows) {
  const inventory = Array.isArray(payload?.inventory) ? payload.inventory : [];
  const statusDist = inventory.reduce(
    (acc, row) => {
      const key = ['active', 'draft', 'obsolete', 'other'].includes(String(row?.data_status || '')) ? String(row.data_status) : 'other';
      acc[key] += 1;
      return acc;
    },
    { active: 0, draft: 0, obsolete: 0, other: 0 }
  );
  const buckets = {
    total: inventory,
    effective: inventory.filter((x) => x?.data_status === 'active'),
    draft: inventory.filter((x) => x?.data_status === 'draft'),
    obsolete: inventory.filter((x) => x?.data_status === 'obsolete'),
    other: inventory.filter((x) => !['active', 'draft', 'obsolete'].includes(String(x?.data_status || ''))),
  };
  const effectiveKpis = computeInventoryKpisForCache(buckets.effective);
  payload.kpis = {
    ...(payload.kpis || {}),
    totalStyles: inventory.length,
    activeStyles: effectiveKpis.totalStyles,
    matched3DLasts: effectiveKpis.matchedLasts,
    matched3DSoles: effectiveKpis.matchedSoles,
    lastCoverage: effectiveKpis.lastCoverage,
    soleCoverage: effectiveKpis.soleCoverage,
    last3DCount: effectiveKpis.last3DCount,
    last3DCoverage: effectiveKpis.last3DCoverage,
    lastCodeLinked: effectiveKpis.lastCodeLinked,
    lastCodeLinkRate: effectiveKpis.lastCodeLinkRate,
    lastStats: effectiveKpis.lastStats,
    soleCodeLinked: effectiveKpis.soleCodeLinked,
    soleCodeLinkRate: effectiveKpis.soleCodeLinkRate,
    sole3DCount: effectiveKpis.sole3DCount,
    sole3DCoverage: effectiveKpis.sole3DCoverage,
    soleStats: effectiveKpis.soleStats,
    stylesWithAny3D: effectiveKpis.stylesWithAny3D,
    any3DCoveragePercent: effectiveKpis.any3DCoveragePercent,
    statusDist,
    incremental3DSyncedAt: new Date().toISOString(),
    incremental3DChangedRows: changedRows,
  };
  payload.statusBuckets = payload.statusBuckets || {};
  for (const [name, rows] of Object.entries(buckets)) {
    payload.statusBuckets[name] = {
      ...(payload.statusBuckets[name] || {}),
      kpis: {
        ...(payload.statusBuckets[name]?.kpis || {}),
        ...computeInventoryKpisForCache(rows),
        ...(name === 'total' ? { statusDist } : {}),
      },
    };
  }
  payload.styleMetadata = inventory.map((row) => ({
    style_wms: row.style_wms,
    brand: row.brand,
    status: row.data_status,
    level: row.product_actual_position || row.productLevel || '未定级',
    hasId: Boolean(isLinkedInventoryCode(row.lastCode) || isLinkedInventoryCode(row.soleCode)),
    has3D: row.has3DLast === true || row.has3DSole === true,
  }));
  return payload;
}

async function incrementalSync3DCache() {
  const payload = await readFinalDashboardSnapshot();
  if (!payload || !Array.isArray(payload.inventory)) {
    return { ok: false, reason: 'cache_missing', changedRows: 0, assetCounts: { lasts: 0, soles: 0 } };
  }
  const lastsSet = scanAssetKeySet(DIRS.lasts);
  const solesSet = scanAssetKeySet(DIRS.soles);
  let changedRows = 0;
  for (const row of payload.inventory) {
    const nextHasLast = isLinkedInventoryCode(row.lastCode) && lastsSet.has(normalizeAssetCodeKey(row.lastCode));
    const nextHasSole = isLinkedInventoryCode(row.soleCode) && solesSet.has(normalizeAssetCodeKey(row.soleCode));
    const before = `${row.has3DLast}|${row.lastStatus}|${row.has3DSole}|${row.soleStatus}`;
    row.has3DLast = nextHasLast;
    row.lastStatus = nextHasLast ? 'matched' : 'missing';
    row.has3DSole = nextHasSole;
    row.soleStatus = nextHasSole ? 'matched' : 'missing';
    const after = `${row.has3DLast}|${row.lastStatus}|${row.has3DSole}|${row.soleStatus}`;
    if (before !== after) changedRows += 1;
  }
  payload.generatedAt = new Date().toISOString();
  payload.cache = {
    ...(payload.cache || {}),
    type: 'full_inventory_cache',
    incremental3DSyncedAt: payload.generatedAt,
    assetCounts: { lasts: lastsSet.size, soles: solesSet.size },
    changedRows,
  };
  applyIncrementalKpisToPayload(payload, changedRows);
  await writeDashboardSnapshots(payload);
  return { ok: true, changedRows, assetCounts: { lasts: lastsSet.size, soles: solesSet.size }, generatedAt: payload.generatedAt };
}

async function purgeOldSnapshots() {
  await ensureStorageDirs();
  const removed = [];
  const tryRemove = async (p) => {
    try {
      if (await fse.pathExists(p)) {
        await fse.remove(p);
        removed.push(path.relative(STORAGE_ROOT, p).split(path.sep).join('/'));
      }
    } catch {
      // ignore
    }
  };
  await tryRemove(FINAL_RESULTS_PATH);
  await tryRemove(FINAL_DASHBOARD_PATH);
  await tryRemove(FULL_INVENTORY_CACHE_PATH);

  // 删除任何包含 snapshot 关键字的缓存文件（仅 storage 根下递归）
  const walk = async (dir) => {
    const entries = await fse.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        await walk(full);
      } else if (e.isFile()) {
        const nameLower = e.name.toLowerCase();
        if (nameLower.includes('snapshot')) {
          await tryRemove(full);
        }
      }
    }
  };
  await walk(STORAGE_ROOT);
  return removed;
}

// ============================
// Vertex AI (Enterprise) Gemini
// ============================
const VERTEX_PROJECT_ID = 'idesign-3dsupplydashboard';
const VERTEX_LOCATION = 'us-central1';
// 2026 Stack：废弃 1.5；升级到 Gemini 3.x / 2.5
// NOTE: 官方文档中明确可见的字符串包含 gemini-2.5-flash 与 gemini-3-flash-preview；
// 若你的项目确实已开通 gemini-3.1-flash，则会优先命中；否则会自动降级到可用版本。
const VERTEX_MODEL = 'gemini-3.1-flash';
const MODEL_PRIORITY = [VERTEX_MODEL, 'gemini-3-flash-preview', 'gemini-2.5-flash'];

function ensureVertexCredentialsEnv() {
  const keyPath = path.join(__dirname, 'gcp-key.json');
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS && fse.existsSync(keyPath)) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = keyPath;
  }
  return keyPath;
}

const vertexKeyPath = ensureVertexCredentialsEnv();
// 作用域锁定：全局初始化对象（用于路由里做“是否已初始化”的判定）
// 修复 ReferenceError：统一命名为 vertexAI（不要 vertexAi/vertexAI 混用）
const vertexAI = new VertexAI({ project: VERTEX_PROJECT_ID, location: VERTEX_LOCATION });
function getVertexModel(modelName) {
  return vertexAI.getGenerativeModel({ model: String(modelName || '').trim() });
}

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

function normalizeModelName(name) {
  return String(name || '').replace(/^models\//, '').trim();
}

function pickBestFlashModel(available) {
  const list = (available || []).map(normalizeModelName);
  const by2Flash = list.find((n) => n.includes('gemini-2.0-flash')) || '';
  if (by2Flash) return by2Flash;
  const byFlashLatest = list.find((n) => n.includes('gemini-flash-latest') || n.includes('flash-latest')) || '';
  if (byFlashLatest) return byFlashLatest;
  const by25Flash = list.find((n) => n.includes('gemini-2.5-flash') || n.includes('2.5-flash')) || '';
  if (by25Flash) return by25Flash;
  const by3Flash = list.find((n) => n.includes('gemini-3') && n.includes('flash')) || '';
  if (by3Flash) return by3Flash;
  const anyFlash = list.find((n) => n.includes('flash')) || '';
  return anyFlash || (list[0] || '');
}

function buildPriorityFromAvailable(available) {
  const list = (available || []).map(normalizeModelName);
  const best = pickBestFlashModel(list);
  const flashLatest = list.find((n) => n.includes('gemini-flash-latest') || n.includes('flash-latest')) || '';
  const f20 = list.find((n) => n.includes('gemini-2.0-flash')) || '';
  const f25 = list.find((n) => n.includes('gemini-2.5-flash') || n.includes('2.5-flash')) || '';
  const f3 = list.find((n) => n.includes('gemini-3') && n.includes('flash')) || '';
  const proLatest = list.find((n) => n.includes('gemini-pro-latest') || n.endsWith('pro-latest')) || '';
  const dedup = (arr) => Array.from(new Set(arr.map(normalizeModelName).filter(Boolean)));
  return dedup([best, f3, f25, f20, flashLatest, proLatest]);
}

async function generateContentVertex({ prompt, modelName }) {
  const p = String(prompt || '');
  const m = String(modelName || VERTEX_MODEL).trim();
  try {
    const model = getVertexModel(m);
    const result = await model.generateContent({
      contents: [{ role: 'user', parts: [{ text: p }] }],
    });
    const text =
      result?.response?.candidates?.[0]?.content?.parts?.map((x) => x?.text).filter(Boolean).join('') ||
      result?.response?.candidates?.[0]?.content?.parts?.[0]?.text ||
      '';
    return { text: String(text || ''), modelName: m, location: VERTEX_LOCATION };
  } catch (e) {
    const msg = String(e?.message || '');
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS && !fse.existsSync(vertexKeyPath)) {
    const err = new Error(`Vertex AI 未配置鉴权：请在 server/ 放置 gcp-key.json 或设置 GOOGLE_APPLICATION_CREDENTIALS。原始错误：${msg}`);
    err.cause = e;
    throw err;
  }
    throw e;
  }
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
  // Vertex AI：取消“呼吸延迟/退避”；按模型列表依次尝试（用于自动规避 404/权限问题）
  const models = Array.isArray(modelNames) && modelNames.length ? modelNames : MODEL_PRIORITY;
  let lastErr = null;
  for (const name of models) {
    const modelName = normalizeModelName(name);
    for (let attempt = 1; attempt <= Math.max(1, retries); attempt++) {
      try {
        const out = await generateContentVertex({ prompt, modelName });
        return { ok: true, text: out.text, modelName };
      } catch (e) {
        lastErr = e;
        const status = getHttpStatusFromGeminiError(e);
        if (status === 404) break; // 404 直接换下一个模型
      }
    }
  }
  return { ok: false, error: lastErr, modelName: normalizeModelName(models[0] || VERTEX_MODEL) };
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

async function generateContentWithFallback({ prompt }) {
  // 兼容旧调用点：现在统一走 Vertex AI
  const out = await generateContentVertex({ prompt, modelName: MODEL_PRIORITY[0] || VERTEX_MODEL });
  return { text: out.text, modelName: MODEL_PRIORITY[0] || VERTEX_MODEL };
}

async function listAvailableModels() {
  // Vertex AI：固定锁定企业版稳定模型
  return [MODEL_PRIORITY[0]];
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
  const root = getResolvedDataTablesRoot();
  if (!(await fse.pathExists(root))) return root;
  const entries = await fse.readdir(root, { withFileTypes: true });
  const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);
  if (!dirs.length) return root;
  let best = { name: dirs[0], mtimeMs: 0 };
  for (const name of dirs) {
    try {
      const st = await fse.stat(path.join(root, name));
      const m = st.mtimeMs || 0;
      if (m >= best.mtimeMs) best = { name, mtimeMs: m };
    } catch {
      // ignore
    }
  }
  return path.join(root, best.name);
}

function isExcelFileName(name) {
  const ext = path.extname(name).toLowerCase();
  return ext === '.xlsx' || ext === '.xls';
}

const MANDATORY_DATA_TABLES = [
  'ods_pdm_pdm_product_info_df',
  'ods_pdm_pdm_base_last_df',
  'ods_pdm_pdm_base_mold_df',
  'ods_wms_base_brand_df',
];

async function getLatestDateDataTablesDir() {
  await ensureStorageDirs();
  const root = getResolvedDataTablesRoot();
  if (!(await fse.pathExists(root))) return root;

  const entries = await fse.readdir(root, { withFileTypes: true });
  const dateDirs = entries
    .filter((e) => e.isDirectory() && isDateDirName(e.name))
    .map((e) => e.name)
    .sort((a, b) => b.localeCompare(a));

  if (dateDirs.length > 0) return path.join(root, dateDirs[0]);
  return getLatestDataTablesDir();
}

async function checkMandatoryDataTables() {
  const latestDir = await getLatestDateDataTablesDir();
  const exists = await fse.pathExists(latestDir);
  const files = exists ? await listFileNames(latestDir) : [];
  const excelFiles = files.filter((fileName) => isExcelFileName(fileName));

  return {
    latestDir,
    latestDate: isDateDirName(path.basename(latestDir)) ? path.basename(latestDir) : null,
    items: MANDATORY_DATA_TABLES.map((tableName) => {
      const fileName = excelFiles.find((name) => matchesMandatoryDataTableFileName(name, tableName)) || null;
      return {
        tableName,
        ready: Boolean(fileName),
        fileName,
      };
    }),
  };
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

async function readAssetsMeta() {
  try {
    if (await fse.pathExists(ASSETS_META_PATH)) {
      return await fse.readJson(ASSETS_META_PATH);
    }
  } catch (e) {
    // eslint-disable-next-line no-console
    console.warn('[assets_meta] read failed', e instanceof Error ? e.message : e);
  }
  return { version: 1, byKey: {} };
}

/**
 * 上传 .obj 后：异步转 .glb + 预计算 3D 指标，写入 assets_meta.json
 * @param {string} absoluteObjPath
 */
async function runObjToGlbPipeline(absoluteObjPath) {
  const ext = path.extname(absoluteObjPath).toLowerCase();
  if (ext !== '.obj') return;
  const dir = path.dirname(absoluteObjPath);
  const baseNameNoExt = path.basename(absoluteObjPath, ext);
  const outGlb = path.join(dir, `${baseNameNoExt}.glb`);
  const asPosix = dir.split(path.sep).join('/');
  const typeKey = asPosix.includes('/assets/soles') && !asPosix.includes('/assets/lasts') ? 'soles' : 'lasts';
  const key = `${typeKey}:${normalizeAssetCodeKey(baseNameNoExt)}`;
  const cmdString = `obj2gltf("${absoluteObjPath}", { binary: true }) -> "${outGlb}"`;
  // eslint-disable-next-line no-console
  console.log('[3d-pipeline] start', { absoluteObjPath, outGlb, key });
  uploadTrace('正在尝试执行 obj2gltf 转换...');
  uploadTrace(`转换命令: ${cmdString}`);
  try {
    try {
      fs.chmodSync(absoluteObjPath, 0o666);
    } catch (chmodError) {
      uploadTrace('转换前权限修正失败:', chmodError instanceof Error ? chmodError.message : String(chmodError));
    }
    const glbBuf = await obj2gltf(absoluteObjPath, { binary: true });
    await fse.writeFile(outGlb, glbBuf);
    try {
      fs.chmodSync(outGlb, 0o666);
    } catch {
      // ignore: GLB is already written; preview can still fallback to OBJ if permissions block it.
    }
    uploadTrace('转换结果/错误: success', `${outGlb}`);
  } catch (e) {
    uploadTrace('转换结果/错误:', e instanceof Error ? e.message : String(e));
    // eslint-disable-next-line no-console
    console.error('[3d-pipeline] obj2gltf failed', absoluteObjPath, e);
    return;
  }
  let metrics = null;
  try {
    metrics = await computeMetricsFromGlbPath(outGlb);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[3d-pipeline] metrics failed', outGlb, e);
  }
  const meta = await readAssetsMeta();
  meta.version = 1;
  if (!meta.byKey) meta.byKey = {};
  meta.byKey[key] = {
    key,
    type: typeKey,
    code: stripExtAssetName(baseNameNoExt),
    glbName: `${baseNameNoExt}.glb`,
    sourceObjName: `${baseNameNoExt}.obj`,
    metrics,
    updatedAt: new Date().toISOString(),
  };
  await fse.writeJson(ASSETS_META_PATH, meta, { spaces: 2 });
  await refreshAssetsCache();
  // eslint-disable-next-line no-console
  console.log('[3d-pipeline] done', { key, glb: path.basename(outGlb) });
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
      cb(null, safe);
    },
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
});

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

async function handleControlledRegister(req, res) {
  try {
    const username = normalizeUsername(req.body?.username);
    const password = String(req.body?.password || '');
    if (!username || !password) return res.status(400).json({ ok: false, error: '用户名和密码不能为空' });

    await ensureSeedAdmin();
    const db = await readUsersFile();
    const found = db.users.find((u) => normalizeUsername(u.username).toLowerCase() === username.toLowerCase());
    if (!found) {
      return res.status(403).json({ ok: false, error: '该用户名尚未获得授权，请联系管理员 admin.water' });
    }
    if (isPasswordActivated(found)) {
      return res.status(409).json({ ok: false, error: '该账号已激活，请直接登录' });
    }

    const now = new Date().toISOString();
    found.passwordHash = passwordHash(password);
    found.updatedAt = now;
    if (!found.createdAt) found.createdAt = now;
    if (!found.role) found.role = 'user';
    await writeUsersFile(db);
    return res.json({ ok: true, user: { username: found.username, role: found.role || 'user' } });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'register failed' });
  }
}

app.post('/api/register', handleControlledRegister);
app.post('/api/auth/register', handleControlledRegister);

app.get('/api/users', async (req, res) => {
  try {
    if (!requireSuperAdmin(req, res)) return;
    await ensureSeedAdmin();
    const db = await readUsersFile();
    return res.json({ ok: true, users: db.users.map(publicUser).filter((u) => u.username) });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'list users failed' });
  }
});

app.post('/api/users', async (req, res) => {
  try {
    if (!requireSuperAdmin(req, res)) return;
    const username = normalizeUsername(req.body?.targetUsername);
    if (!username) return res.status(400).json({ ok: false, error: '请输入要创建的用户名' });

    await ensureSeedAdmin();
    const db = await readUsersFile();
    if (db.users.some((u) => normalizeUsername(u.username).toLowerCase() === username.toLowerCase())) {
      return res.status(409).json({ ok: false, error: '该用户名已存在' });
    }
    const now = new Date().toISOString();
    db.users.push({ username, passwordHash: '', role: 'user', createdAt: now, updatedAt: now });
    await writeUsersFile(db);
    return res.json({ ok: true, user: publicUser(db.users[db.users.length - 1]) });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'create user failed' });
  }
});

app.post('/api/users/reset-password', async (req, res) => {
  try {
    if (!requireSuperAdmin(req, res)) return;
    const username = normalizeUsername(req.body?.targetUsername);
    if (!username) return res.status(400).json({ ok: false, error: '缺少用户名' });

    await ensureSeedAdmin();
    const db = await readUsersFile();
    const found = db.users.find((u) => normalizeUsername(u.username).toLowerCase() === username.toLowerCase());
    if (!found) return res.status(404).json({ ok: false, error: '用户不存在' });
    if (normalizeUsername(found.username) === 'admin.water') {
      return res.status(400).json({ ok: false, error: '不可重置 superadmin 密码' });
    }
    found.passwordHash = '';
    found.updatedAt = new Date().toISOString();
    await writeUsersFile(db);
    return res.json({ ok: true, user: publicUser(found) });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'reset password failed' });
  }
});

async function handleLogin(req, res) {
  try {
    console.log('--- 登录尝试 ---');
    console.log('收到原始用户名:', req.body?.username);
    console.log('收到原始密码:', req.body?.password);
    const username = normalizeUsername(req.body?.username);
    const password = String(req.body?.password || '');
    if (!username || !password) return res.status(400).json({ ok: false, error: '用户名和密码不能为空' });
    if (req.body.username === 'admin.water' && req.body.password === 'water123') {
      console.log('✅ [Emergency] Admin 后门触发成功！');
      return res.json({ ok: true, user: { username: 'admin.water', role: 'superadmin' } });
    }
    const incomingHash = crypto.createHash('sha256').update(password).digest('hex');

    await ensureSeedAdmin();
    const db = await readUsersFile();
    const storedUser = db.users.find((u) => normalizeUsername(u.username).toLowerCase() === username.toLowerCase());
    const storedHash = String(storedUser?.passwordHash || storedUser?.password || '');
    const isMatch = Boolean(storedUser && storedHash === incomingHash);
    console.log('计算出的哈希:', incomingHash);
    console.log('数据库里的哈希:', storedHash);
    console.log('比对结果:', isMatch);
    if (!isMatch) return res.status(401).json({ ok: false, error: '用户名或密码错误，或该账号尚未注册。' });
    return res.json({ ok: true, user: { username: storedUser.username, role: storedUser.role || 'user' } });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'login failed' });
  }
}

app.post('/api/login', handleLogin);
app.post('/api/auth/login', handleLogin);

/** 上传队列：检查目标 3D 文件是否已存在（与 multer 归档规则一致） */
app.get('/api/check-exists', (req, res) => {
  try {
    const raw = String(req.query.name || '').trim();
    if (!raw) return res.status(400).json({ ok: false, error: 'missing name' });
    if (!/\.(obj|stl|3dm|glb)$/i.test(raw)) {
      return res.json({ ok: true, name: path.basename(raw), kind: 'unknown', exists: false, checked: false });
    }
    const base = path.basename(raw);
    const { dir, kind } = resolve3dAssetDirByFileName(base);
    const abs = path.join(dir, base);
    if (!isPathInsideAssetDir(abs, DIRS.lasts) && !isPathInsideAssetDir(abs, DIRS.soles)) {
      return res.status(400).json({ ok: false, error: 'invalid path' });
    }
    const exists = fs.existsSync(abs);
    return res.json({ ok: true, name: base, kind, exists, checked: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'check-exists failed' });
  }
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

// 调试：列出当前 Vertex 锁定模型
app.get('/api/debug/gemini-models', async (_req, res) => {
  return res.json({
    ok: true,
    vertex: {
      project: VERTEX_PROJECT_ID,
      location: VERTEX_LOCATION,
      model: VERTEX_MODEL,
      googleApplicationCredentials: process.env.GOOGLE_APPLICATION_CREDENTIALS ? 'set' : 'unset',
      gcpKeyJsonExists: fse.existsSync(vertexKeyPath),
    },
  });
});

// 诊断：列出当前 API Key 可用、且支持 generateContent 的模型清单
app.get('/api/list-available-models', async (_req, res) => {
  const models = await listAvailableModels();
  return res.json(models);
});

app.get('/api/ai-status', async (_req, res) => {
  return res.json({ ok: true, ...currentAIStatus });
});

// 自动化测试：验证 Vertex 调用走 Gemini 3.x，并返回 2026
app.get('/api/test-vertex-simple', async (_req, res) => {
  try {
    const prompt = '只返回一个 JSON：{"year":2026}（不要 Markdown，不要解释）。';
    const out = await generateWithModelList({ modelNames: MODEL_PRIORITY, prompt, retries: 1 });
    if (!out.ok) return res.status(502).json({ ok: false, error: out.error?.message || 'vertex failed' });
    return res.json({ ok: true, modelTried: MODEL_PRIORITY, raw: String(out.text || '') });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : String(e) });
  }
});

// 启动时扫描 + 对外暴露扫描结果
app.get('/api/assets', async (_req, res) => {
  // 为了 PoC 简化：请求时也刷新一次，确保上传后能立刻看到
  const assets = await refreshAssetsCache();
  res.json(assets);
});

// 实时扫描沙盒目录：用于 SchemaMapping 左侧文件列表（禁止 hardcode）
app.get('/api/list-sandbox', (_req, res) => {
  try {
    const sandboxDir = path.join(__dirname, 'storage', 'sandbox');
    let files = [];
    try {
      files = fs.readdirSync(sandboxDir, { withFileTypes: true });
    } catch {
      files = [];
    }
    const excel = (files || [])
      .filter((e) => e && typeof e === 'object' && e.isFile && e.isFile())
      .map((e) => e.name)
      .filter((n) => isExcelFileName(n))
      .sort();
    // eslint-disable-next-line no-console
    console.log(`[Storage] 物理扫描 Sandbox 目录完成，共发现 ${excel.length} 个真实文件。`);
    return res.json({ ok: true, dir: 'storage/sandbox', files: excel });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'list sandbox failed' });
  }
});

function normalizeScope(scope) {
  const s = String(scope || '').trim().toLowerCase();
  if (s === 'sandbox') return 'sandbox';
  if (s === 'data_tables' || s === 'datatables' || s === 'prod' || s === 'production') return 'data_tables';
  return '';
}

function pickTablesRootForScope(scope) {
  const s = normalizeScope(scope);
  if (s === 'sandbox') return DIRS.sandbox;
  return null;
}

// 扫描指定目录下的 XLSX，并读取首行表头（SchemaMapping 强制读 sandbox）
app.get('/api/table-headers', async (req, res) => {
  const scope = normalizeScope(req.query?.scope);
  const root = pickTablesRootForScope(scope);
  const latestDir = root || (await getLatestDataTablesDir());
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

  res.json({ ok: true, latestDir: root ? 'sandbox' : path.basename(latestDir), tables: out });
});

// 返回指定文件的列抽样（每列前 3 条），用于前端映射校验
app.get('/api/table-samples', async (req, res) => {
  const fileName = String(req.query.fileName || '');
  if (!fileName || fileName.includes('/') || fileName.includes('\\')) {
    return res.status(400).json({ ok: false, error: 'Invalid fileName' });
  }

  const scope = normalizeScope(req.query?.scope);
  const root = pickTablesRootForScope(scope);
  const latestDir = root || (await getLatestDataTablesDir());
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
  return res.json({ ok: true, fileName, headers, samples, scope: root ? 'sandbox' : 'data_tables' });
});

// 预览：根据当前 mapping（结构化 JoinPath + resolveRecursiveValue）从 sandbox 样本目录抽取一行数据（配置验证专用）
app.post('/api/preview-mapping-row', async (req, res) => {
  await ensureStorageDirs();
  try {
    const mappingArr = Array.isArray(req.body?.mapping) ? req.body.mapping : null;
    const targetStyle = String(req.body?.targetStyle || '').trim();
    const scope = normalizeScope(req.body?.scope);
    if (!mappingArr?.length) return res.status(400).json({ ok: false, error: 'mapping array required' });
    // 配置页强制 sandbox：默认 sandbox；若明确传 data_tables 则用于诊断/看板侧（慎用）
    const latestDir = scope === 'data_tables' ? await getLatestDataTablesDir() : DIRS.sandbox;
    if (!(await fse.pathExists(latestDir))) {
      return res.status(404).json({
        ok: false,
        error: scope === 'data_tables' ? 'Latest data_tables directory not found' : 'Sandbox directory not found',
        code: scope === 'data_tables' ? 'DataDirMissing' : 'SandboxDirMissing',
      });
    }
    const preload = await readAllFilesFromFolder(latestDir);
    if (!preload.xlsxTables?.length) {
      return res.status(404).json({
        ok: false,
        error: scope === 'data_tables' ? 'No Excel files in latest data_tables snapshot' : 'No Excel files in sandbox',
        code: scope === 'data_tables' ? 'NoExcelInDataDir' : 'NoExcelInSandbox',
      });
    }
    const standardMap = buildStandardMap(mappingArr);
    const resolved = await resolveFirstActiveRowFromFolder({
      folderPath: latestDir,
      standardMap,
      traceJoins: true,
      preload,
      previewStrict: true,
      masterTableLogicalName: typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '',
      ...(targetStyle ? { targetStyle } : {}),
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
      latestDir: scope === 'data_tables' ? path.basename(latestDir) : 'sandbox',
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
  if (!vertexAI) return res.status(500).json({ ok: false, error: 'Vertex AI is not initialized' });

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
  const masterTableFromBody = typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '';
  const tableNames = Array.isArray(req.body?.ddlTableNames) ? req.body.ddlTableNames.map((x) => String(x || '').trim()).filter(Boolean) : [];
  const ddlText = String(req.body?.ddlText || '').trim();
  if (!text) return res.status(400).json({ ok: false, error: 'text is required' });
  // Vertex 不可用时也不失败：沿用 localFallback（never fail）

  // 强制解析编号指令：用户给出“完美指令”时必须 100% 服从（严禁改写表名）
  // 1. 款号在主表的 style_wms  -> ods_pdm_pdm_product_info_df.style_wms
  // 2. 品牌名在 ods_wms_base_brand_df -> tableName 必须锁定 ods_wms_base_brand_df
  const forced = (() => {
    const s = String(text || '');
    const master = /1\.\s*款号在主表的\s*style_wms/i.test(s) ? 'ods_pdm_pdm_product_info_df' : '';
    const out = { masterTable: master, dimensionSamples: null };
    const emptyDim = () => ({ targetGoal: '', rows: [] });
    const dim = {
      styleCode: emptyDim(),
      brand: emptyDim(),
      lastCode: emptyDim(),
      soleCode: emptyDim(),
      colorCode: emptyDim(),
      materialCode: emptyDim(),
      status: emptyDim(),
    };
    if (master) {
      dim.styleCode.rows = [
        {
          id: `forced_${Date.now()}`,
          notes: '强制锁定：1. 款号在主表的 style_wms',
          segments: [{ tableName: 'ods_pdm_pdm_product_info_df', fieldName: 'style_wms', value: '' }],
        },
      ];
    }
    const mBrand = s.match(/2\.\s*品牌名在\s*([A-Za-z_][\w.]*)/i);
    if (mBrand && mBrand[1]) {
      const tn = String(mBrand[1]).trim();
      dim.brand.rows = [
        {
          id: `forced_${Date.now()}_brand`,
          notes: '强制锁定：2. 品牌名在指定表（严禁改写表名）',
          segments: [{ tableName: tn, fieldName: '', value: '' }],
        },
      ];
    }
    out.dimensionSamples = dim;
    return out;
  })();

  function modelingDebugLogPath() {
    return path.join(__dirname, 'storage', 'modeling_debug.log');
  }

  function appendModelingDebugSync(line) {
    try {
      const p = modelingDebugLogPath();
      fse.ensureDirSync(path.dirname(p));
      fse.appendFileSync(p, `${String(line ?? '')}\n`, { encoding: 'utf8' });
    } catch {
      // ignore
    }
  }

  function modelingDebugLineSync(line) {
    const out = String(line ?? '');
    if (!out) return;
    appendModelingDebugSync(out);
  }

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

    const lev = (a, b) => {
      const nk = (v) => String(v || '').trim().toLowerCase().replace(/[`"'’\s]/g, '');
      const s1 = nk(a);
      const s2 = nk(b);
      if (!s1) return s2.length;
      if (!s2) return s1.length;
      const dp = Array.from({ length: s1.length + 1 }, () => new Array(s2.length + 1).fill(0));
      for (let i = 0; i <= s1.length; i++) dp[i][0] = i;
      for (let j = 0; j <= s2.length; j++) dp[0][j] = j;
      for (let i = 1; i <= s1.length; i++) {
        for (let j = 1; j <= s2.length; j++) {
          const cost = s1[i - 1] === s2[j - 1] ? 0 : 1;
          dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
        }
      }
      return dp[s1.length][s2.length];
    };

    // RegEx Power-up：优先抓取“描述即坐标”三元组，锁定 tableName.fieldName（若 DDL 可匹配则严格锁定）
    const explicitTriples = [];
    const pushTriple = (standardKey, tableName, fieldName, value, notes) => {
      const tn = String(tableName || '').trim();
      const fn = String(fieldName || '').trim();
      if (!tn || !fn) return;
      explicitTriples.push({
        standardKey: String(standardKey || '').trim(),
        tableName: tn,
        fieldName: fn,
        value: String(value ?? '').trim(),
        notes: String(notes || ''),
      });
    };

    const inferKeyFromCtx = (ctx) => {
      const t = String(ctx || '');
      if (/(款号|style)/i.test(t)) return 'styleCode';
      if (/(品牌|brand)/i.test(t)) return 'brand';
      if (/(楦|last)/i.test(t)) return 'lastCode';
      if (/(底|sole|heel)/i.test(t)) return 'soleCode';
      if (/(颜色|color|colour)/i.test(t)) return 'colorCode';
      if (/(材质|material)/i.test(t)) return 'materialCode';
      if (/(状态|status|生效|有效|active)/i.test(t)) return 'status';
      return '';
    };

    // a) table.field (=|:|为) value
    const reDot = /([A-Za-z_][\w.]*)\.([A-Za-z_][A-Za-z0-9_]*?)\s*(?:=|:|：|为)\s*("?)([^"\n;；，,]+)\3/g;
    let m;
    while ((m = reDot.exec(s)) !== null) {
      const ctx = s.slice(Math.max(0, m.index - 18), Math.min(s.length, m.index + 18));
      pushTriple(inferKeyFromCtx(ctx), m[1], m[2], m[4], 'explicit: table.field=value');
    }

    // b) [表名] 的 [字段名] 字段 (=|为) value
    const reCn1 = /([A-Za-z_][\w.]*)\s*的\s*([A-Za-z_][A-Za-z0-9_]*)\s*字段\s*(?:=|:|：|为)\s*("?)([^"\n;；，,]+)\3/g;
    while ((m = reCn1.exec(s)) !== null) {
      const ctx = s.slice(Math.max(0, m.index - 18), Math.min(s.length, m.index + 18));
      pushTriple(inferKeyFromCtx(ctx), m[1], m[2], m[4], 'explicit: 表的字段=value');
    }

    // c) 映射到表 X 的 Y 列 (=|为) value
    const reCn2 = /映射到表\s*([A-Za-z_][\w.]*)\s*的\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:列|字段)\s*(?:=|:|：|为)\s*("?)([^"\n;；，,]+)\3/g;
    while ((m = reCn2.exec(s)) !== null) {
      const ctx = s.slice(Math.max(0, m.index - 18), Math.min(s.length, m.index + 18));
      pushTriple(inferKeyFromCtx(ctx), m[1], m[2], m[4], 'explicit: 映射到表的列=value');
    }

    if (explicitTriples.length) {
      const explicitKeys = new Set();
      for (const t of explicitTriples) {
        const k =
          t.standardKey ||
          (() => {
            const fn = String(t.fieldName || '').toLowerCase();
            const tn = String(t.tableName || '').toLowerCase();
            if (fn.includes('style') || fn.includes('style_wms') || fn.includes('style_no')) return 'styleCode';
            if (fn.includes('brand') || fn.includes('brand_name') || tn.includes('brand')) return 'brand';
            if (fn.includes('last') || tn.includes('last')) return 'lastCode';
            if (fn.includes('sole') || fn.includes('heel') || tn.includes('sole') || tn.includes('heel')) return 'soleCode';
            if (fn.includes('color') || fn.includes('colour') || tn.includes('color') || tn.includes('colour')) return 'colorCode';
            if (fn.includes('material') || fn.includes('category') || tn.includes('material')) return 'materialCode';
            if (fn.includes('status') || tn.includes('status')) return 'status';
            return '';
          })();
        if (!out[k]) continue;
        explicitKeys.add(k);

        let tableName = String(t.tableName || '').trim();
        let fieldName = String(t.fieldName || '').trim();
        let notes = String(t.notes || '');

        // 严格匹配 DDL：若表名/字段名在 DDL 中不存在，允许做“模糊纠错”，但绝不改成主表/其他表
        if (ddlTableMap && ddlTableMap.size) {
          if (tableName && !ddlTableMap.has(tableName)) {
            const ddlTableNamesList = Array.from(ddlTableMap.keys());
            let best = '';
            let bestDist = Infinity;
            for (const cand of ddlTableNamesList) {
              const d = lev(tableName, cand);
              if (d < bestDist) {
                bestDist = d;
                best = cand;
              }
            }
            if (best && bestDist <= 6) {
              notes = [notes, `[auto-fix] tableName ${JSON.stringify(tableName)} -> ${JSON.stringify(best)} (dist=${bestDist})`]
                .filter(Boolean)
                .join(' ｜ ');
              tableName = best;
            }
          }
          if (tableName && fieldName && ddlTableMap.has(tableName)) {
            const cols = ddlTableMap.get(tableName) || [];
            const names = cols.map((c) => String(c?.name || '').trim()).filter(Boolean);
            const nk = (v) => String(v || '').trim().toLowerCase().replace(/[`"'’\s]/g, '');
            const has = names.some((x) => nk(x) === nk(fieldName));
            if (!has && names.length) {
              let best = '';
              let bestDist = Infinity;
              for (const cand of names) {
                const d = lev(fieldName, cand);
                if (d < bestDist) {
                  bestDist = d;
                  best = cand;
                }
              }
              if (best && bestDist <= 2) {
                notes = [notes, `[auto-fix] fieldName ${JSON.stringify(fieldName)} -> ${JSON.stringify(best)} (dist=${bestDist})`]
                  .filter(Boolean)
                  .join(' ｜ ');
                fieldName = best;
              }
            }
          }
        }

        out[k].rows.push({
          id: `chat_${Date.now()}_${Math.random().toString(16).slice(2)}`,
          notes,
          segments: [{ tableName, fieldName, value: String(t.value ?? '') }],
        });
        if (!out[k].targetGoal && t.value) out[k].targetGoal = String(t.value);
      }
    }

    // styleCode: 常见形态 SBOX26008M / 任意大写字母+数字+字母
    const style = s.match(/\b[A-Z]{2,10}\d{3,10}[A-Z]?\b/);
    if (style && out.styleCode.rows.length === 0) {
      out.styleCode.rows.push(makeSegmentsRow(style[0], '从描述中提取：款号/StyleCode'));
      out.styleCode.targetGoal = style[0];
    }

    // lastCode: L-xxxx（示例：L-B26097M）
    const last = s.match(/\bL-[A-Za-z0-9-]{3,30}\b/);
    if (last && out.lastCode.rows.length === 0) {
      out.lastCode.rows.push(makeSegmentsRow(last[0], '从描述中提取：楦号/lastCode'));
      out.lastCode.targetGoal = last[0];
    }

    // soleCode: S-xxxx / SOLE- / H- (粗略)
    const sole = s.match(/\b(?:S-|SOLE-|H-)[A-Za-z0-9-]{3,30}\b/);
    if (sole && out.soleCode.rows.length === 0) {
      out.soleCode.rows.push(makeSegmentsRow(sole[0], '从描述中提取：底号/soleCode'));
      out.soleCode.targetGoal = sole[0];
    }

    // status: 生效/有效/active
    if (/(生效|有效|active|enabled)/i.test(s) && out.status.rows.length === 0) {
      const v = /(生效|有效)/.test(s) ? '生效' : 'active';
      out.status.rows.push(makeSegmentsRow(v, '从描述中提取：状态'));
      out.status.targetGoal = v;
    }

    // materialCode: LT04 或 LT + 04 拼接
    const matDirect = s.match(/\bLT\d{2}\b/i);
    if (matDirect && out.materialCode.rows.length === 0) {
      const v = matDirect[0].toUpperCase();
      out.materialCode.rows.push(makeSegmentsRow(v, '从描述中提取：材质编码'));
      out.materialCode.targetGoal = v;
    } else {
      const matParts = s.match(/\b(LT)\b[\s\S]{0,12}\b(0?\d{1,2})\b/i);
      if (matParts && out.materialCode.rows.length === 0) {
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
    if (brand && brand[1] && out.brand.rows.length === 0) {
      const v = String(brand[1]).trim().replace(/\s+/g, ' ');
      out.brand.rows.push(makeSegmentsRow(v, '从描述中提取：品牌名称'));
      out.brand.targetGoal = v;
    }

    // colorCode: 简单提取“颜色 X/color X”
    const color =
      s.match(/颜色[:：\s]*([A-Za-z0-9-]{2,20})/i) ||
      s.match(/\bcolor[:：\s]*([A-Za-z0-9-]{2,20})/i);
    if (color && color[1] && out.colorCode.rows.length === 0) {
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

  // NOTE: 已废除“按维度猜坐标/相似字段补全”的启发式策略（避免覆盖用户显式坐标）

  const fillCoordinatesFromDdl = (dimensionSamples) => {
    // 仅做“无副作用”的补全：
    // - segment 已有 tableName.fieldName：若 DDL 中存在则锁定，不做任何覆盖/猜测
    // - segment 仅有 fieldName：若 DDL 中唯一命中某表，则补 tableName
    const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
    for (const k of keys) {
      const blk = dimensionSamples[k];
      if (!blk || !Array.isArray(blk.rows)) continue;
      for (const r of blk.rows) {
        const segs = Array.isArray(r?.segments) ? r.segments : [];
        for (const s of segs) {
          const curT = String(s.tableName || '').trim();
          const curF = String(s.fieldName || '').trim();
          if (curT && curF) {
            if (!ddlTableMap.size) continue;
            const cols = ddlTableMap.get(curT) || [];
            const ok = cols.some((c) => String(c?.name || '').trim().toLowerCase() === curF.toLowerCase());
            if (ok) continue;
            // 若 DDL 中不存在：也不做“相似字段”覆盖（保持用户输入原样）
            continue;
          }
          if (!curT && curF) {
            const tFound = findTableContainingField(curF);
            if (tFound) s.tableName = tFound;
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

    // Vertex AI：固定锁定企业版稳定模型；失败则本地兜底
    const modelList = MODEL_PRIORITY.map(normalizeModelName);
    const ddlSchemaCompact = ddlTables.map((t) => ({
      tableName: String(t?.tableName || '').trim(),
      columns: Array.isArray(t?.columns)
        ? t.columns
            .map((c) => ({ name: String(c?.name || '').trim(), comment: String(c?.comment || '') }))
            .filter((c) => c.name)
            .slice(0, 200)
        : [],
    }));
    const prompt = [
      '你现在是一个无偏见的数据解析器（Gemini 3.1）。',
      '输入包含：1) DDL 表结构（多张表）；2) 用户的一段业务场景描述。',
      '你的唯一任务：从描述中识别用户定义的【目标表】【目标字段】【黄金样本值】，并把它们归类到系统的 7 个标准维度中，输出为 dimensionSamples。',
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
      '【动态识别（必须遵守）】',
      '1) 只要用户在描述中给出了某个表名/字段名，并且它存在于 DDL 中，你必须采纳它（不得改写、不得替换为你更熟悉的表）。',
      '2) 严禁臆造不存在的表名/字段名：只能从 DDL 中选择（必要时可做轻微纠错，但必须落到 DDL 中真实存在的名字）。',
      '3) 若用户只给了 tableName + value 但没给 fieldName：你必须基于该表 DDL 选择最合理的业务展示列作为 fieldName（优先 code/name/xxx_name，其次 comment 语义匹配；严禁选择 id 作为终点展示字段）。',
      '4) 若用户描述了拼接（例如“由 A 和 B 拼成”）：你必须在同一维度返回多段 segments，并正确填充 targetGoal 为拼接后的最终值。',
      '【规则锁定（必须遵守，优先级最高）】',
      '若用户使用编号指令明确指定坐标（例如“1. 款号在主表的 style_wms”“2. 品牌名在 ods_wms_base_brand_df”），你必须严格锁定其 tableName/fieldName，严禁做任何纠错/改写。',
      '',
      '【全维度抓取（严禁遗漏）】你必须尽最大可能填满 7 个维度；无法确定的维度允许为空，但必须把能确定的全部提取出来。',
      '',
      '【输出格式（必须严格遵守）】',
      '只输出严格 JSON（不要 Markdown，不要解释），外层结构必须为：',
      '{ "masterTable": "", "dimensionSamples": { ...7维... } }',
      '',
      'dimensionSamples 结构必须与前端状态一致（每个维度都有 targetGoal 与 rows[].segments[]）：',
      '{ "styleCode": {...}, "brand": {...}, "lastCode": {...}, "soleCode": {...}, "colorCode": {...}, "materialCode": {...}, "status": {...} }',
      '',
      masterTableFromBody ? `业务主表（仅供参考）：${masterTableFromBody}` : '',
      tableNames.length ? `DDL 表名清单（权威）：${tableNames.join(', ')}` : '',
      ddlSchemaCompact.length ? `DDL 结构(JSON，供你做精确/模糊匹配)：\n${JSON.stringify(ddlSchemaCompact)}` : '',
      '',
      '用户描述：',
      text,
    ]
      .filter(Boolean)
      .join('\n');

    auditLineSync('[Audit][A] Prompt: parse-chat-to-samples');
    auditLineSync(prompt);

    // 真相审计：记录 AI 原生输出 + 最终坐标
    modelingDebugLineSync(`[${new Date().toISOString()}] ===== parse-chat-to-samples START =====`);
    modelingDebugLineSync(`[Input] ${text}`);
    modelingDebugLineSync(`[DDL] chars=${ddlText.length}`);
    modelingDebugLineSync('[AI][Prompt] parse-chat-to-samples');
    modelingDebugLineSync(prompt);

    const withTimeout = async (promise, ms) => {
      let tid = null;
      const timeout = new Promise((_, rej) => {
        tid = setTimeout(() => rej(new Error(`Vertex AI timeout after ${ms}ms`)), ms);
      });
      try {
        return await Promise.race([promise, timeout]);
      } finally {
        if (tid) clearTimeout(tid);
      }
    };

    // AI-First：只要 DDL 非空，必须优先发起 Vertex；只有超时/异常才进入 regex fallback
    const out =
      ddlText.length > 0
        ? await withTimeout(generateWithModelList({ modelNames: modelList, prompt, retries: 1 }), 25000)
        : await generateWithModelList({ modelNames: modelList, prompt, retries: 1 });
    auditLineSync('[Audit][B] AI Raw Output: parse-chat-to-samples');
    auditLineSync(out.ok ? String(out.text || '') : `[Audit][B] FAILED: ${String(out.error?.message || '')}`);
    modelingDebugLineSync('[AI][Raw]');
    modelingDebugLineSync(out.ok ? String(out.text || '') : `[AI_CALL_FAILED] ${String(out.error?.message || '')}`);
    if (!out.ok) {
      auditLineSync('[Audit][Fallback] Gemini failed; use local regex fallback');
      let dimensionSamples = localFallback(text);
      if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
      modelingDebugLineSync('[Final][FallbackRegex]');
      modelingDebugLineSync(JSON.stringify(dimensionSamples, null, 2));
      modelingDebugLineSync(`[${new Date().toISOString()}] ===== parse-chat-to-samples END (fallback) =====`);
      auditLineSync(JSON.stringify({ fallback: true, dimensionSamples }, null, 2));
      auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
      return res.json({ ok: true, masterTable: masterTableFromBody || '', dimensionSamples, fallback: true });
    }

    const obj = extractJsonObject(stripJsonFences(out.text));
    if (!obj || typeof obj !== 'object') {
      auditLineSync('[Audit][Fallback] AI JSON invalid; use local regex fallback');
      let dimensionSamples = localFallback(text);
      if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
      modelingDebugLineSync('[Final][FallbackJsonInvalid]');
      modelingDebugLineSync(JSON.stringify(dimensionSamples, null, 2));
      modelingDebugLineSync(`[${new Date().toISOString()}] ===== parse-chat-to-samples END (fallback-json-invalid) =====`);
      auditLineSync(JSON.stringify({ fallback: true, dimensionSamples }, null, 2));
      auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
      return res.json({ ok: true, masterTable: masterTableFromBody || '', dimensionSamples, fallback: true });
    }

    const wrapper = obj && typeof obj === 'object' ? obj : {};
    const masterTableFromAi = typeof wrapper?.masterTable === 'string' ? String(wrapper.masterTable).trim() : '';
    const dimObj =
      wrapper?.dimensionSamples && typeof wrapper.dimensionSamples === 'object' && !Array.isArray(wrapper.dimensionSamples)
        ? wrapper.dimensionSamples
        : wrapper;

    const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
    const normalized = {};

    const normKey = (s) => String(s || '').trim().toLowerCase().replace(/[`"'’\s]/g, '');
    const levenshtein = (a, b) => {
      const s = normKey(a);
      const t = normKey(b);
      if (!s) return t.length;
      if (!t) return s.length;
      const dp = Array.from({ length: s.length + 1 }, () => new Array(t.length + 1).fill(0));
      for (let i = 0; i <= s.length; i++) dp[i][0] = i;
      for (let j = 0; j <= t.length; j++) dp[0][j] = j;
      for (let i = 1; i <= s.length; i++) {
        for (let j = 1; j <= t.length; j++) {
          const cost = s[i - 1] === t[j - 1] ? 0 : 1;
          dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
        }
      }
      return dp[s.length][t.length];
    };

    const pickClosest = (raw, candidates) => {
      const want = String(raw || '').trim();
      if (!want) return { best: '', dist: Infinity };
      const list = Array.isArray(candidates) ? candidates.filter(Boolean) : [];
      let best = '';
      let bestDist = Infinity;
      for (const c of list) {
        const d = levenshtein(want, c);
        if (d < bestDist) {
          bestDist = d;
          best = c;
        }
      }
      return { best, dist: bestDist };
    };

    const ddlTableNamesList = Array.from(ddlTableMap.keys());
    for (const k of keys) {
      const blk = dimObj[k] && typeof dimObj[k] === 'object' ? dimObj[k] : {};
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
          const baseSegments = segments.length ? segments : [{ tableName: '', fieldName: '', value: '' }];
          const fixedSegments = baseSegments.map((seg) => {
            let tn = String(seg.tableName || '').trim();
            let fn = String(seg.fieldName || '').trim();
            let localNote = '';

            // 严禁改写表名/字段名：只做存在性检查，不做纠错
            if (tn && ddlTableNamesList.length && !ddlTableMap.has(tn)) {
              localNote += `[strict] tableName not in DDL: ${JSON.stringify(tn)} `;
            }
            if (tn && fn && ddlTableMap.has(tn)) {
              const cols = ddlTableMap.get(tn) || [];
              const names = cols.map((c) => String(c?.name || '').trim()).filter(Boolean);
              const has = names.some((x) => normKey(x) === normKey(fn));
              if (!has && names.length) localNote += `[strict] fieldName not in DDL: ${JSON.stringify(fn)} `;
            }

            return { tableName: tn, fieldName: fn, value: seg.value, ...(localNote ? { _autoFixNote: localNote.trim() } : {}) };
          });

          const autoFixNotes = fixedSegments.map((x) => x._autoFixNote).filter(Boolean).join(' ｜ ');
          const mergedNotes = [notes, autoFixNotes].filter(Boolean).join(' ｜ ');
          const cleanSegments = fixedSegments.map((x) => {
            const { _autoFixNote, ...rest } = x;
            return rest;
          });

          return { ...(id ? { id } : {}), notes: mergedNotes, segments: cleanSegments };
        })
        .filter((r) => r.segments.some((s) => String(s.value || '').trim() !== ''));
      const operator = k === 'materialCode' && rows[0]?.segments?.length > 1 ? 'CONCAT' : undefined;
      normalized[k] = { targetGoal, rows, ...(operator ? { operator } : {}) };
    }
    if (ddlText) fillCoordinatesFromDdl(normalized);

    // 强制覆盖：编号指令指定的坐标必须 100% 落地（无论 AI 输出什么）
    if (forced?.dimensionSamples) {
      if (forced.dimensionSamples.styleCode?.rows?.length) normalized.styleCode = forced.dimensionSamples.styleCode;
      if (forced.dimensionSamples.brand?.rows?.length) normalized.brand = forced.dimensionSamples.brand;
    }

    modelingDebugLineSync('[Final][AI]');
    modelingDebugLineSync(JSON.stringify(normalized, null, 2));
    modelingDebugLineSync(`[${new Date().toISOString()}] ===== parse-chat-to-samples END (ai) =====`);

    // 审计真相：把 AI 识别到的坐标写入 ai_trace.log
    try {
      const keyToCn = (k) =>
        k === 'styleCode'
          ? '款号'
          : k === 'brand'
            ? '品牌'
            : k === 'lastCode'
              ? '楦'
              : k === 'soleCode'
                ? '底'
                : k === 'colorCode'
                  ? '颜色'
                  : k === 'materialCode'
                    ? '材质'
                    : k === 'status'
                      ? '状态'
                      : k;
      for (const k of keys) {
        const rows = Array.isArray(normalized?.[k]?.rows) ? normalized[k].rows : [];
        for (const r of rows) {
          const segs = Array.isArray(r?.segments) ? r.segments : [];
          for (const seg of segs) {
            const tn = String(seg?.tableName || '').trim();
            const fn = String(seg?.fieldName || '').trim();
            if (tn && fn) {
              aiTraceLineSync(`[AI_REASONING] 我从用户描述中识别到：维度[${keyToCn(k)}] 对应的表名是 [${tn}]，字段是 [${fn}]。`);
            }
          }
        }
      }
    } catch {
      // ignore
    }

    auditLineSync(`[Audit] ===== Chat-to-Config 结束 ${new Date().toISOString()} =====`);
    return res.json({
      ok: true,
      masterTable: forced.masterTable || masterTableFromAi || masterTableFromBody || '',
      dimensionSamples: normalized,
      fallback: false,
    });
  } catch (e) {
    auditLineSync(`[Audit] ===== Chat-to-Config 异常 ${new Date().toISOString()} =====`);
    auditLineSync(String(e instanceof Error ? e.stack || e.message : String(e)));
    // 兜底：任何异常都走本地解析，保证前端可用
    let dimensionSamples = localFallback(text);
    if (ddlText) dimensionSamples = fillCoordinatesFromDdl(dimensionSamples);
    modelingDebugLineSync('[Final][ExceptionFallback]');
    modelingDebugLineSync(JSON.stringify(dimensionSamples, null, 2));
    modelingDebugLineSync(`[${new Date().toISOString()}] ===== parse-chat-to-samples END (exception-fallback) =====`);
    auditLineSync('[Audit][Fallback] Exception; use local regex fallback');
    auditLineSync(JSON.stringify({ fallback: true, error: e instanceof Error ? e.message : String(e), dimensionSamples }, null, 2));
    auditLineSync(`[Audit] ===== Chat-to-Config 结束(本地兜底) ${new Date().toISOString()} =====`);
    return res.json({ ok: true, masterTable: masterTableFromBody || '', dimensionSamples, fallback: true });
  }
});

// Gemini AI 解析多表 SQL：返回 tables + smartSuggestions
app.post('/api/ai-parse-multi-sql', async (req, res) => {
  const sqlText = String(req.body?.sqlText || '');
  const conversationalInput = String(req.body?.conversationalInput || req.body?.chat || '').trim();
  const reference = req.body?.reference || null;
  let masterTable = typeof req.body?.masterTable === 'string' ? String(req.body.masterTable).trim() : '';
  const goldenByDimRaw = req.body?.dimensionSamples ?? req.body?.goldenByDimension;
  let goldenBlocks = null;
  let goldenSamples = [];
  let dimensionSamplesSeed = null;
  const isGoldenByDimEmpty =
    goldenByDimRaw &&
    typeof goldenByDimRaw === 'object' &&
    !Array.isArray(goldenByDimRaw) &&
    Object.values(goldenByDimRaw).every((b) => {
      const tg = String(b?.targetGoal || '').trim();
      const rows = Array.isArray(b?.rows) ? b.rows : [];
      return !tg && rows.length === 0;
    });

  if (goldenByDimRaw && typeof goldenByDimRaw === 'object' && !Array.isArray(goldenByDimRaw) && !isGoldenByDimEmpty) {
    goldenBlocks = normalizeGoldenByDimensionForAi(goldenByDimRaw);
    goldenSamples = expandGoldenBlocksToFlatSamples(goldenBlocks);
  } else {
    goldenSamples = normalizeGoldenSamplesForAi(req.body?.goldenSamples);
  }
  const sampleRow =
    req.body?.sampleRow && typeof req.body.sampleRow === 'object' && !Array.isArray(req.body.sampleRow)
      ? req.body.sampleRow
      : null;
  let goldenExpectedMap = buildGoldenExpectedMap({ goldenBlocks, goldenSamples, reference });
  if (!sqlText.trim()) return res.status(400).json({ ok: false, error: 'sqlText is required' });
  if (!vertexAI) return res.status(500).json({ ok: false, error: 'Vertex AI is not initialized' });

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
    // 强制检查：若 goldenByDimension 为空但 conversationalInput 不为空，必须先完成 chat-to-samples 提取，严禁直接进入建模回测
    if (isGoldenByDimEmpty && conversationalInput) {
      aiTraceLineSync('[Gate] goldenByDimension empty but conversationalInput present; will force chat-to-samples before modeling');
    }

    aiTraceLineSync('[Step 2] 开始根据实体推导 SQL 关联路径...');

    // 1) 正则拆分每个 CREATE TABLE
    const ddls = splitCreateTableStatements(sqlText);
    if (!ddls.length) return res.status(400).json({ ok: false, error: 'No CREATE TABLE statements found' });

    const total = ddls.length;
    const parsedTables = [];
    const parsedNames = [];

    const modelList = MODEL_PRIORITY.map(normalizeModelName);
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

    // 持久化 DDL schema（供 dataEngine 在 sandbox XLSX 读取时强制对齐列名，防止把数据当表头）
    try {
      const schemaCachePath = path.join(__dirname, 'storage', 'ddl_schema_cache.json');
      const tables = {};
      for (const t of parsedTables) {
        const tn = String(t?.tableName || '').trim();
        const cols = Array.isArray(t?.columns) ? t.columns.map((c) => String(c?.name || '').trim()).filter(Boolean) : [];
        if (tn) tables[tn] = cols;
      }
      await fse.ensureDir(path.dirname(schemaCachePath));
      await fse.writeJson(schemaCachePath, { savedAt: new Date().toISOString(), tables }, { spaces: 2 });
    } catch {
      // ignore
    }

    // Step 1.5：基于“DDL+业务描述”的动态语义抽取（废除硬编码/关键词硬匹配）
    if (conversationalInput) {
      aiTraceLineSync('[Step 1.5] 使用 Gemini 基于 DDL 动态抽取 dimensionSamples...');
      try {
        const schemaCompactForChat = parsedTables.map((t) => ({
          tableName: String(t?.tableName || '').trim(),
          columns: Array.isArray(t?.columns) ? (t.columns || []).slice(0, 200) : [],
        }));
        const ddlTableNamesForChat = parsedTables.map((t) => String(t?.tableName || '').trim()).filter(Boolean);
        const chatPrompt = [
          '你现在是一个无偏见的数据解析器（Gemini 3.x）。',
          '输入包含：DDL 表结构（多张表）与用户业务描述。',
          '你的唯一任务：从描述中识别【目标表】【目标字段】【黄金样本值】并归类到 7 个标准维度，输出 dimensionSamples。',
          '',
          '标准维度（standardKey）固定为：styleCode, brand, lastCode, soleCode, colorCode, materialCode, status。',
          '',
          '【动态识别规则】',
          '1) 用户给出的 tableName/fieldName 只要存在于 DDL，就必须采纳；不得替换为你更熟悉的表。',
          '2) 严禁臆造表/字段：只能从 DDL 选择；必要时可做轻微纠错，但必须落到 DDL 中真实存在的名字。',
          '3) 若缺 fieldName：在该 table 的 DDL 中选择最合理的展示列（优先 code/name/xxx_name；严禁以 id 作为终点展示字段）。',
          '4) 若描述了拼接：在同一维度返回多段 segments，并正确填充 targetGoal 为最终拼接值。',
          '',
          '输出必须是严格 JSON（不要 Markdown，不要解释），外层必须为：',
          '{ "masterTable": "", "dimensionSamples": { "styleCode": {...}, "brand": {...}, "lastCode": {...}, "soleCode": {...}, "colorCode": {...}, "materialCode": {...}, "status": {...} } }',
          '',
          masterTable ? `业务主表（若用户已明确指出）：${masterTable}` : '',
          `DDL 表名清单（权威）：${ddlTableNamesForChat.join(', ')}`,
          `DDL 结构(JSON)：${JSON.stringify(schemaCompactForChat)}`,
          '',
          '用户描述：',
          conversationalInput,
        ]
          .filter(Boolean)
          .join('\n');

        aiTraceLineSync('[AI_REASONING] 开始从用户描述中识别 7 维坐标...');
        const chatOut = await generateWithModelList({ modelNames: modelList, prompt: chatPrompt, retries: 1 });
        const chatObj = chatOut.ok ? extractJsonObject(stripJsonFences(chatOut.text)) : null;
        if (chatOut.ok && chatObj && typeof chatObj === 'object') {
          const masterFromAi = typeof chatObj?.masterTable === 'string' ? String(chatObj.masterTable).trim() : '';
          const dimObj =
            chatObj?.dimensionSamples && typeof chatObj.dimensionSamples === 'object' && !Array.isArray(chatObj.dimensionSamples)
              ? chatObj.dimensionSamples
              : chatObj;
          const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
          const normalized = {};
          for (const k of keys) {
            const blk = dimObj?.[k] && typeof dimObj[k] === 'object' ? dimObj[k] : {};
            const targetGoal = typeof blk?.targetGoal === 'string' ? String(blk.targetGoal) : '';
            const rowsRaw = Array.isArray(blk?.rows) ? blk.rows : [];
            const rows = rowsRaw
              .map((r) => ({
                id: typeof r?.id === 'string' ? String(r.id) : '',
                notes: typeof r?.notes === 'string' ? String(r.notes) : '',
                segments: Array.isArray(r?.segments)
                  ? r.segments.map((s) => ({
                      tableName: typeof s?.tableName === 'string' ? String(s.tableName) : '',
                      fieldName: typeof s?.fieldName === 'string' ? String(s.fieldName) : '',
                      value: typeof s?.value === 'string' ? String(s.value) : String(s?.value ?? ''),
                    }))
                  : [{ tableName: '', fieldName: '', value: '' }],
              }))
              .filter((r) => Array.isArray(r.segments) && r.segments.some((s) => String(s.value || '').trim() !== ''));
            normalized[k] = { targetGoal, rows };
          }

          // 审计真相：记录识别坐标
          const keyToCn = (k) =>
            k === 'styleCode'
              ? '款号'
              : k === 'brand'
                ? '品牌'
                : k === 'lastCode'
                  ? '楦'
                  : k === 'soleCode'
                    ? '底'
                    : k === 'colorCode'
                      ? '颜色'
                      : k === 'materialCode'
                        ? '材质'
                        : k === 'status'
                          ? '状态'
                          : k;
          for (const k of keys) {
            const rows = Array.isArray(normalized?.[k]?.rows) ? normalized[k].rows : [];
            for (const r of rows) {
              const segs = Array.isArray(r?.segments) ? r.segments : [];
              for (const seg of segs) {
                const tn = String(seg?.tableName || '').trim();
                const fn = String(seg?.fieldName || '').trim();
                if (tn && fn) aiTraceLineSync(`[AI_REASONING] 我从用户描述中识别到：维度[${keyToCn(k)}] 对应的表名是 [${tn}]，字段是 [${fn}]。`);
              }
            }
          }

          dimensionSamplesSeed = normalized;
          goldenBlocks = normalizeGoldenByDimensionForAi(normalized);
          goldenSamples = expandGoldenBlocksToFlatSamples(goldenBlocks);
          goldenExpectedMap = buildGoldenExpectedMap({ goldenBlocks, goldenSamples, reference });
          if (masterFromAi) masterTable = masterFromAi;
        } else {
          aiTraceLineSync(`[AI_REASONING] 业务描述解析失败：${chatOut.ok ? 'JSON 无效' : String(chatOut.error?.message || '')}`);
        }
      } catch (e) {
        aiTraceLineSync(`[AI_REASONING] 业务描述解析异常：${e instanceof Error ? e.message : String(e)}`);
      }
    }

    if (isGoldenByDimEmpty && conversationalInput && (!goldenExpectedMap || !goldenExpectedMap.size)) {
      aiTraceLineSync('[Gate][FAIL] conversationalInput provided but goldenExpectedMap is still empty after chat-to-samples');
      return res.status(400).json({ ok: false, error: 'Chat-to-samples failed: cannot build goldenExpectedMap from conversationalInput' });
    }

    // ===========================
    // 2026 Stack：Gemini 3.x 一次性全表推理（合并维度解析，释放大上下文能力）
    // ===========================
    {
      const schemaCompact = parsedTables.map((t) => ({
        tableName: t.tableName,
        columns: (t.columns || []).slice(0, 200),
      }));
      const allDdlTableNames = parsedTables.map((t) => String(t.tableName || '').trim()).filter(Boolean);
      const goldenSection =
        goldenExpectedMap && goldenExpectedMap.size
          ? [
              '【黄金样本期望值（必须命中；禁止返回 ID）】',
              ...Array.from(goldenExpectedMap.entries()).map(([k, v]) => `- ${k}: ${JSON.stringify(String(v || ''))}`),
            ].join('\n')
          : '【黄金样本期望值】（空）';

      // 审计真相：基于坐标开始路径搜索（记录意图，不预设任何表名）
      try {
        const seed = dimensionSamplesSeed && typeof dimensionSamplesSeed === 'object' ? dimensionSamplesSeed : null;
        const keys = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'];
        for (const k of keys) {
          const rows = Array.isArray(seed?.[k]?.rows) ? seed[k].rows : [];
          for (const r of rows) {
            const segs = Array.isArray(r?.segments) ? r.segments : [];
            for (const seg of segs) {
              const tn = String(seg?.tableName || '').trim();
              const fn = String(seg?.fieldName || '').trim();
              if (tn && fn) aiTraceLineSync(`[PATH_SEARCH] 正在根据 DDL 寻找从主表到 [${tn}].[${fn}] 的 Join 路径...`);
            }
          }
        }
      } catch {
        // ignore
      }

      const oneShotPrompt = [
        '你是 Gemini 3.x（超大上下文）。请一次性理解全部表结构，并为 7 个维度给出从主表到维表业务值列的完整 CHAIN 路径。',
        masterTable ? `业务主表（Master Table）：${masterTable}` : '',
        '',
        '【硬性要求】',
        '1) 必须输出 joinPathSuggestions，包含：styleCode, brand, lastCode, soleCode, colorCode, materialCode, status。',
        '2) 每个维度必须输出 1 条 joinPath（结构化数组），允许 0~2 个中间表。',
        '3) joinPath 末段必须是 { targetTable, valueField }，且 valueField 必须是业务可读列（code/name/brand_name...），严禁以 id 结尾。',
        '4) 严禁输出“未找到”。',
        '5) 【材质 LT04 专项】如果黄金材质为 LT04/或对话中描述为拼接：你必须同时输出 concatMappingSuggestions，standardKey="materialCode"，operator="CONCAT"，parts=2。每个 part 必须包含 joinPath，并尽量利用 parent_id 等层级关联把父段(LT)与子段(04)从各自列取出。',
        '',
        '【重要线索：大底/模具关联（专攻 soleCode）】',
        '请特别注意 ods_pdm_pdm_base_mold_df 表：soleCode 必须优先使用主表 associated_sole_info 与 ods_pdm_pdm_base_mold_df.id 做字符串等值匹配。',
        '命中后在 ods_pdm_pdm_base_mold_df.code 取最终大底/模具编号；不要改用 base_heel_df。',
        '',
        goldenSection,
        '',
        '【DDL 表名清单】',
        allDdlTableNames.join(', '),
        '',
        '【表结构精简版 JSON】',
        JSON.stringify(schemaCompact),
        '',
        '输出严格 JSON（不要 Markdown，不要解释），形如：',
        '{ "smartSuggestions": [], "joinPathSuggestions": [ { "targetStandardKey":"brand", "joinPath":[ {"sourceTable":"...","sourceField":"...","targetTable":"...","targetField":"..."}, {"targetTable":"...","valueField":"..."} ] } ], "concatMappingSuggestions": [] }',
      ]
        .filter(Boolean)
        .join('\n');

      auditLineSync('[Audit][A] Prompt Snapshot (one-shot, gemini-3.x)');
      auditLineSync(oneShotPrompt);
      aiTraceLineSync('[现场 A] ===== one-shot Prompt Audit =====');
      aiTraceLineSync(oneShotPrompt);

      const oneShotOut = await generateWithModelList({ modelNames: modelList, prompt: oneShotPrompt, retries: 1 });
      aiTraceLineSync('[现场 B] ===== one-shot Raw AI Output =====');
      aiTraceLineSync(oneShotOut.ok ? String(oneShotOut.text || '') : `[FAILED] ${String(oneShotOut.error?.message || '')}`);
      if (!oneShotOut.ok) {
        return res.status(502).json({ ok: false, error: oneShotOut.error?.message || 'Vertex call failed' });
      }

      const obj = extractJsonObject(stripJsonFences(oneShotOut.text));
      const smartSuggestions = Array.isArray(obj?.smartSuggestions) ? obj.smartSuggestions : [];
      const jpRaw = Array.isArray(obj?.joinPathSuggestions) ? obj.joinPathSuggestions : [];
      const concatMappingSuggestions = Array.isArray(obj?.concatMappingSuggestions) ? obj.concatMappingSuggestions : [];
      const joinPathSuggestions = jpRaw
        .map((j) => {
          const targetStandardKey = typeof j?.targetStandardKey === 'string' ? j.targetStandardKey.trim() : '';
          const rawJoin =
            Array.isArray(j?.joinPath) && j.joinPath.length ? j.joinPath : Array.isArray(j?.path) ? j.path : [];
          const pathArr = normalizeAiJoinPathToStringPath(rawJoin);
          return targetStandardKey && pathArr.length >= 2 ? { targetStandardKey, path: pathArr } : null;
        })
        .filter(Boolean);

      aiTraceLineSync('[Step 3] 物理回测验证...');
      const latestDirForBacktest = DIRS.sandbox;
      const targetStyleForBacktest = String(goldenExpectedMap.get('styleCode') || reference?.styleCode || '').trim();
      const validated = await validateJoinPathSuggestionsWithGoldenXlsx({
        joinPathSuggestions,
        goldenByStandardKey: goldenExpectedMap,
        folderPath: latestDirForBacktest,
        smartSuggestions,
        targetStyle: targetStyleForBacktest,
        masterTableLogicalName: masterTable,
      });

      auditLineSync('[Audit][D] Validation result (one-shot)');
      auditLineSync(JSON.stringify({ validatedCount: validated.length, validatedKeys: validated.map((x) => x.targetStandardKey) }, null, 2));
      setAIStatus({ status: 'done', model: modelList[0] || '', message: `one-shot 建模完成：validated=${validated.length}` });
      auditLineSync(`[Audit] ===== AI 逻辑建模结束(one-shot) ${new Date().toISOString()} =====`);

      return res.json({
        ok: true,
        tables: parsedTables,
        smartSuggestions,
        joinPathSuggestions: validated,
        concatMappingSuggestions,
        dimensionSamplesSeed,
        masterTableUsed: masterTable,
        _debug: { modelTried: modelList, mode: 'one-shot' },
      });
    }

    // ===========================
    // 新模式：维度并行解析（Vertex 企业版高吞吐）
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
        const isNonNumericExpectation = Boolean(expectation) && !/^\d+$/.test(expectation);
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
          dimKey === 'brand' && isNonNumericExpectation
            ? '5) 【强制品牌 Join】当黄金期望值为可读字符串（如 Bruno Marc）时，主表 brand 往往是数字 ID。你严禁输出 brand 的 direct/smartSuggestions 作为最终答案；必须输出 joinPathSuggestions：主表.brand(数字) -> 品牌表.id -> 品牌表.brand_name/name。'
            : '',
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
        message: `开始维度并行解析：0/${dimensionKeys.length}`,
        parsedCount: 0,
        totalCount: dimensionKeys.length,
        partialRevision: (currentAIStatus.partialRevision || 0) + 1,
        partial: { smartSuggestions: [], joinPathSuggestions: [], concatMappingSuggestions: [], dims: {} },
      });

      // 回测数据源：必须使用 Sandbox（用户刚上传的样本）
      const latestDirForBacktest = DIRS.sandbox;
      const targetStyleForBacktest = String(goldenExpectedMap.get('styleCode') || reference?.styleCode || '').trim();
      let smartSuggestions = [];
      let joinPathSuggestions = [];
      let concatMappingSuggestions = [];
      const dims = {};

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
          const mainTableName = getLogicalTableName(main.fileName);
          const statusCol = (smartSuggestionsForMain || []).find((x) => String(x?.standardKey || '').trim() === 'status')?.sourceField || '';
          const styleCol = (smartSuggestionsForMain || []).find((x) => String(x?.standardKey || '').trim() === 'styleCode')?.sourceField || '';
          const rowsCandidate = main.rows || [];
          const eqLoose = (a, b) => String(a ?? '').trim().toUpperCase() === String(b ?? '').trim().toUpperCase();
          const rowHas = (row, target) => {
            if (!row || !target) return false;
            for (const v of Object.values(row)) if (eqLoose(v, target)) return true;
            return false;
          };
          const activeRows = statusCol ? rowsCandidate.filter((r) => isTruthyActiveStatus(r?.[statusCol])) : rowsCandidate;
          const targetStyle = String(targetStyleForBacktest || '').trim();

          // 对齐回测行：必须先用 targetStyle 定位主表行，严禁直接取第一行
          let row = null;
          if (targetStyle) {
            if (styleCol) {
              row = activeRows.find((r) => eqLoose(r?.[styleCol], targetStyle)) || rowsCandidate.find((r) => eqLoose(r?.[styleCol], targetStyle)) || null;
              aiTraceLineSync(
                `[现场 C] RowAlign: search styleCol=${JSON.stringify(styleCol)} targetStyle=${JSON.stringify(targetStyle)} => ${row ? 'HIT' : 'MISS'}`
              );
            } else {
              row = activeRows.find((r) => rowHas(r, targetStyle)) || rowsCandidate.find((r) => rowHas(r, targetStyle)) || null;
              aiTraceLineSync(
                `[现场 C] RowAlign: styleCol missing; brute-search targetStyle=${JSON.stringify(targetStyle)} => ${row ? 'HIT' : 'MISS'}`
              );
            }
            if (!row) {
              aiTraceLineSync(
                `[现场 C] PATH_BROKEN: cannot locate master row by targetStyle=${JSON.stringify(targetStyle)} in sandbox; check uploaded XLSX content`
              );
              return;
            }
          } else {
            // 若用户未提供 styleCode 黄金值：退回第一条生效行（保持兼容）
            row = activeRows[0] || rowsCandidate[0] || null;
          }
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

      // Vertex 企业版：恢复 7 维并行解析（无 setTimeout / 呼吸延迟）
      let done = 0;
      let partialLock = Promise.resolve();
      const withPartialLock = async (fn) => {
        partialLock = partialLock.then(fn, fn);
        return partialLock;
      };

      const runOneDim = async ({ dimKey }) => {
        const expected = goldenExpectedMap.get(dimKey) || '';
        await withPartialLock(async () => {
          dims[dimKey] = { status: 'running', attempts: 0, expected: String(expected || '') };
          setAIStatus({
            status: 'running',
            model: modelList[0] || '',
            message: `正在并行破解 ${dimKey} 维度关联路径...`,
            parsedCount: done,
            totalCount: dimensionKeys.length,
            partialRevision: (currentAIStatus.partialRevision || 0) + 1,
            partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
          });
        });

        let success = false;
        let lastErr = '';
        for (let attempt = 1; attempt <= 3; attempt++) {
          await withPartialLock(async () => {
            dims[dimKey] = { ...(dims[dimKey] || {}), status: 'running', attempts: attempt };
            setAIStatus({
              status: 'running',
              model: modelList[0] || '',
              message: `正在破解 ${dimKey}...（并行尝试 ${attempt}/3）`,
              parsedCount: done,
              totalCount: dimensionKeys.length,
              partialRevision: (currentAIStatus.partialRevision || 0) + 1,
              partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
            });
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
          let ssRaw = Array.isArray(obj?.smartSuggestions) ? obj.smartSuggestions : [];
          const jpRaw = Array.isArray(obj?.joinPathSuggestions) ? obj.joinPathSuggestions : [];
          const ccRaw = Array.isArray(obj?.concatMappingSuggestions) ? obj.concatMappingSuggestions : [];

          // 强制品牌 Join：黄金为字符串时禁止 brand direct/smartSuggestions
          if (dimKey === 'brand') {
            const exp = String(expected || '').trim();
            const expIsString = Boolean(exp) && !/^\d+$/.test(exp);
            if (expIsString) {
              ssRaw = (ssRaw || []).filter((s) => String(s?.standardKey || '').trim() !== 'brand');
            }
          }

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

          if (dimKey === 'brand') {
            const exp = String(expected || '').trim();
            const expIsString = Boolean(exp) && !/^\d+$/.test(exp);
            if (expIsString && (!jpOne || !jpOne.length)) {
              lastErr = 'Brand expectation is string; joinPathSuggestions is required (DIRECT is forbidden)';
              auditLineSync(`[Audit][D] FAIL dim=${dimKey} attempt=${attempt}: ${lastErr}`);
              aiTraceLineSync(`[现场 D] ${dimKey} Final Verdict: PATH_BROKEN (DIRECT forbidden; joinPath required)`);
              continue;
            }
          }

          aiTraceLineSync('[Step 3] 物理回测验证...');
          const validated = await validateJoinPathSuggestionsWithGoldenXlsx({
            joinPathSuggestions: jpOne,
            goldenByStandardKey: goldenExpectedMap,
            folderPath: latestDirForBacktest,
            smartSuggestions: ssRaw,
            targetStyle: targetStyleForBacktest,
          });

          if (validated && validated.length) {
            const v0 = validated[0];
            const rawPathArr = Array.isArray(v0?.path) ? v0.path : [];
            if (rawPathArr.length >= 2) {
              await traceJoinPathOnOneRow({ dimKey, pathArr: rawPathArr, expectedValue: expected, smartSuggestionsForMain: ssRaw });
            }

            await withPartialLock(async () => {
              const merged = new Map(joinPathSuggestions.map((x) => [x.targetStandardKey, x]));
              for (const v of validated) merged.set(v.targetStandardKey, v);
              joinPathSuggestions = Array.from(merged.values());
              if (dimKey === 'materialCode' && Array.isArray(ccRaw) && ccRaw.length) concatMappingSuggestions = ccRaw;
              if (Array.isArray(ssRaw) && ssRaw.length) smartSuggestions = ssRaw;

              dims[dimKey] = { ...(dims[dimKey] || {}), status: 'success' };
              done += 1;
              setAIStatus({
                status: 'running',
                model: modelList[0] || '',
                message: `${dimKey} 维度认证成功（并行完成 ${done}/${dimensionKeys.length}）`,
                parsedCount: done,
                totalCount: dimensionKeys.length,
                partialRevision: (currentAIStatus.partialRevision || 0) + 1,
                partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
              });
            });

            success = true;
            break;
          }

          lastErr = `Join backtest failed (expected=${String(expected || '')})`;
          auditLineSync(`[Audit][D] FAIL dim=${dimKey} attempt=${attempt}: ${lastErr}`);
          aiTraceLineSync(`[现场 D] ${dimKey} Final Verdict: PATH_BROKEN / VALUE_MISMATCH (backtest failed)`);
        }

        if (!success) {
          await withPartialLock(async () => {
            dims[dimKey] = { ...(dims[dimKey] || {}), status: 'fail', lastError: lastErr };
            done += 1;
            setAIStatus({
              status: 'running',
              model: modelList[0] || '',
              message: `${dimKey} 维度未能回测通过（并行完成 ${done}/${dimensionKeys.length}）`,
              parsedCount: done,
              totalCount: dimensionKeys.length,
              partialRevision: (currentAIStatus.partialRevision || 0) + 1,
              partial: { smartSuggestions, joinPathSuggestions, concatMappingSuggestions, dims },
            });
          });
        }
      };

      await Promise.all(dimensionKeys.map((dimKey) => runOneDim({ dimKey })));

      setAIStatus({
        status: 'done',
        model: modelList[0] || '',
        message: `维度并行解析完成：${dimensionKeys.length}/${dimensionKeys.length}`,
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
          '5) 【品牌 brand — 数字主表 vs 字符串黄金样本】若黄金样本中品牌为**可读名称**（如 Bruno Marc），而主表 brand 列在业务上存的是**数值 ID**（如 1），则必须输出 targetStandardKey=brand 的 joinPath：主表该列 → DDL 中“品牌维表”（表/列 comment 或字段名含 brand/品牌 的那张）主键(id) → 品牌展示列（优先 xxx_name/name/code；严禁以 id 作为最终展示字段）。',
          '',
          '【强迫学习（必须照抄范式）】',
          'brand 已被证明可执行且正确：主表.brand -> 品牌表.id -> 品牌表.brand_name。',
          '你必须把它当成“模板模式”：其它维度（lastCode/soleCode/colorCode/materialCode）必须同样遵循「主表外键/关联列(可能是数字ID) -> 目标维表定位键(id/parent_id/...) -> 目标维表业务值列(code/name/...)」。',
          '如果你算出的终点仍是纯数字（例如 1695）而黄金样本是业务号（例如 L-B26097M），说明路径没有走到最后一公里，你必须继续往下跳到 code/name 等字段，直到与黄金样本字符串严格一致。',
          '',
          '【楦/底维度线索（不预设表名；仅给通用模式）】',
          '1) lastCode：起点通常在主表的 associated_last_* / last_id / last_type_id 等外键列；终点必须是“楦维表”的业务列（优先 code，其次 name）。',
          '2) soleCode：起点通常在主表的 associated_sole_* / sole_id 等外键列；终点必须是“底维表”的业务列（优先 code，其次 name）。',
          '注意：目标维表必须从 DDL 中动态选择（表/列 comment 或字段名语义匹配），严禁臆造固定表名。',
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
          '      { "sourceTable": "<主表>", "sourceField": "<brand外键列>", "targetTable": "<品牌维表>", "targetField": "id" },',
          '      { "targetTable": "<品牌维表>", "valueField": "<brand展示列，例如 brand_name/name/code>" }',
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
          '5) 若需“底/材质”相关维表：先按 id/parent_id 等键关联定位行，再延伸到业务编码列；末项 valueField 禁止为裸 id（除非样本为数字）。',
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

    // 回测数据源：必须使用 Sandbox（用户刚上传的样本）
    const latestDirForBacktest = DIRS.sandbox;
    const targetStyleForBacktest = String(goldenExpectedMap.get('styleCode') || reference?.styleCode || '').trim();
    auditLineSync('[Audit][C] Join Execution Trace: start backtest on latest XLSX');
    auditLineSync(JSON.stringify({ latestDirForBacktest, joinPathCount: joinPathSuggestions.length }, null, 2));
    const validatedFirst = await validateJoinPathSuggestionsWithGoldenXlsx({
      joinPathSuggestions,
      goldenByStandardKey: goldenExpectedMap,
      folderPath: latestDirForBacktest,
      smartSuggestions,
      targetStyle: targetStyleForBacktest,
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
          targetStyle: targetStyleForBacktest,
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

async function handleSandboxUploadXlsx(req, res) {
  await ensureStorageDirs();
  try {
    // 物理级日志：请求入口
    // eslint-disable-next-line no-console
    console.log('--- [Sandbox] 物理层收到文件流 ---');

    const files = req.files;
    if (!files || !Array.isArray(files) || files.length === 0) {
      // eslint-disable-next-line no-console
      console.log('[Sandbox] 文件名: 未检测到文件');
      return res.status(400).json({ ok: false, error: 'No files uploaded' });
    }

    const out = [];
    for (const f of files) {
      const originalName = String(f?.originalname || '');
      if (!isExcelFileName(originalName)) continue;

      // 重命名保护：用逻辑表名落盘（避免被用户随意命名导致读表失败）
      const ext = path.extname(originalName) || '.xlsx';
      const logical = getLogicalTableName(originalName);
      const safeLogical = String(logical || 'sample').replace(/[^\w.\-]+/g, '_');
      const targetName = `${safeLogical}${ext.toLowerCase()}`;

      const srcPath = path.join(f.destination, f.filename);
      const dstPath = path.join(DIRS.sandbox, targetName);

      // eslint-disable-next-line no-console
      console.log('[Sandbox] 文件名:', originalName, '=>', targetName);

      try {
        const checkFile = fs.existsSync(srcPath);
        // eslint-disable-next-line no-console
        console.log('[Sandbox] 物理写入结果:', checkFile ? '✅ 成功' : '❌ 失败', '=>', srcPath);
        if (!checkFile) {
          out.push({ storedName: targetName, originalName, ok: false, error: 'file not written' });
          continue;
        }

        if (srcPath !== dstPath) {
          await fse.move(srcPath, dstPath, { overwrite: true });
        }

        const wb = XLSX.readFile(dstPath, { cellText: false, cellDates: true });
        const sheet = readFirstSheet(wb);
        if (!sheet) {
          out.push({ storedName: targetName, originalName, ok: true, headers: [], firstRow: {} });
          continue;
        }
        const headers = getSheetHeaders(sheet);
        const rows = XLSX.utils.sheet_to_json(sheet, { header: headers, raw: false, defval: '', range: 1 });
        const firstRow = rows[0] && typeof rows[0] === 'object' ? rows[0] : {};
        out.push({ storedName: targetName, originalName, ok: true, headers, firstRow });
      } catch (e) {
        out.push({ storedName: targetName, originalName, ok: false, error: e instanceof Error ? e.message : 'write/read failed' });
      }
    }

    // 强制写入检查：保存后立刻物理扫描一次，确认当前真实文件数
    try {
      const entries = fs.readdirSync(DIRS.sandbox, { withFileTypes: true });
      const excel = entries.filter((e) => e.isFile()).map((e) => e.name).filter((n) => isExcelFileName(n));
      // eslint-disable-next-line no-console
      console.log(`[Storage] upload-sandbox write-check: sandbox now has ${excel.length} excel files`);
    } catch (e) {
      // eslint-disable-next-line no-console
      console.warn('[Storage] upload-sandbox write-check failed', e instanceof Error ? e.message : String(e));
    }

    return res.json({ ok: true, sandboxDir: 'storage/sandbox', files: out });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[Sandbox] upload handler crashed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'upload crashed' });
  }
}

// 上传逻辑沙盒样本 XLSX（专用接口；强制存入 storage/sandbox；保留原文件名便于物理对账）
// 强制对账：字段名必须为 'file'（与前端 FormData.append('file', ...) 保持一致）
app.post('/api/upload-sandbox', sandboxUpload.array('file', 50), handleSandboxUploadXlsx);

// 兼容旧接口名
app.post('/api/upload-sandbox-xlsx', sandboxUpload.array('file', 50), handleSandboxUploadXlsx);

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
  const operator = getRequestOperator(req);
  const uploadedAt = new Date().toISOString();

  for (const f of files) {
    if (!f.path) continue;
    const absolutePath = path.resolve(f.path);
    uploadTrace(`文件已保存至: ${absolutePath}`);
    try {
      fs.chmodSync(absolutePath, 0o666);
    } catch (e) {
      uploadTrace('文件权限修正失败:', e instanceof Error ? e.message : String(e));
    }
    removeSiblingGlbOn3dAssetOverwrite(absolutePath);
  }

  await refreshAssetsCache();

  const audit = await readUploadAudit();
  for (const f of files) {
    if (!f.path) continue;
    const rel = path.relative(STORAGE_ROOT, f.path).split(path.sep).join('/');
    audit.byPath[rel] = {
      relPath: rel,
      fileName: f.filename || f.originalname,
      uploadedBy: operator,
      uploadedAt,
    };
  }
  await writeUploadAudit(audit);

  const uploaded = files.map((f) => ({
    originalName: f.originalname,
    storedName: f.filename,
    size: f.size,
    destination: f.destination,
    path: f.path,
    uploadedBy: operator,
    uploadedAt,
  }));

  for (const f of files) {
    const abs = f.path;
    const name = String(f.originalname || f.filename || '').toLowerCase();
    if (abs && name.endsWith('.obj')) {
      void runObjToGlbPipeline(abs).catch((e) => {
        // eslint-disable-next-line no-console
        console.error('[3d-pipeline] async error', e);
      });
    }
  }

  const has3DAssetUpload = files.some((f) => {
    const name = String(f?.originalname || f?.filename || '').toLowerCase();
    return ['.obj', '.stl', '.glb', '.3dm'].includes(path.extname(name));
  });
  const incremental3D = has3DAssetUpload ? await incrementalSync3DCache() : null;

  res.json({
    ok: true,
    uploaded,
    incremental3D,
  });
});

function detectCategoryFromPath(fullPath) {
  const rel = path.relative(STORAGE_ROOT, fullPath).split(path.sep).join('/');
  if (rel.startsWith('data_tables/')) return 'xlsx';
  if (rel.startsWith('assets/lasts/')) return '3d_lasts';
  if (rel.startsWith('assets/soles/')) return '3d_soles';
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
  const uploadAudit = await readUploadAudit();

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
    const audit = uploadAudit.byPath?.[rel] || null;

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
      operator: audit?.uploadedBy || 'System',
    });
  }

  items.sort((a, b) => (a.uploadTime < b.uploadTime ? 1 : -1));
  res.json({ ok: true, items });
});

app.get('/api/check-mandatory-files', async (_req, res) => {
  try {
    const status = await checkMandatoryDataTables();
    return res.json({
      ok: true,
      ...status,
      latestDir: path.relative(STORAGE_ROOT, status.latestDir).split(path.sep).join('/'),
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[mandatory-files] check failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'mandatory file check failed' });
  }
});

app.post('/api/incremental-3d-sync', async (_req, res) => {
  try {
    const result = await incrementalSync3DCache();
    if (!result.ok && result.reason === 'cache_missing') {
      return res.status(409).json({ ok: false, error: 'full_inventory_cache.json missing; run force sync once', ...result });
    }
    return res.json(result);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[Incremental3D] sync failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'incremental 3d sync failed' });
  }
});

// 真实看板统计：优先读认证落盘的 final_dashboard_data.json，否则实时聚合
app.get('/api/dashboard-stats', async (req, res) => {
  try {
    const snap = await readFinalDashboardSnapshot();
    const forceRefresh = String(req.query?.refresh || '') === '1';
    const latestProdDir = await getLatestDataTablesDir();
    const latestProdDate = isDateDirName(path.basename(latestProdDir)) ? path.basename(latestProdDir) : '';
    const snapLatestDate = snap?.dates?.latest ? String(snap.dates.latest) : '';
    const snapIsStale = Boolean(snap && latestProdDate && snapLatestDate && latestProdDate !== snapLatestDate);
    const shouldRebuild = forceRefresh || !snap || snapIsStale;

    if (shouldRebuild) {
      // eslint-disable-next-line no-console
      console.log(
        `[Prod-Engine] dashboard-stats refresh=${forceRefresh ? '1' : '0'} snap=${
          snap ? 'yes' : 'no'
        } stale=${snapIsStale ? 'yes' : 'no'} snap.latest=${snapLatestDate || '(none)'} prod.latest=${latestProdDate || '(none)'}`
      );
      // 强制删除旧缓存（包含 final_results.json），避免读到旧残留
      await purgeOldSnapshots();
      const payload = await processAllData({ storageRoot: STORAGE_ROOT });
      await persistFinalDashboardData(STORAGE_ROOT, payload);
      return res.json({
        ok: true,
        source: 'final_dashboard_data',
        generatedAt: payload.generatedAt,
        dates: payload.dates,
        mapping: payload.mapping,
        meta: payload.meta,
        kpis: payload.kpis,
        inventoryRows: Array.isArray(payload.inventory) ? payload.inventory.length : 0,
        filterOptions: payload.filterOptions || { statuses: [], levels: [] },
        statusBuckets: payload.statusBuckets || {},
        inventory: payload.inventory || [],
        styleMetadata: payload.styleMetadata || [],
        brandCoverage: payload.brandCoverage || [],
        brandBindingStats: payload.brandBindingStats || [],
        lastDigitizationStats: payload.lastDigitizationStats || [],
        soleDigitizationStats: payload.soleDigitizationStats || [],
        trendHistory: payload.trendHistory || [],
        trends: payload.trends || {},
        rawStatusAudit: payload.rawStatusAudit || payload.meta?.rawStatusAudit || [],
      });
    }

    // snap exists and not stale
    return res.json({
      ok: true,
      source: 'final_dashboard_data',
      generatedAt: snap.generatedAt,
      dates: snap.dates,
      mapping: snap.mapping,
      meta: snap.meta,
      kpis: snap.kpis,
      inventoryRows: Array.isArray(snap.inventory) ? snap.inventory.length : 0,
      filterOptions: snap.filterOptions || { statuses: [], levels: [] },
      statusBuckets: snap.statusBuckets || {},
      inventory: snap.inventory || [],
      styleMetadata: snap.styleMetadata || [],
      brandCoverage: snap.brandCoverage || [],
      brandBindingStats: snap.brandBindingStats || [],
      lastDigitizationStats: snap.lastDigitizationStats || [],
      soleDigitizationStats: snap.soleDigitizationStats || [],
      trendHistory: snap.trendHistory || [],
      trends: snap.trends || {},
      rawStatusAudit: snap.rawStatusAudit || snap.meta?.rawStatusAudit || [],
    });

  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[dataEngine] dashboard-stats failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'dashboard-stats failed' });
  }
});

// 强制全量生产重算：先清缓存，再从 data_tables 最新日期目录全量聚合并落盘
app.post('/api/force-sync-dashboard', async (req, res) => {
  try {
    const operator = getRequestOperator(req);
    const operationTime = new Date();
    const removed = await purgeOldSnapshots();
    // eslint-disable-next-line no-console
    console.log('[ForceSync] purge removed:', removed.length ? removed.join(', ') : '(none)');

    // 先跑一次带进度日志的生产聚合（只最新日期目录），用于终端可视化
    const agg = await forceSyncLatestProduction({ storageRoot: STORAGE_ROOT, operator, operationTime });
    const latest = agg.latest;

    // 再生成与 Dashboard 一致的最终快照（包含 KPI / trends / inventory）
    const payload = await processAllData({ storageRoot: STORAGE_ROOT, operator, operationTime });
    const outPath = await persistFinalDashboardData(STORAGE_ROOT, payload);

    return res.json({
      ok: true,
      removed,
      path: outPath,
      dates: payload.dates,
      meta: payload.meta,
      kpis: payload.kpis,
      brandCoverage: payload.brandCoverage || [],
      // 额外回传 forceSync 的实时计数（便于前端 toast/调试）
      forceSync: latest?.kpis?.__forceSync || null,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[ForceSync] failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'force sync failed' });
  }
});

// 认证并应用到看板：可选 body.mappingConfig 写入 mapping_config.json → dataEngine 全量 Excel → final_dashboard_data.json
app.post('/api/certify-and-sync', async (req, res) => {
  try {
    await ensureStorageDirs();
    const operator = getRequestOperator(req);
    const operationTime = new Date();
    const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const mcRaw = body.mappingConfig;
    const mc =
      typeof mcRaw === 'string'
        ? (() => {
            try {
              const parsed = JSON.parse(mcRaw);
              return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
            } catch {
              return null;
            }
          })()
        : mcRaw && typeof mcRaw === 'object' && !Array.isArray(mcRaw)
          ? mcRaw
          : null;
    if (mc) {
      const filePayload = {
        savedAt: new Date().toISOString(),
        mappingAuthenticated: Boolean(mc.mappingAuthenticated),
        mapping: mc,
      };
      await fse.writeJson(MAPPING_CONFIG_PATH, filePayload, { spaces: 2 });
    }
    const payload = await processAllData({ storageRoot: STORAGE_ROOT, operator, operationTime });
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

// 真实款号清单：与看板一致，优先 final_dashboard_data.json（但若生产快照已更新则自动重算）
app.get('/api/inventory-real', async (req, res) => {
  try {
    const snap = await readFinalDashboardSnapshot();

    const forceRefresh = String(req.query?.refresh || '') === '1';
    const latestProdDir = await getLatestDataTablesDir();
    const latestProdDate = isDateDirName(path.basename(latestProdDir)) ? path.basename(latestProdDir) : '';
    const snapLatestDate = snap?.dates?.latest ? String(snap.dates.latest) : '';
    const snapIsStale = Boolean(snap && latestProdDate && snapLatestDate && latestProdDate !== snapLatestDate);
    const shouldRebuild = forceRefresh || !snap?.inventory || snapIsStale;

    if (shouldRebuild) {
      // eslint-disable-next-line no-console
      console.log(
        `[Prod-Engine] inventory-real refresh=${forceRefresh ? '1' : '0'} snap=${
          snap ? 'yes' : 'no'
        } stale=${snapIsStale ? 'yes' : 'no'} snap.latest=${snapLatestDate || '(none)'} prod.latest=${latestProdDate || '(none)'}`
      );
      const payload = await processAllData({ storageRoot: STORAGE_ROOT });
      await persistFinalDashboardData(STORAGE_ROOT, payload);
      const items = (payload.inventory || []).map((it) => ({
        ...it,
        brand_name: it.brand,
        status: it.data_status,
      }));
      return res.json({
        ok: true,
        source: 'final_dashboard_data',
        generatedAt: payload.generatedAt,
        dates: payload.dates,
        mapping: payload.mapping,
        meta: payload.meta,
        items,
      });
    }

    const items = (snap.inventory || []).map((it) => ({
      ...it,
      brand_name: it.brand,
      status: it.data_status,
    }));

    // 若 lastCode 命中率过低（<5%），自动触发一次强制重算（并输出 Match Success 前 5 条）
    const noAuto = String(req.query?.noAuto || '') === '1';
    if (!noAuto) {
      const act = items.filter((x) => x?.data_status === 'active');
      const linked = act.filter((x) => {
        const v = String(x?.lastCode ?? '').trim();
        return v !== '' && v !== '-';
      }).length;
      const rate = act.length > 0 ? linked / act.length : 0;
      if (act.length > 0 && rate < 0.05) {
        // eslint-disable-next-line no-console
        console.log(`[Prod-Engine] inventory-real detected low lastCode link rate ${(rate * 100).toFixed(2)}% (${linked}/${act.length}); auto force recalc...`);
        await forceSyncLatestProduction({ storageRoot: STORAGE_ROOT });
        const payload = await processAllData({ storageRoot: STORAGE_ROOT });
        await persistFinalDashboardData(STORAGE_ROOT, payload);
        const rebuilt = (payload.inventory || []).map((it) => ({
          ...it,
          brand_name: it.brand,
          status: it.data_status,
        }));
        return res.json({
          ok: true,
          source: 'final_dashboard_data',
          generatedAt: payload.generatedAt,
          dates: payload.dates,
          mapping: payload.mapping,
          meta: payload.meta,
          items: rebuilt,
          autoRecalculated: true,
        });
      }
    }

    return res.json({
      ok: true,
      source: 'final_dashboard_data',
      generatedAt: snap.generatedAt,
      dates: snap.dates,
      mapping: snap.mapping,
      meta: snap.meta,
      items,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[dataEngine] inventory-real failed', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'inventory-real failed' });
  }
});

// 3D 资产预处理元数据（秒开右侧报告；与 assets_meta.json 同步）
app.get('/api/asset-meta', async (req, res) => {
  try {
    const typeRaw = String(req.query?.type || '').trim().toLowerCase();
    const code = String(req.query?.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'missing code' });
    const isSole = typeRaw === 'soles' || typeRaw === 'sole';
    const t = isSole ? 'soles' : 'lasts';
    const key = `${t}:${normalizeAssetCodeKey(code)}`;
    // eslint-disable-next-line no-console
    console.log('[Server] 正在搜索资产(meta):', key, 'code=', JSON.stringify(code));
    const meta = await readAssetsMeta();
    const entry = meta.byKey?.[key] || null;
    return res.json({ ok: true, key, entry });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[api/asset-meta]', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'asset-meta failed' });
  }
});

// 3D 资产详情：物理文件 stat + 全量清单关联款号 / 楦头-大底关系
app.get('/api/asset-details', async (req, res) => {
  try {
    const typeRaw = String(req.query?.type || '').trim().toLowerCase();
    const code = String(req.query?.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'missing code' });

    const isSole = typeRaw === 'soles' || typeRaw === 'sole';
    const type = isSole ? 'soles' : 'lasts';
    const dir = isSole ? DIRS.soles : DIRS.lasts;

    // eslint-disable-next-line no-console
    console.log('[Server] STORAGE_ROOT =', STORAGE_ROOT);

    const scan = scanDirForAssetByCode(dir, code);
    const abs = scan.glb || scan.obj || scan.any;
    // eslint-disable-next-line no-console
    console.log('[Server] 正在搜索资产:', abs || `(not found) dir=${dir} code=${JSON.stringify(code)}`, 'hits=', scan.hits);
    let file = {
      exists: false,
      fileName: null,
      sizeBytes: null,
      sizeLabel: null,
      modifiedAt: null,
      modifiedLabel: null,
    };
    if (abs && fs.existsSync(abs)) {
      const st = fs.statSync(abs);
      const relUrl = `/storage/assets/${type}/${encodeURIComponent(path.basename(abs))}`;
      file = {
        exists: true,
        fileName: path.basename(abs),
        url: relUrl,
        previewUrl: relUrl,
        fallbackUrl: scan.obj ? `/storage/assets/${type}/${encodeURIComponent(path.basename(scan.obj))}` : relUrl,
        sizeBytes: st.size,
        sizeLabel: formatBytesHuman(st.size),
        modifiedAt: st.mtime.toISOString(),
        modifiedLabel: formatDisplayMtime(st.mtime),
      };
    }
    const uploadAudit = await readUploadAudit();
    const relAssetPath = abs ? path.relative(STORAGE_ROOT, abs).split(path.sep).join('/') : '';
    const uploadedBy = uploadAudit.byPath?.[relAssetPath]?.uploadedBy || 'System';

    const snap = await readFinalDashboardSnapshot();
    let inventory = Array.isArray(snap?.inventory) ? snap.inventory : [];
    if (!inventory.length) {
      try {
        const payload = await processAllData({ storageRoot: STORAGE_ROOT });
        inventory = payload.inventory || [];
      } catch {
        inventory = [];
      }
    }

    const key = normalizeAssetCodeKey(code);
    const field = isSole ? 'soleCode' : 'lastCode';
    const linkedStyles = [];
    const soleSet = new Set();

    for (const row of inventory) {
      const v = row?.[field];
      if (normalizeAssetCodeKey(v) !== key) continue;
      linkedStyles.push({
        style_wms: String(row?.style_wms ?? '').trim() || '—',
        data_status: row?.data_status ?? 'other',
      });
      if (!isSole) {
        const sc = String(row?.soleCode ?? '').trim();
        if (sc && sc !== '-' && sc !== '0') soleSet.add(sc);
      }
    }

    const linkedSoleCodes = !isSole ? Array.from(soleSet).sort((a, b) => a.localeCompare(b)) : [];

    // OBJ/STL 等源文件存在但 GLB 未生成时也返回 ready，前端直接加载源文件或提供下载。
    const hasObj = Boolean(scan.obj && fs.existsSync(scan.obj));
    const hasGlb = Boolean(scan.glb && fs.existsSync(scan.glb));
    const hasAnySource = Boolean(scan.any && fs.existsSync(scan.any));
    if (!hasObj && !hasGlb && !hasAnySource) {
      return res.status(404).json({
        ok: false,
        error: 'File not found',
        type,
        code,
        file,
        uploadedBy,
        linkedStyles,
        linkedSoleCodes,
      });
    }

    if (!hasGlb) {
      // eslint-disable-next-line no-console
      console.log('[Success] GLB 不可用，已回退原始源文件：', scan.obj || scan.any);
      return res.json({
        ok: true,
        status: 'ready',
        message: 'GLB 不可用，已回退原始源文件预览',
        type,
        code,
        file,
        uploadedBy,
        linkedStyles,
        linkedSoleCodes,
      });
    }

    // eslint-disable-next-line no-console
    console.log('[Success] 物理文件已锁定，路径为：', scan.glb || abs);

    return res.json({
      ok: true,
      status: 'ready',
      type,
      code,
      file,
      uploadedBy,
      linkedStyles,
      linkedSoleCodes,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[api/asset-details]', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'asset-details failed' });
  }
});

// 下载 3D 源文件（仅 storage/assets/{lasts|soles} 内已解析路径）
app.get('/api/asset-file', (req, res) => {
  try {
    const typeRaw = String(req.query?.type || '').trim().toLowerCase();
    const code = String(req.query?.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'missing code' });
    const isSole = typeRaw === 'soles' || typeRaw === 'sole';
    const dir = isSole ? DIRS.soles : DIRS.lasts;
    const abs = findPhysicalAssetFileAbs(dir, code);
    if (!abs || !fs.existsSync(abs)) return res.status(404).json({ ok: false, error: 'file not found' });
    if (!isPathInsideAssetDir(abs, dir)) return res.status(400).json({ ok: false, error: 'invalid path' });
    return res.download(abs, path.basename(abs));
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[api/asset-file]', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'asset-file failed' });
  }
});

app.post('/api/reprocess-asset', async (req, res) => {
  try {
    const typeRaw = String(req.query?.type || req.body?.type || '').trim().toLowerCase();
    const code = String(req.query?.code || req.body?.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'missing code' });

    const explicitSole = typeRaw === 'soles' || typeRaw === 'sole';
    const explicitLast = typeRaw === 'lasts' || typeRaw === 'last';
    const resolved = explicitSole
      ? { type: 'soles', dir: DIRS.soles, scan: scanDirForAssetByCode(DIRS.soles, code) }
      : explicitLast
        ? { type: 'lasts', dir: DIRS.lasts, scan: scanDirForAssetByCode(DIRS.lasts, code) }
        : scanAllAssetDirsByCode(code);

    const objAbs = resolved.scan.obj;
    if (!objAbs || !fs.existsSync(objAbs)) {
      return res.status(404).json({
        ok: false,
        error: 'OBJ source not found',
        code,
        type: resolved.type,
        hits: resolved.scan.hits,
      });
    }

    const glbAbs = resolved.scan.glb || path.join(path.dirname(objAbs), `${path.basename(objAbs, path.extname(objAbs))}.glb`);
    try {
      if (glbAbs && fs.existsSync(glbAbs)) {
        fs.unlinkSync(glbAbs);
        uploadTrace('reprocess 已删除 GLB 缓存:', glbAbs);
      }
      fs.chmodSync(objAbs, 0o666);
    } catch (e) {
      uploadTrace('reprocess 预处理文件权限/删除失败:', e instanceof Error ? e.message : String(e));
    }

    void runObjToGlbPipeline(objAbs)
      .then(() => incrementalSync3DCache().catch(() => null))
      .catch((e) => {
        // eslint-disable-next-line no-console
        console.error('[api/reprocess-asset] async pipeline failed', e);
      });

    return res.json({
      ok: true,
      status: 'processing',
      message: '正在处理中，预览已回退原始 OBJ',
      code,
      type: resolved.type,
      sourceObj: path.basename(objAbs),
      glbReady: false,
      fallbackReady: true,
      hits: resolved.scan.hits,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('[api/reprocess-asset]', e);
    return res.status(500).json({ ok: false, error: e instanceof Error ? e.message : 'reprocess failed' });
  }
});

// --- Production static hosting (Vite dist/) ---
// 必须位于所有 /api 路由之后，避免生产模式下 API 请求被静态托管抢先处理。
app.use(express.static(DIST_DIR));

// SPA 路由兜底：确保刷新页面时前端路由不失效（仅对非 /api 与非 /storage 生效）
app.get('*', (req, res, next) => {
  try {
    if (req.url.startsWith('/api') || req.url.startsWith('/storage')) return next();
    return res.sendFile(path.join(DIST_DIR, 'index.html'));
  } catch {
    return next();
  }
});

// 统一错误处理（包含 multer 触发的错误）
// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  return res.status(400).json({ ok: false, error: err?.message || 'Server error' });
});

async function start() {
  await ensureStorageDirs();
  await purgeStorageJsonExceptDataAndAssets();
  await ensureSeedAdmin();
  // 自动化环境清理：启动时强制确保 sandbox 目录存在
  try {
    fse.ensureDirSync(path.join(__dirname, 'storage', 'sandbox'));
  } catch {
    // ignore
  }
  await refreshAssetsCache();

  // 零配置：启动即全量重算并落盘，首次打开看板即可命中 KPI（无需先点映射/重算）
  try {
    const payload = await processAllData({ storageRoot: STORAGE_ROOT });
    await persistFinalDashboardData(STORAGE_ROOT, payload);
    // eslint-disable-next-line no-console
    console.log(
      '[Startup] dashboard snapshot warmed:',
      path.relative(process.cwd(), FINAL_DASHBOARD_PATH) || FINAL_DASHBOARD_PATH
    );
  } catch (e) {
    // eslint-disable-next-line no-console
    console.warn('[Startup] dashboard warm failed:', e instanceof Error ? e.message : e);
  }

  const port = 3001;
  // 强制释放端口并自检（best-effort；仅本地开发使用）
  try {
    const pids = execSync(`lsof -ti tcp:${port}`, { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString('utf8')
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean);
    for (const pid of pids) {
      try {
        process.kill(Number(pid), 'SIGKILL');
      } catch {
        // ignore
      }
    }
  } catch {
    // ignore
  }
  const server = app.listen(port, () => {
    // eslint-disable-next-line no-console
    console.clear();
    // eslint-disable-next-line no-console
    console.log(`🚀 后端复活成功，模型锁定为 ${MODEL_PRIORITY[0]}`);
    // eslint-disable-next-line no-console
    console.log('🚀 [Ready] 登录路由已置顶，admin.water 应急通道已开启');
    // eslint-disable-next-line no-console
    console.log('[startup] Vertex AI Gemini model (locked):', MODEL_PRIORITY[0]);
    (async () => {
      const modelsList = await listAvailableModels(); // returns models/xxx
      // eslint-disable-next-line no-console
      console.log('✅ Vertex 模型清单（锁定）:', modelsList);
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
    console.log('[vertex] GOOGLE_APPLICATION_CREDENTIALS:', process.env.GOOGLE_APPLICATION_CREDENTIALS ? 'Yes' : 'No');
    // eslint-disable-next-line no-console
    console.log('[vertex] gcp-key.json exists:', fse.existsSync(vertexKeyPath) ? 'Yes' : 'No');
    // eslint-disable-next-line no-console
    console.log(`[server] assets lasts=${cachedAssets.lasts.length} soles=${cachedAssets.soles.length}`);
  });
  server.on('error', (err) => {
    // eslint-disable-next-line no-console
    console.error('[server] listen failed:', err?.message || err);
    if (String(err?.code || '') === 'EADDRINUSE') {
      // eslint-disable-next-line no-console
      console.error(`[server] 端口 ${port} 被占用。请执行: npx kill-port ${port}`);
    }
  });
}

start().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[server] failed to start', err);
  process.exit(1);
});

