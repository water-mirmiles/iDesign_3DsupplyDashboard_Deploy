import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import fse from 'fs-extra';

const testDir = path.join(os.tmpdir(), `supply3d-auth-flow-${Date.now()}`);
const usersPath = path.join(testDir, 'users.json');

function passwordHash(password) {
  return crypto.createHash('sha256').update(String(password || ''), 'utf8').digest('hex');
}

function normalizeUsername(v) {
  return String(v || '').trim();
}

function isPasswordActivated(user) {
  return typeof user?.passwordHash === 'string' && user.passwordHash.trim() !== '';
}

async function readUsersFile() {
  try {
    if (!(await fse.pathExists(usersPath))) return { version: 1, users: [] };
    const data = await fse.readJson(usersPath);
    return { version: 1, users: Array.isArray(data?.users) ? data.users : [] };
  } catch {
    return { version: 1, users: [] };
  }
}

async function writeUsersFile(payload) {
  await fse.ensureDir(testDir);
  await fse.writeJson(usersPath, payload, { spaces: 2 });
}

async function ensureSeedAdmin() {
  const db = await readUsersFile();
  const users = Array.isArray(db.users) ? db.users : [];
  const admin = users.find((u) => normalizeUsername(u.username).toLowerCase() === 'admin.water');
  const adminHash = passwordHash('water123');
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
    return;
  }
  admin.passwordHash = adminHash;
  admin.role = 'superadmin';
  admin.updatedAt = new Date().toISOString();
  await writeUsersFile({ version: 1, users });
}

async function controlledRegister(username, password) {
  const u = normalizeUsername(username);
  if (!u || !password) return { ok: false, error: '用户名和密码不能为空' };
  await ensureSeedAdmin();
  const db = await readUsersFile();
  const found = db.users.find((x) => normalizeUsername(x.username).toLowerCase() === u.toLowerCase());
  if (!found) return { ok: false, error: '该用户名尚未获得授权，请联系管理员 admin.water' };
  if (isPasswordActivated(found)) return { ok: false, error: '该账号已激活，请直接登录' };
  found.passwordHash = passwordHash(password);
  found.updatedAt = new Date().toISOString();
  await writeUsersFile(db);
  return { ok: true, user: { username: found.username, role: found.role || 'user' } };
}

async function adminCreateUser(operator, targetUsername) {
  if (operator !== 'admin.water') return { ok: false, error: '仅 admin.water 可管理用户' };
  const username = normalizeUsername(targetUsername);
  await ensureSeedAdmin();
  const db = await readUsersFile();
  if (db.users.some((u) => normalizeUsername(u.username).toLowerCase() === username.toLowerCase())) {
    return { ok: false, error: '该用户名已存在' };
  }
  const now = new Date().toISOString();
  db.users.push({ username, passwordHash: '', role: 'user', createdAt: now, updatedAt: now });
  await writeUsersFile(db);
  return { ok: true, user: { username, activated: false } };
}

async function login(username, password) {
  await ensureSeedAdmin();
  const db = await readUsersFile();
  const incomingHash = passwordHash(password);
  const found = db.users.find(
    (u) => normalizeUsername(u.username).toLowerCase() === normalizeUsername(username).toLowerCase() && u.passwordHash === incomingHash
  );
  if (!found) return { ok: false, error: '用户名或密码错误，或该账号尚未注册。' };
  return { ok: true, user: { username: found.username, role: found.role || 'user' } };
}

function assertStep(name, condition, detail) {
  if (!condition) {
    throw new Error(`${name} 失败：${detail}`);
  }
  console.log(`PASS ${name}: ${detail}`);
}

async function main() {
  await fse.remove(testDir);
  await ensureSeedAdmin();

  const randomRegister = await controlledRegister(`random_${Date.now()}`, 'x123456');
  assertStep('随机名字注册应失败', !randomRegister.ok && randomRegister.error.includes('尚未获得授权'), randomRegister.error);

  const created = await adminCreateUser('admin.water', 'designer_01');
  assertStep('admin.water 创建 designer_01', created.ok && created.user.username === 'designer_01', JSON.stringify(created));

  const activated = await controlledRegister('designer_01', 'designerPass123');
  assertStep('designer_01 设置密码应成功', activated.ok && activated.user.username === 'designer_01', JSON.stringify(activated));

  const loginAfterLogout = await login('designer_01', 'designerPass123');
  assertStep('模拟登出后重新登录', loginAfterLogout.ok && loginAfterLogout.user.username === 'designer_01', JSON.stringify(loginAfterLogout));

  const db = await readUsersFile();
  const admin = db.users.find((u) => u.username === 'admin.water');
  assertStep('Seed Admin 存在且为 superadmin', Boolean(admin && admin.role === 'superadmin' && isPasswordActivated(admin)), JSON.stringify(admin));
  assertStep('Seed Admin 哈希与 water123 对齐', admin?.passwordHash === passwordHash('water123'), admin?.passwordHash || '');

  console.log(`DONE auth flow test passed. temp=${testDir}`);
}

main()
  .catch((err) => {
    console.error('FAIL auth flow test:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await fse.remove(testDir);
  });
