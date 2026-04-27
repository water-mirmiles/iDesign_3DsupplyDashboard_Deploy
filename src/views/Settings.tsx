import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { AlertCircle, CheckCircle2, KeyRound, Loader2, Plus, ShieldCheck, UserCog } from 'lucide-react';

type CurrentUser = { username: string };

type ManagedUser = {
  username: string;
  role: string;
  activated: boolean;
  createdAt?: string | null;
  updatedAt?: string | null;
};

type ApiResponse<T = unknown> = { ok?: boolean; error?: string } & T;

export default function Settings({ currentUser }: { currentUser: CurrentUser | null }) {
  const isSuperAdmin = currentUser?.username === 'admin.water';
  const [users, setUsers] = useState<ManagedUser[]>([]);
  const [newUsername, setNewUsername] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const activeCount = useMemo(() => users.filter((u) => u.activated).length, [users]);
  const pendingCount = users.length - activeCount;

  const loadUsers = useCallback(async () => {
    if (!isSuperAdmin) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(`/api/users?operator=${encodeURIComponent(currentUser.username)}`);
      const json = (await resp.json().catch(() => ({}))) as ApiResponse<{ users?: ManagedUser[] }>;
      if (!resp.ok || !json.ok) throw new Error(json.error || '读取用户列表失败');
      setUsers(Array.isArray(json.users) ? json.users : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : '读取用户列表失败');
    } finally {
      setLoading(false);
    }
  }, [currentUser?.username, isSuperAdmin]);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  const createUser = async (e: React.FormEvent) => {
    e.preventDefault();
    const targetUsername = newUsername.trim();
    if (!targetUsername || !currentUser) return;
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      const resp = await fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operator: currentUser.username, targetUsername }),
      });
      const json = (await resp.json().catch(() => ({}))) as ApiResponse<{ user?: ManagedUser }>;
      if (!resp.ok || !json.ok) throw new Error(json.error || '创建用户失败');
      setNewUsername('');
      setSuccess(`已授权 ${targetUsername}，等待其在登录页设置密码。`);
      await loadUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : '创建用户失败');
    } finally {
      setSaving(false);
    }
  };

  const resetPassword = async (targetUsername: string) => {
    if (!currentUser) return;
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      const resp = await fetch('/api/users/reset-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operator: currentUser.username, targetUsername }),
      });
      const json = (await resp.json().catch(() => ({}))) as ApiResponse<{ user?: ManagedUser }>;
      if (!resp.ok || !json.ok) throw new Error(json.error || '重置密码失败');
      setSuccess(`${targetUsername} 的密码已清空，可在注册页重新设置。`);
      await loadUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : '重置密码失败');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="w-full space-y-6">
      <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="flex items-center gap-2 text-sm font-semibold text-blue-700">
              <ShieldCheck className="h-4 w-4" />
              系统设置
            </div>
            <h1 className="mt-2 text-2xl font-bold text-slate-900">管理员受控用户体系</h1>
            <p className="mt-1 text-sm text-slate-500">当前登录：{currentUser?.username || '未登录'}</p>
          </div>
          {isSuperAdmin ? (
            <div className="grid grid-cols-2 gap-3 text-center">
              <div className="rounded-xl bg-emerald-50 px-4 py-3">
                <div className="text-2xl font-bold text-emerald-700">{activeCount}</div>
                <div className="text-xs text-emerald-700">已激活</div>
              </div>
              <div className="rounded-xl bg-amber-50 px-4 py-3">
                <div className="text-2xl font-bold text-amber-700">{pendingCount}</div>
                <div className="text-xs text-amber-700">待激活</div>
              </div>
            </div>
          ) : null}
        </div>
      </div>

      {!isSuperAdmin ? (
        <div className="rounded-2xl border border-amber-200 bg-amber-50 p-6 text-amber-800">
          <div className="flex items-start gap-3">
            <AlertCircle className="mt-0.5 h-5 w-5 shrink-0" />
            <div>
              <h2 className="font-semibold">无用户管理权限</h2>
              <p className="mt-1 text-sm">仅 admin.water 可以创建团队账号和重置密码。</p>
            </div>
          </div>
        </div>
      ) : (
        <div className="grid w-full gap-6 lg:grid-cols-[minmax(320px,420px)_1fr]">
          <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
            <div className="flex items-center gap-2 text-lg font-semibold text-slate-900">
              <UserCog className="h-5 w-5 text-blue-600" />
              创建授权用户
            </div>
            <p className="mt-2 text-sm text-slate-500">先预设用户名，密码保持为空；成员在登录页点击注册后自行设置密码。</p>
            <form onSubmit={createUser} className="mt-5 space-y-4">
              <input
                value={newUsername}
                onChange={(e) => setNewUsername(e.target.value)}
                className="w-full rounded-xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-100"
                placeholder="例如 designer_01"
              />
              <button
                type="submit"
                disabled={saving || !newUsername.trim()}
                className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-blue-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                创建用户
              </button>
            </form>
            {error ? <div className="mt-4 rounded-xl bg-red-50 px-4 py-3 text-sm text-red-700">{error}</div> : null}
            {success ? <div className="mt-4 rounded-xl bg-emerald-50 px-4 py-3 text-sm text-emerald-700">{success}</div> : null}
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="flex items-center justify-between border-b border-slate-200 px-6 py-4">
              <div>
                <h2 className="text-lg font-semibold text-slate-900">用户列表</h2>
                <p className="text-sm text-slate-500">查看账号激活状态，或清空密码要求成员重新激活。</p>
              </div>
              <button
                onClick={() => void loadUsers()}
                className="rounded-lg border border-slate-200 px-3 py-2 text-sm text-slate-600 transition hover:bg-slate-50"
              >
                刷新
              </button>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th className="px-6 py-3">用户名</th>
                    <th className="px-6 py-3">角色</th>
                    <th className="px-6 py-3">状态</th>
                    <th className="px-6 py-3">更新时间</th>
                    <th className="px-6 py-3 text-right">操作</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {loading ? (
                    <tr>
                      <td colSpan={5} className="px-6 py-10 text-center text-slate-500">
                        <Loader2 className="mx-auto mb-2 h-5 w-5 animate-spin" />
                        正在读取用户列表...
                      </td>
                    </tr>
                  ) : (
                    users.map((user) => (
                      <tr key={user.username} className="hover:bg-slate-50">
                        <td className="px-6 py-4 font-medium text-slate-900">{user.username}</td>
                        <td className="px-6 py-4 text-slate-600">{user.role}</td>
                        <td className="px-6 py-4">
                          <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-semibold ${user.activated ? 'bg-emerald-50 text-emerald-700' : 'bg-amber-50 text-amber-700'}`}>
                            {user.activated ? <CheckCircle2 className="h-3.5 w-3.5" /> : <KeyRound className="h-3.5 w-3.5" />}
                            {user.activated ? '已激活' : '待激活'}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-slate-500">{user.updatedAt ? user.updatedAt.slice(0, 16).replace('T', ' ') : '—'}</td>
                        <td className="px-6 py-4 text-right">
                          <button
                            onClick={() => void resetPassword(user.username)}
                            disabled={saving || user.username === 'admin.water'}
                            className="rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40"
                          >
                            重置密码
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
