import React, { useState } from 'react';
import { Box, Lock, User, ArrowRight } from 'lucide-react';

interface LoginProps {
  onLogin: (info: { username: string; rememberMe: boolean }) => void;
}

type StoredUser = { username: string; password: string };

function loadUsers(): StoredUser[] {
  try {
    const raw = localStorage.getItem('users');
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed)
      ? parsed.filter((u) => typeof u?.username === 'string' && typeof u?.password === 'string')
      : [];
  } catch {
    return [];
  }
}

function saveUsers(users: StoredUser[]) {
  localStorage.setItem('users', JSON.stringify(users));
}

export default function Login({ onLogin }: LoginProps) {
  const [isLogin, setIsLogin] = useState(true);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [rememberMe, setRememberMe] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    const u = username.trim();
    const p = password;
    if (!u || !p) return;

    const users = loadUsers();

    if (isLogin) {
      const found = users.find((x) => x.username === u && x.password === p);
      if (!found) {
        setError('用户名或密码错误，或该账号尚未注册。');
        return;
      }
      onLogin({ username: u, rememberMe });
      return;
    }

    if (users.some((x) => x.username === u)) {
      setError('该用户名已存在，请直接登录。');
      return;
    }

    saveUsers([...users, { username: u, password: p }]);
    onLogin({ username: u, rememberMe });
  };

  return (
    <div className="min-h-screen bg-slate-900 flex items-center justify-center p-4 relative overflow-hidden">
      {/* Background decoration */}
      <div className="absolute top-[-10%] left-[-10%] w-96 h-96 bg-blue-500/20 rounded-full blur-3xl" />
      <div className="absolute bottom-[-10%] right-[-10%] w-96 h-96 bg-indigo-500/20 rounded-full blur-3xl" />

      <div className="w-full max-w-md bg-slate-800/50 backdrop-blur-xl border border-slate-700 p-8 rounded-2xl shadow-2xl relative z-10">
        <div className="flex flex-col items-center mb-8">
          <div className="w-12 h-12 bg-blue-600 rounded-xl flex items-center justify-center mb-4 shadow-lg shadow-blue-500/30">
            <Box className="w-6 h-6 text-white" />
          </div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Supply3D</h1>
          <p className="text-slate-400 text-sm mt-2">数字化供应链资产管理系统</p>
          <div className="mt-3 px-3 py-1 bg-amber-500/10 border border-amber-500/20 rounded-full">
            <span className="text-xs font-medium text-amber-400">仅限公司内网访问</span>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {error && (
            <div className="text-sm text-red-200 bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-3">
              {error}
            </div>
          )}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">用户名</label>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                <User className="h-5 w-5 text-slate-500" />
              </div>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="block w-full pl-10 pr-3 py-2.5 border border-slate-600 rounded-lg bg-slate-900/50 text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                placeholder="请输入域账号"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">密码</label>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                <Lock className="h-5 w-5 text-slate-500" />
              </div>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="block w-full pl-10 pr-3 py-2.5 border border-slate-600 rounded-lg bg-slate-900/50 text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                placeholder="请输入密码"
                required
              />
            </div>
          </div>

          <div className="flex items-center justify-between">
            <label className="flex items-center gap-2 text-sm text-slate-300 select-none">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={(e) => setRememberMe(e.target.checked)}
                className="h-4 w-4 rounded border-slate-600 bg-slate-900/50 text-blue-600 focus:ring-2 focus:ring-blue-500"
              />
              记住我
            </label>
            <div className="text-xs text-slate-500">
              {isLogin ? '登录校验本地用户列表' : '注册将写入本地存储'}
            </div>
          </div>

          <button
            type="submit"
            className="w-full flex items-center justify-center gap-2 py-2.5 px-4 border border-transparent rounded-lg shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 focus:ring-offset-slate-900 transition-all mt-6"
          >
            {isLogin ? '登录系统' : '注册账号'}
            <ArrowRight className="w-4 h-4" />
          </button>
        </form>

        <div className="mt-6 text-center">
          <button
            onClick={() => setIsLogin(!isLogin)}
            className="text-sm text-slate-400 hover:text-white transition-colors"
          >
            {isLogin ? '没有账号？点击注册' : '已有账号？点击登录'}
          </button>
        </div>
      </div>
    </div>
  );
}
