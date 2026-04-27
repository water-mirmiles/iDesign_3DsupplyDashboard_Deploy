import React from 'react';
import { LayoutDashboard, Database, GitMerge, List, Settings, LogOut, Package2, Network } from 'lucide-react';
import { cn } from '@/lib/utils';

interface SidebarProps {
  currentView: string;
  onNavigate: (view: string) => void;
  onLogout: () => void;
  currentUser?: { username: string } | null;
}

export function Sidebar({ currentView, onNavigate, onLogout, currentUser }: SidebarProps) {
  const navItems = [
    { id: 'dashboard', label: '概览看板', icon: LayoutDashboard },
    { id: 'inventory', label: '款号详细清单', icon: List },
    { id: 'relation', label: '楦底关联查询', icon: Network },
    { id: 'data-center', label: '数据导入中心', icon: Database },
    { id: 'schema-mapping', label: '字段映射管理', icon: GitMerge },
  ];

  return (
    <div className="w-64 bg-slate-900 h-screen flex flex-col text-slate-300 border-r border-slate-800 shrink-0">
      {/* Logo Area */}
      <div className="h-16 flex items-center px-6 border-b border-slate-800">
        <div className="flex items-center gap-3 text-white">
          <div className="bg-blue-600 p-1.5 rounded-lg">
            <Package2 className="w-5 h-5" />
          </div>
          <span className="font-bold text-lg tracking-tight">Supply3D</span>
        </div>
      </div>

      {/* Navigation */}
      <div className="flex-1 py-6 px-3 space-y-1">
        <div className="px-3 mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">
          主菜单
        </div>
        {navItems.map((item) => {
          const isActive = currentView === item.id;
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              className={cn(
                "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-200",
                isActive 
                  ? "bg-blue-600 text-white shadow-md shadow-blue-900/20" 
                  : "hover:bg-slate-800 hover:text-white"
              )}
            >
              <Icon className={cn("w-5 h-5", isActive ? "text-white" : "text-slate-400")} />
              {item.label}
            </button>
          );
        })}
      </div>

      {/* Bottom Actions */}
      <div className="p-4 border-t border-slate-800 space-y-1">
        <button
          onClick={() => onNavigate('settings')}
          className={cn(
            'w-full rounded-lg border border-slate-800 px-3 py-2.5 text-left transition-colors',
            currentView === 'settings' ? 'bg-blue-600 text-white shadow-md shadow-blue-900/20' : 'bg-slate-950/40 hover:bg-slate-800'
          )}
        >
          <div className="flex items-center gap-3 text-sm font-medium text-slate-300">
            <Settings className={cn('w-5 h-5', currentView === 'settings' ? 'text-white' : 'text-slate-400')} />
            系统设置
          </div>
          <div className={cn('mt-2 truncate pl-8 text-xs', currentView === 'settings' ? 'text-blue-100' : 'text-slate-500')} title={currentUser?.username || '未登录'}>
            当前登录：<span className={cn('font-medium', currentView === 'settings' ? 'text-white' : 'text-slate-300')}>{currentUser?.username || '未登录'}</span>
          </div>
        </button>
        <button 
          onClick={onLogout}
          className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-red-400 hover:bg-slate-800 hover:text-red-300 transition-colors"
        >
          <LogOut className="w-5 h-5" />
          退出登录
        </button>
      </div>
    </div>
  );
}

export function Layout({
  children,
  currentView,
  onNavigate,
  onLogout,
  currentUser,
}: {
  children: React.ReactNode;
  currentView: string;
  onNavigate: (view: string) => void;
  onLogout: () => void;
  currentUser?: { username: string } | null;
}) {
  return (
    <div className="flex h-screen min-h-0 bg-slate-50 overflow-hidden font-sans">
      <Sidebar currentView={currentView} onNavigate={onNavigate} onLogout={onLogout} currentUser={currentUser} />
      <main className="flex-1 h-screen min-h-0 overflow-y-auto">
        <div className="w-full px-4 py-4">
          {children}
        </div>
      </main>
    </div>
  );
}
