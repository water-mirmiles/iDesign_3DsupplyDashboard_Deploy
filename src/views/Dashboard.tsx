import React, { useState, useEffect } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer,
  AreaChart, Area
} from 'recharts';
import { Box, Layers, Hash, Factory, Loader2, Activity, TrendingUp, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AssetTrendStats } from '@/types';

type DashboardStatsResponse = {
  ok: boolean;
  dates: { latest: string; prev: string };
  mapping: { hasConfig: boolean; configPath: string };
  meta: { mainTable: string | null; requiredCols?: string[]; reason?: string };
  kpis: {
    activeStyles: number;
    matched3DLasts: number;
    matched3DSoles: number;
    lastCoverage: number;
    soleCoverage: number;
    deltaActiveStyles: number;
    deltaMatched3DLasts: number;
    deltaMatched3DSoles: number;
  };
  brandCoverage: Array<{ brand: string; linked: number; unlinked: number }>;
  error?: string;
};

type TimePeriod = 'day' | 'week' | 'month' | 'quarter' | 'year';

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStatsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [trendPeriod, setTrendPeriod] = useState<TimePeriod>('week');
  const [chartData, setChartData] = useState<AssetTrendStats[]>([]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const resp = await fetch('/api/dashboard-stats');
        const json = (await resp.json()) as DashboardStatsResponse;
        if (!resp.ok || !json.ok) throw new Error(json.error || `加载失败（HTTP ${resp.status}）`);
        if (cancelled) return;
        setStats(json);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : '加载失败');
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, []);

  const handlePeriodChange = (period: TimePeriod) => {
    // 真实趋势后续由数据引擎生成；当前先保留切换控件但不做假异步
    if (period === trendPeriod) return;
    setTrendPeriod(period);
  };

  const getProgressColor = (percent: number) => {
    if (percent >= 80) return 'bg-emerald-500';
    if (percent >= 50) return 'bg-amber-500';
    return 'bg-red-500';
  };

  const getTrendBadge = (value: number, isPercent = false) => {
    if (value > 0) {
      return (
        <div className="flex items-center text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded mb-1">
          <TrendingUp className="w-3 h-3 mr-1" />
          较上月 +{value}{isPercent ? '%' : ''}
        </div>
      );
    } else if (value < 0) {
      return (
        <div className="flex items-center text-xs font-medium text-red-600 bg-red-50 px-1.5 py-0.5 rounded mb-1">
          <TrendingUp className="w-3 h-3 mr-1 rotate-180" />
          较上月 {value}{isPercent ? '%' : ''}
        </div>
      );
    } else {
      return (
        <div className="flex items-center text-xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded mb-1">
          <Activity className="w-3 h-3 mr-1" />
          较上月持平
        </div>
      );
    }
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500 pb-10">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">概览看板</h1>
        <p className="text-sm text-slate-500 mt-1">全局 3D 资产覆盖率与新增趋势</p>
      </div>

      {!isLoading && stats && (!stats.mapping?.hasConfig || !stats.meta?.mainTable) && (
        <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 text-sm text-amber-800 flex items-start gap-3">
          <AlertCircle className="w-5 h-5 mt-0.5" />
          <div className="min-w-0">
            <div className="font-medium">数据引擎尚未找到“款号主表”或映射未完成</div>
            <div className="text-xs mt-1 text-amber-700">
              {stats.mapping?.hasConfig ? `主表识别失败：${stats.meta?.reason || '请检查映射字段是否覆盖 款号/品牌/状态'}` : '未检测到 mapping_config.json（请在“字段映射管理”保存映射）'}
            </div>
          </div>
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* KPI Cards - 5 Cards Layout */}
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {/* Card 1: Total Brands */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">品牌总数</h3>
            <div className="p-2 rounded-lg bg-blue-50">
              <Factory className="w-4 h-4 text-blue-600" />
            </div>
          </div>
          <div className="mt-4">
            <span className="text-3xl font-bold text-slate-900">{(stats?.brandCoverage?.length || 0).toLocaleString()}</span>
          </div>
        </div>

        {/* Card 2: Styles Total & Active */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">款号总数</h3>
            <div className="p-2 rounded-lg bg-indigo-50">
              <Hash className="w-4 h-4 text-indigo-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">{(stats?.kpis?.activeStyles || 0).toLocaleString()}</span>
              {getTrendBadge(stats?.kpis?.deltaActiveStyles || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">生效款号（按状态过滤）</div>
          </div>
        </div>

        {/* Card 3: 3D Lasts Progress */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">3D 楦头进度</h3>
            <div className="p-2 rounded-lg bg-sky-50">
              <Box className="w-4 h-4 text-sky-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">{(stats?.kpis?.matched3DLasts || 0).toLocaleString()}</span>
              {getTrendBadge(stats?.kpis?.deltaMatched3DLasts || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              覆盖率: <span className="font-medium text-slate-700">{stats?.kpis?.lastCoverage ?? 0}%</span>
            </div>
          </div>
        </div>

        {/* Card 4: 3D Soles Progress */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">3D 大底进度</h3>
            <div className="p-2 rounded-lg bg-violet-50">
              <Layers className="w-4 h-4 text-violet-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">{(stats?.kpis?.matched3DSoles || 0).toLocaleString()}</span>
              {getTrendBadge(stats?.kpis?.deltaMatched3DSoles || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              覆盖率: <span className="font-medium text-slate-700">{stats?.kpis?.soleCoverage ?? 0}%</span>
            </div>
          </div>
        </div>

        {/* Card 5: Overall Coverage */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">整体覆盖率</h3>
            <div className="p-2 rounded-lg bg-emerald-50">
              <Activity className="w-4 h-4 text-emerald-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">
                {Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))}%
              </span>
              {getTrendBadge(0, true)}
            </div>
            <div className="mt-2 h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
              <div 
                className={cn("h-full rounded-full transition-all duration-1000", getProgressColor(Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))))} 
                style={{ width: `${Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))}%` }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Left: Stacked Bar Chart for Brand Coverage */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">各品牌 3D 覆盖统计</h3>
            <span className="text-xs text-slate-500">按款号数量统计</span>
          </div>
          <div className="flex-1 min-h-[350px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart 
                data={stats?.brandCoverage || []} 
                margin={{ top: 10, right: 10, left: -20, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="brand" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} dy={10} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip 
                  cursor={{ fill: '#f8fafc' }}
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '13px', paddingTop: '20px' }} />
                <Bar dataKey="linked" name="已关联 3D 资产" stackId="a" fill="#10b981" radius={[0, 0, 4, 4]} maxBarSize={40} />
                <Bar dataKey="unlinked" name="未关联/缺失" stackId="a" fill="#f59e0b" radius={[4, 4, 0, 0]} maxBarSize={40} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Right: Trend Area Chart */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">3D 资产新增趋势</h3>
            <div className="flex bg-slate-100 p-1 rounded-lg">
              {(['day', 'week', 'month', 'quarter', 'year'] as const).map((period) => (
                <button
                  key={period}
                  onClick={() => handlePeriodChange(period)}
                  className={cn(
                    "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
                    trendPeriod === period ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
                  )}
                >
                  {period === 'day' ? '日' : period === 'week' ? '周' : period === 'month' ? '月' : period === 'quarter' ? '季' : '年'}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 min-h-[350px] w-full relative">
            {isLoading && (
              <div className="absolute inset-0 z-10 flex items-center justify-center bg-white/50 backdrop-blur-sm rounded-lg">
                <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
              </div>
            )}
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                <defs>
                  <linearGradient id="colorLasts" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorSoles" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} dy={10} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip 
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '13px', paddingTop: '20px' }} />
                <Area type="monotone" dataKey="newLasts" name="新增 3D 楦" stroke="#0ea5e9" strokeWidth={2} fillOpacity={1} fill="url(#colorLasts)" />
                <Area type="monotone" dataKey="newSoles" name="新增 3D 底" stroke="#8b5cf6" strokeWidth={2} fillOpacity={1} fill="url(#colorSoles)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

      </div>
    </div>
  );
}

