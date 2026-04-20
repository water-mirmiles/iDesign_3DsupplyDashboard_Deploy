import React, { useState, useEffect } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer, LabelList,
  AreaChart, Area
} from 'recharts';
import { Box, Layers, Hash, Factory, Loader2, Activity, TrendingUp, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AssetTrendStats } from '@/types';

type DashboardStatsResponse = {
  ok: boolean;
  source?: 'final_dashboard_data' | 'live';
  generatedAt?: string;
  dates: { latest: string; prev: string };
  mapping: { hasConfig: boolean; configPath: string };
  meta: { mainTable: string | null; requiredCols?: string[]; reason?: string };
  kpis: {
    styles?: { totalAll: number; totalEffective: number };
    activeStyles: number;
    matched3DLasts: number;
    matched3DSoles: number;
    lastCoverage: number;
    soleCoverage: number;
    last3DCount?: number;
    last3DCoverage?: number;
    lastCodeLinked?: number;
    lastCodeLinkRate?: number;
    soleCodeLinked?: number;
    soleCodeLinkRate?: number;
    sole3DCount?: number;
    sole3DCoverage?: number;
    /** 生效款中至少命中楦或底之一 */
    stylesWithAny3D?: number;
    /** (生效款且 has3D) / 生效款总数 · 百分比 */
    any3DCoveragePercent?: number;
    deltaActiveStyles: number;
    deltaMatched3DLasts: number;
    deltaMatched3DSoles: number;
  };
  brandCoverage: Array<{ brand: string; linked: number; unlinked: number }>;
  brandBindingStats?: Array<{
    brand: string;
    lastBindingRate: number;
    soleBindingRate: number;
    totalActive: number;
    lastLinkedCount: number;
    soleLinkedCount: number;
    last3DMatchedCount: number;
  }>;
  trends?: { assetTrend?: AssetTrendStats[] };
  error?: string;
};

type TimePeriod = 'day' | 'week' | 'month' | 'quarter' | 'year';

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStatsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isForceSyncing, setIsForceSyncing] = useState(false);

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
        setChartData(json.trends?.assetTrend || []);
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

  const handleForceSync = async () => {
    if (isForceSyncing) return;
    setIsForceSyncing(true);
    setError(null);
    try {
      const resp = await fetch('/api/force-sync-dashboard', { method: 'POST' });
      const json = await resp.json().catch(() => null);
      if (!resp.ok || !json?.ok) throw new Error(json?.error || `重算失败（HTTP ${resp.status}）`);
      // 重算后强制拉取最新 stats（避免仍读旧快照）
      const resp2 = await fetch('/api/dashboard-stats?refresh=1');
      const json2 = (await resp2.json()) as DashboardStatsResponse;
      if (!resp2.ok || !json2.ok) throw new Error(json2.error || `刷新失败（HTTP ${resp2.status}）`);
      setStats(json2);
      setChartData(json2.trends?.assetTrend || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : '重算失败');
    } finally {
      setIsForceSyncing(false);
    }
  };

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

  const handleBrandBindingClick = (brand: string) => {
    const b = String(brand || '').trim();
    if (!b) return;
    // 本项目无路由：用 localStorage 驱动导航与预过滤
    localStorage.setItem('inventoryBrandFilter', b);
    localStorage.setItem('currentView', 'inventory');
    window.location.reload();
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
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold text-slate-900">概览看板</h1>
          <p className="text-sm text-slate-500 mt-1">全局 3D 资产覆盖率与新增趋势</p>
          {stats?.source === 'final_dashboard_data' && stats.generatedAt && (
            <p className="text-xs text-emerald-700 mt-1">
              数据来自认证快照 · {new Date(stats.generatedAt).toLocaleString('zh-CN')}
            </p>
          )}
        </div>
        <button
          onClick={() => void handleForceSync()}
          disabled={isForceSyncing}
          className={cn(
            'shrink-0 inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border shadow-sm transition-colors',
            isForceSyncing
              ? 'bg-slate-100 text-slate-500 border-slate-200 cursor-not-allowed'
              : 'bg-white text-slate-700 border-slate-200 hover:bg-slate-50'
          )}
        >
          {isForceSyncing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Activity className="w-4 h-4" />}
          一键重算看板数据
        </button>
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
              <span className="text-3xl font-bold text-slate-900">
                {(stats?.kpis?.styles?.totalAll ?? stats?.kpis?.totalStyles ?? 0).toLocaleString()}
              </span>
              {getTrendBadge(stats?.kpis?.deltaActiveStyles || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              其中生效款：<span className="font-medium text-slate-700">{(stats?.kpis?.styles?.totalEffective ?? stats?.kpis?.activeStyles ?? 0).toLocaleString()}</span>
            </div>
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
              <span className="text-3xl font-bold text-slate-900">
                {(stats?.kpis?.last3DCount ?? stats?.kpis?.matched3DLasts ?? 0).toLocaleString()}
              </span>
              {getTrendBadge(stats?.kpis?.deltaMatched3DLasts || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              编号绑定率：{' '}
              <span className="font-medium text-slate-700">
                {stats?.kpis?.lastCodeLinkRate ?? 0}%
              </span>
            </div>
            <div className="mt-1 text-xs text-slate-500">
              3D 覆盖率：{' '}
              <span className="font-medium text-slate-700">
                {stats?.kpis?.last3DCoverage ?? stats?.kpis?.lastCoverage ?? 0}%
              </span>
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
              <span className="text-3xl font-bold text-slate-900">
                {(stats?.kpis?.sole3DCount ?? stats?.kpis?.matched3DSoles ?? 0).toLocaleString()}
              </span>
              {getTrendBadge(stats?.kpis?.deltaMatched3DSoles || 0)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              编号绑定率: <span className="font-medium text-slate-700">{stats?.kpis?.soleCodeLinkRate ?? 0}%</span>
            </div>
            <div className="mt-1 text-xs text-slate-500">
              3D 覆盖率: <span className="font-medium text-slate-700">{stats?.kpis?.sole3DCoverage ?? stats?.kpis?.soleCoverage ?? 0}%</span>
            </div>
          </div>
        </div>

        {/* Card 5: Overall Coverage */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">整体 3D 覆盖</h3>
            <div className="p-2 rounded-lg bg-emerald-50">
              <Activity className="w-4 h-4 text-emerald-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">
                {stats?.kpis?.any3DCoveragePercent != null
                  ? stats.kpis.any3DCoveragePercent
                  : Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))}
                %
              </span>
              {getTrendBadge(0, true)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              {stats?.kpis?.stylesWithAny3D != null
                ? `生效款中任一 3D 命中：${stats.kpis.stylesWithAny3D} / ${stats?.kpis?.activeStyles ?? 0}`
                : '楦/底覆盖率均值（无快照细分时）'}
            </div>
            <div className="mt-2 h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
              <div
                className={cn(
                  'h-full rounded-full transition-all duration-1000',
                  getProgressColor(
                    stats?.kpis?.any3DCoveragePercent != null
                      ? stats.kpis.any3DCoveragePercent
                      : Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))
                  )
                )}
                style={{
                  width: `${
                    stats?.kpis?.any3DCoveragePercent != null
                      ? stats.kpis.any3DCoveragePercent
                      : Math.round((((stats?.kpis?.lastCoverage ?? 0) + (stats?.kpis?.soleCoverage ?? 0)) / 2))
                  }%`,
                }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Left column: two stacked cards */}
        <div className="flex flex-col gap-6">
          {/* Brand Coverage */}
          <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-base font-semibold text-slate-900">各品牌 3D 覆盖统计</h3>
              <span className="text-xs text-slate-500">按款号数量统计</span>
            </div>
          <div className="flex-1 min-h-[400px] w-full">
              <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={stats?.brandCoverage || []}
                layout="vertical"
                margin={{ top: 10, right: 10, left: 20, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis type="number" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis
                  type="category"
                  dataKey="brand"
                  axisLine={false}
                  tickLine={false}
                  width={150}
                  tick={{ fill: '#64748b', fontSize: 12, textAnchor: 'end' }}
                />
                <RechartsTooltip
                  cursor={{ fill: '#f8fafc' }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const p: any = payload?.[0]?.payload || {};
                    return (
                      <div className="rounded-lg border border-slate-200 bg-white shadow-sm px-3 py-2 text-xs text-slate-700">
                        <div className="font-medium text-slate-900 mb-1">{p.brand}</div>
                        <div>该品牌总生效款：{p.totalActive ?? (p.linked + p.unlinked)}</div>
                        <div>已绑定楦：{p.lastLinkedCount ?? '-'}</div>
                        <div>已匹配 3D：{p.last3DMatchedCount ?? p.linked}</div>
                      </div>
                    );
                  }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '13px', paddingTop: '12px' }} />
                <Bar dataKey="linked" name="已关联 3D" stackId="a" fill="#10b981" maxBarSize={18} />
                <Bar dataKey="unlinked" name="未关联/缺失" stackId="a" fill="#f59e0b" maxBarSize={18} />
              </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Brand Binding Stats */}
          <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-base font-semibold text-slate-900">各品牌基础数据绑定率 (Data Completeness)</h3>
              <span className="text-xs text-slate-500">点击品牌跳转清单</span>
            </div>
            <div className="flex-1 min-h-[400px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={stats?.brandBindingStats || []}
                  layout="vertical"
                  margin={{ top: 10, right: 10, left: 20, bottom: 0 }}
                  onClick={(state: any) => {
                    const brand = state?.activePayload?.[0]?.payload?.brand;
                    if (brand) handleBrandBindingClick(String(brand));
                  }}
                >
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                  <XAxis type="number" domain={[0, 100]} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <YAxis
                    type="category"
                    dataKey="brand"
                    axisLine={false}
                    tickLine={false}
                    width={150}
                    tick={{ fill: '#64748b', fontSize: 12, textAnchor: 'end' }}
                  />
                  <RechartsTooltip
                    cursor={{ fill: '#f8fafc' }}
                    content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const p: any = payload?.[0]?.payload || {};
                      return (
                        <div className="rounded-lg border border-slate-200 bg-white shadow-sm px-3 py-2 text-xs text-slate-700">
                          <div className="font-medium text-slate-900 mb-1">{p.brand}</div>
                          <div>该品牌总生效款：{p.totalActive ?? '-'}</div>
                          <div>已绑定楦：{p.lastLinkedCount ?? '-'}</div>
                          <div>已匹配 3D：{p.last3DMatchedCount ?? '-'}</div>
                        </div>
                      );
                    }}
                  />
                  <Legend
                    iconType="circle"
                    verticalAlign="bottom"
                    wrapperStyle={{ fontSize: '13px', paddingTop: '10px' }}
                    formatter={(value) =>
                      value === 'lastBindingRate'
                        ? '楦头绑定率 (Last ID Linked)'
                        : value === 'soleBindingRate'
                          ? '大底绑定率 (Sole ID Linked)'
                          : value
                    }
                  />
                  <Bar dataKey="lastBindingRate" name="lastBindingRate" fill="#0ea5e9" radius={[4, 4, 4, 4]} maxBarSize={16}>
                    <LabelList dataKey="lastBindingRate" position="right" formatter={(v: any) => `${v}%`} />
                  </Bar>
                  <Bar dataKey="soleBindingRate" name="soleBindingRate" fill="#6366f1" radius={[4, 4, 4, 4]} maxBarSize={16}>
                    <LabelList dataKey="soleBindingRate" position="right" formatter={(v: any) => `${v}%`} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
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

