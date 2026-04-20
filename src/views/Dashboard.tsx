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
    totalEffective: number;
    lastLinked: number;
    soleLinked: number;
    last3DMatched: number;
  }>;
  lastDigitizationStats?: Array<{
    brand: string;
    totalEffective: number;
    hasCode: number;
    has3D: number;
    segment_no_code: number;
    segment_has_code_no_3d: number;
    segment_completed_3d: number;
    completionRate: number;
  }>;
  soleDigitizationStats?: Array<{
    brand: string;
    totalEffective: number;
    hasCode: number;
    has3D: number;
    segment_no_code: number;
    segment_has_code_no_3d: number;
    segment_completed_3d: number;
    completionRate: number;
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

  const drilldownToInventory = (brand: string, dimension: 'last' | 'sole', bucket: 'no_code' | 'has_code_no_3d' | 'completed_3d') => {
    const b = String(brand || '').trim();
    if (!b) return;
    localStorage.setItem('inventoryBrandFilter', b);
    localStorage.setItem('inventoryDrilldown', JSON.stringify({ dimension, bucket }));
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

  const LEGEND_TEXT_STYLE = { color: '#334155' }; // slate-700
  const COLORS = {
    completed3D: '#059669', // emerald-600
    hasCodeNo3D: '#0ea5e9', // sky-500
    noCode: '#e2e8f0', // slate-200
    noCodeStroke: '#cbd5e1', // slate-300
    label: '#475569', // slate-600
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

      {/* Digitization Leaderboards */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Last Digitization */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">楦头数字化全链路进度 (Last Digitization)</h3>
            <span className="text-xs text-slate-500">Total → HasCode → Has3D</span>
          </div>
          <div className="flex-1 min-h-[420px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats?.lastDigitizationStats || []} layout="vertical" margin={{ top: 10, right: 16, left: 20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis type="number" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis type="category" dataKey="brand" axisLine={false} tickLine={false} width={160} tick={{ fill: '#64748b', fontSize: 12, textAnchor: 'end' }} />
                <RechartsTooltip
                  cursor={{ fill: '#f8fafc' }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const p: any = payload?.[0]?.payload || {};
                    return (
                      <div className="rounded-lg border border-slate-700 bg-slate-900/90 shadow-sm px-3 py-2 text-xs text-white">
                        <div className="font-medium text-white mb-1">{p.brand}</div>
                        <div className="text-white/90">总生效款：{p.totalEffective}</div>
                        <div className="text-white/90">已绑编号：{p.hasCode}</div>
                        <div className="text-white/90">已完成 3D：{p.has3D}</div>
                        <div className="text-white/90">完成率：{p.completionRate}%</div>
                        <div className="text-white/90">
                          总缺口：{Math.max(0, Number(p.totalEffective || 0) - Number(p.has3D || 0))} 款
                        </div>
                      </div>
                    );
                  }}
                />
                <Legend
                  iconType="circle"
                  verticalAlign="bottom"
                  wrapperStyle={{ fontSize: '13px', paddingTop: '10px', ...LEGEND_TEXT_STYLE }}
                  formatter={(value) => <span style={LEGEND_TEXT_STYLE}>{String(value)}</span>}
                  payload={[
                    { value: '已匹配 3D', type: 'circle', color: COLORS.completed3D },
                    { value: '已绑编号 (待 3D)', type: 'circle', color: COLORS.hasCodeNo3D },
                    { value: '基础信息缺失 (未绑编号)', type: 'circle', color: COLORS.noCode },
                  ]}
                />
                <Bar
                  dataKey="segment_completed_3d"
                  name="已匹配 3D"
                  stackId="a"
                  fill={COLORS.completed3D}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.lastDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'last', 'completed_3d');
                  }}
                >
                  <LabelList
                    dataKey="completionRate"
                    position="right"
                    formatter={(v: any) => `${v}%`}
                    fill={COLORS.label}
                    fontSize={12}
                    fontWeight={700}
                  />
                </Bar>
                <Bar
                  dataKey="segment_has_code_no_3d"
                  name="已绑编号 (待 3D)"
                  stackId="a"
                  fill={COLORS.hasCodeNo3D}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.lastDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'last', 'has_code_no_3d');
                  }}
                />
                <Bar
                  dataKey="segment_no_code"
                  name="基础信息缺失 (未绑编号)"
                  stackId="a"
                  fill={COLORS.noCode}
                  stroke={COLORS.noCodeStroke}
                  strokeWidth={0.5}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.lastDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'last', 'no_code');
                  }}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Sole Digitization */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">大底数字化全链路进度 (Sole Digitization)</h3>
            <span className="text-xs text-slate-500">Total → HasCode → Has3D</span>
          </div>
          <div className="flex-1 min-h-[420px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats?.soleDigitizationStats || []} layout="vertical" margin={{ top: 10, right: 16, left: 20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis type="number" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis type="category" dataKey="brand" axisLine={false} tickLine={false} width={160} tick={{ fill: '#64748b', fontSize: 12, textAnchor: 'end' }} />
                <RechartsTooltip
                  cursor={{ fill: '#f8fafc' }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const p: any = payload?.[0]?.payload || {};
                    return (
                      <div className="rounded-lg border border-slate-700 bg-slate-900/90 shadow-sm px-3 py-2 text-xs text-white">
                        <div className="font-medium text-white mb-1">{p.brand}</div>
                        <div className="text-white/90">总生效款：{p.totalEffective}</div>
                        <div className="text-white/90">已绑编号：{p.hasCode}</div>
                        <div className="text-white/90">已完成 3D：{p.has3D}</div>
                        <div className="text-white/90">完成率：{p.completionRate}%</div>
                        <div className="text-white/90">
                          总缺口：{Math.max(0, Number(p.totalEffective || 0) - Number(p.has3D || 0))} 款
                        </div>
                      </div>
                    );
                  }}
                />
                <Legend
                  iconType="circle"
                  verticalAlign="bottom"
                  wrapperStyle={{ fontSize: '13px', paddingTop: '10px', ...LEGEND_TEXT_STYLE }}
                  formatter={(value) => <span style={LEGEND_TEXT_STYLE}>{String(value)}</span>}
                  payload={[
                    { value: '已匹配 3D', type: 'circle', color: COLORS.completed3D },
                    { value: '已绑编号 (待 3D)', type: 'circle', color: COLORS.hasCodeNo3D },
                    { value: '基础信息缺失 (未绑编号)', type: 'circle', color: COLORS.noCode },
                  ]}
                />
                <Bar
                  dataKey="segment_completed_3d"
                  name="已匹配 3D"
                  stackId="a"
                  fill={COLORS.completed3D}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.soleDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'sole', 'completed_3d');
                  }}
                >
                  <LabelList
                    dataKey="completionRate"
                    position="right"
                    formatter={(v: any) => `${v}%`}
                    fill={COLORS.label}
                    fontSize={12}
                    fontWeight={700}
                  />
                </Bar>
                <Bar
                  dataKey="segment_has_code_no_3d"
                  name="已绑编号 (待 3D)"
                  stackId="a"
                  fill={COLORS.hasCodeNo3D}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.soleDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'sole', 'has_code_no_3d');
                  }}
                />
                <Bar
                  dataKey="segment_no_code"
                  name="基础信息缺失 (未绑编号)"
                  stackId="a"
                  fill={COLORS.noCode}
                  stroke={COLORS.noCodeStroke}
                  strokeWidth={0.5}
                  maxBarSize={18}
                  onClick={(_, idx) => {
                    const row: any = (stats?.soleDigitizationStats || [])[idx];
                    if (row?.brand) drilldownToInventory(String(row.brand), 'sole', 'no_code');
                  }}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}

