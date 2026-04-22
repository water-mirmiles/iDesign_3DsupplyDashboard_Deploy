import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer, LabelList,
  AreaChart, Area
} from 'recharts';
import { Box, Layers, Hash, Factory, Loader2, Activity, TrendingUp, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AssetTrendStats } from '@/types';

type TrendHistoryPoint = { date: string; styles: number; lasts3D: number; soles3D: number };

type DashboardStatsResponse = {
  ok: boolean;
  source?: 'final_dashboard_data' | 'live';
  generatedAt?: string;
  dates: { latest: string; prev: string };
  mapping: { hasConfig: boolean; configPath: string };
  meta: {
    mainTable: string | null;
    requiredCols?: string[];
    reason?: string;
    uniqueBrandCount?: number;
    dataStatusColumn?: string | null;
    rawStatusAudit?: Array<{ value: string; rowCount: number }>;
  };
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
    deltaActiveStyles: number | null;
    deltaMatched3DLasts: number | null;
    deltaMatched3DSoles: number | null;
    deltaTotalPoolStyles?: number | null;
    delta3DLasts?: number | null;
    delta3DSoles?: number | null;
  };
  statusBuckets?: {
    total?: { kpis?: any; lastDigitizationStats?: any[]; soleDigitizationStats?: any[] };
    effective?: { kpis?: any; lastDigitizationStats?: any[]; soleDigitizationStats?: any[] };
    draft?: { kpis?: any; lastDigitizationStats?: any[]; soleDigitizationStats?: any[] };
    obsolete?: { kpis?: any; lastDigitizationStats?: any[]; soleDigitizationStats?: any[] };
    other?: { kpis?: any; lastDigitizationStats?: any[]; soleDigitizationStats?: any[] };
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
  trendHistory?: TrendHistoryPoint[];
  inventory?: Array<any>;
  /** 主表状态列原始值普查（全表物理行） */
  rawStatusAudit?: Array<{ value: string; rowCount: number }>;
  error?: string;
};

type TimePeriod = 'day' | 'week' | 'month' | 'quarter' | 'year';

/** 无 inventory 时，用后端分桶的榜单合并（生效+草稿） */
function mergeBrandDigitizationRows(a: any[] = [], b: any[] = []) {
  const map = new Map<string, { brand: string; totalEffective: number; hasCode: number; has3D: number }>();
  const ingest = (row: any) => {
    const brand = String(row?.brand || '').trim();
    if (!brand) return;
    const cur = map.get(brand) || { brand, totalEffective: 0, hasCode: 0, has3D: 0 };
    cur.totalEffective += Number(row?.totalEffective || 0);
    cur.hasCode += Number(row?.hasCode || 0);
    cur.has3D += Number(row?.has3D || 0);
    map.set(brand, cur);
  };
  for (const r of a) ingest(r);
  for (const r of b) ingest(r);
  return Array.from(map.values())
    .map((x) => {
      const segment_no_code = Math.max(0, x.totalEffective - x.hasCode);
      const segment_has_code_no_3d = Math.max(0, x.hasCode - x.has3D);
      const segment_completed_3d = Math.max(0, x.has3D);
      const completionRate = x.totalEffective > 0 ? Math.round((x.has3D / x.totalEffective) * 1000) / 10 : 0;
      return {
        brand: x.brand,
        totalEffective: x.totalEffective,
        hasCode: x.hasCode,
        has3D: x.has3D,
        segment_no_code,
        segment_has_code_no_3d,
        segment_completed_3d,
        completionRate,
      };
    })
    .sort((x, y) => (y.totalEffective || 0) - (x.totalEffective || 0))
    .slice(0, 30);
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStatsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isForceSyncing, setIsForceSyncing] = useState(false);
  const [statusScope, setStatusScope] = useState<'effective' | 'includeDraft' | 'total'>('effective');

  const [trendPeriod, setTrendPeriod] = useState<TimePeriod>('week');
  const [chartData, setChartData] = useState<TrendHistoryPoint[]>([]);

  /** 入库款号按归一化状态计数（与 Tab 求和一致；无分桶快照时回落 scopeKPIs） */
  const invStatusCounts = useMemo(() => {
    const sd: any = stats?.statusBuckets?.total?.kpis?.statusDist ?? stats?.kpis?.statusDist;
    if (!sd) return null;
    return {
      effective: Number(sd.active ?? 0),
      draft: Number(sd.draft ?? 0),
      obsolete: Number(sd.obsolete ?? 0),
      other: Number(sd.other ?? 0),
    };
  }, [stats?.statusBuckets?.total?.kpis?.statusDist, stats?.kpis?.statusDist]);

  const brandTotalAll = useMemo(() => {
    const inv = stats?.inventory || [];
    const set = new Set<string>();
    for (const it of inv) {
      const b = String(it?.brand || '').trim();
      if (b) set.add(b);
    }
    if (set.size > 0) return set.size;
    const n = Number(stats?.meta?.uniqueBrandCount);
    if (Number.isFinite(n) && n > 0) return n;
    return stats?.brandCoverage?.length || 0;
  }, [stats?.inventory, stats?.meta?.uniqueBrandCount, stats?.brandCoverage]);

  const scopeKPIs = useMemo(() => {
    const sb: any = stats?.statusBuckets || null;
    const fallback = stats?.kpis || null;
    const eff = sb?.effective?.kpis || null;
    const dra = sb?.draft?.kpis || null;
    const tot = sb?.total?.kpis || null;

    const merge = (a: any, b: any) => {
      const A = a || {};
      const B = b || {};
      const totalStyles = Number(A.totalStyles || 0) + Number(B.totalStyles || 0);
      const matchedLasts = Number(A.matchedLasts || A.last3DCount || 0) + Number(B.matchedLasts || B.last3DCount || 0);
      const matchedSoles = Number(A.matchedSoles || A.sole3DCount || 0) + Number(B.matchedSoles || B.sole3DCount || 0);
      const stylesWithAny3D = Number(A.stylesWithAny3D || 0) + Number(B.stylesWithAny3D || 0);
      const lastCodeLinked = Number(A.lastCodeLinked || 0) + Number(B.lastCodeLinked || 0);
      const soleCodeLinked = Number(A.soleCodeLinked || 0) + Number(B.soleCodeLinked || 0);

      const last3DCoverage = totalStyles > 0 ? Math.round((matchedLasts / totalStyles) * 1000) / 10 : 0;
      const sole3DCoverage = totalStyles > 0 ? Math.round((matchedSoles / totalStyles) * 1000) / 10 : 0;
      const lastCoverage = totalStyles > 0 ? Math.round((matchedLasts / totalStyles) * 100) : 0;
      const soleCoverage = totalStyles > 0 ? Math.round((matchedSoles / totalStyles) * 100) : 0;
      const any3DCoveragePercent = totalStyles > 0 ? Math.round((stylesWithAny3D / totalStyles) * 100) : 0;
      const lastCodeLinkRate = totalStyles > 0 ? Math.round((lastCodeLinked / totalStyles) * 1000) / 10 : 0;
      const soleCodeLinkRate = totalStyles > 0 ? Math.round((soleCodeLinked / totalStyles) * 1000) / 10 : 0;

      return {
        styles: { totalAll: totalStyles, totalEffective: totalStyles },
        totalStyles,
        activeStyles: totalStyles,
        matched3DLasts: matchedLasts,
        matched3DSoles: matchedSoles,
        last3DCount: matchedLasts,
        sole3DCount: matchedSoles,
        stylesWithAny3D,
        any3DCoveragePercent,
        last3DCoverage,
        sole3DCoverage,
        lastCoverage,
        soleCoverage,
        lastCodeLinked,
        soleCodeLinked,
        lastCodeLinkRate,
        soleCodeLinkRate,
        deltaActiveStyles: 0,
        deltaMatched3DLasts: 0,
        deltaMatched3DSoles: 0,
      };
    };

    if (!sb) return fallback;
    if (statusScope === 'effective') return eff || fallback;
    if (statusScope === 'includeDraft') return merge(eff, dra);
    // 全量池：必须用 statusBuckets.total（含作废），禁止误用 draft 或仅生效的顶层 kpis
    const t = tot;
    if (t && typeof t.totalStyles === 'number') {
      return {
        ...t,
        styles: {
          totalAll: t.totalStyles,
          totalEffective: stats?.kpis?.styles?.totalEffective ?? stats?.kpis?.activeStyles ?? 0,
        },
      };
    }
    return fallback;
  }, [stats, statusScope]);

  /** 与详细清单一致：当前 Tab 口径下 has3DLast === true 的行数（优先于分桶 KPI 展示） */
  const kpiLast3DMatchedFromInventory = useMemo(() => {
    const inv = stats?.inventory || [];
    if (!inv.length) return null;
    const want = (ds: string) => {
      const s = String(ds || '').trim();
      if (statusScope === 'effective') return s === 'active';
      if (statusScope === 'includeDraft') return s === 'active' || s === 'draft';
      return true;
    };
    return inv.filter((it) => want(String(it?.data_status || '')) && it.has3DLast === true).length;
  }, [stats?.inventory, statusScope]);

  const kpiSole3DMatchedFromInventory = useMemo(() => {
    const inv = stats?.inventory || [];
    if (!inv.length) return null;
    const want = (ds: string) => {
      const s = String(ds || '').trim();
      if (statusScope === 'effective') return s === 'active';
      if (statusScope === 'includeDraft') return s === 'active' || s === 'draft';
      return true;
    };
    return inv.filter((it) => {
      if (!want(String(it?.data_status || ''))) return false;
      return it.has3DSole === true || String(it?.soleStatus || '') === 'matched';
    }).length;
  }, [stats?.inventory, statusScope]);

  const displayLast3DKpi =
    kpiLast3DMatchedFromInventory !== null ? kpiLast3DMatchedFromInventory : (scopeKPIs?.last3DCount ?? scopeKPIs?.matched3DLasts ?? 0);
  const displaySole3DKpi =
    kpiSole3DMatchedFromInventory !== null ? kpiSole3DMatchedFromInventory : (scopeKPIs?.sole3DCount ?? scopeKPIs?.matched3DSoles ?? 0);

  /** 仅生效 / 含草稿 / 全量池：effective + draft + invalid(=obsolete) + other，与入库行数一致 */
  const tabStyleTotalCount = useMemo(() => {
    const c = invStatusCounts;
    if (!c) return Number(scopeKPIs?.styles?.totalAll ?? scopeKPIs?.totalStyles ?? 0);
    if (statusScope === 'effective') return c.effective;
    if (statusScope === 'includeDraft') return c.effective + c.draft;
    return c.effective + c.draft + c.obsolete + c.other;
  }, [invStatusCounts, statusScope, scopeKPIs]);

  const digitizationStats = useCallback(
    (dim: 'last' | 'sole') => {
      const inv = stats?.inventory || [];
      const want = (s: string) => {
        if (statusScope === 'effective') return s === 'active';
        if (statusScope === 'includeDraft') return s === 'active' || s === 'draft';
        return true; // 全量池：含作废 + 其他
      };
      const codeField = dim === 'last' ? 'lastCode' : 'soleCode';
      const statusField = dim === 'last' ? 'lastStatus' : 'soleStatus';
      const isLinkedCode = (v: any) => {
        const t = String(v ?? '').trim();
        return t !== '' && t !== '-' && t !== '0';
      };

      const byBrand = new Map<string, { brand: string; total: number; hasCode: number; has3D: number }>();
      for (const it of inv) {
        const s = String(it?.data_status || '').trim();
        if (!want(s)) continue;
        const brand = String(it?.brand || 'Unknown').trim() || 'Unknown';
        const cur = byBrand.get(brand) || { brand, total: 0, hasCode: 0, has3D: 0 };
        cur.total += 1;
        const hasCode = isLinkedCode(it?.[codeField]);
        if (hasCode) cur.hasCode += 1;
        const has3D =
          dim === 'last'
            ? hasCode && (it?.has3DLast === true || String(it?.lastStatus || '') === 'matched')
            : hasCode && (it?.has3DSole === true || String(it?.soleStatus || '') === 'matched');
        if (has3D) cur.has3D += 1;
        byBrand.set(brand, cur);
      }

      // 品牌全集：从全量 inventory 扫一遍，确保切换状态时品牌不“消失”
      const allBrands = (() => {
        const set = new Set<string>();
        for (const it of inv) {
          const b = String(it?.brand || '').trim();
          if (b) set.add(b);
        }
        return Array.from(set.values());
      })();

      return allBrands
        .map((b) => byBrand.get(b) || { brand: b, total: 0, hasCode: 0, has3D: 0 })
        .map((x) => {
          const segment_no_code = Math.max(0, x.total - x.hasCode);
          const segment_has_code_no_3d = Math.max(0, x.hasCode - x.has3D);
          const segment_completed_3d = Math.max(0, x.has3D);
          const completionRate = x.total > 0 ? Math.round((x.has3D / x.total) * 1000) / 10 : 0;
          return {
            brand: x.brand,
            totalEffective: x.total,
            hasCode: x.hasCode,
            has3D: x.has3D,
            segment_no_code,
            segment_has_code_no_3d,
            segment_completed_3d,
            completionRate,
          };
        })
        .sort((a, b) => (b.totalEffective || 0) - (a.totalEffective || 0))
        .slice(0, 30);
    },
    [stats?.inventory, statusScope]
  );

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const resp = await fetch(`/api/dashboard-stats?t=${Date.now()}`);
        const json = (await resp.json()) as DashboardStatsResponse;
        if (!resp.ok || !json.ok) throw new Error(json.error || `加载失败（HTTP ${resp.status}）`);
        if (cancelled) return;
        // eslint-disable-next-line no-console
        console.log('Dashboard Raw Data Received:', json);
        setStats(json);
        setChartData(Array.isArray(json.trendHistory) ? json.trendHistory : []);
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
      const resp2 = await fetch(`/api/dashboard-stats?refresh=1&t=${Date.now()}`);
      const json2 = (await resp2.json()) as DashboardStatsResponse;
      if (!resp2.ok || !json2.ok) throw new Error(json2.error || `刷新失败（HTTP ${resp2.status}）`);
      // eslint-disable-next-line no-console
      console.log('Dashboard Raw Data Received:', json2);
      setStats(json2);
      setChartData(Array.isArray(json2.trendHistory) ? json2.trendHistory : []);
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

  const getTrendBadge = (value: number | null | undefined, isPercent = false) => {
    if (value == null) {
      return (
        <div className="flex items-center text-xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded mb-1">
          <Activity className="w-3 h-3 mr-1" />
          首个快照
        </div>
      );
    }
    if (value > 0) {
      return (
        <div className="flex items-center text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded mb-1">
          <TrendingUp className="w-3 h-3 mr-1" />
          较上月 +{value}{isPercent ? '%' : ''}
        </div>
      );
    } else {
      return (
        <div className="flex items-center text-xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded mb-1">
          <Activity className="w-3 h-3 mr-1" />
          较上月 {value}
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
  const lastChartRows = useMemo(() => {
    const inv = stats?.inventory;
    if (Array.isArray(inv) && inv.length > 0) return digitizationStats('last');
    const sb: any = stats?.statusBuckets;
    if (!sb) return stats?.lastDigitizationStats || [];
    if (statusScope === 'effective') return sb.effective?.lastDigitizationStats || stats?.lastDigitizationStats || [];
    if (statusScope === 'total') return sb.total?.lastDigitizationStats || [];
    return mergeBrandDigitizationRows(sb.effective?.lastDigitizationStats, sb.draft?.lastDigitizationStats);
  }, [stats?.inventory, stats?.statusBuckets, stats?.lastDigitizationStats, statusScope, digitizationStats]);

  const soleChartRows = useMemo(() => {
    const inv = stats?.inventory;
    if (Array.isArray(inv) && inv.length > 0) return digitizationStats('sole');
    const sb: any = stats?.statusBuckets;
    if (!sb) return stats?.soleDigitizationStats || [];
    if (statusScope === 'effective') return sb.effective?.soleDigitizationStats || stats?.soleDigitizationStats || [];
    if (statusScope === 'total') return sb.total?.soleDigitizationStats || [];
    return mergeBrandDigitizationRows(sb.effective?.soleDigitizationStats, sb.draft?.soleDigitizationStats);
  }, [stats?.inventory, stats?.statusBuckets, stats?.soleDigitizationStats, statusScope, digitizationStats]);

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
        <div className="shrink-0 flex items-center gap-3">
          <div className="inline-flex items-center rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden">
            <button
              className={cn(
                'px-3 py-2 text-sm font-medium transition-colors',
                statusScope === 'effective' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700 hover:bg-slate-50'
              )}
              onClick={() => setStatusScope('effective')}
            >
              仅生效款
            </button>
            <button
              className={cn(
                'px-3 py-2 text-sm font-medium transition-colors border-l border-slate-200',
                statusScope === 'includeDraft' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700 hover:bg-slate-50'
              )}
              onClick={() => setStatusScope('includeDraft')}
            >
              包含草稿
            </button>
            <button
              className={cn(
                'px-3 py-2 text-sm font-medium transition-colors border-l border-slate-200',
                statusScope === 'total' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700 hover:bg-slate-50'
              )}
              onClick={() => setStatusScope('total')}
            >
              全量池
            </button>
          </div>

          <button
            onClick={() => void handleForceSync()}
            disabled={isForceSyncing}
            className={cn(
              'inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border shadow-sm transition-colors',
              isForceSyncing
                ? 'bg-slate-100 text-slate-500 border-slate-200 cursor-not-allowed'
                : 'bg-white text-slate-700 border-slate-200 hover:bg-slate-50'
            )}
          >
            {isForceSyncing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Activity className="w-4 h-4" />}
            一键重算看板数据
          </button>
        </div>
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
            <span className="text-3xl font-bold text-slate-900">{(brandTotalAll || 0).toLocaleString()}</span>
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
              <span className="text-3xl font-bold text-slate-900">{(tabStyleTotalCount || 0).toLocaleString()}</span>
              {getTrendBadge(stats?.kpis?.deltaTotalPoolStyles)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              当前口径：
              <span className="font-medium text-slate-700">
                {statusScope === 'effective'
                  ? '仅生效'
                  : statusScope === 'includeDraft'
                    ? '生效+草稿'
                    : '全量池（effective + draft + invalid）'}
              </span>
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
                {displayLast3DKpi.toLocaleString()}
              </span>
              {getTrendBadge(stats?.kpis?.delta3DLasts ?? stats?.kpis?.deltaMatched3DLasts)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              编号绑定率：{' '}
              <span className="font-medium text-slate-700">
                {scopeKPIs?.lastCodeLinkRate ?? 0}%
              </span>
            </div>
            <div className="mt-1 text-xs text-slate-500">
              3D 覆盖率：{' '}
              <span className="font-medium text-slate-700">
                {scopeKPIs?.last3DCoverage ?? scopeKPIs?.lastCoverage ?? 0}%
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
                {displaySole3DKpi.toLocaleString()}
              </span>
              {getTrendBadge(stats?.kpis?.delta3DSoles ?? stats?.kpis?.deltaMatched3DSoles)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              编号绑定率: <span className="font-medium text-slate-700">{scopeKPIs?.soleCodeLinkRate ?? 0}%</span>
            </div>
            <div className="mt-1 text-xs text-slate-500">
              3D 覆盖率: <span className="font-medium text-slate-700">{scopeKPIs?.sole3DCoverage ?? scopeKPIs?.soleCoverage ?? 0}%</span>
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
                {scopeKPIs?.any3DCoveragePercent != null
                  ? scopeKPIs.any3DCoveragePercent
                  : Math.round((((scopeKPIs?.lastCoverage ?? 0) + (scopeKPIs?.soleCoverage ?? 0)) / 2))}
                %
              </span>
              {getTrendBadge(0, true)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              {scopeKPIs?.stylesWithAny3D != null
                ? `当前口径任一 3D 命中：${scopeKPIs.stylesWithAny3D} / ${scopeKPIs?.activeStyles ?? scopeKPIs?.totalStyles ?? 0}`
                : '楦/底覆盖率均值（无快照细分时）'}
            </div>
            <div className="mt-2 h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
              <div
                className={cn(
                  'h-full rounded-full transition-all duration-1000',
                  getProgressColor(
                    scopeKPIs?.any3DCoveragePercent != null
                      ? scopeKPIs.any3DCoveragePercent
                      : Math.round((((scopeKPIs?.lastCoverage ?? 0) + (scopeKPIs?.soleCoverage ?? 0)) / 2))
                  )
                )}
                style={{
                  width: `${
                    scopeKPIs?.any3DCoveragePercent != null
                      ? scopeKPIs.any3DCoveragePercent
                      : Math.round((((scopeKPIs?.lastCoverage ?? 0) + (scopeKPIs?.soleCoverage ?? 0)) / 2))
                  }%`,
                }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* 3D 资产新增趋势（暂时隐藏；需要时把 false 改成 true） */}
      {false && (
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
          <div className="flex items-center justify-between gap-4 mb-4">
            <div className="min-w-0">
              <h3 className="text-base font-semibold text-slate-900">3D 资产新增趋势</h3>
              <div className="text-xs text-slate-500 mt-1">按日回溯 data_tables 目录生成（生效口径累计）</div>
            </div>
            <div className="shrink-0 flex items-center gap-2">
              <span className="text-xs text-slate-500">周期</span>
              <select
                value={trendPeriod}
                onChange={(e) => handlePeriodChange(e.target.value as TimePeriod)}
                className="text-xs rounded-md border border-slate-200 bg-white px-2 py-1 text-slate-700"
              >
                <option value="month">月</option>
                <option value="week">周（占位）</option>
                <option value="quarter">季（占位）</option>
                <option value="year">年（占位）</option>
              </select>
            </div>
          </div>
          <div className="h-[260px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData || []} margin={{ top: 10, right: 16, left: 8, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip
                  cursor={{ stroke: '#94a3b8', strokeDasharray: '4 4' }}
                  contentStyle={{ borderRadius: 12, borderColor: '#e2e8f0' }}
                />
                <Legend wrapperStyle={{ fontSize: '12px', ...LEGEND_TEXT_STYLE }} />
                <Area type="monotone" dataKey="lasts3D" name="3D 楦头累计" stroke="#0284c7" fill="#bae6fd" strokeWidth={2} />
                <Area type="monotone" dataKey="soles3D" name="3D 大底累计" stroke="#7c3aed" fill="#ddd6fe" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          {(!chartData || chartData.length === 0) && (
            <div className="text-xs text-slate-500 mt-3">暂无趋势点：请确认 `server/storage/data_tables/` 下存在多个日期目录。</div>
          )}
        </div>
      )}

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
              <BarChart data={lastChartRows || []} layout="vertical" margin={{ top: 10, right: 16, left: 20, bottom: 0 }}>
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
                    const row: any = (lastChartRows || [])[idx];
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
                    const row: any = (lastChartRows || [])[idx];
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
                    const row: any = (lastChartRows || [])[idx];
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
              <BarChart data={soleChartRows || []} layout="vertical" margin={{ top: 10, right: 16, left: 20, bottom: 0 }}>
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
                    const row: any = (soleChartRows || [])[idx];
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
                    const row: any = (soleChartRows || [])[idx];
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
                    const row: any = (soleChartRows || [])[idx];
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

