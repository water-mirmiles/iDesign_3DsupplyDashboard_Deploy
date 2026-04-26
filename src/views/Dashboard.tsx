import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer, LabelList,
  AreaChart, Area
} from 'recharts';
import { Box, Layers, Hash, Factory, Loader2, Activity, TrendingUp, AlertCircle, FileWarning, ChevronDown, Search } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AssetTrendStats } from '@/types';
import { CORE_MAIN_TABLE_NAME } from '@/lib/dataManifest';

type TrendHistoryPoint = { date: string; styles: number; lasts3D: number; soles3D: number };

type DashboardStatsResponse = {
  ok: boolean;
  source?: 'final_dashboard_data' | 'live';
  generatedAt?: string;
  dates: { latest: string; prev: string };
  mapping: { hasConfig: boolean; configPath: string; usedDefaultMapping?: boolean };
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
  styleMetadata?: Array<{ style_wms: string; brand: string; status: string; level: string; hasId: boolean; has3D: boolean }>;
  filterOptions?: {
    statuses?: string[];
    levels?: string[];
  };
  /** 主表状态列原始值普查（全表物理行） */
  rawStatusAudit?: Array<{ value: string; rowCount: number }>;
  error?: string;
};

type TimePeriod = 'day' | 'week' | 'month' | 'quarter' | 'year';

type MandatoryFilesResponse = {
  ok: boolean;
  items: Array<{
    tableName: string;
    ready: boolean;
    fileName: string | null;
  }>;
};

function ChartEmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="flex h-full min-h-[360px] w-full items-center justify-center rounded-xl border border-dashed border-slate-200 bg-slate-50/70 px-6 text-center">
      <div>
        <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-white shadow-sm">
          <FileWarning className="h-6 w-6 text-slate-400" />
        </div>
        <div className="mt-4 text-sm font-semibold text-slate-800">{title}</div>
        <p className="mt-2 max-w-md text-xs leading-5 text-slate-500">{description}</p>
      </div>
    </div>
  );
}

function statusLabel(status: string) {
  const s = String(status || '').trim();
  if (s === 'active') return '生效';
  if (s === 'draft') return '草稿';
  if (s === 'obsolete') return '作废';
  if (s === 'other') return '其他';
  return s || '未知';
}

function levelLabel(level: string) {
  const s = String(level || '').trim();
  if (!s || s === '未定级') return '未定级';
  if (/^[SABCDEF]$/.test(s)) return `${s}级`;
  return s;
}

function normalizeLevelForFilter(level: string) {
  const s = String(level || '').trim().toUpperCase().replace(/\s+/g, '');
  if (!s) return '未定级';
  return s.endsWith('级') ? s.slice(0, -1) || '未定级' : s;
}

function levelSelectionMatches(selectedLevel: string, optionLevel: string, coreLevels: string[]) {
  const selectedNormalized = normalizeLevelForFilter(selectedLevel);
  const optionNormalized = normalizeLevelForFilter(optionLevel);
  if (coreLevels.includes(selectedNormalized) || coreLevels.includes(optionNormalized)) {
    return selectedNormalized === optionNormalized;
  }
  return selectedLevel === optionLevel || selectedNormalized === optionNormalized;
}

function isLinkedCode(v: any) {
  const s = String(v ?? '').trim();
  return s !== '' && s !== '-' && s !== '0';
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStatsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isForceSyncing, setIsForceSyncing] = useState(false);
  const [mandatoryStatus, setMandatoryStatus] = useState<MandatoryFilesResponse | null>(null);
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>(['active']);
  const [selectedLevels, setSelectedLevels] = useState<string[]>([]);
  const [filtersInitialized, setFiltersInitialized] = useState(false);
  const [isOtherLevelsOpen, setIsOtherLevelsOpen] = useState(false);
  const [otherLevelSearch, setOtherLevelSearch] = useState('');

  const [trendPeriod, setTrendPeriod] = useState<TimePeriod>('week');
  const [chartData, setChartData] = useState<TrendHistoryPoint[]>([]);
  const coreLevelOptions = useMemo(() => ['S', 'A', 'B', 'C', 'EOL'], []);
  const defaultLevelOptions = useMemo(() => ['S', 'A', 'B', 'C'], []);

  const isCoreMainMissing = useMemo(() => {
    const mainTable = mandatoryStatus?.items.find((item) => item.tableName === CORE_MAIN_TABLE_NAME);
    return mandatoryStatus !== null && mainTable?.ready === false;
  }, [mandatoryStatus]);

  const statusOptions = useMemo(() => {
    const fromApi = stats?.filterOptions?.statuses || [];
    const preferred = ['active', 'draft', 'obsolete'];
    const all = Array.from(new Set([...preferred, ...fromApi])).filter(Boolean);
    return all.filter((s) => s !== 'other' || fromApi.includes('other'));
  }, [stats?.filterOptions?.statuses]);

  const levelOptions = useMemo(() => {
    const levels = stats?.filterOptions?.levels || [];
    return levels.filter(Boolean);
  }, [stats?.filterOptions?.levels]);

  const otherLevelOptions = useMemo(() => {
    const coreSet = new Set(coreLevelOptions);
    return levelOptions.filter((level) => !coreSet.has(normalizeLevelForFilter(level)));
  }, [coreLevelOptions, levelOptions]);

  const filteredOtherLevelOptions = useMemo(() => {
    const q = otherLevelSearch.trim().toLowerCase();
    if (!q) return otherLevelOptions;
    return otherLevelOptions.filter((level) => String(level).toLowerCase().includes(q));
  }, [otherLevelOptions, otherLevelSearch]);

  const selectedOtherCount = useMemo(
    () => selectedLevels.filter((level) => !coreLevelOptions.includes(normalizeLevelForFilter(level))).length,
    [coreLevelOptions, selectedLevels]
  );

  const allStatusesSelected = useMemo(
    () => statusOptions.length > 0 && statusOptions.every((status) => selectedStatuses.includes(status)),
    [selectedStatuses, statusOptions]
  );

  const allLevelsSelected = useMemo(
    () =>
      levelOptions.length > 0 &&
      levelOptions.every((level) => selectedLevels.some((selected) => levelSelectionMatches(selected, level, coreLevelOptions))),
    [coreLevelOptions, levelOptions, selectedLevels]
  );

  const selectedLevelSummary = useMemo(() => {
    if (allLevelsSelected) return '全部定级';
    if (selectedLevels.length === 0) return '未选择定级';
    const core = coreLevelOptions.filter((level) => selectedLevels.some((x) => normalizeLevelForFilter(x) === level));
    const parts = [...core.map(levelLabel)];
    if (selectedOtherCount > 0) parts.push(`其他 ${selectedOtherCount} 项`);
    return parts.join('、') || '未选择定级';
  }, [allLevelsSelected, coreLevelOptions, selectedLevels, selectedOtherCount]);

  const selectedStatusSummary = useMemo(() => {
    if (allStatusesSelected) return '全部状态';
    return selectedStatuses.map(statusLabel).join('、') || '未选择状态';
  }, [allStatusesSelected, selectedStatuses]);

  const criteriaText = useMemo(
    () => `当前统计口径：${selectedStatusSummary} · ${selectedLevelSummary}`,
    [selectedLevelSummary, selectedStatusSummary]
  );

  const handleToggleAllStatuses = useCallback(() => {
    setSelectedStatuses(allStatusesSelected ? [] : statusOptions);
  }, [allStatusesSelected, statusOptions]);

  const handleToggleAllLevels = useCallback(() => {
    setSelectedLevels(allLevelsSelected ? [] : levelOptions);
  }, [allLevelsSelected, levelOptions]);

  useEffect(() => {
    if (!stats || filtersInitialized) return;
    setSelectedStatuses(statusOptions.includes('active') ? ['active'] : statusOptions);
    setSelectedLevels(defaultLevelOptions);
    setFiltersInitialized(true);
  }, [defaultLevelOptions, filtersInitialized, stats, statusOptions]);

  const filteredInventory = useMemo(() => {
    const inv = stats?.inventory || [];
    return inv.filter((item) => {
      const status = String(item?.data_status || '').trim();
      const rawLevel = String(item?.product_actual_position ?? item?.productLevel ?? '未定级').trim() || '未定级';
      const levelMatched = selectedLevels.some((level) => levelSelectionMatches(level, rawLevel, coreLevelOptions));
      return selectedStatuses.includes(status) && levelMatched;
    });
  }, [coreLevelOptions, selectedLevels, selectedStatuses, stats?.inventory]);

  const brandTotalAll = useMemo(() => {
    const set = new Set<string>();
    for (const it of filteredInventory) {
      const b = String(it?.brand || '').trim();
      if (b) set.add(b);
    }
    if (set.size > 0) return set.size;
    return 0;
  }, [filteredInventory]);

  const scopeKPIs = useMemo(() => {
    const totalStyles = filteredInventory.length;
    const matchedLasts = filteredInventory.filter((x) => x?.has3DLast === true || String(x?.lastStatus || '') === 'matched').length;
    const matchedSoles = filteredInventory.filter((x) => x?.has3DSole === true || String(x?.soleStatus || '') === 'matched').length;
    const lastCodeLinked = filteredInventory.filter((x) => isLinkedCode(x?.lastCode)).length;
    const soleCodeLinked = filteredInventory.filter((x) => isLinkedCode(x?.soleCode)).length;
    const stylesWithAny3D = filteredInventory.filter(
      (x) => x?.has3DLast === true || x?.has3DSole === true || String(x?.lastStatus || '') === 'matched' || String(x?.soleStatus || '') === 'matched'
    ).length;
    const pct1 = (num: number) => (totalStyles > 0 ? Math.round((num / totalStyles) * 1000) / 10 : 0);
    const pct0 = (num: number) => (totalStyles > 0 ? Math.round((num / totalStyles) * 100) : 0);
    return {
      styles: { totalAll: totalStyles, totalEffective: totalStyles },
      totalStyles,
      activeStyles: totalStyles,
      matched3DLasts: matchedLasts,
      matched3DSoles: matchedSoles,
      last3DCount: matchedLasts,
      sole3DCount: matchedSoles,
      stylesWithAny3D,
      any3DCoveragePercent: pct0(stylesWithAny3D),
      last3DCoverage: pct1(matchedLasts),
      sole3DCoverage: pct1(matchedSoles),
      lastCoverage: pct0(matchedLasts),
      soleCoverage: pct0(matchedSoles),
      lastCodeLinked,
      soleCodeLinked,
      lastCodeLinkRate: pct1(lastCodeLinked),
      soleCodeLinkRate: pct1(soleCodeLinked),
      deltaActiveStyles: 0,
      deltaMatched3DLasts: 0,
      deltaMatched3DSoles: 0,
    };
  }, [filteredInventory]);

  const displayLast3DKpi = scopeKPIs?.last3DCount ?? scopeKPIs?.matched3DLasts ?? 0;
  const displaySole3DKpi = scopeKPIs?.sole3DCount ?? scopeKPIs?.matched3DSoles ?? 0;
  const tabStyleTotalCount = scopeKPIs?.totalStyles ?? scopeKPIs?.styles?.totalAll ?? 0;

  const digitizationStats = useCallback(
    (dim: 'last' | 'sole') => {
      const inv = filteredInventory;
      const codeField = dim === 'last' ? 'lastCode' : 'soleCode';

      const byBrand = new Map<string, { brand: string; total: number; hasCode: number; has3D: number }>();
      for (const it of inv) {
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
    [filteredInventory]
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

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      try {
        const resp = await fetch(`/api/check-mandatory-files?t=${Date.now()}`);
        const json = (await resp.json()) as MandatoryFilesResponse;
        if (!cancelled && resp.ok && json?.ok) setMandatoryStatus(json);
      } catch {
        // 缺失检测失败时不阻断原有看板加载
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
    return sb.effective?.lastDigitizationStats || stats?.lastDigitizationStats || [];
  }, [stats?.inventory, stats?.statusBuckets, stats?.lastDigitizationStats, digitizationStats]);

  const soleChartRows = useMemo(() => {
    const inv = stats?.inventory;
    if (Array.isArray(inv) && inv.length > 0) return digitizationStats('sole');
    const sb: any = stats?.statusBuckets;
    if (!sb) return stats?.soleDigitizationStats || [];
    return sb.effective?.soleDigitizationStats || stats?.soleDigitizationStats || [];
  }, [stats?.inventory, stats?.statusBuckets, stats?.soleDigitizationStats, digitizationStats]);

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

      {isCoreMainMissing && (
        <div className="flex min-h-[520px] w-full items-center justify-center rounded-2xl border border-dashed border-slate-300 bg-white px-6 py-16 text-center shadow-sm">
          <div className="max-w-2xl">
            <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-slate-100">
              <FileWarning className="h-10 w-10 text-slate-400" />
            </div>
            <h2 className="mt-6 text-2xl font-semibold text-slate-900">核心主表缺失</h2>
            <p className="mt-3 text-base text-slate-600">
              请先前往数据中心上传 <span className="font-mono font-semibold text-slate-900">ods_pdm_pdm_product_info_df.xlsx</span>
            </p>
            <p className="mt-2 text-sm text-slate-500">
              该表提供款号、品牌、状态和关联 ID。缺少它时，看板无法生成可信统计。
            </p>
          </div>
        </div>
      )}

      {!isLoading &&
        !isCoreMainMissing &&
        stats &&
        (!stats.meta?.mainTable ||
          (!stats.mapping?.hasConfig &&
            !stats.mapping?.usedDefaultMapping &&
            Number(stats.kpis?.lastCodeLinkRate ?? 0) <= 0)) && (
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

      {!isCoreMainMissing && error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {!isCoreMainMissing && stats && (
        <div className="w-full rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="min-w-0">
              <h3 className="text-base font-semibold text-slate-900">筛选中心</h3>
              <p className="mt-1 truncate text-xs text-slate-500">
                按款式状态和产品定级取交集，KPI 与品牌排行实时重算。
              </p>
            </div>
            <button
              type="button"
              onClick={() => {
                setSelectedStatuses(statusOptions.includes('active') ? ['active'] : statusOptions);
                setSelectedLevels(defaultLevelOptions);
                setOtherLevelSearch('');
                setIsOtherLevelsOpen(false);
              }}
              className="text-xs font-medium text-blue-600 hover:text-blue-700"
            >
              重置筛选
            </button>
          </div>
          <div className="mt-4 flex min-h-[48px] flex-col gap-4 lg:flex-row lg:items-center lg:gap-5">
            <div className="flex shrink-0 items-center gap-3">
              <div className="text-xs font-medium text-slate-500">款式状态</div>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={handleToggleAllStatuses}
                  className={cn(
                    'inline-flex items-center rounded-lg border px-3 py-2 text-sm font-medium transition-colors',
                    allStatusesSelected ? 'border-blue-600 bg-blue-600 text-white shadow-sm' : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                  )}
                >
                  全部
                </button>
                {statusOptions.map((status) => {
                  const checked = selectedStatuses.includes(status);
                  return (
                    <button
                      type="button"
                      key={status}
                      onClick={() =>
                        setSelectedStatuses((prev) =>
                          prev.includes(status) ? prev.filter((x) => x !== status) : [...prev, status]
                        )
                      }
                      className={cn(
                        'inline-flex items-center rounded-lg border px-3 py-2 text-sm font-medium transition-colors',
                        checked ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                      )}
                    >
                      {statusLabel(status)}
                    </button>
                  );
                })}
              </div>
            </div>
            <div className="hidden h-8 w-px bg-slate-200 lg:block" />
            <div className="relative flex min-w-0 flex-1 items-center gap-3">
              <div className="shrink-0 text-xs font-medium text-slate-500">产品定级</div>
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={handleToggleAllLevels}
                  className={cn(
                    'rounded-lg border px-3 py-2 text-sm font-semibold transition-colors',
                    allLevelsSelected ? 'border-blue-600 bg-blue-600 text-white shadow-sm' : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                  )}
                >
                  全部
                </button>
                {coreLevelOptions.map((level) => {
                  const checked = selectedLevels.some((x) => normalizeLevelForFilter(x) === level);
                  return (
                    <button
                      key={level}
                      type="button"
                      onClick={() =>
                        setSelectedLevels((prev) =>
                          checked ? prev.filter((x) => normalizeLevelForFilter(x) !== level) : [...prev, level]
                        )
                      }
                      className={cn(
                        'rounded-lg border px-3 py-2 text-sm font-semibold transition-colors',
                        checked ? 'border-blue-600 bg-blue-600 text-white shadow-sm' : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                      )}
                    >
                      {level === 'EOL' ? 'EOL' : `${level}级`}
                    </button>
                  );
                })}
                <div className="relative">
                  <button
                    type="button"
                    onClick={() => setIsOtherLevelsOpen((v) => !v)}
                    className={cn(
                      'inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm font-semibold transition-colors',
                      selectedOtherCount > 0
                        ? 'border-blue-600 bg-blue-50 text-blue-700'
                        : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                    )}
                  >
                    其他定级
                    {selectedOtherCount > 0 && <span className="rounded-full bg-blue-600 px-1.5 py-0.5 text-[10px] text-white">{selectedOtherCount}</span>}
                    <ChevronDown className={cn('h-4 w-4 transition-transform', isOtherLevelsOpen && 'rotate-180')} />
                  </button>

                  {isOtherLevelsOpen && (
                    <div className="absolute left-0 top-full z-30 mt-2 w-[min(560px,calc(100vw-3rem))] rounded-xl border border-slate-200 bg-white p-3 shadow-xl">
                      <div className="relative">
                        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                        <input
                          value={otherLevelSearch}
                          onChange={(e) => setOtherLevelSearch(e.target.value)}
                          placeholder="搜索其他定级..."
                          className="w-full rounded-lg border border-slate-200 bg-slate-50 py-2 pl-9 pr-3 text-sm text-slate-700 outline-none focus:border-blue-300 focus:bg-white focus:ring-2 focus:ring-blue-100"
                        />
                      </div>
                      <div className="mt-3 max-h-64 overflow-y-auto pr-1">
                        {filteredOtherLevelOptions.length === 0 ? (
                          <div className="rounded-lg bg-slate-50 px-3 py-6 text-center text-sm text-slate-400">没有匹配的其他定级</div>
                        ) : (
                          <div className="space-y-1">
                            {filteredOtherLevelOptions.map((level) => {
                              const checked = selectedLevels.includes(level);
                              return (
                                <label
                                  key={level}
                                  className="flex cursor-pointer items-start gap-2 rounded-lg px-2 py-2 text-sm text-slate-700 hover:bg-slate-50"
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() =>
                                      setSelectedLevels((prev) =>
                                        prev.includes(level) ? prev.filter((x) => x !== level) : [...prev, level]
                                      )
                                    }
                                    className="mt-0.5 h-4 w-4 accent-blue-600"
                                  />
                                  <span className="leading-5">{levelLabel(level)}</span>
                                </label>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
                <span className="whitespace-nowrap text-xs text-slate-500">
                  当前命中 <span className="font-semibold text-slate-900">{filteredInventory.length}</span> / {stats.inventory?.length || 0} 款
                </span>
              </div>
              {levelOptions.length === 0 && <span className="text-sm text-slate-400">暂无产品定级字段</span>}
            </div>
          </div>
          {filteredInventory.length === 0 && (
            <div className="mt-3 rounded-lg border border-dashed border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-500">
              当前筛选条件下无款式数据
            </div>
          )}
        </div>
      )}

      {/* KPI Cards - 5 Cards Layout */}
      {!isCoreMainMissing && <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
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
            <div
              className="mt-1 block max-w-[220px] overflow-hidden text-ellipsis whitespace-nowrap text-xs text-slate-500"
              title={criteriaText}
            >
              <span>当前统计口径：</span>
              <span className="font-medium text-slate-700">{criteriaText.replace(/^当前统计口径：/, '')}</span>
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
      </div>}

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
      {!isCoreMainMissing && <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Last Digitization */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">楦头数字化全链路进度 (Last Digitization)</h3>
            <span className="text-xs text-slate-500">Total → HasCode → Has3D</span>
          </div>
          <div className="flex-1 min-h-[420px] w-full">
            {!lastChartRows || lastChartRows.length === 0 ? (
              <ChartEmptyState
                title="暂无楦头品牌进度数据"
                description="请确认核心主表、楦头关系表已上传，并完成一次看板重算。"
              />
            ) : (
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
            )}
          </div>
        </div>

        {/* Sole Digitization */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">大底数字化全链路进度 (Sole Digitization)</h3>
            <span className="text-xs text-slate-500">Total → HasCode → Has3D</span>
          </div>
          <div className="flex-1 min-h-[420px] w-full">
            {!soleChartRows || soleChartRows.length === 0 ? (
              <ChartEmptyState
                title="暂无大底品牌进度数据"
                description="请确认核心主表、大底关系表已上传，并完成一次看板重算。"
              />
            ) : (
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
            )}
          </div>
        </div>
      </div>}
    </div>
  );
}

