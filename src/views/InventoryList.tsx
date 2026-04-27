import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Search, Filter, Download, MoreHorizontal, CheckCircle2, XCircle, Database, Box, Layers, X, DownloadCloud } from 'lucide-react';
import { cn } from '@/lib/utils';
import { InventoryItem } from '@/types';
import ThreeDViewer from '@/components/ThreeDViewer';
import { getStorageBaseUrl } from '@/lib/storageBaseUrl';
import { formatMetricsForUi, type Last3DMetrics } from '@/lib/last3dMetrics';

type InventoryRealResponse = {
  ok: boolean;
  items: InventoryItem[];
  meta?: { mainTable: string | null; reason?: string };
  mapping?: { hasConfig: boolean };
  error?: string;
};

type AssetDetailsResponse = {
  ok: boolean;
  status?: 'ready' | 'processing';
  message?: string;
  type: 'lasts' | 'soles';
  code: string;
  glbUrl?: string | null;
  objUrl?: string | null;
  file: {
    exists: boolean;
    fileName: string | null;
    physicalPath?: string;
    url?: string;
    previewUrl?: string;
    fallbackUrl?: string;
    glbUrl?: string | null;
    objUrl?: string | null;
    sizeBytes: number | null;
    sizeLabel: string | null;
    modifiedAt: string | null;
    modifiedLabel: string | null;
  };
  uploadedBy: string;
  linkedStyles: Array<{ style_wms: string; data_status: string }>;
  linkedSoleCodes: string[];
  error?: string;
};

type AssetMetaResponse = {
  ok: boolean;
  key: string;
  entry: {
    metrics: Last3DMetrics;
    glbName?: string;
    updatedAt?: string;
  } | null;
  error?: string;
};

type AssetFilter = 'all' | 'matched' | 'missing';

const SEARCH_HISTORY_KEY = 'supply3d_search_history_v2';
const ASSET_FILTER_OPTIONS: Array<{ value: AssetFilter; label: string }> = [
  { value: 'all', label: '全部' },
  { value: 'matched', label: '已匹配' },
  { value: 'missing', label: '缺失' },
];

function styleStatusLabel(status: string) {
  switch (status) {
    case 'active':
      return '生效';
    case 'draft':
      return '草稿';
    case 'obsolete':
      return '作废';
    case 'other':
      return '其他';
    default:
      return status || '—';
  }
}

const DataStatusBadge = ({ status }: { status: InventoryItem['data_status'] }) => {
  switch (status) {
    case 'active':
      return <span className="px-2 py-1 bg-emerald-50 text-emerald-700 border border-emerald-200 rounded-md text-xs font-medium">生效</span>;
    case 'draft':
      return <span className="px-2 py-1 bg-slate-100 text-slate-600 border border-slate-200 rounded-md text-xs font-medium">草稿</span>;
    case 'obsolete':
      return <span className="px-2 py-1 bg-red-50 text-red-700 border border-red-200 rounded-md text-xs font-medium">作废</span>;
    case 'other':
      return <span className="px-2 py-1 bg-amber-50 text-amber-800 border border-amber-200 rounded-md text-xs font-medium">其他</span>;
    default:
      return null;
  }
};

const LevelBadge = ({ level }: { level?: string }) => {
  const raw = String(level || '').trim();
  const normalized = raw.toUpperCase().replace(/\s+/g, '').replace(/级$/, '');
  const display = raw || '未定级';
  const label = /^[SABC]$/.test(normalized) ? `${normalized}级` : display;
  const isLong = label.length > 8;
  return (
    <span
      title={display}
      className={cn(
        'inline-flex max-w-[104px] items-center justify-center rounded-full border px-2.5 py-1 text-[11px] font-bold',
        isLong && 'truncate',
        normalized === 'S' && 'border-purple-200 bg-purple-100 text-purple-700',
        normalized === 'A' && 'border-blue-200 bg-blue-50 text-blue-700',
        normalized === 'B' && 'border-sky-200 bg-sky-50 text-sky-700',
        normalized === 'C' && 'border-slate-200 bg-slate-100 text-slate-700',
        normalized === 'EOL' && 'border-red-200 bg-red-50 text-red-700',
        !['S', 'A', 'B', 'C', 'EOL'].includes(normalized) && 'border-slate-200 bg-slate-50 text-slate-600'
      )}
    >
      {label}
    </span>
  );
};

function getProductLevel(item: InventoryItem) {
  return String(item.product_actual_position || item.productLevel || '').trim();
}

function normalizeLevel(level?: string) {
  return String(level || '').trim().toUpperCase().replace(/\s+/g, '').replace(/级$/, '');
}

function levelSortRank(level?: string) {
  const normalized = normalizeLevel(level);
  const rank: Record<string, number> = { S: 0, A: 1, B: 2, C: 3, EOL: 4 };
  return rank[normalized] ?? 99;
}

function toggleValue(list: string[], value: string) {
  return list.includes(value) ? list.filter((x) => x !== value) : [...list, value];
}

function getDateValue(v?: string) {
  const s = String(v || '').trim();
  return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : '';
}

function AssetFilterToggleGroup({
  label,
  value,
  onChange,
}: {
  label: string;
  value: AssetFilter;
  onChange: (value: AssetFilter) => void;
}) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-2 py-1.5 shadow-sm">
      <span className="whitespace-nowrap text-xs font-semibold text-slate-500">{label}</span>
      <div className="flex items-center gap-1">
        {ASSET_FILTER_OPTIONS.map((option) => (
          <button
            key={option.value}
            type="button"
            onClick={() => onChange(option.value)}
            className={cn(
              'rounded-md border px-2.5 py-1 text-xs font-semibold transition-colors',
              value === option.value
                ? 'border-blue-600 bg-blue-600 text-white shadow-sm'
                : 'border-slate-200 bg-slate-50 text-slate-600 hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700'
            )}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

interface PreviewModalProps {
  isOpen: boolean;
  onClose: () => void;
  assetCode: string;
  assetType: 'last' | 'sole';
  /** 行数据 target_audience，供 3D 行业归一化 */
  targetAudience?: string;
}

const PreviewModal = ({ isOpen, onClose, assetCode, assetType, targetAudience }: PreviewModalProps) => {
  const [showAllStyles, setShowAllStyles] = useState(false);
  const [showAllSoles, setShowAllSoles] = useState(false);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [details, setDetails] = useState<AssetDetailsResponse | null>(null);
  const [viewerReady, setViewerReady] = useState(false);
  const [viewerError, setViewerError] = useState<string | null>(null);
  const [lastMetrics, setLastMetrics] = useState<Last3DMetrics | null>(null);
  /** 仅当 GET /api/asset-meta 有 metrics 时设置，供 GLB 跳过前端重复测画 */
  const [precomputedFromApi, setPrecomputedFromApi] = useState<Last3DMetrics | null>(null);
  const [precomputedKey, setPrecomputedKey] = useState<string>('');
  const [glbBust, setGlbBust] = useState(() => Date.now());
  const [scan3d, setScan3d] = useState(false);
  const [reprocessing, setReprocessing] = useState(false);

  const apiType = assetType === 'last' ? 'lasts' : 'soles';

  const API_BASE = 'http://localhost:3001';

  async function safeFetchJson<T>(url: string): Promise<{ response: Response; json: T | null }> {
    const response = await fetch(url);
    if (!response.ok) {
      const text = await response.text();
      // eslint-disable-next-line no-console
      console.error('服务器返回了非预期内容:', text);
      throw new Error(`HTTP 错误: ${response.status}`);
    }
    const json = (await response.json()) as T;
    return { response, json };
  }

  const loadPreviewData = useCallback(async () => {
    if (!assetCode.trim()) return;
    setLoading(true);
    setFetchError(null);
    setDetails(null);
    setLastMetrics(null);
    setPrecomputedFromApi(null);
    setPrecomputedKey('');
    try {
      const qs = new URLSearchParams({ type: apiType, code: assetCode.trim() });
      const [meta, det] = await Promise.all([
        safeFetchJson<AssetMetaResponse>(`${API_BASE}/api/asset-meta?${qs.toString()}`),
        safeFetchJson<AssetDetailsResponse>(`${API_BASE}/api/asset-details?${qs.toString()}`),
      ]);

      const metaJson = meta.json;
      if (metaJson?.ok && metaJson.entry?.metrics) {
        setPrecomputedFromApi(metaJson.entry.metrics);
        setLastMetrics(metaJson.entry.metrics);
        setPrecomputedKey(String(metaJson.entry.updatedAt || metaJson.key || '1'));
      }
      const detJson = det.json;
      if (!detJson?.ok) throw new Error(detJson?.error || '资产尚未处理');
      setDetails(detJson);
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载失败';
      setFetchError(msg.includes('HTTP 错误: 404') ? '资产尚未处理' : msg);
    } finally {
      setLoading(false);
    }
  }, [apiType, assetCode]);

  const handleReprocessAsset = useCallback(async () => {
    if (!assetCode.trim() || reprocessing) return;
    setReprocessing(true);
    setFetchError(null);
    try {
      const qs = new URLSearchParams({ type: apiType, code: assetCode.trim() });
      const resp = await fetch(`${API_BASE}/api/reprocess-asset?${qs.toString()}`, { method: 'POST' });
      const json = (await resp.json().catch(() => null)) as { ok?: boolean; error?: string } | null;
      if (!resp.ok || !json?.ok) throw new Error(json?.error || `重试失败（HTTP ${resp.status}）`);
      setGlbBust(Date.now());
      await loadPreviewData();
    } catch (e) {
      setFetchError(e instanceof Error ? e.message : '重试失败');
    } finally {
      setReprocessing(false);
    }
  }, [API_BASE, apiType, assetCode, loadPreviewData, reprocessing]);

  useEffect(() => {
    if (!isOpen || !assetCode.trim()) return;
    setShowAllStyles(false);
    setShowAllSoles(false);
    setViewerReady(false);
    setViewerError(null);
    setLastMetrics(null);
    setPrecomputedFromApi(null);
    setPrecomputedKey('');
    setScan3d(false);
    setReprocessing(false);
    void loadPreviewData();
  }, [isOpen, assetCode, loadPreviewData]);

  const file = details?.file;
  const notProcessed = Boolean(fetchError) && String(fetchError).includes('资产尚未处理');
  const fileUrl = useMemo(() => {
    if (!isOpen) return null;
    if (!file?.exists || !file.fileName) return null;
    if (file.previewUrl) return file.previewUrl.startsWith('http') ? file.previewUrl : `${getStorageBaseUrl()}${file.previewUrl}`;
    return `${getStorageBaseUrl()}/storage/assets/${apiType}/${encodeURIComponent(String(file.fileName))}`;
  }, [isOpen, file?.exists, file?.fileName, file?.previewUrl, apiType]);
  const viewerGlbUrl = details?.glbUrl || file?.glbUrl || null;
  const viewerObjUrl = details?.objUrl || file?.objUrl || file?.fallbackUrl || null;

  useEffect(() => {
    if (!isOpen || !fileUrl) return;
    setGlbBust(Date.now());
  }, [isOpen, fileUrl, precomputedKey]);

  useEffect(() => {
    if (!isOpen) return;
    if (!fileUrl) {
      setScan3d(false);
      return;
    }
    setScan3d(!lastMetrics);
  }, [isOpen, fileUrl, lastMetrics]);

  if (!isOpen) return null;

  const styles = details?.linkedStyles ?? [];
  const soles = details?.linkedSoleCodes ?? [];
  const displayedStyles = showAllStyles ? styles : styles.slice(0, 5);
  const displayedSoles = showAllSoles ? soles : soles.slice(0, 3);
  const downloadHref = `/api/asset-file?type=${encodeURIComponent(apiType)}&code=${encodeURIComponent(assetCode.trim())}`;
  const reportDisp = lastMetrics ? formatMetricsForUi(lastMetrics) : null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="bg-white w-[90vw] h-[85vh] rounded-2xl shadow-2xl overflow-hidden flex animate-in zoom-in-95 duration-200 relative">
        {/* 全局关闭按钮：始终置顶，不被 3D 画布遮挡 */}
        <button
          type="button"
          onClick={onClose}
          className="absolute top-4 right-4 z-50 p-2 bg-black/30 hover:bg-black/40 backdrop-blur rounded-full text-white/90 hover:text-white transition-colors"
          aria-label="关闭预览"
        >
          <X className="w-5 h-5" />
        </button>
        
        {/* Left: 3D Canvas Area (70%) */}
        <div className="flex-[7] min-w-0 bg-slate-900 relative flex items-center justify-center border-r border-slate-700">
          <div className="absolute top-4 left-4 flex items-center gap-2 bg-black/40 backdrop-blur-md px-3 py-1.5 rounded-lg text-white/80 text-sm">
            {assetType === 'last' ? <Box className="w-4 h-4" /> : <Layers className="w-4 h-4" />}
            <span className="font-mono">{assetCode}</span>
          </div>
          
          {/* Three.js Viewer */}
          {fileUrl ? (
            <div
              className="absolute inset-0"
              key={`${fileUrl}__${viewerGlbUrl || ''}__${viewerObjUrl || ''}__${String(targetAudience ?? '')}__${precomputedKey || '0'}`}
            >
              <ThreeDViewer
                fileUrl={fileUrl}
                glbUrl={viewerGlbUrl}
                objUrl={viewerObjUrl}
                assetStatus={details?.status}
                targetAudience={targetAudience}
                precomputedMetrics={fileUrl && fileUrl.toLowerCase().endsWith('.glb') ? precomputedFromApi : null}
                precomputedKey={precomputedKey}
                glbCacheToken={glbBust}
                className="absolute inset-0 relative"
                onMetrics={(m) => {
                  setLastMetrics(m);
                  setScan3d(false);
                }}
                onLoaded={() => setViewerReady(true)}
                onError={(e) => {
                  setViewerError(e.message);
                  setViewerReady(false);
                  setScan3d(false);
                  setLastMetrics(null);
                }}
              />
            </div>
          ) : null}

          {/* Overlay states */}
          <div
            className={cn(
              'relative z-10 text-center px-8 max-w-lg transition-opacity duration-300',
              fileUrl && viewerReady ? 'opacity-0 pointer-events-none' : 'opacity-100'
            )}
          >
            {loading ? (
              <>
                <div className="w-16 h-16 border-4 border-slate-700 border-t-blue-500 rounded-full animate-spin mx-auto mb-4" />
                <p className="text-slate-400 font-medium tracking-widest">3D Viewer 加载中...</p>
                <p className="text-slate-600 text-sm mt-2">正在读取物理文件与关联关系</p>
              </>
            ) : fetchError ? (
              <>
                <XCircle className="w-14 h-14 text-red-400 mx-auto mb-4" />
                <p className="text-red-300 font-medium">{fetchError}</p>
                {notProcessed ? (
                  <button
                    type="button"
                    onClick={() => void handleReprocessAsset()}
                    disabled={reprocessing}
                    className="mt-3 rounded-lg border border-red-300/40 bg-red-500/10 px-3 py-1.5 text-xs font-semibold text-red-100 hover:bg-red-500/20 disabled:opacity-60"
                  >
                    {reprocessing ? '正在重试...' : '重新触发预处理'}
                  </button>
                ) : null}
              </>
            ) : viewerError ? (
              <>
                <XCircle className="w-14 h-14 text-red-400 mx-auto mb-4" />
                <p className="text-red-300 font-medium">{viewerError}</p>
                <p className="text-slate-500 text-sm mt-2">请确认该文件可通过 /storage 访问且为有效 GLB/OBJ</p>
              </>
            ) : file?.exists && file.fileName ? (
              <>
                <div className="w-16 h-16 border-4 border-slate-700 border-t-blue-500 rounded-full animate-spin mx-auto mb-4" />
                <p className="text-slate-400 font-medium tracking-widest">3D 模型解析中...</p>
                <p className="text-slate-600 text-sm mt-2">
                  {scan3d ? '正在进行三维特征扫描…' : '正在下载/解析模型…'}
                </p>
                <p className="text-slate-500 text-xs mt-1 font-mono break-all px-2">{file.fileName}</p>
              </>
            ) : (
              <>
                <Database className="w-14 h-14 text-amber-400/80 mx-auto mb-4" />
                <p className="text-slate-300 font-medium">未找到本地物理文件</p>
                <p className="text-slate-500 text-sm mt-2">仍可查看右侧关联款号；请确认已上传至 storage/assets/{assetType === 'last' ? 'lasts' : 'soles'}</p>
              </>
            )}
          </div>
        </div>

        {/* Right: Asset Details (30%) */}
        <div className="flex-[3] min-w-[350px] max-w-[520px] shrink-0 bg-white flex flex-col">
          <div className="p-6 border-b border-slate-100 flex justify-between items-start">
            <div>
              <h2 className="text-xl font-bold text-slate-900">资产详情</h2>
              <p className="text-sm text-slate-500 mt-1">{assetType === 'last' ? '3D 楦头模型' : '3D 大底模型'}</p>
            </div>
          </div>

          <div className="p-6 flex-1 overflow-y-auto space-y-6">
            <div className="space-y-4">
              <div>
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">资产编号</label>
                <p className="text-sm font-mono text-slate-900 mt-1 bg-slate-50 px-2 py-1 rounded border border-slate-100">{assetCode}</p>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">源文件名</label>
                <p className="text-sm text-slate-900 mt-1 font-mono">
                  {loading ? '…' : file?.exists && file.fileName ? file.fileName : '—'}
                </p>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">上传日期</label>
                  <p className="text-sm text-slate-900 mt-1">
                    {loading ? '…' : notProcessed ? '资产尚未处理' : file?.modifiedLabel || '—'}
                  </p>
                </div>
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">上传用户</label>
                  <p className="text-sm text-slate-900 mt-1">{details?.uploadedBy ?? 'System'}</p>
                </div>
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">文件大小</label>
                  <p className="text-sm text-slate-900 mt-1">
                    {loading ? '…' : notProcessed ? '资产尚未处理' : file?.sizeLabel || '—'}
                  </p>
                </div>
              </div>
            </div>

            {assetType === 'last' && (fileUrl || !loading) ? (
              <div className="pt-2 border-t border-slate-100">
                <h3 className="text-sm font-semibold text-slate-800">3D 数字化扫描报告 (AI Analysis)</h3>
                {scan3d && !lastMetrics && !viewerError && file?.exists ? (
                  <p className="text-sm text-indigo-600 mt-2 flex items-center gap-2">
                    <span className="inline-block w-2 h-2 bg-indigo-500 rounded-full animate-pulse" />
                    正在进行三维特征扫描…
                  </p>
                ) : null}
                {lastMetrics && reportDisp ? (
                  <div className="mt-3 space-y-2 text-sm text-slate-700">
                    <div className="grid grid-cols-3 gap-2">
                      <div>
                        <div className="text-[10px] text-slate-500 uppercase">长度 L (X)</div>
                        <div className="font-mono">{reportDisp.L}</div>
                      </div>
                      <div>
                        <div className="text-[10px] text-slate-500 uppercase">宽度 W (Z)</div>
                        <div className="font-mono">{reportDisp.W}</div>
                      </div>
                      <div>
                        <div className="text-[10px] text-slate-500 uppercase">高度 H (Y)</div>
                        <div className="font-mono">{reportDisp.H}</div>
                      </div>
                    </div>
                    <div>
                      <span className="text-slate-500">跟高(估算)：</span>
                      <span className="font-mono ml-1">{reportDisp.heel}</span>
                    </div>
                    <div>
                      <span className="text-slate-500">体积(估算)：</span>
                      <span className="font-mono ml-1">{reportDisp.vol}</span>
                    </div>
                    <div className="text-xs text-slate-500">
                      <div>中国码(建议)：{lastMetrics.shoeSizeChinaHint}</div>
                      <div>欧码(建议)：{lastMetrics.shoeSizeEurHint}</div>
                    </div>
                    <p className="text-[11px] text-slate-400 leading-snug">{lastMetrics.volumeNote}</p>
                  </div>
                ) : !scan3d && !lastMetrics && file?.exists && !viewerError ? (
                  <p className="text-sm text-slate-500 mt-2">等待 3D 模型解码…</p>
                ) : null}
              </div>
            ) : null}

            <div className="pt-6 border-t border-slate-100">
              <div className="flex justify-between items-center mb-3">
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block">关联款号列表</label>
                {styles.length > 5 ? (
                  <button
                    type="button"
                    onClick={() => setShowAllStyles(!showAllStyles)}
                    className="text-xs text-blue-600 hover:text-blue-700 font-medium transition-colors"
                  >
                    {showAllStyles ? '收起' : `查看全部 (${styles.length})`}
                  </button>
                ) : null}
              </div>
              {styles.length === 0 && !loading ? (
                <p className="text-sm text-slate-500">暂无关联款号（全量清单中无相同编号）</p>
              ) : (
                <div className="space-y-2">
                  {displayedStyles.map((row, idx) => (
                    <div key={`${row.style_wms}-${idx}`} className="flex items-center justify-between p-2 rounded-lg bg-slate-50 border border-slate-100 gap-2">
                      <span className="text-sm font-mono text-slate-700 truncate">{row.style_wms}</span>
                      <span
                        className={cn(
                          'text-xs shrink-0 px-1.5 py-0.5 rounded font-medium',
                          row.data_status === 'active' && 'text-emerald-700 bg-emerald-50',
                          row.data_status === 'draft' && 'text-slate-600 bg-slate-100',
                          row.data_status === 'obsolete' && 'text-red-700 bg-red-50',
                          row.data_status === 'other' && 'text-amber-800 bg-amber-50',
                          !['active', 'draft', 'obsolete', 'other'].includes(row.data_status) && 'text-slate-600 bg-slate-100'
                        )}
                      >
                        {styleStatusLabel(row.data_status)}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {assetType === 'last' && (
              <div className="pt-6 border-t border-slate-100">
                <div className="flex justify-between items-center mb-3">
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block">关联大底列表</label>
                  {soles.length > 3 ? (
                    <button
                      type="button"
                      onClick={() => setShowAllSoles(!showAllSoles)}
                      className="text-xs text-blue-600 hover:text-blue-700 font-medium transition-colors"
                    >
                      {showAllSoles ? '收起' : `查看全部 (${soles.length})`}
                    </button>
                  ) : null}
                </div>
                {soles.length === 0 && !loading ? (
                  <p className="text-sm text-slate-500">该款号维度下无大底编号</p>
                ) : (
                  <div className="space-y-2">
                    {displayedSoles.map((sole, idx) => (
                      <div key={`${sole}-${idx}`} className="flex items-center justify-between p-2 rounded-lg bg-slate-50 border border-slate-100">
                        <span className="text-sm font-mono text-slate-700">{sole}</span>
                        <span className="text-xs text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded font-medium">清单关联</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="p-6 border-t border-slate-100 bg-slate-50">
            {file?.exists ? (
              <a
                href={downloadHref}
                download
                className="w-full flex items-center justify-center gap-2 py-3 bg-blue-600 text-white rounded-xl font-medium hover:bg-blue-700 transition-colors shadow-sm"
              >
                <DownloadCloud className="w-5 h-5" />
                下载源文件{file.fileName ? `（${pathBasenameOnly(file.fileName)}）` : ''}
              </a>
            ) : (
              <button
                type="button"
                disabled
                className="w-full flex items-center justify-center gap-2 py-3 bg-slate-300 text-slate-500 rounded-xl font-medium cursor-not-allowed"
              >
                <DownloadCloud className="w-5 h-5" />
                无本地文件可下载
              </button>
            )}
          </div>
        </div>

      </div>
    </div>
  );
};

function pathBasenameOnly(name: string) {
  const parts = name.split(/[/\\]/);
  return parts[parts.length - 1] || name;
}

export default function InventoryList() {
  const [previewModal, setPreviewModal] = useState<{
    isOpen: boolean;
    assetCode: string;
    type: 'last' | 'sole';
    targetAudience?: string;
  }>({ isOpen: false, assetCode: '', type: 'last' });
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [searchHistory, setSearchHistory] = useState<string[]>([]);
  const [isSearchFocused, setIsSearchFocused] = useState(false);
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const [selectedLevels, setSelectedLevels] = useState<string[]>([]);
  const [selectedBrands, setSelectedBrands] = useState<string[]>([]);
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>([]);
  const [lastAssetFilter, setLastAssetFilter] = useState<AssetFilter>('all');
  const [soleAssetFilter, setSoleAssetFilter] = useState<AssetFilter>('all');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [drilldown, setDrilldown] = useState<{ dimension: 'last' | 'sole'; bucket: 'no_code' | 'has_code_no_3d' | 'completed_3d' } | null>(null);
  const [levelSortDirection, setLevelSortDirection] = useState<'none' | 'asc' | 'desc'>('none');
  const [items, setItems] = useState<InventoryItem[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const openPreview = (assetCode: string | undefined, type: 'last' | 'sole', targetAudience?: string) => {
    if (assetCode) {
      setPreviewModal({ isOpen: true, assetCode, type, targetAudience });
    }
  };

  const handlePerformSearch = useCallback((term: string) => {
    const q = term.trim();
    if (!q) return;
    setSearchHistory((prev) => {
      const newHistory = [q, ...prev.filter((item) => item.trim().toLowerCase() !== q.toLowerCase())].slice(0, 10);
      localStorage.setItem(SEARCH_HISTORY_KEY, JSON.stringify(newHistory));
      // eslint-disable-next-line no-console
      console.log('✅ 搜索历史已存入磁盘:', newHistory);
      return newHistory;
    });
    setIsSearchFocused(false);
  }, []);

  const removeSearchHistoryItem = useCallback((keyword: string) => {
    setSearchHistory((prev) => {
      const next = prev.filter((item) => item !== keyword);
      localStorage.setItem(SEARCH_HISTORY_KEY, JSON.stringify(next));
      return next;
    });
  }, []);

  const clearSearchHistory = useCallback(() => {
    localStorage.removeItem(SEARCH_HISTORY_KEY);
    setSearchHistory([]);
  }, []);

  const resetAllFilters = useCallback(() => {
    setSearchTerm('');
    setSelectedLevels([]);
    setSelectedBrands([]);
    setSelectedStatuses([]);
    setLastAssetFilter('all');
    setSoleAssetFilter('all');
    setDateFrom('');
    setDateTo('');
    setDrilldown(null);
  }, []);

  const loadInventory = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const resp = await fetch('/api/inventory-real');
      const json = (await resp.json()) as InventoryRealResponse;
      if (!resp.ok || !json.ok) throw new Error(json.error || `加载失败（HTTP ${resp.status}）`);
      setItems(json.items || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : '加载失败');
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    let savedHistory: string[] = [];
    try {
      const parsed = JSON.parse(localStorage.getItem(SEARCH_HISTORY_KEY) || '[]');
      savedHistory = Array.isArray(parsed) ? parsed.map((item) => String(item || '').trim()).filter(Boolean).slice(0, 10) : [];
    } catch {
      savedHistory = [];
    }
    setSearchHistory(savedHistory);
    // eslint-disable-next-line no-console
    console.log('📦 搜索历史已从磁盘读取:', savedHistory);
  }, []);

  useEffect(() => {
    // Dashboard 点击品牌跳转：通过 localStorage 预设过滤
    try {
      const preset = localStorage.getItem('inventoryBrandFilter');
      if (preset) {
        setSelectedBrands([preset]);
        localStorage.removeItem('inventoryBrandFilter');
      }
      const dd = localStorage.getItem('inventoryDrilldown');
      if (dd) {
        const parsed = JSON.parse(dd);
        if (parsed && (parsed.dimension === 'last' || parsed.dimension === 'sole') && typeof parsed.bucket === 'string') {
          setDrilldown({
            dimension: parsed.dimension,
            bucket: parsed.bucket,
          } as any);
        }
        localStorage.removeItem('inventoryDrilldown');
      }
    } catch {
      // ignore
    }
    void loadInventory();
  }, [loadInventory]);

  useEffect(() => {
    const onRefresh = () => {
      void loadInventory();
    };
    window.addEventListener('3d-assets-refresh', onRefresh);
    return () => window.removeEventListener('3d-assets-refresh', onRefresh);
  }, [loadInventory]);

  const brandOptions = useMemo(() => {
    const set = new Set<string>();
    for (const it of items) {
      const b = (it.brand || '').trim();
      if (b) set.add(b);
    }
    return Array.from(set.values()).sort((a, b) => a.localeCompare(b));
  }, [items]);

  const levelOptions = useMemo(() => {
    const set = new Set<string>();
    for (const it of items) {
      const level = getProductLevel(it) || '未定级';
      set.add(level);
    }
    return Array.from(set.values()).sort((a, b) => {
      const ra = levelSortRank(a);
      const rb = levelSortRank(b);
      if (ra !== rb) return ra - rb;
      return a.localeCompare(b, 'zh-CN', { numeric: true });
    });
  }, [items]);

  const statusOptions = useMemo(() => {
    const pri: Record<string, number> = { active: 0, draft: 1, obsolete: 2, other: 3 };
    const set = new Set<string>();
    for (const it of items) {
      const status = String(it.data_status || '').trim();
      if (status) set.add(status);
    }
    return Array.from(set.values()).sort((a, b) => (pri[a] ?? 9) - (pri[b] ?? 9));
  }, [items]);

  const activeAdvancedFilterCount = useMemo(() => {
    return (
      selectedLevels.length +
      selectedBrands.length +
      selectedStatuses.length +
      (lastAssetFilter !== 'all' ? 1 : 0) +
      (soleAssetFilter !== 'all' ? 1 : 0) +
      (dateFrom ? 1 : 0) +
      (dateTo ? 1 : 0) +
      (drilldown ? 1 : 0)
    );
  }, [dateFrom, dateTo, drilldown, lastAssetFilter, selectedBrands.length, selectedLevels.length, selectedStatuses.length, soleAssetFilter]);

  const filteredItems = useMemo(() => {
    let out = items;
    const q = searchTerm.trim().toLowerCase();
    if (q) {
      out = out.filter((item) => {
        const style = String(item.style_wms || '').toLowerCase();
        const brand = String(item.brand || '').toLowerCase();
        const lastCode = String(item.lastCode || '').toLowerCase();
        const soleCode = String(item.soleCode || '').toLowerCase();
        const level = String(getProductLevel(item)).toLowerCase();
        return style.includes(q) || brand.includes(q) || lastCode.includes(q) || soleCode.includes(q) || level.includes(q);
      });
    }

    if (selectedLevels.length > 0) out = out.filter((x) => selectedLevels.includes(getProductLevel(x) || '未定级'));
    if (selectedBrands.length > 0) out = out.filter((x) => selectedBrands.includes((x.brand || '').trim()));
    if (selectedStatuses.length > 0) out = out.filter((x) => selectedStatuses.includes(String(x.data_status || '').trim()));
    if (lastAssetFilter !== 'all') out = out.filter((x) => x.lastStatus === lastAssetFilter);
    if (soleAssetFilter !== 'all') out = out.filter((x) => x.soleStatus === soleAssetFilter);
    if (dateFrom) out = out.filter((x) => getDateValue(x.lastUpdated) >= dateFrom);
    if (dateTo) out = out.filter((x) => getDateValue(x.lastUpdated) <= dateTo);

    // 图表钻取：按责任区间过滤（无 UI 暴露，来自 Dashboard 点击）
    if (drilldown) {
      if (drilldown.dimension === 'last') {
        if (drilldown.bucket === 'no_code') out = out.filter((x) => !String(x.lastCode || '').trim());
        if (drilldown.bucket === 'has_code_no_3d')
          out = out.filter((x) => Boolean(String(x.lastCode || '').trim()) && x.lastStatus !== 'matched');
        if (drilldown.bucket === 'completed_3d') out = out.filter((x) => x.lastStatus === 'matched');
      } else {
        if (drilldown.bucket === 'no_code') out = out.filter((x) => !String(x.soleCode || '').trim());
        if (drilldown.bucket === 'has_code_no_3d')
          out = out.filter((x) => Boolean(String(x.soleCode || '').trim()) && x.soleStatus !== 'matched');
        if (drilldown.bucket === 'completed_3d') out = out.filter((x) => x.soleStatus === 'matched');
      }
    }

    // 默认排序：生效在前，其次草稿，最后作废；产品定级排序由表头显式触发。
    const pri: Record<string, number> = { active: 0, draft: 1, other: 2, obsolete: 3 };
    out = [...out].sort((a, b) => {
      if (levelSortDirection !== 'none') {
        const ra = levelSortRank(getProductLevel(a));
        const rb = levelSortRank(getProductLevel(b));
        if (ra !== rb) return levelSortDirection === 'asc' ? ra - rb : rb - ra;
        const la = getProductLevel(a);
        const lb = getProductLevel(b);
        const levelCmp = la.localeCompare(lb, 'zh-CN', { numeric: true });
        if (levelCmp !== 0) return levelSortDirection === 'asc' ? levelCmp : -levelCmp;
      }
      const pa = pri[String(a?.data_status || '')] ?? 9;
      const pb = pri[String(b?.data_status || '')] ?? 9;
      if (pa !== pb) return pa - pb;
      return String(a?.style_wms || '').localeCompare(String(b?.style_wms || ''), 'en', { numeric: true });
    });
    return out;
  }, [
    dateFrom,
    dateTo,
    drilldown,
    items,
    lastAssetFilter,
    levelSortDirection,
    searchTerm,
    selectedBrands,
    selectedLevels,
    selectedStatuses,
    soleAssetFilter,
  ]);

  return (
    <div className="space-y-6 animate-in fade-in duration-500 pb-10">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">款号详细清单</h1>
          <p className="text-sm text-slate-500 mt-1">管理所有款号及其 3D 资产的匹配状态</p>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-sm text-slate-500">
            当前命中：<span className="font-semibold text-slate-900">{filteredItems.length}</span> 条 / 共 {items.length} 条
          </span>
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-50 transition-colors shadow-sm">
            <Download className="w-4 h-4" />
            导出报表
          </button>
        </div>
      </div>

      <div className="overflow-visible rounded-xl border border-slate-200 bg-white shadow-sm">
        {/* Toolbar */}
        <div className="relative z-50 flex flex-wrap items-center justify-between gap-4 border-b border-slate-200 bg-slate-50/50 p-4">
          <div className="flex flex-1 flex-wrap items-center gap-3">
            <div className="relative z-[100] min-w-[18rem] max-w-md flex-1 overflow-visible">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input 
                type="text" 
                placeholder="搜索款号或品牌..." 
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onFocus={() => setIsSearchFocused(true)}
                onBlur={() => {
                  window.setTimeout(() => setIsSearchFocused(false), 120);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handlePerformSearch(searchTerm);
                }}
                className="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              {isSearchFocused === true && searchHistory.length > 0 && (
                <div
                  className="absolute left-0 top-full z-[9999] mt-1 max-h-80 w-full overflow-y-auto rounded-lg border border-slate-200 bg-white p-3 shadow-2xl dark:border-slate-700 dark:bg-slate-900"
                  onMouseDown={(e) => e.preventDefault()}
                >
                  <div className="mb-2 flex items-center justify-between">
                    <span className="text-xs font-semibold text-slate-500 dark:text-slate-300">最近搜索</span>
                  </div>
                  <div className="space-y-1" onMouseDown={(e) => e.preventDefault()}>
                    {searchHistory.map((item) => (
                      <div
                        key={item}
                        className="group flex items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-700 hover:bg-slate-50 dark:text-slate-200 dark:hover:bg-slate-800"
                      >
                        <button
                          type="button"
                          onMouseDown={(e) => {
                            e.preventDefault();
                            setSearchTerm(item);
                            setIsSearchFocused(false);
                          }}
                          className="min-w-0 flex-1 truncate text-left"
                          title={item}
                        >
                          {item}
                        </button>
                        <button
                          type="button"
                          onMouseDown={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            removeSearchHistoryItem(item);
                          }}
                          className="rounded p-1 text-slate-300 opacity-70 hover:bg-red-50 hover:text-red-500 group-hover:opacity-100 dark:hover:bg-red-950/40"
                          aria-label={`删除搜索历史 ${item}`}
                        >
                          <X className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    ))}
                  </div>
                  <button
                    type="button"
                    onMouseDown={(e) => {
                      e.preventDefault();
                      clearSearchHistory();
                    }}
                    className="mt-3 w-full border-t border-slate-100 pt-3 text-left text-xs font-medium text-slate-400 hover:text-red-500 dark:border-slate-800"
                  >
                    清空历史
                  </button>
                </div>
              )}
            </div>
            <button
              type="button"
              onClick={() => handlePerformSearch(searchTerm)}
              className="inline-flex items-center gap-2 rounded-lg bg-slate-900 px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-slate-800"
            >
              <Search className="h-4 w-4" />
              搜索
            </button>
            <AssetFilterToggleGroup label="楦头 3D" value={lastAssetFilter} onChange={setLastAssetFilter} />
            <AssetFilterToggleGroup label="大底 3D" value={soleAssetFilter} onChange={setSoleAssetFilter} />
            <button
              type="button"
              onClick={() => setIsAdvancedOpen(true)}
              className="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 shadow-sm transition-colors hover:bg-slate-50"
            >
              <Filter className="h-4 w-4" />
              高级筛选
              {activeAdvancedFilterCount > 0 && (
                <span className="rounded-full bg-blue-600 px-1.5 py-0.5 text-[10px] font-bold text-white">{activeAdvancedFilterCount}</span>
              )}
            </button>
          </div>
          <div className="text-sm text-slate-500">
            共 <span className="font-medium text-slate-900">{filteredItems.length}</span> 条记录
          </div>
        </div>

        {/* Table */}
        <div className="relative z-0 overflow-x-auto">
          {error && (
            <div className="p-4 text-sm text-red-700 bg-red-50 border-b border-red-200">
              {error}
            </div>
          )}
          {isLoading && (
            <div className="p-4 text-sm text-slate-600 bg-slate-50 border-b border-slate-200">
              正在加载真实清单数据...
            </div>
          )}
          <table className="w-full text-sm text-left whitespace-nowrap">
            <thead className="text-xs text-slate-500 bg-slate-50 uppercase border-b border-slate-200">
              <tr>
                <th className="px-5 py-3 font-medium">款号 (Style_WMS)</th>
                <th className="px-5 py-3 font-medium w-[120px] text-center">
                  <button
                    type="button"
                    onClick={() =>
                      setLevelSortDirection((prev) => (prev === 'none' ? 'asc' : prev === 'asc' ? 'desc' : 'none'))
                    }
                    className="inline-flex items-center justify-center gap-1 rounded-md px-2 py-1 hover:bg-slate-100"
                    title="按 S > A > B > C > EOL > 其他排序"
                  >
                    产品定级 (Level)
                    <span className="text-[10px] text-slate-400">
                      {levelSortDirection === 'asc' ? '↑' : levelSortDirection === 'desc' ? '↓' : '↕'}
                    </span>
                  </button>
                </th>
                <th className="px-5 py-3 font-medium">品牌</th>
                <th className="px-5 py-3 font-medium">颜色 (Color)</th>
                <th className="px-5 py-3 font-medium">材质 (Material)</th>
                <th className="px-5 py-3 font-medium">楦头编号</th>
                <th className="px-5 py-3 font-medium">3D 楦头状态</th>
                <th className="px-5 py-3 font-medium">大底编号</th>
                <th className="px-5 py-3 font-medium">3D 大底状态</th>
                <th className="px-5 py-3 font-medium">状态 (Data_Status)</th>
                <th className="px-5 py-3 font-medium">更新人</th>
                <th className="px-5 py-3 font-medium">最后更新</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {!isLoading && filteredItems.length === 0 && (
                <tr>
                  <td colSpan={12} className="px-5 py-12 text-center text-sm text-slate-500">
                    当前筛选条件下无款式数据
                  </td>
                </tr>
              )}
              {filteredItems.map((item) => (
                <tr
                  key={item.id}
                  className={cn(
                    'transition-colors group',
                    item.data_status === 'draft'
                      ? 'bg-blue-50/10 hover:bg-blue-50/20'
                      : item.data_status === 'other'
                        ? 'bg-amber-50/20 hover:bg-amber-50/30'
                        : 'hover:bg-slate-50/50'
                  )}
                >
                  <td className="px-5 py-4 font-medium text-slate-900">
                    {item.style_wms}
                  </td>
                  <td className="px-5 py-4 w-[120px] text-center">
                    <LevelBadge level={getProductLevel(item)} />
                  </td>
                  <td className="px-5 py-4 text-slate-600">
                    <span className="bg-slate-100 px-2.5 py-1 rounded-md text-xs font-medium">{item.brand}</span>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex items-center gap-2">
                      <div 
                        className="w-4 h-4 rounded shadow-sm border border-slate-200" 
                        style={{ backgroundColor: item.colorHex || '#ccc' }}
                        title={item.colorHex}
                      />
                      <span className="text-slate-600 font-mono text-xs">{item.colorCode}</span>
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex items-center gap-2">
                      <div className="w-6 h-4 rounded overflow-hidden bg-slate-200 border border-slate-200 shrink-0">
                        {item.materialThumb && <img src={item.materialThumb} alt="material" className="w-full h-full object-cover" referrerPolicy="no-referrer" />}
                      </div>
                      <span className="text-slate-600 font-mono text-xs">{item.materialCode}</span>
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <span className="font-mono text-xs text-slate-600 bg-slate-100 px-2 py-1 rounded">{item.lastCode || '-'}</span>
                  </td>
                  <td className="px-5 py-4">
                    {item.lastStatus === 'matched' ? (
                      <button 
                        onClick={() =>
                          openPreview(
                            item.lastCode ? String(item.lastCode).trim() : undefined,
                            'last',
                            item.target_audience
                          )
                        }
                        className="flex items-center gap-1.5 text-emerald-600 bg-emerald-50 hover:bg-emerald-100 px-2 py-1 rounded-md w-fit transition-colors cursor-pointer group/btn"
                      >
                        <CheckCircle2 className="w-4 h-4 group-hover/btn:scale-110 transition-transform" />
                        <span className="font-medium text-xs">已匹配</span>
                      </button>
                    ) : (
                      <div className="flex items-center gap-1.5 text-red-600 bg-red-50 px-2 py-1 rounded-md w-fit border border-red-100">
                        <XCircle className="w-4 h-4" />
                        <span className="font-medium text-xs">缺失</span>
                      </div>
                    )}
                  </td>
                  <td className="px-5 py-4">
                    <span className="font-mono text-xs text-slate-600 bg-slate-100 px-2 py-1 rounded">{item.soleCode || '-'}</span>
                  </td>
                  <td className="px-5 py-4">
                    {item.soleStatus === 'matched' ? (
                      <button 
                        onClick={() =>
                          openPreview(
                            item.soleCode ? String(item.soleCode).trim() : undefined,
                            'sole',
                            item.target_audience
                          )
                        }
                        className="flex items-center gap-1.5 text-emerald-600 bg-emerald-50 hover:bg-emerald-100 px-2 py-1 rounded-md w-fit transition-colors cursor-pointer group/btn"
                      >
                        <CheckCircle2 className="w-4 h-4 group-hover/btn:scale-110 transition-transform" />
                        <span className="font-medium text-xs">已匹配</span>
                      </button>
                    ) : (
                      <div className="flex items-center gap-1.5 text-red-600 bg-red-50 px-2 py-1 rounded-md w-fit border border-red-100">
                        <XCircle className="w-4 h-4" />
                        <span className="font-medium text-xs">缺失</span>
                      </div>
                    )}
                  </td>
                  <td className="px-5 py-4">
                    <DataStatusBadge status={item.data_status} />
                  </td>
                  <td className="px-5 py-4 text-slate-600">{item.updatedBy}</td>
                  <td className="px-5 py-4 text-slate-500">{item.lastUpdated}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        {/* Pagination */}
        <div className="p-4 border-t border-slate-200 bg-slate-50/50 flex items-center justify-between">
          <span className="text-sm text-slate-500">
            当前展示 <span className="font-medium text-slate-900">{filteredItems.length}</span> 条
            {items.length ? <>（总计 {items.length} 条）</> : null}
          </span>
        </div>
      </div>

      {isAdvancedOpen && (
        <div className="fixed inset-0 z-50 flex justify-end bg-slate-900/30 backdrop-blur-sm">
          <div className="flex h-full w-full max-w-xl flex-col bg-white shadow-2xl">
            <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
              <div>
                <h2 className="text-lg font-semibold text-slate-900">高级筛选</h2>
                <p className="mt-1 text-sm text-slate-500">
                  当前命中 {filteredItems.length} 条 / 共 {items.length} 条
                </p>
              </div>
              <button
                type="button"
                onClick={() => setIsAdvancedOpen(false)}
                className="rounded-lg p-2 text-slate-400 hover:bg-slate-100 hover:text-slate-700"
                aria-label="关闭高级筛选"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="flex-1 space-y-6 overflow-y-auto px-6 py-5">
              <section>
                <div className="mb-3 text-sm font-semibold text-slate-900">产品定级 (Level)</div>
                <div className="grid grid-cols-2 gap-2">
                  {levelOptions.map((level) => (
                    <label key={level} className="flex cursor-pointer items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 hover:bg-slate-50">
                      <input
                        type="checkbox"
                        checked={selectedLevels.includes(level)}
                        onChange={() => setSelectedLevels((prev) => toggleValue(prev, level))}
                        className="h-4 w-4 accent-blue-600"
                      />
                      <LevelBadge level={level} />
                    </label>
                  ))}
                </div>
              </section>

              <section>
                <div className="mb-3 flex items-center justify-between">
                  <div className="text-sm font-semibold text-slate-900">品牌 (Brand)</div>
                  {selectedBrands.length > 0 && (
                    <button type="button" onClick={() => setSelectedBrands([])} className="text-xs text-blue-600 hover:text-blue-700">
                      清空品牌
                    </button>
                  )}
                </div>
                <div className="grid max-h-56 grid-cols-2 gap-2 overflow-y-auto pr-1">
                  {brandOptions.map((brand) => (
                    <label key={brand} className="flex cursor-pointer items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-sm text-slate-700 hover:bg-slate-50">
                      <input
                        type="checkbox"
                        checked={selectedBrands.includes(brand)}
                        onChange={() => setSelectedBrands((prev) => toggleValue(prev, brand))}
                        className="h-4 w-4 accent-blue-600"
                      />
                      <span className="truncate" title={brand}>{brand}</span>
                    </label>
                  ))}
                </div>
              </section>

              <section className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                <div>
                  <div className="mb-3 text-sm font-semibold text-slate-900">楦头状态</div>
                  <div className="grid grid-cols-3 gap-2">
                    {ASSET_FILTER_OPTIONS.map(({ value, label }) => (
                      <button
                        key={value}
                        type="button"
                        onClick={() => setLastAssetFilter(value)}
                        className={cn(
                          'rounded-lg border px-3 py-2 text-sm font-medium',
                          lastAssetFilter === value ? 'border-blue-600 bg-blue-600 text-white' : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                        )}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>
                <div>
                  <div className="mb-3 text-sm font-semibold text-slate-900">大底状态</div>
                  <div className="grid grid-cols-3 gap-2">
                    {ASSET_FILTER_OPTIONS.map(({ value, label }) => (
                      <button
                        key={value}
                        type="button"
                        onClick={() => setSoleAssetFilter(value)}
                        className={cn(
                          'rounded-lg border px-3 py-2 text-sm font-medium',
                          soleAssetFilter === value ? 'border-blue-600 bg-blue-600 text-white' : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                        )}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>
              </section>

              <section>
                <div className="mb-3 text-sm font-semibold text-slate-900">业务状态 (Data_Status)</div>
                <div className="flex flex-wrap gap-2">
                  {statusOptions.map((status) => (
                    <button
                      key={status}
                      type="button"
                      onClick={() => setSelectedStatuses((prev) => toggleValue(prev, status))}
                      className={cn(
                        'rounded-lg border px-3 py-2 text-sm font-medium',
                        selectedStatuses.includes(status)
                          ? 'border-blue-600 bg-blue-600 text-white'
                          : 'border-slate-200 bg-slate-50 text-slate-700 hover:bg-white'
                      )}
                    >
                      {styleStatusLabel(status)}
                    </button>
                  ))}
                </div>
              </section>

              <section>
                <div className="mb-3 text-sm font-semibold text-slate-900">更新时间段</div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <label className="text-xs font-medium text-slate-500">
                    从
                    <input
                      type="date"
                      value={dateFrom}
                      onChange={(e) => setDateFrom(e.target.value)}
                      className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm text-slate-700 outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </label>
                  <label className="text-xs font-medium text-slate-500">
                    到
                    <input
                      type="date"
                      value={dateTo}
                      onChange={(e) => setDateTo(e.target.value)}
                      className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm text-slate-700 outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </label>
                </div>
              </section>
            </div>

            <div className="flex items-center justify-between border-t border-slate-200 px-6 py-4">
              <button
                type="button"
                onClick={resetAllFilters}
                className="rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
              >
                重置所有筛选
              </button>
              <button
                type="button"
                onClick={() => setIsAdvancedOpen(false)}
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800"
              >
                查看结果
              </button>
            </div>
          </div>
        </div>
      )}

      <PreviewModal
        isOpen={previewModal.isOpen}
        onClose={() => setPreviewModal({ ...previewModal, isOpen: false })}
        assetCode={previewModal.assetCode}
        assetType={previewModal.type}
        targetAudience={previewModal.targetAudience}
      />
    </div>
  );
}
