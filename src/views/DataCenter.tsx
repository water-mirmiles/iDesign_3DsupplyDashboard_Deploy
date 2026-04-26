import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { UploadCloud, FileSpreadsheet, Box, CheckCircle2, Clock, FileWarning, Search, Info, Calendar, Play, Trash2, RefreshCw, Circle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { ImportHistory } from '@/types';
import { MANDATORY_DATA_FILES } from '@/lib/dataManifest';

const mockHistory: ImportHistory[] = [
  { id: '1', fileName: '20240520_StyleList.xlsx', type: 'xlsx', status: 'success', uploadTime: '2024-05-20 14:30', snapshotDate: '2024-05-20', operator: 'Admin', matchedCount: 1250, version: 'v1.2', updateType: 'overwrite', targetTable: '款号主表' },
  { id: '2', fileName: 'NK_Lasts_Batch1.zip', type: '3d_model', status: 'success', uploadTime: '2024-05-20 11:15', operator: 'Admin', matchedCount: 45, version: 'v1.0', updateType: 'retain', targetTable: '3D 楦头库' },
  { id: '3', fileName: '20240519_AD_Soles.xlsx', type: 'xlsx', status: 'processing', uploadTime: '2024-05-20 09:00', snapshotDate: '2024-05-19', operator: 'Admin', version: 'v1.1', updateType: 'overwrite', targetTable: '大底关联表' },
  { id: '4', fileName: 'Invalid_Data.xlsx', type: 'xlsx', status: 'failed', uploadTime: '2024-05-19 16:45', operator: 'Admin', targetTable: '未知' },
];

type UploadStatus = 'queued' | 'uploading' | 'success' | 'failed';
type UploadQueueItem = {
  id: string;
  file: File;
  status: UploadStatus;
  progress: number; // 0-100
  error?: string;
  /** 3D 且服务端已有同名文件 */
  willOverwrite?: boolean;
  /** 3D 重名检查中 */
  existsCheckPending?: boolean;
};

type MandatoryFilesResponse = {
  ok: boolean;
  latestDate: string | null;
  latestDir: string | null;
  items: Array<{
    tableName: string;
    ready: boolean;
    fileName: string | null;
  }>;
};

function getCurrentUsername() {
  try {
    const parsed = JSON.parse(localStorage.getItem('currentUser') || 'null');
    if (parsed?.username) return String(parsed.username).trim();
  } catch {
    // ignore
  }
  return localStorage.getItem('username') || 'System';
}

function formatBytes(bytes: number) {
  const units = ['B', 'KB', 'MB', 'GB'];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export default function DataCenter() {
  const [activeTab, setActiveTab] = useState<'xlsx' | '3d'>('xlsx');
  const [queue, setQueue] = useState<UploadQueueItem[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [autoOverwrite, setAutoOverwrite] = useState(true);
  const [pipelineSyncing, setPipelineSyncing] = useState(false);
  const [history, setHistory] = useState<ImportHistory[]>(mockHistory);
  const [mandatoryStatus, setMandatoryStatus] = useState<MandatoryFilesResponse | null>(null);
  const [isCheckingMandatory, setIsCheckingMandatory] = useState(false);

  const xlsxInputRef = useRef<HTMLInputElement | null>(null);
  const assetInputRef = useRef<HTMLInputElement | null>(null);

  const accept = useMemo(() => {
    return activeTab === 'xlsx' ? '.xlsx,.xls' : '.obj,.stl,.3dm';
  }, [activeTab]);

  const loadMandatoryStatus = useCallback(async () => {
    setIsCheckingMandatory(true);
    try {
      const resp = await fetch(`/api/check-mandatory-files?t=${Date.now()}`);
      const json = (await resp.json()) as MandatoryFilesResponse;
      if (resp.ok && json?.ok) {
        setMandatoryStatus(json);
        return json;
      }
    } catch {
      // 保持上一次检测结果，避免短暂网络抖动清空清单
    } finally {
      setIsCheckingMandatory(false);
    }
    return null;
  }, []);

  const loadHistory = async () => {
    try {
      const resp = await fetch('/api/history');
      if (!resp.ok) return;
      const json = (await resp.json()) as {
        ok: boolean;
        items: Array<{
          id: string;
          fileName: string;
          size: number;
          uploadTime: string;
          snapshotDate?: string;
          operator?: string;
          category: 'xlsx' | '3d_lasts' | '3d_soles' | 'unknown';
        }>;
      };
      if (!json?.ok) return;

      const mapped: ImportHistory[] = json.items.map((it) => ({
        id: it.id,
        fileName: it.fileName,
        type: it.category === 'xlsx' ? 'xlsx' : '3d_model',
        status: 'success',
        uploadTime: it.uploadTime,
        snapshotDate: it.snapshotDate,
        operator: it.operator || 'System',
        targetTable: it.category === 'xlsx' ? '数据表文件' : it.category === '3d_soles' ? '3D 大底库' : '3D 楦头库',
      }));
      setHistory(mapped);
    } catch {
      // ignore for PoC
    }
  };

  useEffect(() => {
    void loadHistory();
    void loadMandatoryStatus();
    const id = window.setInterval(() => {
      void loadMandatoryStatus();
    }, 5000);
    const onFocus = () => void loadMandatoryStatus();
    window.addEventListener('focus', onFocus);
    return () => {
      window.clearInterval(id);
      window.removeEventListener('focus', onFocus);
    };
  }, [loadMandatoryStatus]);

  const mandatoryReadyByTable = useMemo(() => {
    const map = new Map<string, { ready: boolean; fileName: string | null }>();
    for (const item of mandatoryStatus?.items || []) {
      map.set(item.tableName, { ready: item.ready, fileName: item.fileName });
    }
    return map;
  }, [mandatoryStatus]);

  const mandatoryReadyCount = useMemo(() => {
    return MANDATORY_DATA_FILES.filter((file) => mandatoryReadyByTable.get(file.tableName)?.ready).length;
  }, [mandatoryReadyByTable]);

  const areAllMandatoryFilesReady = (status: MandatoryFilesResponse | null) => {
    if (!status?.items?.length) return false;
    return MANDATORY_DATA_FILES.every((file) => status.items.some((item) => item.tableName === file.tableName && item.ready));
  };

  const handlePickFile = () => {
    setUploadError(null);
    if (isUploading) return;
    if (activeTab === 'xlsx') xlsxInputRef.current?.click();
    else assetInputRef.current?.click();
  };

  const addFilesToQueue = async (files: File[]) => {
    const allowedExts = activeTab === 'xlsx'
      ? new Set(['xlsx', 'xls'])
      : new Set(['obj', 'stl', '3dm']);

    const picked = files.filter((f) => {
      const ext = f.name.split('.').pop()?.toLowerCase();
      return ext ? allowedExts.has(ext) : false;
    });
    if (picked.length === 0) return;

    const toAdd: UploadQueueItem[] = [];
    for (const f of picked) {
      toAdd.push({
        id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
        file: f,
        status: 'queued',
        progress: 0,
        existsCheckPending: activeTab === '3d' && /\.(obj|stl|3dm|glb)$/i.test(f.name),
      });
    }
    if (toAdd.length === 0) return;

    setQueue((prev) => {
      const existing = new Set(prev.map((p) => `${p.file.name}::${p.file.size}`));
      const next = [...prev];
      for (const item of toAdd) {
        const key = `${item.file.name}::${item.file.size}`;
        if (existing.has(key)) continue;
        existing.add(key);
        next.push(item);
      }
      return next;
    });

    if (activeTab === '3d') {
      for (const it of toAdd) {
        if (!it.existsCheckPending) continue;
        let willOverwrite = false;
        try {
          const r = await fetch(`/api/check-exists?${new URLSearchParams({ name: it.file.name })}`);
          const j = (await r.json()) as { ok: boolean; checked?: boolean; exists?: boolean };
          willOverwrite = Boolean(r.ok && j?.checked && j?.exists);
        } catch {
          willOverwrite = false;
        }
        setQueue((p) => p.map((q) => (q.id === it.id ? { ...q, willOverwrite, existsCheckPending: false } : q)));
      }
    }
  };

  const handleFileSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []) as File[];
    e.target.value = '';
    void addFilesToQueue(files);
  };

  const onDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (isUploading) return;
    const files = Array.from(e.dataTransfer.files || []) as File[];
    void addFilesToQueue(files);
  };

  const uploadOneWithProgress = (itemId: string, file: File) => {
    return new Promise<void>((resolve, reject) => {
      const form = new FormData();
      form.append('files', file);
      form.append('username', getCurrentUsername());

      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/upload', true);

      xhr.upload.onprogress = (evt) => {
        if (!evt.lengthComputable) return;
        const p = Math.round((evt.loaded / evt.total) * 100);
        setQueue((prev) => prev.map((it) => (it.id === itemId ? { ...it, progress: p } : it)));
      };

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else reject(new Error(xhr.responseText || `上传失败（HTTP ${xhr.status}）`));
      };
      xhr.onerror = () => reject(new Error('网络错误'));
      xhr.onabort = () => reject(new Error('已取消'));

      xhr.send(form);
    });
  };

  /** 上传后强制重算看板并落盘，避免仍读未含新 3D 文件的旧快照 */
  const triggerDashboardPhysicalSync = async () => {
    try {
      const resp = await fetch('/api/force-sync-dashboard', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: getCurrentUsername() }),
      });
      const json = (await resp.json().catch(() => null)) as { ok?: boolean; error?: string } | null;
      if (!resp.ok || !json?.ok) {
        // eslint-disable-next-line no-console
        console.warn('[DataCenter] force-sync-dashboard 失败', json?.error || resp.status);
      }
    } catch (e) {
      // eslint-disable-next-line no-console
      console.warn('[DataCenter] force-sync-dashboard', e);
    }
  };

  const startUpload = async () => {
    if (isUploading) return;
    const toUpload = queue.filter((q) => q.status === 'queued' || q.status === 'failed');
    if (toUpload.length === 0) return;

    if (activeTab === '3d' && !autoOverwrite) {
      const blocked = toUpload.find((q) => q.willOverwrite);
      if (blocked) {
        setUploadError('队列中存在将覆盖的同名文件。请打开「自动覆盖同名文件」或重命名后重试。');
        return;
      }
    }

    setIsUploading(true);
    setUploadError(null);
    const hadAllMandatoryFiles = areAllMandatoryFilesReady(mandatoryStatus);
    if (activeTab === '3d') setPipelineSyncing(true);

    try {
      let anyUploadOk = false;
      for (const q of toUpload) {
        setQueue((prev) => prev.map((it) => (it.id === q.id ? { ...it, status: 'uploading', progress: 0, error: undefined } : it)));
        try {
          await uploadOneWithProgress(q.id, q.file);
          anyUploadOk = true;
          setQueue((prev) => prev.map((it) => (it.id === q.id ? { ...it, status: 'success', progress: 100 } : it)));
        } catch (e) {
          const message = e instanceof Error ? e.message : '上传失败';
          setQueue((prev) => prev.map((it) => (it.id === q.id ? { ...it, status: 'failed', error: message } : it)));
        }
      }
      await loadHistory();
      const nextMandatoryStatus = await loadMandatoryStatus();
      const shouldForceSync =
        anyUploadOk &&
        (activeTab === '3d' || (!hadAllMandatoryFiles && areAllMandatoryFilesReady(nextMandatoryStatus || mandatoryStatus)));
      if (anyUploadOk) {
        try {
          if (shouldForceSync) {
            setPipelineSyncing(true);
            await triggerDashboardPhysicalSync();
          }
        } finally {
          if (activeTab === '3d' && anyUploadOk) {
            try {
              window.dispatchEvent(new Event('3d-assets-refresh'));
            } catch {
              // ignore
            }
          }
        }
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : '上传失败';
      setUploadError(message);
    } finally {
      setIsUploading(false);
      setPipelineSyncing(false);
    }
  };

  const clearQueue = () => {
    if (isUploading) return;
    setQueue([]);
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500 w-full">
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">数据导入中心</h1>
        <p className="text-sm text-slate-500 mt-1">上传业务表格与 3D 资产文件，系统将自动进行匹配</p>
        {pipelineSyncing && (
          <div className="mt-3 flex items-center gap-2 text-sm text-amber-800 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2 w-full max-w-2xl">
            <RefreshCw className="w-4 h-4 shrink-0 animate-spin" />
            正在重算 3D 对账、看板与清单数据（与后端物理文件对齐）…
          </div>
        )}
      </div>

      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden w-full">
        {/* Tabs */}
        <div className="flex border-b border-slate-200">
          <button
            onClick={() => setActiveTab('xlsx')}
            className={cn(
              "flex-1 py-4 text-sm font-medium flex items-center justify-center gap-2 transition-colors relative",
              activeTab === 'xlsx' ? "text-blue-600 bg-blue-50/50" : "text-slate-500 hover:text-slate-700 hover:bg-slate-50"
            )}
          >
            <FileSpreadsheet className="w-4 h-4" />
            XLSX 表格导入
            {activeTab === 'xlsx' && <div className="absolute bottom-0 left-0 w-full h-0.5 bg-blue-600" />}
          </button>
          <button
            onClick={() => setActiveTab('3d')}
            className={cn(
              "flex-1 py-4 text-sm font-medium flex items-center justify-center gap-2 transition-colors relative",
              activeTab === '3d' ? "text-blue-600 bg-blue-50/50" : "text-slate-500 hover:text-slate-700 hover:bg-slate-50"
            )}
          >
            <Box className="w-4 h-4" />
            3D 模型文件上传
            {activeTab === '3d' && <div className="absolute bottom-0 left-0 w-full h-0.5 bg-blue-600" />}
          </button>
        </div>

        {/* Upload Area */}
        <div className="p-8">
          <div className="mb-6 rounded-xl border border-slate-200 bg-slate-50/70 p-5 shadow-sm w-full">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h3 className="text-base font-semibold text-slate-900">数据准备核对清单 (Must-have Files)</h3>
                <p className="mt-1 text-xs text-slate-500">
                  自动扫描最新数据目录
                  {mandatoryStatus?.latestDate ? `：${mandatoryStatus.latestDate}` : ''}
                  ，已就绪 {mandatoryReadyCount}/{MANDATORY_DATA_FILES.length}。
                </p>
              </div>
              <button
                type="button"
                onClick={() => void loadMandatoryStatus()}
                className="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
              >
                <RefreshCw className={cn('h-3.5 w-3.5', isCheckingMandatory && 'animate-spin')} />
                重新检测
              </button>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3 lg:grid-cols-4">
              {MANDATORY_DATA_FILES.map((file) => {
                const status = mandatoryReadyByTable.get(file.tableName);
                const isReady = Boolean(status?.ready);
                return (
                  <div
                    key={file.tableName}
                    className={cn(
                      'rounded-lg border bg-white p-4',
                      isReady ? 'border-emerald-200' : 'border-slate-200'
                    )}
                  >
                    <div className="flex items-start gap-3">
                      {isReady ? (
                        <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-500" />
                      ) : (
                        <Circle className="mt-0.5 h-5 w-5 shrink-0 text-slate-300" />
                      )}
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="rounded-md bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                            {file.requiredLabel}
                          </span>
                          <span className={cn('text-xs font-semibold', isReady ? 'text-emerald-700' : 'text-slate-500')}>
                            {isReady ? '[已就绪]' : '[缺失]'}
                          </span>
                        </div>
                        <div className="mt-2 text-sm font-semibold text-slate-900">{file.title}</div>
                        <div className="mt-1 truncate font-mono text-xs text-slate-600" title={file.tableName}>
                          {file.tableName}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-500">{file.description}</p>
                        <p className="mt-2 text-[11px] text-slate-400">
                          {isReady ? `已匹配文件：${status?.fileName}` : `建议上传：${file.expectedFileName}`}
                        </p>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {activeTab === '3d' && (
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3 p-3 rounded-lg border border-amber-100 bg-amber-50/50">
              <label className="flex items-center gap-2.5 text-sm text-slate-800 cursor-pointer select-none">
                <input
                  type="checkbox"
                  className="rounded border-amber-300 text-amber-600 focus:ring-amber-500"
                  checked={autoOverwrite}
                  onChange={(e) => setAutoOverwrite(e.target.checked)}
                />
                <span>
                  <span className="font-medium">自动覆盖同名文件</span>
                  <span className="text-slate-500">（关：若服务端已有同名 3D 将阻止上传）</span>
                </span>
              </label>
            </div>
          )}
          <input
            ref={xlsxInputRef}
            type="file"
            accept=".xlsx,.xls"
            multiple
            className="hidden"
            onChange={handleFileSelected}
          />
          <input
            ref={assetInputRef}
            type="file"
            accept=".obj,.stl,.3dm"
            multiple
            className="hidden"
            onChange={handleFileSelected}
          />

          <div
            onClick={handlePickFile}
            onDragOver={onDragOver}
            onDrop={onDrop}
            className={cn(
              "border-2 border-dashed border-slate-300 rounded-xl bg-slate-50 transition-colors cursor-pointer flex flex-col items-center justify-center py-14 px-6 text-center w-full",
              isUploading ? "opacity-70 cursor-not-allowed" : "hover:bg-slate-100"
            )}
          >
            <div className="p-4 bg-white rounded-full shadow-sm mb-4">
              <UploadCloud className="w-8 h-8 text-blue-500" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">
              {isUploading ? '上传中...' : '点击选择或拖拽文件到此处（支持多选）'}
            </h3>
            <p className="text-sm text-slate-500 max-w-2xl mb-4">
              {activeTab === 'xlsx' 
                ? "支持 .xlsx, .xls 格式。请确保表格包含款号、品牌等关键字段以便系统解析。"
                : "支持 .obj, .stl, .3dm 格式。后端将按文件名关键字自动分拨到 楦头/大底 目录。"}
            </p>

            <div className="flex items-center gap-2 text-xs text-amber-600 bg-amber-50 px-3 py-1.5 rounded-md border border-amber-100">
              <Info className="w-3.5 h-3.5" />
              <span>建议命名规范：<strong>日期前缀_文件名.xlsx</strong>（例如：20240414_product_info.xlsx，系统将自动提取为快照时间点）</span>
            </div>

            {uploadError && (
              <div className="mt-4 text-xs text-red-600 bg-red-50 border border-red-200 px-3 py-2 rounded-lg max-w-2xl w-full">
                {uploadError}
              </div>
            )}

            <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
              <button
                type="button"
                onClick={(ev) => {
                  ev.stopPropagation();
                  handlePickFile();
                }}
                disabled={isUploading}
                className="px-4 py-2 bg-white border border-slate-200 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-50 transition-colors shadow-sm disabled:opacity-50"
              >
                选择文件
              </button>
              <button
                type="button"
                onClick={(ev) => {
                  ev.stopPropagation();
                  void startUpload();
                }}
                disabled={isUploading || queue.every((q) => q.status !== 'queued' && q.status !== 'failed')}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm disabled:opacity-50"
              >
                <Play className="w-4 h-4" />
                开始上传
              </button>
              <button
                type="button"
                onClick={(ev) => {
                  ev.stopPropagation();
                  clearQueue();
                }}
                disabled={isUploading || queue.length === 0}
                className="flex items-center gap-2 px-4 py-2 bg-slate-900 text-white text-sm font-medium rounded-lg hover:bg-slate-800 transition-colors shadow-sm disabled:opacity-50"
              >
                <Trash2 className="w-4 h-4" />
                清空队列
              </button>
            </div>
          </div>

          <div className="mt-6 bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden w-full">
            <div className="px-5 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50/50">
              <h3 className="text-base font-semibold text-slate-900">待上传队列</h3>
              <div className="text-sm text-slate-500">
                共 <span className="font-medium text-slate-900">{queue.length}</span> 个文件
              </div>
            </div>
            {queue.length === 0 ? (
              <div className="p-8 text-sm text-slate-500">
                暂无待上传文件。可点击选择或直接拖拽多个文件到上方区域。
              </div>
            ) : (
              <div className="divide-y divide-slate-100">
                {queue.map((q) => (
                  <div key={q.id} className="px-5 py-4 flex flex-col gap-2">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="font-medium text-slate-900 truncate max-w-[70vw]">{q.file.name}</div>
                        <div className="text-xs text-slate-500">{formatBytes(q.file.size)}</div>
                      </div>
                      <div className="flex items-center flex-wrap gap-2">
                        {activeTab === '3d' && q.willOverwrite && (q.status === 'queued' || q.status === 'failed') && (
                          <span className="text-xs px-2 py-0.5 rounded-md bg-amber-50 text-amber-800 border border-amber-200 font-medium">
                            覆盖更新
                          </span>
                        )}
                        {activeTab === '3d' && q.existsCheckPending && (q.status === 'queued' || q.status === 'failed') && (
                          <span className="text-xs text-slate-500">检查重名…</span>
                        )}
                        {q.status === 'queued' && <span className="text-xs px-2 py-1 rounded-md bg-slate-100 text-slate-600 border border-slate-200">等待中</span>}
                        {q.status === 'uploading' && <span className="text-xs px-2 py-1 rounded-md bg-blue-50 text-blue-700 border border-blue-200">上传中</span>}
                        {q.status === 'success' && <span className="text-xs px-2 py-1 rounded-md bg-emerald-50 text-emerald-700 border border-emerald-200">成功</span>}
                        {q.status === 'failed' && <span className="text-xs px-2 py-1 rounded-md bg-red-50 text-red-700 border border-red-200">失败</span>}
                      </div>
                    </div>
                    <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                      <div
                        className={cn(
                          "h-full transition-all duration-200",
                          q.status === 'failed' ? "bg-red-500" : q.status === 'success' ? "bg-emerald-500" : "bg-blue-500"
                        )}
                        style={{ width: `${q.progress}%` }}
                      />
                    </div>
                    {q.status === 'failed' && q.error && (
                      <div className="text-xs text-red-600 bg-red-50 border border-red-200 px-3 py-2 rounded-lg">
                        {q.error}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* History List */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden w-full">
        <div className="px-5 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50/50">
          <h3 className="text-base font-semibold text-slate-900">历史导入记录</h3>
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <input 
              type="text" 
              placeholder="搜索文件名..." 
              className="pl-9 pr-4 py-1.5 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent w-64"
            />
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left whitespace-nowrap">
            <thead className="text-xs text-slate-500 bg-slate-50 uppercase border-b border-slate-200">
              <tr>
                <th className="px-5 py-3 font-medium">文件名</th>
                <th className="px-5 py-3 font-medium">快照时间点</th>
                <th className="px-5 py-3 font-medium">目标业务表</th>
                <th className="px-5 py-3 font-medium">版本 / 类型</th>
                <th className="px-5 py-3 font-medium">状态</th>
                <th className="px-5 py-3 font-medium">匹配数量</th>
                <th className="px-5 py-3 font-medium">上传时间</th>
                <th className="px-5 py-3 font-medium">操作人</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {history.map((item) => (
                <tr key={item.id} className="hover:bg-slate-50/50 transition-colors">
                  <td className="px-5 py-3 font-medium text-slate-900 flex items-center gap-2">
                    {item.type === 'xlsx' ? <FileSpreadsheet className="w-4 h-4 text-emerald-500" /> : <Box className="w-4 h-4 text-blue-500" />}
                    {item.fileName}
                  </td>
                  <td className="px-5 py-3">
                    {item.snapshotDate ? (
                      <div className="flex items-center gap-1.5 text-indigo-600 bg-indigo-50 px-2 py-1 rounded-md w-fit">
                        <Calendar className="w-3.5 h-3.5" />
                        <span className="font-medium text-xs">{item.snapshotDate}</span>
                      </div>
                    ) : (
                      <span className="text-slate-400 text-xs italic">-</span>
                    )}
                  </td>
                  <td className="px-5 py-3 text-slate-600 font-medium">
                    {item.targetTable || '-'}
                  </td>
                  <td className="px-5 py-3">
                    {item.version ? (
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-mono text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded">{item.version}</span>
                        <span className={cn(
                          "text-[10px] font-medium px-1.5 py-0.5 rounded border",
                          item.updateType === 'overwrite' ? "bg-blue-50 text-blue-600 border-blue-200" : "bg-purple-50 text-purple-600 border-purple-200"
                        )}>
                          {item.updateType === 'overwrite' ? '覆盖更新' : '历史留存'}
                        </span>
                      </div>
                    ) : '-'}
                  </td>
                  <td className="px-5 py-3">
                    <div className="flex items-center gap-1.5">
                      {item.status === 'success' && <><CheckCircle2 className="w-4 h-4 text-emerald-500" /><span className="text-emerald-700 font-medium">成功</span></>}
                      {item.status === 'processing' && <><Clock className="w-4 h-4 text-blue-500 animate-pulse" /><span className="text-blue-700 font-medium">处理中</span></>}
                      {item.status === 'failed' && <><FileWarning className="w-4 h-4 text-red-500" /><span className="text-red-700 font-medium">失败</span></>}
                    </div>
                  </td>
                  <td className="px-5 py-3 text-slate-600">
                    {item.matchedCount !== undefined ? (
                      <span className="bg-slate-100 px-2 py-0.5 rounded text-slate-700 font-medium">{item.matchedCount}</span>
                    ) : '-'}
                  </td>
                  <td className="px-5 py-3 text-slate-500">{item.uploadTime}</td>
                  <td className="px-5 py-3 text-slate-600">{item.operator}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
