import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Database,
  Link as LinkIcon,
  Loader2,
  Sparkles,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { GlobalSchemaField } from '@/types';

const API_BASE = 'http://localhost:3001';

const initialStandardFields: GlobalSchemaField[] = [
  { id: 'f1', standardName: '款号', standardKey: 'styleCode', mappedSources: [], description: '产品的唯一标识符' },
  { id: 'f2', standardName: '品牌', standardKey: 'brand', mappedSources: [], description: '所属品牌名称' },
  { id: 'f3', standardName: '楦编号', standardKey: 'lastCode', mappedSources: [], description: '关联的 3D 楦头编号' },
  { id: 'f4', standardName: '大底编号', standardKey: 'soleCode', mappedSources: [], description: '关联的 3D 大底编号' },
  { id: 'f5', standardName: '颜色', standardKey: 'colorCode', mappedSources: [], description: '产品颜色编码' },
  { id: 'f6', standardName: '材质', standardKey: 'materialCode', mappedSources: [], description: '产品材质编码' },
  { id: 'f7', standardName: '状态', standardKey: 'status', mappedSources: [], description: '当前生命周期状态' },
];

type TableHeadersResponse = {
  ok: boolean;
  latestDir: string;
  tables: Record<string, string[]>;
};

type TableSamplesResponse = {
  ok: boolean;
  fileName: string;
  headers: string[];
  samples: Record<string, string[]>;
};

type MappingPart = {
  sourceField: string;
  sourceTable: string;
  physicalColumn?: string;
  joinPath?: string[];
};

type MappingEntry = {
  standardKey: string;
  standardName: string;
  physicalColumn?: string;
  sourceTable?: string;
  sourceField?: string;
  joinPath?: string[];
  operator?: 'CONCAT';
  parts?: MappingPart[];
};

function tokenToMappingPart(token: string): MappingPart | null {
  const t = String(token || '').trim();
  if (!t) return null;
  if (t.startsWith('CHAIN|')) {
    const joinPath = t.slice('CHAIN|'.length).split('->').map((x) => x.trim()).filter(Boolean);
    const head = joinPath[0] || '';
    const dot = head.indexOf('.');
    const tbl = dot > 0 ? head.slice(0, dot) : '';
    const fld = dot > 0 ? head.slice(dot + 1) : head;
    return { sourceField: fld, sourceTable: tbl, joinPath, physicalColumn: t };
  }
  if (t.includes('@')) {
    const at = t.lastIndexOf('@');
    const col = t.slice(0, at);
    const file = t.slice(at + 1);
    return { sourceField: col, sourceTable: file, physicalColumn: col };
  }
  return { sourceField: t, sourceTable: '', physicalColumn: t };
}

/** 写入 mapping_config 的完整条目（含 joinPath / CONCAT 多段） */
function buildCertifiedMapping(fields: GlobalSchemaField[]): MappingEntry[] {
  return fields.map((f) => {
    const tokens = (f.mappedSources || []).map((s) => String(s || '').trim()).filter(Boolean);
    if (tokens.length >= 2) {
      const parts = tokens.map((tok) => tokenToMappingPart(tok)).filter(Boolean) as MappingPart[];
      if (parts.length >= 2) {
        return {
          standardKey: f.standardKey,
          standardName: f.standardName,
          operator: 'CONCAT',
          parts,
          physicalColumn: `CONCAT|${tokens.join('||')}`,
        };
      }
    }
    if (tokens.length === 1 && tokens[0].startsWith('CONCAT|')) {
      const sub = tokens[0]
        .slice('CONCAT|'.length)
        .split('||')
        .map((s) => s.trim())
        .filter(Boolean);
      if (sub.length >= 2) {
        const parts = sub.map((tok) => tokenToMappingPart(tok)).filter(Boolean) as MappingPart[];
        if (parts.length >= 2) {
          return {
            standardKey: f.standardKey,
            standardName: f.standardName,
            operator: 'CONCAT',
            parts,
            physicalColumn: tokens[0],
          };
        }
      }
    }
    const token = tokens[0] || '';
    if (token.startsWith('CHAIN|')) {
      const joinPath = token.slice('CHAIN|'.length).split('->').map((x) => x.trim()).filter(Boolean);
      const head = joinPath[0] || '';
      const dot = head.indexOf('.');
      const tbl = dot > 0 ? head.slice(0, dot) : '';
      const fld = dot > 0 ? head.slice(dot + 1) : head;
      return {
        standardKey: f.standardKey,
        standardName: f.standardName,
        joinPath,
        sourceField: fld,
        sourceTable: tbl,
        physicalColumn: token,
      };
    }
    if (token.includes('@')) {
      const at = token.lastIndexOf('@');
      const col = token.slice(0, at);
      const file = token.slice(at + 1);
      return {
        standardKey: f.standardKey,
        standardName: f.standardName,
        sourceField: col,
        sourceTable: file,
        physicalColumn: col,
      };
    }
    return {
      standardKey: f.standardKey,
      standardName: f.standardName,
      sourceField: '',
      sourceTable: '',
      physicalColumn: '',
    };
  });
}

function mappingEntryIsPublishable(m: MappingEntry): boolean {
  if (m.operator === 'CONCAT' && Array.isArray(m.parts) && m.parts.length >= 2) return true;
  return Boolean(m.physicalColumn);
}

type DdlColumn = {
  table?: string;
  column: string;
  comment?: string;
};

type AiTable = { tableName: string; columns: Array<{ name: string; comment?: string }> };
type AiSuggestion = { standardKey: string; sourceField: string; sourceTable: string };
type AiJoinPath = { targetStandardKey: string; path: string[] };
type AiConcatPart = { sourceField: string; sourceTable: string; joinPath?: string[] };
type AiConcatSuggestion = { standardKey: string; parts: AiConcatPart[] };

function normalize(s: string) {
  return (s || '').trim().toLowerCase();
}

function sortDdlColumnsForGolden(
  colsRaw: { name: string; comment?: string }[],
  masterTable: string,
  activeTable: string
) {
  if (!masterTable || activeTable !== masterTable) return colsRaw;
  const score = (c: { name: string; comment?: string }) => {
    const hay = `${c.name || ''} ${(c.comment || '')}`.toLowerCase();
    const hasStyle = hay.includes('款号') || hay.includes('style') || hay.includes('style_wms');
    const hasBrand = hay.includes('品牌') || hay.includes('brand');
    const hasStatus = hay.includes('状态') || hay.includes('status') || hay.includes('data_status');
    return (hasStyle ? 30 : 0) + (hasBrand ? 20 : 0) + (hasStatus ? 10 : 0);
  };
  return [...colsRaw].sort((a, b) => score(b) - score(a));
}

// NOTE: 沙盒校验（7 维逐项比对）已从主流程移除；保留样本 XLSX 上传仅用于 AI sampleRow 与右侧数据预览。

type GoldenSampleSegment = { tableName: string; fieldName: string; value: string };

/** 一条黄金样本：对应一个标准维度，可含多段物理字段（拼接语义） */
type GoldenSampleEntry = {
  id: string;
  standardKey: string;
  segments: GoldenSampleSegment[];
};

type DdlSchemaResponse = {
  ok: boolean;
  tables?: AiTable[];
  tableMap?: Record<string, Column[]>;
  error?: string;
};

type Column = { name: string; comment?: string };

export type SchemaMappingProps = {
  /** 认证并引擎同步成功后跳转看板 */
  onAfterCertify?: () => void;
};

export default function SchemaMapping({ onAfterCertify }: SchemaMappingProps = {}) {
  const [standardFields, setStandardFields] = useState<GlobalSchemaField[]>(initialStandardFields);
  const [activeFieldKey, setActiveFieldKey] = useState<string>(initialStandardFields[0].standardKey);

  const [ddlText, setDdlText] = useState('');
  const [ddlParsed, setDdlParsed] = useState<DdlColumn[]>([]);
  // Step 1 parsing result: tableName -> columns (from backend local parser)
  const [parsedTables, setParsedTables] = useState<AiTable[]>([]);
  const [parsedTableMap, setParsedTableMap] = useState<Record<string, Column[]>>({});
  const [isParsingDdl, setIsParsingDdl] = useState(false);
  const ddlParseTimerRef = useRef<number | null>(null);
  const ddlParseAbortRef = useRef<AbortController | null>(null);
  const ddlLenRef = useRef<number>(0);
  const ddlPasteArmedRef = useRef<boolean>(false);

  const [aiTables, setAiTables] = useState<AiTable[]>([]);
  const [aiSuggestions, setAiSuggestions] = useState<AiSuggestion[]>([]);
  const [aiJoinPaths, setAiJoinPaths] = useState<AiJoinPath[]>([]);
  const [activeAiTable, setActiveAiTable] = useState<string>('');
  const [activeAiField, setActiveAiField] = useState<{ tableName: string; fieldName: string } | null>(null);
  const [isAiModeling, setIsAiModeling] = useState(false);
  const [masterTable, setMasterTable] = useState('');
  const [goldenSamples, setGoldenSamples] = useState<GoldenSampleEntry[]>([]);
  const [concatPickNext, setConcatPickNext] = useState(false);
  const [ddlRefOpen, setDdlRefOpen] = useState(false);
  const [aiBusy503, setAiBusy503] = useState(false);
  const [aiQueueHint, setAiQueueHint] = useState(false);
  const [aiStatusText, setAiStatusText] = useState('');
  const [aiProgress, setAiProgress] = useState<{ done: number; total: number; tables: string[] }>({ done: 0, total: 0, tables: [] });

  const [tables, setTables] = useState<Record<string, string[]>>({});
  const [latestDir, setLatestDir] = useState<string>('');
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [samples, setSamples] = useState<Record<string, string[]>>({});
  const [headerSearch, setHeaderSearch] = useState('');
  const [resolvedRow, setResolvedRow] = useState<Record<string, string> | null>(null);
  const [resolvedMeta, setResolvedMeta] = useState<{ latestDir?: string; mainTable?: string; warning?: string } | null>(null);
  const [resolvingRow, setResolvingRow] = useState(false);

  const [isSuccess, setIsSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasAttemptedAuth, setHasAttemptedAuth] = useState(false);

  const [isUploadingDataTables, setIsUploadingDataTables] = useState(false);

  const [authSyncing, setAuthSyncing] = useState(false);
  const [certifyEngineRunning, setCertifyEngineRunning] = useState(false);
  const [authSyncHint, setAuthSyncHint] = useState<string>('');
  const authSyncHintTimerRef = useRef<number | null>(null);

  const ddlTableNames = useMemo(() => {
    const out = Object.keys(parsedTableMap || {}).map((k) => String(k || '').trim()).filter(Boolean);
    if (out.length) return out;
    const text = String(ddlText || '');
    const re = /create\s+table\s+([`"]?)([\w.]+)\1/gi;
    const fallback: string[] = [];
    const seen = new Set<string>();
    let m: RegExpExecArray | null = null;
    while ((m = re.exec(text))) {
      const name = String(m[2] || '').trim();
      if (!name) continue;
      const k = name.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      fallback.push(name);
    }
    return fallback;
  }, [parsedTableMap, ddlText]);

  const mappingPreview: MappingEntry[] = useMemo(() => buildCertifiedMapping(standardFields), [standardFields]);

  // 主表字段对齐由 AI 自动完成：不在点击前做“尚未映射”的硬校验
  const masterTableErrors = useMemo(() => {
    if (!masterTable) return ['请先选择业务主表'];
    return [];
  }, [masterTable]);

  // Step 1：DDL 粘贴后，后端即时解析“表名 + 字段列表”
  const runParseDdlSchema = useCallback(
    async (sqlText: string, reason: 'typing' | 'debounced' | 'manual') => {
      const text = String(sqlText || '').trim();
      if (!text) {
        setParsedTables([]);
        setParsedTableMap({});
        return;
      }
      if (ddlParseAbortRef.current) ddlParseAbortRef.current.abort();
      const controller = new AbortController();
      ddlParseAbortRef.current = controller;
      setIsParsingDdl(true);
      try {
        const resp = await fetch(`${API_BASE}/api/parse-ddl-schema`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({ sqlText }),
        });
        const rawText = await resp.text();
        let json: DdlSchemaResponse | null = null;
        try {
          json = rawText ? (JSON.parse(rawText) as DdlSchemaResponse) : null;
        } catch {
          json = null;
        }
        if (!resp.ok || !json?.ok) {
          // eslint-disable-next-line no-console
          console.warn('[parse-ddl-schema] failed', { reason, status: resp.status, statusText: resp.statusText, body: json ?? rawText });
          setParsedTables([]);
          setParsedTableMap({});
          return;
        }
        if (json.tableMap && typeof json.tableMap === 'object' && !Array.isArray(json.tableMap)) {
          setParsedTableMap(json.tableMap as Record<string, Column[]>);
          setParsedTables(Array.isArray(json.tables) ? json.tables : []);
          return;
        }
        // backward-compatible fallback: build map from tables array
        const tablesArr = Array.isArray(json.tables) ? json.tables : [];
        setParsedTables(tablesArr);
        const map: Record<string, Column[]> = {};
        for (const t of tablesArr) {
          const tn = String(t?.tableName || '').trim();
          if (!tn) continue;
          const cols = Array.isArray(t?.columns)
            ? t.columns
                .map((c: any) => ({ name: String(c?.name || '').trim(), comment: c?.comment ? String(c.comment) : '' }))
                .filter((c: any) => c.name)
            : [];
          map[tn] = cols;
        }
        setParsedTableMap(map);
      } catch (e) {
        if (String((e as any)?.name || '').includes('AbortError')) return;
        // eslint-disable-next-line no-console
        console.warn('[parse-ddl-schema] exception', { reason, message: e instanceof Error ? e.message : String(e) });
        setParsedTables([]);
        setParsedTableMap({});
      } finally {
        setIsParsingDdl(false);
      }
    },
    []
  );

  useEffect(() => {
    if (ddlParseTimerRef.current) window.clearTimeout(ddlParseTimerRef.current);
    if (ddlParseAbortRef.current) ddlParseAbortRef.current.abort();
    const text = String(ddlText || '').trim();
    if (!text) {
      setParsedTables([]);
      return;
    }
    ddlParseTimerRef.current = window.setTimeout(() => {
      void runParseDdlSchema(ddlText, 'debounced');
    }, 500);
    return () => {
      if (ddlParseTimerRef.current) window.clearTimeout(ddlParseTimerRef.current);
      if (ddlParseAbortRef.current) ddlParseAbortRef.current.abort();
    };
  }, [ddlText, runParseDdlSchema]);

  const saveSchemaDraft = useCallback(async () => {
    try {
      const data = {
        ddlText,
        masterTable,
        goldenSamples: goldenSamples.map((s) => ({
          standardKey: s.standardKey,
          segments: s.segments.map((g) => ({ tableName: g.tableName, fieldName: g.fieldName, value: g.value })),
        })),
      };
      // eslint-disable-next-line no-console
      console.log('🚀 准备发送草稿:', data);

      const resp = await fetch(`${API_BASE}/api/save-schema-draft`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        let detail = `HTTP ${resp.status}`;
        try {
          const parsed = text ? (JSON.parse(text) as { error?: string; message?: string }) : null;
          if (parsed?.error) detail = `${detail}: ${parsed.error}`;
          else if (parsed?.message) detail = `${detail}: ${parsed.message}`;
          // eslint-disable-next-line no-console
          console.error('[save-schema-draft] 请求失败', { status: resp.status, statusText: resp.statusText, body: parsed ?? text });
        } catch {
          if (text) detail = `${detail} — ${text.slice(0, 500)}`;
          // eslint-disable-next-line no-console
          console.error('[save-schema-draft] 请求失败', { status: resp.status, statusText: resp.statusText, raw: text.slice(0, 2000) });
        }
        throw new Error(detail);
      }
      return { ok: true as const };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      // eslint-disable-next-line no-console
      console.error('[save-schema-draft] 保存异常', msg, e);
      return { ok: false as const, error: msg };
    }
  }, [ddlText, goldenSamples, masterTable]);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/load-schema-draft`);
        if (!resp.ok) return;
        const json = (await resp.json()) as { ok?: boolean; draft?: Record<string, unknown> };
        if (!alive) return;
        const d = json.draft && typeof json.draft === 'object' && !Array.isArray(json.draft) ? json.draft : {};
        if (typeof d.ddlText === 'string') setDdlText(d.ddlText);
        if (typeof (d as any).masterTable === 'string') setMasterTable((d as any).masterTable);
        const gs = (d as any).goldenSamples;
        if (Array.isArray(gs)) {
          const normalized: GoldenSampleEntry[] = gs
            .map((x: any, idx: number) => {
              if (Array.isArray(x?.segments) && x.segments.length) {
                return {
                  id: typeof x?.id === 'string' ? x.id : `${Date.now()}_${idx}`,
                  standardKey: typeof x?.standardKey === 'string' ? x.standardKey : 'materialCode',
                  segments: x.segments.map((seg: any) => ({
                    tableName: typeof seg?.tableName === 'string' ? seg.tableName : '',
                    fieldName: typeof seg?.fieldName === 'string' ? seg.fieldName : '',
                    value: typeof seg?.value === 'string' ? seg.value : '',
                  })),
                };
              }
              return {
                id: `${Date.now()}_${idx}`,
                standardKey: typeof x?.standardKey === 'string' ? x.standardKey : 'materialCode',
                segments: [
                  {
                    tableName: typeof x?.tableName === 'string' ? x.tableName : '',
                    fieldName: typeof x?.fieldName === 'string' ? x.fieldName : '',
                    value: typeof x?.value === 'string' ? x.value : '',
                  },
                ],
              };
            })
            .filter(
              (x: GoldenSampleEntry) =>
                x.segments.some((s) => s.tableName || s.fieldName || s.value)
            );
          setGoldenSamples(normalized);
        }
      } catch {
        // ignore load errors — 草稿可选
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    return () => {
      if (authSyncHintTimerRef.current) window.clearTimeout(authSyncHintTimerRef.current);
    };
  }, []);

  useEffect(() => {
    const shouldPoll = isAiModeling;
    if (!shouldPoll) {
      setAiStatusText('');
      return;
    }
    let alive = true;
    const tick = async () => {
      try {
        const resp = await fetch('/api/ai-status');
        if (!resp.ok) return;
        const json = (await resp.json()) as { ok: boolean; status?: string; message?: string; model?: string; parsedCount?: number; totalCount?: number; parsedTables?: string[] };
        if (!alive || !json?.ok) return;
        const model = json.model ? `正在尝试模型: ${json.model}` : '';
        const msg = json.message ? String(json.message) : '';
        const merged = [model, msg].filter(Boolean).join(' · ');
        setAiStatusText(merged);
        setAiProgress({
          done: Number(json.parsedCount || 0),
          total: Number(json.totalCount || 0),
          tables: Array.isArray(json.parsedTables) ? json.parsedTables.map(String) : [],
        });
      } catch {
        // ignore polling errors
      }
    };
    void tick();
    const id = window.setInterval(() => void tick(), 1000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, [isAiModeling]);

  const allPhysicalColumns = useMemo(() => {
    const set = new Set<string>();
    for (const cols of Object.values(tables) as string[][]) cols.forEach((c) => set.add(c));
    return set;
  }, [tables]);

  const detectedTableCount = useMemo(() => {
    const m = String(ddlText || '').match(/create\s+table\b/gi);
    return m ? m.length : 0;
  }, [ddlText]);

  const activeField = useMemo(() => {
    return standardFields.find((f) => f.standardKey === activeFieldKey) || standardFields[0];
  }, [activeFieldKey, standardFields]);

  const activeFieldKeywords = useMemo(() => {
    const f = initialStandardFields.find((x) => x.standardKey === activeFieldKey) || activeField;
    const base = [f.standardName, f.standardKey];
    const extras =
      f.standardKey === 'styleCode' ? ['款号', 'style', 'style_wms'] :
      f.standardKey === 'brand' ? ['品牌', 'brand'] :
      f.standardKey === 'lastCode' ? ['楦', 'last', '楦头'] :
      f.standardKey === 'soleCode' ? ['底', 'sole', '大底', 'outsole'] :
      f.standardKey === 'colorCode' ? ['颜色', 'color'] :
      f.standardKey === 'materialCode' ? ['材质', 'material'] :
      f.standardKey === 'status' ? ['状态', 'status', 'data_status', 'lifecycle'] :
      [];
    return Array.from(new Set([...base, ...extras].filter(Boolean)));
  }, [activeField, activeFieldKey]);

  const activeDdlMatches = useMemo(() => {
    if (!ddlParsed.length) return new Set<string>();
    const set = new Set<string>();
    for (const c of ddlParsed) {
      const comment = c.comment || '';
      if (!comment) continue;
      const hit = activeFieldKeywords.some((k) => comment.includes(k) || normalize(comment).includes(normalize(k)));
      if (hit) set.add(normalize(c.column));
    }
    return set;
  }, [activeFieldKeywords, ddlParsed]);

  const chainPreviewForStandardKey = (standardKey: string) => {
    const f = standardFields.find((x) => x.standardKey === standardKey);
    const token = f?.mappedSources?.[0] || '';
    if (!token.startsWith('CHAIN|')) return '';
    const path = token.slice('CHAIN|'.length).split('->').map((x) => x.trim()).filter(Boolean);
    if (!path.length) return '';
    const pretty = path
      .map((p, idx) => {
        const [t, col] = p.split('.');
        if (!t || !col) return p;
        if (idx === 0) return `[Info表] ${col}`;
        if (idx === path.length - 1) return `[目标表] ${t}.${col}`;
        return `[关联ID] ${t}.${col}`;
      })
      .join(' -> ');
    return pretty;
  };

  const sampleGrid = useMemo(() => {
    // 右侧“Excel 风格”抽样表：以当前 selectedFile 为主（表头来自 XLSX）
    const cols = mappingPreview.filter(
      (m) =>
        m.physicalColumn &&
        !String(m.physicalColumn).startsWith('CHAIN|') &&
        !String(m.physicalColumn).startsWith('CONCAT|')
    ) as Array<Required<Pick<MappingEntry, 'standardKey' | 'standardName' | 'physicalColumn'>>>;
    const rows = [0, 1, 2].map((rowIdx) => {
      const r: Record<string, string> = {};
      for (const c of cols) {
        const vs = samples?.[c.physicalColumn] || [];
        r[c.standardKey] = String(vs[rowIdx] ?? '');
      }
      return r;
    });
    return { cols, rows };
  }, [mappingPreview, samples, selectedFile]);

  const filteredHeaders = useMemo(() => {
    const cols = selectedFile ? (tables[selectedFile] || []) : [];
    const q = normalize(headerSearch);
    if (!q) return cols;
    return cols.filter((c) => normalize(c).includes(q));
  }, [headerSearch, selectedFile, tables]);

  const hasUnmappedWarning = (f: GlobalSchemaField) => {
    if (f.mappedSources?.length) return false;
    // 若物理池中存在可能候选（同名列），则不提示“完全找不到”
    const maybe = allPhysicalColumns.has(f.standardKey) || allPhysicalColumns.has(f.standardName);
    return !maybe && allPhysicalColumns.size > 0;
  };

  const refreshHeaders = async () => {
    setError(null);
    try {
      const resp = await fetch('/api/table-headers');
      if (!resp.ok) throw new Error(`获取表头失败（HTTP ${resp.status}）`);
      const json = (await resp.json()) as TableHeadersResponse;
      if (!json.ok) throw new Error('获取表头失败');
      setTables(json.tables || {});
      setLatestDir(json.latestDir || '');
      const firstFile = Object.keys(json.tables || {})[0] || '';
      setSelectedFile((prev) => prev || firstFile);
    } catch (e) {
      setError(e instanceof Error ? e.message : '获取表头失败');
    }
  };

  const handleDataTablesUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    setError(null);
    const files = e.target.files;
    if (!files?.length) return;
    const all = Array.from(files) as File[];
    const fd = new FormData();
    for (const f of all) fd.append('files', f);
    try {
      setIsUploadingDataTables(true);
      const resp = await fetch(`${API_BASE}/api/upload`, { method: 'POST', body: fd });
      if (!resp.ok) throw new Error(`上传失败（HTTP ${resp.status}）`);
      const json = (await resp.json().catch(() => null)) as any;
      if (!json?.ok) throw new Error(json?.error || '上传失败');
      await refreshHeaders();
    } catch (err) {
      setError(err instanceof Error ? err.message : '上传失败');
    } finally {
      setIsUploadingDataTables(false);
    }
    e.target.value = '';
  };

  const loadSamples = async (fileName: string) => {
    if (!fileName) return;
    try {
      const resp = await fetch(`/api/table-samples?fileName=${encodeURIComponent(fileName)}`);
      if (!resp.ok) return;
      const json = (await resp.json()) as TableSamplesResponse;
      if (!json.ok) return;
      setSamples(json.samples || {});
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    void refreshHeaders();
  }, []);

  useEffect(() => {
    void loadSamples(selectedFile);
  }, [selectedFile]);

  // Step 4：基于当前 mapping（含 Join Path）从最新 XLSX 抽取一行真实值用于预览
  useEffect(() => {
    const hasAny = mappingPreview.some((m) => mappingEntryIsPublishable(m));
    if (!hasAny) {
      setResolvedRow(null);
      setResolvedMeta(null);
      return;
    }
    let alive = true;
    setResolvingRow(true);
    (async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/preview-mapping-row`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mapping: mappingPreview }),
        });
        const json = (await resp.json().catch(() => null)) as any;
        if (!alive) return;
        if (!resp.ok || !json?.ok) {
          setResolvedRow(null);
          setResolvedMeta(null);
          return;
        }
        const row =
          json.row && typeof json.row === 'object' && !Array.isArray(json.row)
            ? Object.fromEntries(Object.entries(json.row).map(([k, v]) => [String(k), String(v ?? '')]))
            : null;
        setResolvedRow(row);
        setResolvedMeta({
          latestDir: typeof json.latestDir === 'string' ? json.latestDir : undefined,
          mainTable: typeof json.mainTable === 'string' ? json.mainTable : undefined,
          warning: typeof json.warning === 'string' ? json.warning : undefined,
        });
      } catch {
        if (!alive) return;
        setResolvedRow(null);
        setResolvedMeta(null);
      } finally {
        if (!alive) return;
        setResolvingRow(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [mappingPreview]);

  /** 将 AI 返回的直连字段 + Join 链路 + 复合拼接映射一次性写入标准字段（concat 最后覆盖同 key） */
  const applyAiResultsToFields = (sug: AiSuggestion[], jp: AiJoinPath[], concatSuggestions: AiConcatSuggestion[] = []) => {
    setStandardFields((prev) => {
      const next = prev.map((f) => ({ ...f, mappedSources: [...(f.mappedSources || [])] }));
      for (const s of sug) {
        const target = next.find((f) => f.standardKey === s.standardKey);
        if (!target) continue;
        target.mappedSources = [`${s.sourceField}@${s.sourceTable}`];
      }
      for (const j of jp) {
        const target = next.find((f) => f.standardKey === j.targetStandardKey);
        if (!target) continue;
        target.mappedSources = [`CHAIN|${j.path.join('->')}`];
      }
      for (const c of concatSuggestions) {
        const target = next.find((f) => f.standardKey === c.standardKey);
        if (!target || !Array.isArray(c.parts) || c.parts.length < 2) continue;
        const tokens = c.parts
          .map((p) => {
            const jpArr = Array.isArray(p.joinPath) ? p.joinPath.map((x) => String(x).trim()).filter(Boolean) : [];
            if (jpArr.length >= 2) return `CHAIN|${jpArr.join('->')}`;
            const sf = String(p.sourceField || '').trim();
            const st = String(p.sourceTable || '').trim();
            if (sf && st) return `${sf}@${st}`;
            return '';
          })
          .filter(Boolean);
        if (tokens.length >= 2) target.mappedSources = tokens;
      }
      return next;
    });
    setIsSuccess(true);
  };

  const handleAiLogicModeling = async (): Promise<{ ok: true } | { ok: false; error: string }> => {
    setError(null);
    setAiBusy503(false);
    setAiQueueHint(true);
    setIsAiModeling(true);
    try {
      setAiTables([]);
      const controller = new AbortController();
      const t = window.setTimeout(() => controller.abort(), 180000);
      const resp = await fetch('/api/ai-parse-multi-sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          sqlText: ddlText,
          masterTable: masterTable || undefined,
          goldenSamples: goldenSamples.map((s) => ({
            standardKey: s.standardKey,
            segments: s.segments.map((g) => ({ tableName: g.tableName, fieldName: g.fieldName, value: g.value })),
          })),
        }),
      });
      window.clearTimeout(t);
      const text = await resp.text();
      if (!resp.ok) {
        if (resp.status === 503) {
          setAiBusy503(true);
          setAiQueueHint(true);
          throw new Error('Google AI 忙碌中，请 5 秒后重试。');
        }
        setAiQueueHint(false);
        throw new Error(text || `AI 多表解析失败（HTTP ${resp.status}）`);
      }
      // eslint-disable-next-line no-console
      console.log('[ai-parse-multi-sql] raw:', text);
      const json = JSON.parse(text) as {
        ok: boolean;
        tables?: AiTable[];
        smartSuggestions?: AiSuggestion[];
        joinPathSuggestions?: AiJoinPath[];
        concatMappingSuggestions?: AiConcatSuggestion[];
        error?: string;
        _debug?: any;
      };
      // eslint-disable-next-line no-console
      console.log('[ai-parse-multi-sql] parsed:', json);
      if (!json.ok) throw new Error(json.error || 'AI 多表解析失败');

      const parsedTables = (json.tables || []).filter((t) => t?.tableName);
      const sug = json.smartSuggestions || [];
      const jp = json.joinPathSuggestions || [];
      const concatSug = json.concatMappingSuggestions || [];
      setAiSuggestions(sug);
      setAiJoinPaths(jp);
      setAiTables(parsedTables);
      const first = parsedTables[0]?.tableName || '';
      setActiveAiTable((prev) => (prev && parsedTables.some((x) => x.tableName === prev) ? prev : first));

      const flattened: DdlColumn[] = [];
      for (const tbl of parsedTables) {
        for (const c of tbl.columns || []) {
          if (!c?.name) continue;
          flattened.push({ table: tbl.tableName, column: c.name, comment: c.comment || undefined });
        }
      }
      setDdlParsed(flattened);
      setAiQueueHint(false);
      applyAiResultsToFields(sug, jp, concatSug);
      return { ok: true };
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'AI 多表解析失败';
      if (String((e as any)?.name || '').includes('AbortError')) {
        setAiQueueHint(false);
        setAiBusy503(false);
        setError('AI 解析超时（>180s），建议分批粘贴 SQL 或减少沙盒文件体积。');
        return { ok: false, error: 'AI 解析超时（>180s），建议分批粘贴 SQL 或减少沙盒文件体积。' };
      }
      if (String(msg).includes('503')) setAiBusy503(true);
      setError(msg);
      return { ok: false, error: msg };
    } finally {
      setIsAiModeling(false);
    }
  };

  const findAnyXlsxFileContainingColumn = (col: string) => {
    const needle = normalize(col);
    for (const [fileName, cols] of Object.entries(tables) as Array<[string, string[]]>) {
      if (cols.some((c) => normalize(c) === needle)) return fileName;
    }
    return '';
  };

  const inspectPhysicalField = async (fieldName: string) => {
    // 点击物理字段：自动去 XLSX 里找同名列并拉取抽样（如果存在）
    const file = findAnyXlsxFileContainingColumn(fieldName);
    if (file) {
      setSelectedFile(file);
      await loadSamples(file);
    }
  };

  const assignPhysicalColumn = (physicalColumn: string, sourceFile: string) => {
    const tok = `${physicalColumn}@${sourceFile}`;
    const append = concatPickNext;
    if (append) setConcatPickNext(false);
    setStandardFields((prev) =>
      prev.map((f) => {
        if (f.standardKey !== activeFieldKey) return f;
        const cur = (f.mappedSources || []).map((s) => String(s).trim()).filter(Boolean);
        if (append) {
          if (!cur.length) return { ...f, mappedSources: [tok] };
          return { ...f, mappedSources: [...cur, tok] };
        }
        return { ...f, mappedSources: [tok] };
      })
    );
  };

  const handleSaveMappingAuthenticated = async (): Promise<{ ok: true } | { ok: false; error: string }> => {
    setError(null);
    try {
      const payload = {
        latestDir,
        mapping: mappingPreview,
        mappingAuthenticated: true,
      };
      const resp = await fetch('/api/save-mapping', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!resp.ok) throw new Error(`保存失败（HTTP ${resp.status}）`);
      const json = (await resp.json()) as { ok: boolean; error?: string };
      if (!json.ok) throw new Error(json.error || '保存失败');
      setIsSuccess(true);
      return { ok: true };
    } catch (e) {
      const msg = e instanceof Error ? e.message : '保存失败';
      setError(msg);
      return { ok: false, error: msg };
    }
  };

  // NOTE: 旧版“沙盒 7 维校验/沙盒上传”已从主流程移除（改为 data_tables 文件列表 + 真实数据预览 + 手工纠偏）

  const handleAuthAndSync = async () => {
    if (authSyncing || certifyEngineRunning) return;
    setHasAttemptedAuth(true);
    setError(null);
    setAuthSyncHint('');
    const hasAny = mappingPreview.some((m) => mappingEntryIsPublishable(m));
    if (!hasAny) return setError('尚无可发布的结果：请先执行 AI 逻辑建模或手动纠偏映射');

    setAuthSyncing(true);
    try {
      setAuthSyncHint('保存配置中…');
      const savedMapping = await handleSaveMappingAuthenticated();
      if (savedMapping.ok === false) throw new Error(savedMapping.error);

      const draft = await saveSchemaDraft();
      if (draft.ok === false) throw new Error(draft.error);

      setCertifyEngineRunning(true);
      setAuthSyncHint('');
      const syncResp = await fetch('/api/certify-and-sync', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
      const syncJson = (await syncResp.json().catch(() => ({}))) as { ok?: boolean; error?: string };
      if (!syncResp.ok || !syncJson.ok) {
        throw new Error(syncJson.error || `引擎同步失败（HTTP ${syncResp.status}）`);
      }

      setAuthSyncHint('逻辑已认证！全量数据已写入看板快照。');
      if (authSyncHintTimerRef.current) window.clearTimeout(authSyncHintTimerRef.current);
      authSyncHintTimerRef.current = window.setTimeout(() => setAuthSyncHint(''), 6000);
      onAfterCertify?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : '一键认证失败');
    } finally {
      setCertifyEngineRunning(false);
      setAuthSyncing(false);
    }
  };

  // 并发保护：仅在“AI 建模 / 发布 / 真实值预览抓取 / 样本 XLSX 解析”期间禁止用户修改配置
  // DDL 的即时解析（isParsingDdl）不应阻塞用户继续编辑
  const busy = isAiModeling || authSyncing || certifyEngineRunning || isUploadingDataTables || resolvingRow;
  const activeMapping = mappingPreview.find((m) => m.standardKey === activeFieldKey);
  const activeToken = String(activeMapping?.physicalColumn || '');
  const resolvedValueForActive = useMemo(() => {
    const row = resolvedRow || {};
    if (activeFieldKey === 'styleCode') return String((row as any).style_wms ?? '');
    return String((row as any)[activeFieldKey] ?? '');
  }, [activeFieldKey, resolvedRow]);

  return (
    <>
      {certifyEngineRunning && (
        <div className="fixed inset-0 z-[100] flex flex-col items-center justify-center gap-6 bg-slate-950/90 px-6 text-center">
          <Loader2 className="h-10 w-10 text-indigo-400 animate-spin shrink-0" aria-hidden />
          <p className="text-sm font-medium text-white max-w-md leading-relaxed">
            正在执行全量数据透视与 3D 资产对齐，请稍候…
          </p>
          <div className="w-full max-w-md h-2 rounded-full bg-slate-800 overflow-hidden">
            <div className="h-full w-full bg-gradient-to-r from-indigo-600 via-sky-400 to-indigo-600 opacity-90 animate-pulse" />
          </div>
        </div>
      )}
      {busy && !certifyEngineRunning && (
        <div className="fixed inset-0 z-40 bg-black/20 backdrop-blur-[1px]">
          <div className="absolute inset-x-0 top-0">
            <div className="mx-auto w-full px-3 py-2 text-[11px] text-white bg-slate-900/90 border-b border-white/10">
              {isAiModeling ? 'AI 建模中…' : authSyncing ? '正在发布到看板…' : resolvingRow ? '正在抓取真实数据预览…' : '处理中…'}
            </div>
          </div>
        </div>
      )}

      <div className="w-full px-2 lg:px-4 min-h-full flex flex-col gap-4 pb-8">
        {/* Step 1: 提供原材料（SQL & 样本文件） */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-3 py-2 bg-slate-900 text-white flex flex-wrap items-center justify-between gap-2">
            <div className="text-sm font-semibold">Step 1 · 提供原材料（SQL & 样本文件）</div>
            <div className="text-[11px] text-slate-200 flex items-center gap-2">
              {isParsingDdl ? (
                <span className="animate-pulse">AI 正在深度理解表结构...</span>
              ) : (
                <>
                  <span>
                    已检测到 <span className="font-mono">{detectedTableCount}</span> 张表
                  </span>
                  <span className="text-slate-500">|</span>
                  <span>
                    已解析 <span className="font-mono">{Object.keys(parsedTableMap || {}).length}</span> 张表结构
                  </span>
                </>
              )}
            </div>
          </div>
          <div className="p-3 grid grid-cols-12 gap-3">
            {/* Left: Physical files (data_tables) */}
            <div className="col-span-12 lg:col-span-6 border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-2">
                <div className="text-[11px] font-semibold text-slate-800">物理文件区（data_tables）</div>
                <label className="inline-flex items-center gap-2 px-3 py-1.5 text-[11px] font-medium rounded-lg bg-white border border-emerald-400 text-emerald-900 cursor-pointer hover:bg-emerald-50">
                  <input
                    type="file"
                    accept=".xlsx,.xls"
                    multiple
                    className="hidden"
                    disabled={busy}
                    onChange={(e) => void handleDataTablesUpload(e)}
                  />
                  {isUploadingDataTables ? '上传中…' : '上传样本 XLSX'}
                </label>
              </div>
              <div className="p-3 space-y-2">
                <div className="text-[11px] text-slate-600">
                  最新目录：<span className="font-mono text-slate-800">{latestDir || '-'}</span>
                  <span className="mx-2 text-slate-300">|</span>
                  文件数：<span className="font-mono text-slate-800">{Object.keys(tables || {}).length}</span>
                </div>
                <div className="max-h-[260px] overflow-y-auto border border-slate-200 rounded-lg bg-white">
                  {Object.keys(tables || {}).length ? (
                    <div className="divide-y divide-slate-100">
                      {Object.keys(tables || {}).map((fileName) => (
                        <button
                          key={fileName}
                          type="button"
                          disabled={busy}
                          onClick={() => {
                            setSelectedFile(fileName);
                            void loadSamples(fileName);
                          }}
                          className={cn(
                            'w-full text-left px-3 py-2 text-[11px] font-mono hover:bg-slate-50',
                            selectedFile === fileName ? 'bg-indigo-50 text-indigo-900' : 'text-slate-700'
                          )}
                        >
                          {fileName}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <div className="p-3 text-[11px] text-slate-500">暂无文件。请先上传，或检查 `server/storage/data_tables`。</div>
                  )}
                </div>
              </div>
            </div>

            {/* Right: SQL / DDL */}
            <div className="col-span-12 lg:col-span-6 border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-2">
                <div className="text-[11px] font-semibold text-slate-800">逻辑定义区（SQL/DDL）</div>
                <div className="text-[11px] text-slate-600">
                  已检测到 <span className="font-mono text-slate-900">{detectedTableCount}</span> 张表
                </div>
              </div>
              <div className="p-3 space-y-2">
                <textarea
                  value={ddlText}
                  onPaste={(e) => {
                    // 只要发生 paste，就武断认为是“大幅变化”，下一次 onChange 立即解析
                    ddlPasteArmedRef.current = true;
                    // 解析在 onChange 中触发（此处不直接 fetch，避免读到旧 value）
                    // eslint-disable-next-line no-console
                    console.log('[ddl] paste armed', { clipChars: e.clipboardData?.getData('text')?.length || 0 });
                  }}
                  onChange={(e) => {
                    const next = e.target.value;
                    const prevLen = ddlLenRef.current || 0;
                    const nextLen = next.length;
                    const delta = Math.abs(nextLen - prevLen);
                    ddlLenRef.current = nextLen;
                    setDdlText(next);

                    // 粘贴/大幅变化：立即解析
                    const hugeChange = delta >= 200;
                    if (ddlPasteArmedRef.current || hugeChange) {
                      ddlPasteArmedRef.current = false;
                      if (ddlParseTimerRef.current) window.clearTimeout(ddlParseTimerRef.current);
                      void runParseDdlSchema(next, 'typing');
                    }
                    // 小幅编辑：静默走防抖 useEffect
                  }}
                  placeholder="粘贴多表 DDL（含 COMMENT 更佳）。粘贴后系统会即时解析表名与字段列表…"
                  className="w-full min-h-[240px] max-h-[360px] p-3 text-xs border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent resize-y font-mono text-slate-700 bg-slate-50/30"
                  disabled={busy}
                />
                {isParsingDdl && <div className="text-[11px] text-slate-500">正在解析 DDL…</div>}
              </div>
            </div>
          </div>
        </div>

        {/* Step 2: 动态样本配置（核心重构） */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-3 py-2 bg-slate-900 text-white flex items-center justify-between gap-2">
            <div className="text-sm font-semibold">Step 2 · AI 学习区（动态样本配置）</div>
            <div className="text-[11px] text-slate-200">严禁分析后清空：配置会一直保留并可保存草稿</div>
          </div>
          <div className="p-3 space-y-3">
            <div className="grid grid-cols-12 gap-2 items-end">
              <div className="col-span-12 lg:col-span-6">
                <div className="text-[11px] font-medium text-slate-700 mb-1">业务主表（Master Table）</div>
                <select
                  value={masterTable}
                  onChange={(e) => setMasterTable(e.target.value)}
                  disabled={ddlTableNames.length === 0 || busy}
                  className="w-full text-[11px] px-2 py-2 rounded-lg bg-white border border-slate-200 text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:bg-slate-50"
                >
                  <option value="">
                    {ddlTableNames.length === 0 ? '请先粘贴 DDL（解析出表名后可选）…' : '请选择 DDL 中的表名…'}
                  </option>
                  {ddlTableNames.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-span-12 lg:col-span-6 flex flex-wrap gap-2 justify-end">
                <button
                  type="button"
                  onClick={() => void saveSchemaDraft()}
                  disabled={busy}
                  className="px-3 py-2 text-[11px] font-semibold rounded-lg bg-slate-900 text-white hover:bg-slate-800 disabled:opacity-50"
                  title="写入 server/storage/schema_draft.json"
                >
                  保存当前配置草稿
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => {
                    setError(null);
                    setHasAttemptedAuth(false);
                    setAuthSyncHint('');
                    setIsSuccess(false);
                    setDdlText('');
                    setParsedTables([]);
                    setMasterTable('');
                    setGoldenSamples([]);
                    setConcatPickNext(false);
                    setStandardFields(initialStandardFields);
                    setActiveFieldKey(initialStandardFields[0].standardKey);
                    setAiTables([]);
                    setAiSuggestions([]);
                    setAiJoinPaths([]);
                    setDdlParsed([]);
                    setResolvedRow(null);
                    setResolvedMeta(null);
                  }}
                  className="px-3 py-2 text-[11px] font-semibold rounded-lg bg-white border border-red-200 text-red-700 hover:bg-red-50 disabled:opacity-50"
                >
                  一键清空配置
                </button>
              </div>
            </div>

            <div className="border border-slate-200 rounded-xl">
              <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-2 flex-wrap">
                <div className="text-[11px] font-semibold text-slate-800">动态黄金样本（Dynamic Golden Sample）</div>
                <button
                  type="button"
                  disabled={Object.keys(parsedTableMap || {}).length === 0 || busy}
                  onClick={() =>
                    setGoldenSamples((prev) => [
                      ...prev,
                      {
                        id: `${Date.now()}_${Math.random().toString(16).slice(2)}`,
                        standardKey: 'materialCode',
                        segments: [{ tableName: '', fieldName: '', value: '' }],
                      },
                    ])
                  }
                  className="px-2.5 py-1.5 text-[11px] font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                >
                  + 添加样本组
                </button>
              </div>
              <div className="p-3 space-y-3">
                <div className="text-[11px] text-slate-600">
                  每条样本对应一个<strong>标准维度</strong>。同一维度下可用「+ 组合」添加多段物理字段，表示拼接关系（例如材质：<code className="font-mono">LT</code> +{' '}
                  <code className="font-mono">04</code> → <code className="font-mono">LT04</code>）。
                </div>
                {goldenSamples.length === 0 ? (
                  <div className="text-[11px] text-slate-500">
                    {Object.keys(parsedTableMap || {}).length === 0
                      ? '请先解析 SQL 或等待解析完成…（解析成功后即可添加样本行）'
                      : '还没有样本。点击「+ 添加样本组」，按标准维度 + 多段 table.field=value 约束 AI。'}
                  </div>
                ) : (
                  goldenSamples.map((entry) => (
                    <div key={entry.id} className="border border-slate-200 rounded-lg p-3 space-y-2 bg-white">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-[11px] text-slate-600 shrink-0">标准维度</span>
                        <select
                          value={entry.standardKey}
                          disabled={busy}
                          onChange={(e) => {
                            const sk = e.target.value;
                            setGoldenSamples((prev) => prev.map((x) => (x.id === entry.id ? { ...x, standardKey: sk } : x)));
                          }}
                          className="text-[11px] px-2 py-1.5 rounded-lg bg-white border border-slate-200 text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500 min-w-[8rem]"
                        >
                          {initialStandardFields.map((f) => (
                            <option key={f.standardKey} value={f.standardKey}>
                              {f.standardName} ({f.standardKey})
                            </option>
                          ))}
                        </select>
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() => setGoldenSamples((prev) => prev.filter((x) => x.id !== entry.id))}
                          className="ml-auto text-[11px] px-2 py-1 rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-50"
                          title="删除该样本组"
                        >
                          删除整组
                        </button>
                      </div>
                      {entry.segments.map((seg, segIdx) => {
                        const colsRaw = seg.tableName ? parsedTableMap[seg.tableName] || [] : [];
                        const cols = sortDdlColumnsForGolden(colsRaw, masterTable, seg.tableName);
                        return (
                          <div key={`${entry.id}_seg_${segIdx}`} className="grid grid-cols-12 gap-2 items-center">
                            <select
                              value={seg.tableName}
                              disabled={busy}
                              onChange={(e) => {
                                const nextTable = e.target.value;
                                setGoldenSamples((prev) =>
                                  prev.map((x) => {
                                    if (x.id !== entry.id) return x;
                                    const nextSegs = x.segments.map((s, i) =>
                                      i === segIdx ? { ...s, tableName: nextTable, fieldName: '' } : s
                                    );
                                    return { ...x, segments: nextSegs };
                                  })
                                );
                              }}
                              className="col-span-12 lg:col-span-3 text-[11px] px-2 py-2 rounded-lg bg-white border border-slate-200 text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                            >
                              <option value="">选择表名…</option>
                              {ddlTableNames.map((t) => (
                                <option key={t} value={t}>
                                  {t}
                                </option>
                              ))}
                            </select>
                            <select
                              value={seg.fieldName}
                              disabled={!seg.tableName || busy}
                              onChange={(e) => {
                                const nextField = e.target.value;
                                setGoldenSamples((prev) =>
                                  prev.map((x) => {
                                    if (x.id !== entry.id) return x;
                                    const nextSegs = x.segments.map((s, i) =>
                                      i === segIdx ? { ...s, fieldName: nextField } : s
                                    );
                                    return { ...x, segments: nextSegs };
                                  })
                                );
                              }}
                              className="col-span-12 lg:col-span-4 text-[11px] px-2 py-2 rounded-lg bg-white border border-slate-200 text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:bg-slate-50"
                            >
                              <option value="">
                                {seg.tableName && cols.length === 0 ? '请先解析 SQL 或等待解析...' : '选择字段…'}
                              </option>
                              {cols.map((c) => {
                                const hay = `${c.name || ''} ${(c.comment || '')}`.toLowerCase();
                                const isHot =
                                  masterTable &&
                                  seg.tableName === masterTable &&
                                  (hay.includes('款号') ||
                                    hay.includes('style') ||
                                    hay.includes('style_wms') ||
                                    hay.includes('品牌') ||
                                    hay.includes('brand') ||
                                    hay.includes('状态') ||
                                    hay.includes('status') ||
                                    hay.includes('data_status'));
                                return (
                                  <option key={c.name} value={c.name}>
                                    {isHot ? `★ ${c.name}` : c.name}
                                    {c.comment ? ` (${c.comment})` : ''}
                                  </option>
                                );
                              })}
                            </select>
                            <input
                              value={seg.value}
                              disabled={busy}
                              onChange={(e) => {
                                const v = e.target.value;
                                setGoldenSamples((prev) =>
                                  prev.map((x) => {
                                    if (x.id !== entry.id) return x;
                                    const nextSegs = x.segments.map((s, i) => (i === segIdx ? { ...s, value: v } : s));
                                    return { ...x, segments: nextSegs };
                                  })
                                );
                              }}
                              placeholder="该段样本值（如 LT / 04）"
                              className="col-span-12 lg:col-span-3 text-[11px] border border-slate-200 rounded-lg px-2 py-2 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                            />
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() =>
                                setGoldenSamples((prev) =>
                                  prev.map((x) => {
                                    if (x.id !== entry.id) return x;
                                    return {
                                      ...x,
                                      segments: [
                                        ...x.segments.slice(0, segIdx + 1),
                                        { tableName: '', fieldName: '', value: '' },
                                        ...x.segments.slice(segIdx + 1),
                                      ],
                                    };
                                  })
                                )
                              }
                              className="col-span-6 sm:col-span-6 lg:col-span-1 text-[11px] px-2 py-2 rounded-lg bg-indigo-50 border border-indigo-200 text-indigo-800 hover:bg-indigo-100"
                              title="为当前标准维度追加一段物理字段（拼接）"
                            >
                              + 组合
                            </button>
                            <button
                              type="button"
                              disabled={busy || entry.segments.length <= 1}
                              onClick={() =>
                                setGoldenSamples((prev) =>
                                  prev.map((x) => {
                                    if (x.id !== entry.id) return x;
                                    if (x.segments.length <= 1) return x;
                                    return { ...x, segments: x.segments.filter((_, i) => i !== segIdx) };
                                  })
                                )
                              }
                              className="col-span-6 sm:col-span-6 lg:col-span-1 text-[11px] px-2 py-2 rounded-lg bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 disabled:opacity-40"
                              title="移除此段"
                            >
                              删段
                            </button>
                          </div>
                        );
                      })}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Step 3: AI 逻辑建模 */}
        <div className="bg-slate-950 border border-slate-900 rounded-xl shadow-sm overflow-hidden">
          <div className="p-3">
            <button
              type="button"
              disabled={busy}
              onClick={async () => {
                setHasAttemptedAuth(true);
                setError(null);
                if (!ddlText.trim()) return setError('请先粘贴 SQL/DDL');
                if (!masterTable.trim()) return setError('请先选择业务主表（Master Table）');
                const ai = await handleAiLogicModeling();
                if (ai.ok === false) return;
                // 分析后：严禁清空输入；仅做一次草稿落盘（可选但强烈建议）
                await saveSchemaDraft();
              }}
              className="w-full py-4 rounded-xl font-semibold tracking-wide shadow-sm bg-white text-slate-950 hover:bg-slate-100 disabled:opacity-60"
              title="SQL + 动态样本配置 → Gemini 推导主表 Join 链路 → 回填 7 维结果"
            >
              <div className="flex items-center justify-center gap-3">
                <Sparkles className="w-5 h-5" />
                <span className="text-base lg:text-lg">Step 3 · 执行 AI 逻辑建模</span>
              </div>
              <div className="mt-1 text-[11px] text-slate-600">SQL → 动态样本配置 → AI 链路推导 → 下方检查与应用</div>
            </button>

            {(isAiModeling || aiStatusText || aiQueueHint || aiBusy503) && (
              <div className="mt-2 text-[11px] text-slate-200">
                {isAiModeling ? (aiStatusText || 'AI 正在解析并建模…') : null}
                {aiProgress.total > 0 && isAiModeling && (
                  <div className="mt-1 flex items-center gap-2">
                    <div className="flex-1 h-2 rounded bg-slate-800 overflow-hidden">
                      <div
                        className="h-2 bg-indigo-500"
                        style={{ width: `${Math.min(100, Math.round((aiProgress.done / Math.max(1, aiProgress.total)) * 100))}%` }}
                      />
                    </div>
                    <div className="text-[10px] font-mono text-slate-300">
                      {Math.min(aiProgress.done + 1, aiProgress.total)}/{aiProgress.total}
                    </div>
                  </div>
                )}
                {(aiBusy503 || aiQueueHint) && !isAiModeling && <div className="text-amber-200">AI 曾排队或繁忙；可再次点击重试。</div>}
              </div>
            )}
          </div>
        </div>

        {/* Step 4: 检查与应用（整页随内容增高，由 Layout 主栏滚动） */}
        <div className="grid grid-cols-12 gap-2 w-full">
          <div className="col-span-12 lg:col-span-3 flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
            <div className="px-3 py-2 bg-slate-900 text-white">
              <div className="text-sm font-semibold">Step 4 · 结果维度</div>
              <div className="text-[11px] text-slate-300">直连/Join 路径回填后在此呈现</div>
            </div>
            <div className="p-2 space-y-2">
              {mappingPreview.map((m) => {
                const isActive = m.standardKey === activeFieldKey;
                const token = String(m.physicalColumn || '');
                const has = mappingEntryIsPublishable(m);
                const isJoin = token.startsWith('CHAIN|');
                const isConcat = m.operator === 'CONCAT' || token.startsWith('CONCAT|');
                return (
                  <button
                    key={m.standardKey}
                    type="button"
                    onClick={() => setActiveFieldKey(m.standardKey)}
                    className={cn(
                      "w-full text-left rounded-xl border px-3 py-2 transition-all",
                      isActive ? "border-indigo-500 ring-2 ring-indigo-100" : "border-slate-200 hover:bg-slate-50",
                      !has ? "bg-slate-50" : isConcat ? "bg-violet-50/50" : isJoin ? "bg-amber-50/40" : "bg-emerald-50/40"
                    )}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-slate-900">{m.standardName}</div>
                      <span className="text-[10px] font-mono text-slate-500 bg-white border border-slate-200 px-1.5 py-0.5 rounded">
                        {m.standardKey}
                      </span>
                    </div>
                    <div className="mt-1 text-[11px]">
                      {!has ? (
                        <span className="text-slate-400">未回填</span>
                      ) : isConcat ? (
                        <span className="text-violet-900 font-medium">拼接 CONCAT</span>
                      ) : isJoin ? (
                        <span className="text-amber-800 font-medium">Join 路径</span>
                      ) : (
                        <span className="text-emerald-800 font-medium">直连</span>
                      )}
                    </div>
                    {has && (
                      <div className="mt-1 text-[10px] font-mono text-slate-600 break-all">
                        {isConcat
                          ? (m.parts || []).map((p) => p.sourceField || p.physicalColumn).join(' + ')
                          : isJoin
                            ? chainPreviewForStandardKey(m.standardKey)
                            : token}
                      </div>
                    )}
                  </button>
                );
              })}
            </div>
          </div>

          <div className="col-span-12 lg:col-span-9 flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
            <div className="px-3 py-2 border-b border-slate-200 bg-slate-50/50 flex flex-wrap items-center justify-between gap-2">
              <div className="min-w-0">
                <div className="text-sm font-semibold text-slate-900">预览校验与纠偏</div>
                <div className="text-[11px] text-slate-500 truncate">
                  当前维度：<span className="font-semibold text-slate-800">{activeField.standardName}</span>
                  <span className="mx-2 text-slate-300">|</span>
                  预览来源：<span className="font-mono">{resolvedMeta?.latestDir || latestDir || '-'}</span>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => setConcatPickNext((v) => !v)}
                  disabled={busy}
                  className={cn(
                    'px-3 py-1.5 text-[11px] font-semibold rounded-lg border disabled:opacity-50',
                    concatPickNext
                      ? 'bg-amber-100 border-amber-300 text-amber-950'
                      : 'bg-white border-slate-200 text-slate-700 hover:bg-slate-50'
                  )}
                  title="开启后，下一次在下方点击物理列将追加为当前维度的拼接段（CONCAT），而非覆盖"
                >
                  {concatPickNext ? '拼接选列：开' : '拼接选列：关'}
                </button>
                <button
                  type="button"
                  onClick={() => void handleAuthAndSync()}
                  disabled={busy}
                  className="px-3 py-1.5 text-[11px] font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                  title="认证并应用到看板（写入 mapping_config 并触发看板重算）"
                >
                  {authSyncing ? '发布中…' : '认证并应用到看板'}
                </button>
              </div>
            </div>

            {hasAttemptedAuth && error && (
              <div className="m-3 text-[11px] text-red-700 bg-red-50 border border-red-200 px-3 py-2 rounded-lg">{error}</div>
            )}
            {authSyncHint && (
              <div className="mx-3 mt-3 text-[11px] text-indigo-700 bg-indigo-50 border border-indigo-200 px-3 py-2 rounded-lg">
                {authSyncHint}
              </div>
            )}
            {resolvedMeta?.warning && (
              <div className="mx-3 mt-3 text-[11px] text-amber-800 bg-amber-50 border border-amber-200 px-3 py-2 rounded-lg">
                {resolvedMeta.warning}
              </div>
            )}

            <div className="p-3 space-y-3">
              {/* 1) 当前维度的链路/直连信息 */}
              <div className="border border-slate-200 rounded-xl overflow-hidden">
                <div className="px-3 py-2 bg-slate-900 text-white text-xs font-semibold flex items-center justify-between">
                  <span>AI 回填结果</span>
                  <span className="text-[10px] text-slate-300 font-mono">{activeField.standardKey}</span>
                </div>
                <div className="p-3 bg-white space-y-2">
                  <div className="text-[11px] text-slate-600">
                    类型：{' '}
                    {activeMapping?.operator === 'CONCAT' || activeToken.startsWith('CONCAT|') ? (
                      <span className="font-semibold text-violet-900">多段拼接 CONCAT</span>
                    ) : activeToken.startsWith('CHAIN|') ? (
                      <span className="font-semibold text-amber-800">Join 路径</span>
                    ) : activeToken ? (
                      <span className="font-semibold text-emerald-800">直连</span>
                    ) : (
                      <span className="font-semibold text-slate-500">未回填</span>
                    )}
                  </div>
                  {activeMapping?.operator === 'CONCAT' && activeMapping.parts?.length ? (
                    <div className="text-[12px] font-mono text-slate-900 space-y-1">
                      {activeMapping.parts.map((p, i) => (
                        <div key={`${p.sourceField}_${i}`}>
                          {i + 1}. {p.joinPath?.length ? `CHAIN(${p.joinPath.join(' → ')})` : `${p.sourceField}@${p.sourceTable}`}
                        </div>
                      ))}
                    </div>
                  ) : activeToken.startsWith('CHAIN|') ? (
                    <div className="text-[12px] text-slate-800">{chainPreviewForStandardKey(activeFieldKey)}</div>
                  ) : activeToken ? (
                    <div className="text-[12px] font-mono text-slate-900">{activeToken}</div>
                  ) : null}

                  <div className="text-[11px] text-slate-600">
                    真实值预览：{' '}
                    {resolvingRow ? (
                      <span className="text-slate-400">抓取中…</span>
                    ) : (
                      <span className="font-mono text-slate-900">{resolvedValueForActive || '—'}</span>
                    )}
                  </div>
                </div>
              </div>

              {/* 2) 纠偏：选表/字段（直连纠偏） */}
              <div className="border border-slate-200 rounded-xl overflow-hidden">
                <div className="px-3 py-2 bg-slate-900 text-white text-xs font-semibold flex items-center justify-between gap-2">
                  <span>纠偏（手动选择表/字段）</span>
                  <span className="text-[10px] text-slate-300">点击字段：立即替换当前维度映射</span>
                </div>
                <div className="p-3 bg-white space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="text-[11px] text-slate-600">搜索字段</div>
                    <input
                      value={headerSearch}
                      onChange={(e) => setHeaderSearch(e.target.value)}
                      placeholder="输入列名关键词…"
                      className="text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 w-full max-w-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      disabled={busy}
                    />
                  </div>
                  <div className="grid grid-cols-12 gap-2">
                    <div className="col-span-12 xl:col-span-4 border border-slate-200 rounded-lg bg-white overflow-hidden">
                      <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 bg-slate-50">
                        表（{Object.keys(tables || {}).length}）
                      </div>
                      <div className="max-h-[240px] overflow-y-auto p-2 space-y-1">
                        {Object.keys(tables || {}).length ? (
                          Object.keys(tables || {}).map((fileName) => (
                            <button
                              key={fileName}
                              type="button"
                              disabled={busy}
                              onClick={() => {
                                setSelectedFile(fileName);
                                void loadSamples(fileName);
                              }}
                              className={cn(
                                'w-full text-left px-2 py-1.5 rounded-md border text-[11px] font-mono',
                                selectedFile === fileName ? 'border-indigo-400 bg-indigo-50' : 'border-slate-200 hover:bg-slate-50'
                              )}
                            >
                              {fileName}
                            </button>
                          ))
                        ) : (
                          <div className="text-[11px] text-slate-500 p-2">暂无生产表头（如果你刚启动服务，请稍等或刷新页面）。</div>
                        )}
                      </div>
                    </div>
                    <div className="col-span-12 xl:col-span-8 border border-slate-200 rounded-lg bg-white overflow-hidden">
                      <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 bg-slate-50 flex items-center justify-between">
                        <span>字段（{selectedFile ? (tables?.[selectedFile]?.length || 0) : 0}）</span>
                        <span className="text-slate-400">当前表：{selectedFile || '-'}</span>
                      </div>
                      <div className="max-h-[240px] overflow-y-auto p-2">
                        {selectedFile ? (
                          <div className="flex flex-wrap gap-1.5">
                            {filteredHeaders.map((c) => (
                              <button
                                key={`${selectedFile}-${c}`}
                                type="button"
                                disabled={busy}
                                onClick={() => {
                                  assignPhysicalColumn(c, selectedFile);
                                  void loadSamples(selectedFile);
                                }}
                                className="px-2 py-1 text-[11px] font-mono rounded-md border shadow-sm transition-colors bg-white border-slate-200 hover:border-indigo-300 hover:bg-indigo-50 disabled:opacity-50"
                                title={`${c}（来自 ${selectedFile}）`}
                              >
                                <span>{c}</span>
                                <span className="ml-1 text-[10px] text-slate-400">(来自 {selectedFile})</span>
                              </button>
                            ))}
                          </div>
                        ) : (
                          <div className="text-[11px] text-slate-500 p-2">先选择一个表，再选择字段。</div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* 3) 底层参考（DDL comment 命中） */}
              <div className="border border-slate-200 rounded-xl overflow-hidden bg-slate-50/40">
                <button
                  type="button"
                  onClick={() => setDdlRefOpen((o) => !o)}
                  className="w-full px-3 py-2 flex items-center justify-between gap-2 text-left hover:bg-slate-100/80 transition-colors"
                  disabled={busy}
                >
                  <div className="text-xs font-semibold text-slate-800">底层参考（DDL COMMENT 命中）</div>
                  {ddlRefOpen ? <ChevronUp className="w-4 h-4 text-slate-500 shrink-0" /> : <ChevronDown className="w-4 h-4 text-slate-500 shrink-0" />}
                </button>
                {ddlRefOpen && (
                  <div className="p-3 pt-0 space-y-3 border-t border-slate-200 bg-white">
                    {ddlParsed.length > 0 ? (
                      <div className="border border-slate-200 rounded-lg bg-white">
                        <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 flex items-center justify-between">
                          <span>
                            DDL 列（含 COMMENT）：{ddlParsed.filter((c) => c.comment).length}/{ddlParsed.length}
                          </span>
                          <span className="text-slate-400">命中：{activeDdlMatches.size}</span>
                        </div>
                        <div className="max-h-40 overflow-y-auto px-3 py-2 text-[11px] font-mono">
                          {ddlParsed.slice(0, 120).map((c, idx) => {
                            const hit = activeDdlMatches.has(normalize(c.column));
                            return (
                              <div
                                key={`${c.table || ''}-${c.column}-${idx}`}
                                className={cn('flex gap-2 py-0.5', hit && 'bg-indigo-50 border border-indigo-200 rounded px-2')}
                              >
                                <span className="text-slate-600">
                                  {c.table ? `${c.table}.` : ''}
                                  {c.column}
                                </span>
                                {c.comment && <span className={cn(hit ? 'text-indigo-800' : 'text-emerald-700')}>#{c.comment}</span>}
                              </div>
                            );
                          })}
                          {ddlParsed.length > 120 && <div className="text-slate-400 py-1">… 共 {ddlParsed.length} 列</div>}
                        </div>
                      </div>
                    ) : (
                      <div className="text-[11px] text-slate-500">尚未解析 DDL。请先执行 AI 建模。</div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
