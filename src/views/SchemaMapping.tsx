import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AlertCircle, CheckCircle2, ChevronDown, ChevronUp, Database, Link as LinkIcon, Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';
import { GlobalSchemaField } from '@/types';

const API_BASE = 'http://localhost:3001';
const REQUIRED_MASTER_TABLE = 'ods_pdm_pdm_product_info_df';

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

type MappingEntry = {
  standardKey: string;
  standardName: string;
  physicalColumn?: string;
  sourceTable?: string;
  sourceField?: string;
  joinPath?: string[];
};

/** 写入 mapping_config 的完整条目（含 joinPath） */
function buildCertifiedMapping(fields: GlobalSchemaField[]): MappingEntry[] {
  return fields.map((f) => {
    const token = f.mappedSources?.[0] || '';
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

type DdlColumn = {
  table?: string;
  column: string;
  comment?: string;
};

type AiTable = { tableName: string; columns: Array<{ name: string; comment?: string }> };
type AiSuggestion = { standardKey: string; sourceField: string; sourceTable: string };
type AiJoinPath = { targetStandardKey: string; path: string[] };

function normalize(s: string) {
  return (s || '').trim().toLowerCase();
}

const SANDBOX_DIM_KEYS = ['styleCode', 'brand', 'lastCode', 'soleCode', 'colorCode', 'materialCode', 'status'] as const;
type SandboxDimKey = (typeof SANDBOX_DIM_KEYS)[number];

const SANDBOX_DIM_LABEL: Record<SandboxDimKey, string> = {
  styleCode: '款号',
  brand: '品牌',
  lastCode: '楦编号',
  soleCode: '大底编号',
  colorCode: '颜色',
  materialCode: '材质',
  status: '状态',
};

type SchemaDraftGolden = {
  styleCode?: string;
  brand?: string;
  lastCode?: string;
  soleCode?: string;
  colorCode?: string;
  materialCode?: string;
  status?: string;
};

type SandboxUploadedFile = {
  originalName: string;
  storedName?: string;
  headers?: string[];
  firstRow?: Record<string, string>;
  error?: string;
};

export default function SchemaMapping() {
  const [standardFields, setStandardFields] = useState<GlobalSchemaField[]>(initialStandardFields);
  const [activeFieldKey, setActiveFieldKey] = useState<string>(initialStandardFields[0].standardKey);

  const [ddlText, setDdlText] = useState('');
  const [ddlParsed, setDdlParsed] = useState<DdlColumn[]>([]);
  const [aiTables, setAiTables] = useState<AiTable[]>([]);
  const [aiSuggestions, setAiSuggestions] = useState<AiSuggestion[]>([]);
  const [aiJoinPaths, setAiJoinPaths] = useState<AiJoinPath[]>([]);
  const [activeAiTable, setActiveAiTable] = useState<string>('');
  const [activeAiField, setActiveAiField] = useState<{ tableName: string; fieldName: string } | null>(null);
  const [isAiModeling, setIsAiModeling] = useState(false);
  const [referenceStyleCode, setReferenceStyleCode] = useState('');
  const [referenceBrand, setReferenceBrand] = useState('');
  const [referenceLastCode, setReferenceLastCode] = useState('');
  const [referenceSoleCode, setReferenceSoleCode] = useState('');
  const [referenceColorCode, setReferenceColorCode] = useState('');
  const [referenceMaterialCode, setReferenceMaterialCode] = useState('');
  const [referenceStatus, setReferenceStatus] = useState('');
  const [masterTable, setMasterTable] = useState('');
  const [aiReportOpen, setAiReportOpen] = useState(false);
  const [aiBusy503, setAiBusy503] = useState(false);
  const [aiQueueHint, setAiQueueHint] = useState(false);
  const [aiStatusText, setAiStatusText] = useState('');
  const [aiProgress, setAiProgress] = useState<{ done: number; total: number; tables: string[] }>({ done: 0, total: 0, tables: [] });

  const [tables, setTables] = useState<Record<string, string[]>>({});
  const [latestDir, setLatestDir] = useState<string>('');
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [samples, setSamples] = useState<Record<string, string[]>>({});
  const [headerSearch, setHeaderSearch] = useState('');

  const [isSuccess, setIsSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /** 沙盒样本：上传文件清单（不做跨表合并，避免命名冲突） */
  const [sandboxFiles, setSandboxFiles] = useState<SandboxUploadedFile[]>([]);
  const [activeSandboxFileIdx, setActiveSandboxFileIdx] = useState(0);
  const [sandboxUploadHint, setSandboxUploadHint] = useState<string>('');
  const [sandboxChecks, setSandboxChecks] = useState<Record<string, { expected: string; actual: string; pass: boolean }> | null>(null);
  const [sandboxAllPass, setSandboxAllPass] = useState<boolean | null>(null);
  const [sandboxValidating, setSandboxValidating] = useState(false);
  const [sandboxDetailOpen, setSandboxDetailOpen] = useState(false);
  const [referenceDataOpen, setReferenceDataOpen] = useState(false);
  const [isParsingFiles, setIsParsingFiles] = useState(false);

  const [authSyncing, setAuthSyncing] = useState(false);
  const [authSyncHint, setAuthSyncHint] = useState<string>('');
  const authSyncHintTimerRef = useRef<number | null>(null);

  const ddlTableNames = useMemo(() => {
    const text = String(ddlText || '');
    const re = /create\s+table\s+([`"]?)([\w.]+)\1/gi;
    const out: string[] = [];
    const seen = new Set<string>();
    let m: RegExpExecArray | null = null;
    while ((m = re.exec(text))) {
      const name = String(m[2] || '').trim();
      if (!name) continue;
      const k = name.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(name);
    }
    return out;
  }, [ddlText]);

  const mappingPreview: MappingEntry[] = useMemo(() => buildCertifiedMapping(standardFields), [standardFields]);

  const normalizeTableName = (s: string) => String(s || '').trim().toLowerCase().replace(/\.xlsx$/i, '');

  const masterTableErrors = useMemo(() => {
    const errors: string[] = [];
    if (!masterTable) {
      errors.push('错误：请先指定业务主表（Master Table）。');
      return errors;
    }
    // 若 DDL 中包含固定主表名，则必须选择该主表
    const ddlHasRequired = ddlTableNames.some((t) => normalizeTableName(t) === normalizeTableName(REQUIRED_MASTER_TABLE));
    if (ddlHasRequired && normalizeTableName(masterTable) !== normalizeTableName(REQUIRED_MASTER_TABLE)) {
      errors.push(`错误：当前 DDL 检测到主表 ${REQUIRED_MASTER_TABLE}，请将其指定为业务主表。`);
      return errors;
    }
    const required: Array<{ key: string; label: string }> = [
      { key: 'styleCode', label: '款号' },
      { key: 'brand', label: '品牌' },
      { key: 'status', label: '状态' },
    ];
    for (const r of required) {
      const m = mappingPreview.find((x) => x.standardKey === r.key);
      if (!m?.physicalColumn) {
        errors.push(`错误：主表 ${masterTable} 尚未映射【${r.label}】字段。`);
        continue;
      }
      if (String(m.physicalColumn).startsWith('CHAIN|')) {
        errors.push(`错误：主表 ${masterTable} 的【${r.label}】必须直接映射主表列（不支持 CHAIN 链路）。`);
        continue;
      }
      const src = String(m.sourceTable || '');
      if (!src) {
        errors.push(`错误：主表 ${masterTable} 尚未映射【${r.label}】字段。`);
        continue;
      }
      if (normalizeTableName(src) !== normalizeTableName(masterTable)) {
        errors.push(`错误：主表 ${masterTable} 的【${r.label}】目前映射在 ${src}，必须映射到主表。`);
      }
    }
    return errors;
  }, [ddlTableNames, masterTable, mappingPreview]);

  const saveSchemaDraft = useCallback(async () => {
    try {
      // Golden Record（按产品要求：5 维度）
      const goldenRecord: SchemaDraftGolden = {
        styleCode: referenceStyleCode,
        lastCode: referenceLastCode,
        soleCode: referenceSoleCode,
        colorCode: referenceColorCode,
        materialCode: referenceMaterialCode,
      };

      const data = { ddlText, goldenRecord, masterTable };
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
  }, [ddlText, masterTable, referenceColorCode, referenceLastCode, referenceMaterialCode, referenceSoleCode, referenceStyleCode]);

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
        const g = d.goldenRecord;
        if (g && typeof g === 'object' && !Array.isArray(g)) {
          const gr = g as SchemaDraftGolden;
          if (typeof gr.styleCode === 'string') setReferenceStyleCode(gr.styleCode);
          if (typeof gr.lastCode === 'string') setReferenceLastCode(gr.lastCode);
          if (typeof gr.soleCode === 'string') setReferenceSoleCode(gr.soleCode);
          if (typeof gr.colorCode === 'string') setReferenceColorCode(gr.colorCode);
          if (typeof gr.materialCode === 'string') setReferenceMaterialCode(gr.materialCode);
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
      (m) => m.physicalColumn && !String(m.physicalColumn).startsWith('CHAIN|')
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

  /** 将 AI 返回的直连字段 + Join 链路一次性写入标准字段（Join 优先覆盖同 key） */
  const applyAiResultsToFields = (sug: AiSuggestion[], jp: AiJoinPath[]) => {
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
      const activeSandboxFile = sandboxFiles[activeSandboxFileIdx];
      const sampleRow =
        activeSandboxFile?.firstRow && Object.keys(activeSandboxFile.firstRow).length > 0
          ? activeSandboxFile.firstRow
          : undefined;
      const resp = await fetch('/api/ai-parse-multi-sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          sqlText: ddlText,
          sampleRow: sampleRow || undefined,
          masterTable: masterTable || undefined,
          reference: referenceStyleCode.trim()
            ? {
                styleCode: referenceStyleCode.trim(),
                ...(referenceBrand.trim() ? { brand: referenceBrand.trim() } : {}),
                ...(referenceLastCode.trim() ? { lastCode: referenceLastCode.trim() } : {}),
                ...(referenceSoleCode.trim() ? { soleCode: referenceSoleCode.trim() } : {}),
                ...(referenceColorCode.trim() ? { colorCode: referenceColorCode.trim() } : {}),
                ...(referenceMaterialCode.trim() ? { materialCode: referenceMaterialCode.trim() } : {}),
                ...(referenceStatus.trim() ? { status: referenceStatus.trim() } : {}),
              }
            : null,
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
      const json = JSON.parse(text) as { ok: boolean; tables?: AiTable[]; smartSuggestions?: AiSuggestion[]; joinPathSuggestions?: AiJoinPath[]; error?: string; _debug?: any };
      // eslint-disable-next-line no-console
      console.log('[ai-parse-multi-sql] parsed:', json);
      if (!json.ok) throw new Error(json.error || 'AI 多表解析失败');

      const parsedTables = (json.tables || []).filter((t) => t?.tableName);
      const sug = json.smartSuggestions || [];
      const jp = json.joinPathSuggestions || [];
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
      applyAiResultsToFields(sug, jp);
      setAiReportOpen(true);
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
    setStandardFields((prev) =>
      prev.map((f) => {
        if (f.standardKey !== activeFieldKey) return f;
        return {
          ...f,
          // 简化：单选映射，保存为 "col@file"（兼容 mappedSources: string[]）
          mappedSources: [`${physicalColumn}@${sourceFile}`],
        };
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
        sandboxValidatedAt: sandboxAllPass ? new Date().toISOString() : undefined,
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

  const handleSandboxUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    setError(null);
    const files = e.target.files;
    if (!files?.length) return;
    const all = Array.from(files) as File[];
    const fd = new FormData();
    for (const f of all) fd.append('files', f);
    try {
      setIsParsingFiles(true);
      const resp = await fetch('/api/upload-sandbox-xlsx', { method: 'POST', body: fd });
      if (!resp.ok) throw new Error(`沙盒上传失败（HTTP ${resp.status}）`);
      const json = (await resp.json()) as { ok: boolean; files?: SandboxUploadedFile[] };
      if (!json.ok) throw new Error('沙盒上传失败');
      const normalized = (json.files || []).map((f) => ({
        originalName: String((f as any).originalName || (f as any).storedName || 'unknown.xlsx'),
        storedName: typeof (f as any).storedName === 'string' ? (f as any).storedName : undefined,
        headers: Array.isArray((f as any).headers) ? (f as any).headers.map(String) : undefined,
        firstRow:
          (f as any).firstRow && typeof (f as any).firstRow === 'object' && !Array.isArray((f as any).firstRow)
            ? Object.fromEntries(Object.entries((f as any).firstRow).map(([k, v]) => [String(k), String(v ?? '')]))
            : undefined,
        error: typeof (f as any).error === 'string' ? (f as any).error : undefined,
      }));
      setSandboxFiles(normalized);
      setActiveSandboxFileIdx(0);
      setSandboxUploadHint(`已上传 ${normalized.length} 个样本文件（不合并，按表分组）`);
      setSandboxChecks(null);
      setSandboxAllPass(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '沙盒上传失败');
    } finally {
      setIsParsingFiles(false);
    }
    e.target.value = '';
  };

  const handleSandboxValidate = async (): Promise<{ ok: true; allPass: boolean } | { ok: false; error: string }> => {
    setError(null);
    setSandboxValidating(true);
    setSandboxChecks(null);
    setSandboxAllPass(null);
    try {
      if (!masterTable) throw new Error('请先指定业务主表（Master Table）');
      const styleMap = mappingPreview.find((m) => m.standardKey === 'styleCode');
      if (!styleMap?.physicalColumn || String(styleMap.physicalColumn).startsWith('CHAIN|')) {
        throw new Error('沙盒校验前请先将【款号】直接映射到主表列');
      }
      const styleCol = String(styleMap.physicalColumn);
      const sandboxMaster = sandboxFiles.find((f) => normalizeTableName(f.originalName) === normalizeTableName(masterTable) || f.originalName.includes(masterTable));
      if (sandboxFiles.length > 0 && !sandboxMaster) {
        throw new Error(`沙盒样本中未找到主表文件：${masterTable}（请确保样本文件名包含主表名）`);
      }
      if (sandboxMaster) {
        const headers = sandboxMaster.headers || Object.keys(sandboxMaster.firstRow || {});
        if (!headers.some((h) => normalize(h) === normalize(styleCol))) {
          throw new Error(`沙盒主表 ${sandboxMaster.originalName} 缺少款号列：${styleCol}`);
        }
      }

      const mapping = buildCertifiedMapping(standardFields);
      const resp = await fetch('/api/sandbox-validate-mapping', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mapping,
          expected: {
            styleCode: referenceStyleCode.trim(),
            brand: referenceBrand.trim(),
            lastCode: referenceLastCode.trim(),
            soleCode: referenceSoleCode.trim(),
            colorCode: referenceColorCode.trim(),
            materialCode: referenceMaterialCode.trim(),
            status: referenceStatus.trim(),
          },
        }),
      });
      const json = (await resp.json()) as {
        ok: boolean;
        checks?: Record<string, { expected: string; actual: string; pass: boolean }>;
        allPass?: boolean;
        error?: string;
        resolvedRow?: Record<string, string>;
      };
      if (!json.ok) throw new Error(json.error || '沙盒校验失败');
      setSandboxChecks(json.checks || null);
      setSandboxAllPass(Boolean(json.allPass));
      // eslint-disable-next-line no-console
      console.log('[sandbox-validate]', json);
      return { ok: true, allPass: Boolean(json.allPass) };
    } catch (err) {
      const msg = err instanceof Error ? err.message : '沙盒校验失败';
      setError(msg);
      return { ok: false, error: msg };
    } finally {
      setSandboxValidating(false);
    }
  };

  const handleAuthAndSync = async () => {
    if (authSyncing) return;
    setError(null);
    setAuthSyncHint('');
    if (masterTableErrors.length > 0) {
      setError(masterTableErrors[0]);
      return;
    }
    if (!ddlText.trim()) {
      setError('请先粘贴 DDL/SQL');
      return;
    }
    if (!referenceStyleCode.trim()) {
      setError('请先填写黄金样本：款号（styleCode）');
      return;
    }
    if (sandboxFiles.length === 0) {
      setError('请先上传样本 XLSX（用于自动沙盒验证）');
      return;
    }

    setAuthSyncing(true);
    try {
      setAuthSyncHint('AI 解析中…');
      const ai = await handleAiLogicModeling();
      if (ai.ok === false) throw new Error(ai.error);

      setAuthSyncHint('沙盒验证中…');
      const sb = await handleSandboxValidate();
      if (sb.ok === false) throw new Error(sb.error);
      if (!sb.allPass) throw new Error('沙盒验证未通过：请检查黄金样本或映射链路');

      setAuthSyncHint('保存配置中…');
      const savedMapping = await handleSaveMappingAuthenticated();
      if (savedMapping.ok === false) throw new Error(savedMapping.error);

      const draft = await saveSchemaDraft();
      if (draft.ok === false) throw new Error(draft.error);

      setAuthSyncHint('逻辑已认证！看板数据已根据最新映射完成重算。');
      if (authSyncHintTimerRef.current) window.clearTimeout(authSyncHintTimerRef.current);
      authSyncHintTimerRef.current = window.setTimeout(() => setAuthSyncHint(''), 6000);
    } catch (e) {
      setError(e instanceof Error ? e.message : '一键认证失败');
    } finally {
      setAuthSyncing(false);
    }
  };

  const sandboxPassSummary = useMemo(() => {
    if (!sandboxChecks) return null;
    const passed = SANDBOX_DIM_KEYS.filter((k) => sandboxChecks[k]?.pass).length;
    return { passed, total: SANDBOX_DIM_KEYS.length };
  }, [sandboxChecks]);

  return (
    <>
    <div className="grid grid-cols-12 gap-2 w-full px-2 lg:px-4 h-[calc(100vh-8rem)]">
      {/* Left: col-span-2 */}
      <div className="col-span-12 lg:col-span-2 min-h-0 flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm">
        <div className="px-3 py-2 border-b border-slate-200 bg-slate-50/50">
          <div className="text-sm font-semibold text-slate-900">标准字段</div>
          <div className="text-[11px] text-slate-500 mt-0.5">点击后开始映射</div>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-1 min-h-0">
          {standardFields.map((field) => {
            const isActive = field.standardKey === activeFieldKey;
            const warn = hasUnmappedWarning(field);
            return (
              <button
                key={field.id}
                type="button"
                onClick={() => setActiveFieldKey(field.standardKey)}
                className={cn(
                  "w-full text-left px-2.5 py-2 rounded-lg border transition-all",
                  "bg-white hover:bg-slate-50",
                  isActive ? "border-indigo-500 ring-2 ring-indigo-200 shadow-sm" : "border-slate-200",
                  warn ? "border-red-300" : ""
                )}
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <Database className={cn("w-4 h-4 shrink-0", warn ? "text-red-500" : isActive ? "text-indigo-600" : "text-slate-500")} />
                    <span className={cn("text-sm font-medium truncate", isActive ? "text-slate-900" : "text-slate-800")}>
                      {field.standardName}
                    </span>
                    {warn && <AlertCircle className="w-4 h-4 text-red-500 shrink-0" />}
                  </div>
                  <span className={cn("text-[10px] font-mono px-1.5 py-0.5 rounded border shrink-0",
                    isActive ? "bg-indigo-50 text-indigo-700 border-indigo-200" : "bg-slate-50 text-slate-500 border-slate-200"
                  )}>
                    {field.standardKey}
                  </span>
                </div>
                <div className="mt-1.5">
                  {field.mappedSources.length > 0 ? (
                    <div className="flex flex-wrap gap-1">
                      {field.mappedSources.slice(0, 2).map((source, idx) => (
                        <span key={idx} className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[11px] font-medium bg-slate-900 text-white border border-slate-800">
                          <LinkIcon className="w-3 h-3" />
                          {source}
                        </span>
                      ))}
                      {field.mappedSources.length > 2 && (
                        <span className="text-[11px] text-slate-500">+{field.mappedSources.length - 2}</span>
                      )}
                    </div>
                  ) : (
                    <div className="text-[11px] text-slate-500">未映射</div>
                  )}
                  {chainPreviewForStandardKey(field.standardKey) && (
                    <div className="mt-1 text-[10px] text-slate-500">
                      {chainPreviewForStandardKey(field.standardKey)}
                    </div>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Middle: col-span-6 */}
      <div className="col-span-12 lg:col-span-6 min-h-0 flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm">
        <div className="px-3 py-2 border-b border-slate-200 bg-slate-50/50 flex items-center justify-between gap-2">
          <div className="min-w-0">
            <div className="text-sm font-semibold text-slate-900">AI Logic & Source Tables</div>
            <div className="text-[11px] text-slate-500 truncate">
              最新目录：<span className="font-mono text-slate-700">{latestDir || '-'}</span>
              <span className="mx-2 text-slate-300">|</span>
              当前槽位：<span className="font-medium text-slate-800">{activeField.standardName}</span>
            </div>
          </div>
        </div>

        {error && (
          <div className="mx-3 mt-3 text-[11px] text-red-700 bg-red-50 border border-red-200 px-3 py-2 rounded-lg">
            {error}
          </div>
        )}

        {/* Middle body independent scroll */}
        <div className="flex-1 min-h-0 overflow-y-auto p-3 space-y-3">
          <div className="border border-indigo-200 rounded-xl bg-gradient-to-b from-indigo-50/50 to-white overflow-hidden shadow-sm">
            <div className="px-3 py-2 bg-slate-900 text-white text-xs font-semibold flex items-center justify-between gap-2">
              <span>AI 学习与样本认证</span>
              {sandboxAllPass === true && (
                <span className="inline-flex items-center gap-1 text-[10px] font-normal text-emerald-300">
                  <CheckCircle2 className="w-3.5 h-3.5" /> 7 维逻辑已打通
                </span>
              )}
            </div>
            <div className="px-3 py-2 border-b border-slate-200 bg-white">
              <div className="text-[11px] font-semibold text-slate-900">第一步：指定业务主表</div>
              <div className="mt-1 flex flex-wrap items-center gap-2">
                <select
                  value={masterTable}
                  onChange={(e) => setMasterTable(e.target.value)}
                  className="text-[11px] px-2 py-1.5 rounded-lg bg-white border border-slate-200 text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                >
                  <option value="">请选择 DDL 中的表名…</option>
                  {ddlTableNames.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
                {masterTable ? (
                  <span className="text-[11px] text-slate-600">
                    已锁定 <span className="font-mono text-slate-900">{masterTable}</span> 为业务底盘
                  </span>
                ) : (
                  <span className="text-[11px] text-amber-700">未选择主表时无法认证</span>
                )}
              </div>
            </div>
            {masterTable && (
              <div className="px-3 py-2 text-[11px] bg-slate-50 border-b border-slate-200 text-slate-700">
                已锁定 <span className="font-mono text-slate-900">{masterTable}</span> 为业务底盘（Master Table）
              </div>
            )}
            <div className="p-3 grid grid-cols-12 gap-2 border-b border-slate-100">
              <input
                value={referenceStyleCode}
                onChange={(e) => setReferenceStyleCode(e.target.value)}
                placeholder="款号（styleCode）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceBrand}
                onChange={(e) => setReferenceBrand(e.target.value)}
                placeholder="品牌（brand）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceStatus}
                onChange={(e) => setReferenceStatus(e.target.value)}
                placeholder="状态（status）例如：生效"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceLastCode}
                onChange={(e) => setReferenceLastCode(e.target.value)}
                placeholder="楦号（lastCode）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceSoleCode}
                onChange={(e) => setReferenceSoleCode(e.target.value)}
                placeholder="底号（soleCode）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceColorCode}
                onChange={(e) => setReferenceColorCode(e.target.value)}
                placeholder="颜色（colorCode）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <input
                value={referenceMaterialCode}
                onChange={(e) => setReferenceMaterialCode(e.target.value)}
                placeholder="材质（materialCode）"
                className="col-span-12 sm:col-span-6 md:col-span-4 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <div className="col-span-12 text-[11px] text-slate-600">
                Golden Record 与沙盒解析结果逐项比对；样本 Excel 文件名需与生产侧逻辑表名一致。
              </div>
            </div>
            {masterTableErrors.length > 0 && (
              <div className="px-3 pb-3">
                <div className="text-[11px] text-red-700 bg-red-50 border border-red-200 px-3 py-2 rounded-lg space-y-1">
                  {masterTableErrors.map((t, idx) => (
                    <div key={idx}>{t}</div>
                  ))}
                </div>
              </div>
            )}
            <div className="p-3 space-y-2 bg-emerald-50/20">
              {sandboxFiles.length === 0 ? (
                <p className="text-[11px] text-amber-800 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
                  上传样本 Excel 以认证逻辑（可多选；不合并，避免跨表同名字段冲突）。
                </p>
              ) : (
                <div className="space-y-2">
                  <p className="text-[11px] text-emerald-900">{sandboxUploadHint || `已就绪：${sandboxFiles.length} 个样本文件`}</p>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-[11px] text-slate-600">AI 样本来源：</span>
                    <select
                      value={activeSandboxFileIdx}
                      onChange={(e) => setActiveSandboxFileIdx(Number(e.target.value) || 0)}
                      className="text-[11px] border border-slate-200 rounded-lg px-2 py-1 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    >
                      {sandboxFiles.map((f, idx) => (
                        <option key={`${f.originalName}-${idx}`} value={idx}>
                          {f.originalName}
                        </option>
                      ))}
                    </select>
                    <span className="text-[11px] text-slate-500">
                      {sandboxFiles[activeSandboxFileIdx]?.firstRow
                        ? `${Object.keys(sandboxFiles[activeSandboxFileIdx]?.firstRow || {}).length} 列样本行`
                        : '无首行样本'}
                    </span>
                  </div>
                </div>
              )}
              <div className="flex flex-wrap items-center gap-2">
                <label className="inline-flex items-center gap-2 px-3 py-1.5 text-[11px] font-medium rounded-lg bg-white border border-emerald-400 text-emerald-900 cursor-pointer hover:bg-emerald-50">
                  <input
                    type="file"
                    accept=".xlsx,.xls"
                    multiple
                    className="hidden"
                    disabled={isParsingFiles}
                    onChange={(e) => void handleSandboxUpload(e)}
                  />
                  {isParsingFiles ? '解析中…' : '选择样本 XLSX'}
                </label>
                <button
                  type="button"
                  onClick={() => void handleAuthAndSync()}
                  disabled={authSyncing || isAiModeling || sandboxValidating || isParsingFiles}
                  className="px-3 py-1.5 text-[11px] font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                  title="AI 解析 → Join 推导 → 沙盒验证 → 保存并触发看板重算"
                >
                  {authSyncing ? '认证中…' : '一键认证并同步看板 (Auth & Sync)'}
                </button>
              </div>
              {authSyncHint && (
                <div className="text-[11px] text-indigo-700 bg-indigo-50 border border-indigo-200 px-3 py-2 rounded-lg">
                  {authSyncHint}
                </div>
              )}
              {sandboxPassSummary && (
                <div className="flex flex-wrap items-center gap-3 text-[11px]">
                  <span className="font-semibold text-slate-800">
                    逻辑通过率：<span className="text-emerald-700">{sandboxPassSummary.passed}</span>/{sandboxPassSummary.total}
                  </span>
                  <button
                    type="button"
                    onClick={() => setSandboxDetailOpen(true)}
                    className="text-indigo-600 font-medium hover:underline"
                  >
                    查看测试明细
                  </button>
                </div>
              )}
            </div>
          </div>

          <div className={cn('border rounded-xl overflow-hidden', activeFieldKey ? 'border-indigo-400 ring-2 ring-indigo-100' : 'border-slate-200')}>
            <div className="px-3 py-2 bg-slate-900 text-white flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold">DDL/SQL 批量粘贴区</div>
              <div className="text-[11px] text-slate-300">当前检测到 {detectedTableCount} 张表</div>
            </div>
            <div className="p-3 grid grid-cols-1 gap-2">
              {isAiModeling && (
                <div className="text-[11px] text-indigo-700 bg-indigo-50 border border-indigo-200 px-3 py-2 rounded-lg space-y-1">
                  <div>正在解析 DDL、结合 Golden / 样本推导 Join Path…</div>
                  {aiProgress.total > 0 && (
                    <div className="flex items-center gap-2">
                      <div className="flex-1 h-2 rounded bg-indigo-100 overflow-hidden">
                        <div
                          className="h-2 bg-indigo-500"
                          style={{ width: `${Math.min(100, Math.round((aiProgress.done / Math.max(1, aiProgress.total)) * 100))}%` }}
                        />
                      </div>
                      <div className="text-[10px] font-mono text-indigo-700">
                        {Math.min(aiProgress.done + 1, aiProgress.total)}/{aiProgress.total}
                      </div>
                    </div>
                  )}
                  {aiProgress.tables?.length > 0 && (
                    <div className="text-[10px] text-indigo-700">已完成：{aiProgress.tables.join('、')}</div>
                  )}
                </div>
              )}
              {(aiBusy503 || aiQueueHint) && !isAiModeling && (
                <div className="text-[11px] text-amber-700 bg-amber-50 border border-amber-200 px-3 py-2 rounded-lg">
                  AI 曾排队或繁忙；可再次点击「AI 逻辑建模」重试。
                </div>
              )}
              {aiStatusText && isAiModeling && (
                <div className="text-[11px] text-sky-700 bg-sky-50 border border-sky-200 px-3 py-2 rounded-lg">
                  {aiStatusText}
                </div>
              )}
              <textarea
                value={ddlText}
                onChange={(e) => setDdlText(e.target.value)}
                placeholder="粘贴多表 DDL（含 COMMENT 更佳）…"
                className="w-full min-h-[220px] max-h-[360px] p-3 text-xs border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent resize-y font-mono text-slate-700 bg-slate-50/30"
              />
            </div>
          </div>

          <div className="border border-slate-200 rounded-xl overflow-hidden bg-slate-50/40">
            <button
              type="button"
              onClick={() => setReferenceDataOpen((o) => !o)}
              className="w-full px-3 py-2 flex items-center justify-between gap-2 text-left hover:bg-slate-100/80 transition-colors"
            >
              <div className="text-xs font-semibold text-slate-800">底层参考数据（解析表目录 · 字段 · DDL 列）</div>
              {referenceDataOpen ? <ChevronUp className="w-4 h-4 text-slate-500 shrink-0" /> : <ChevronDown className="w-4 h-4 text-slate-500 shrink-0" />}
            </button>
            {referenceDataOpen && (
              <div className="p-3 pt-0 space-y-3 border-t border-slate-200 bg-white">
                {ddlParsed.length > 0 && (
                  <div className="border border-slate-200 rounded-lg bg-white">
                    <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 flex items-center justify-between">
                      <span>
                        DDL 列（含 COMMENT）：{ddlParsed.filter((c) => c.comment).length}/{ddlParsed.length}
                      </span>
                      <span className="text-slate-400">当前槽位命中：{activeDdlMatches.size}</span>
                    </div>
                    <div className="max-h-32 overflow-y-auto px-3 py-2 text-[11px] font-mono">
                      {ddlParsed.slice(0, 80).map((c, idx) => {
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
                      {ddlParsed.length > 80 && <div className="text-slate-400 py-1">… 共 {ddlParsed.length} 列</div>}
                    </div>
                  </div>
                )}
                <div className="flex items-center gap-2 mb-1">
                  <input
                    value={headerSearch}
                    onChange={(e) => setHeaderSearch(e.target.value)}
                    placeholder="搜索字段…"
                    className="text-[11px] border border-slate-200 rounded-lg px-2 py-1 bg-white text-slate-700 w-full max-w-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  />
                </div>
                <div className="grid grid-cols-12 gap-2">
                  <div className="col-span-12 xl:col-span-5 border border-slate-200 rounded-lg bg-white overflow-hidden">
                    <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 bg-slate-50">
                      生产表头池（{Object.keys(tables || {}).length} 表）
                    </div>
                    <div className="max-h-[240px] overflow-y-auto p-2 space-y-1">
                      {Object.keys(tables || {}).length ? (
                        Object.keys(tables || {}).map((fileName) => (
                          <button
                            key={fileName}
                            type="button"
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
                        <div className="text-[11px] text-slate-500 p-2">暂无生产表头。点击顶部「刷新表头」。</div>
                      )}
                    </div>
                  </div>
                  <div className="col-span-12 xl:col-span-7 border border-slate-200 rounded-lg bg-white overflow-hidden">
                    <div className="px-3 py-2 border-b border-slate-200 text-[11px] text-slate-600 bg-slate-50 flex items-center justify-between">
                      <span>字段（{selectedFile ? (tables?.[selectedFile]?.length || 0) : 0}）</span>
                      <span className="text-slate-400">点击列名：映射到当前槽位（col@file）</span>
                    </div>
                    <div className="max-h-[240px] overflow-y-auto p-2">
                      {selectedFile ? (
                        <div className="flex flex-wrap gap-1.5">
                          {(tables?.[selectedFile] || [])
                            .filter((c) => !normalize(headerSearch) || normalize(c).includes(normalize(headerSearch)))
                            .map((c) => (
                              <button
                                key={`${selectedFile}-${c}`}
                                type="button"
                                onClick={() => {
                                  assignPhysicalColumn(c, selectedFile);
                                  void loadSamples(selectedFile);
                                }}
                                className="px-2 py-1 text-[11px] font-mono rounded-md border shadow-sm transition-colors bg-white border-slate-200 hover:border-indigo-300 hover:bg-indigo-50"
                                title={`${c}（来自 ${selectedFile}）`}
                              >
                                <span>{c}</span>
                                <span className="ml-1 text-[10px] text-slate-400">(来自 {selectedFile})</span>
                              </button>
                            ))}
                        </div>
                      ) : (
                        <div className="text-[11px] text-slate-500 p-2">请选择左侧表名查看字段。</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Right: col-span-4 */}
      <div className="col-span-12 lg:col-span-4 min-h-0 flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm">
        <div className="px-3 py-2 border-b border-slate-200 bg-slate-50/50">
          <div className="text-sm font-semibold text-slate-900">映射预览与抽样校验</div>
          <div className="text-[11px] text-slate-500 mt-0.5">
            校验文件：<span className="font-mono text-slate-700">{selectedFile || '-'}</span>
          </div>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto p-3 space-y-2">
          {/* Compact mapping list */}
          <div className="grid grid-cols-1 gap-1.5">
            {mappingPreview.map((m) => {
              const isActive = m.standardKey === activeFieldKey;
              const unmapped = !m.physicalColumn;
              return (
                <div
                  key={m.standardKey}
                  className={cn(
                    "rounded-lg border px-3 py-2 transition-all",
                    "border-slate-200",
                    isActive ? "border-indigo-500 ring-2 ring-indigo-200 animate-pulse" : "",
                    unmapped ? "bg-red-50/30" : "bg-white"
                  )}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-medium text-slate-900">{m.standardName}</div>
                    <span className="text-[10px] font-mono text-slate-500 bg-slate-50 border border-slate-200 px-1.5 py-0.5 rounded">
                      {m.standardKey}
                    </span>
                  </div>
                  <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px]">
                    {m.physicalColumn ? (
                      <>
                        <span className="px-2 py-1 rounded-md bg-slate-900 text-white border border-slate-800 font-mono">
                          {m.physicalColumn}
                        </span>
                        <span className="text-slate-400">来源</span>
                        <span className="font-mono text-slate-700">{m.sourceTable || '-'}</span>
                      </>
                    ) : (
                      <span className="inline-flex items-center gap-1 text-red-700">
                        <AlertCircle className="w-4 h-4" /> 未映射
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Excel-like sample table */}
          <div className={cn(
            "border border-slate-200 rounded-xl overflow-hidden",
            activeFieldKey ? "ring-2 ring-indigo-100" : ""
          )}>
            <div className="px-3 py-2 bg-slate-900 text-white text-xs font-semibold">
              抽样数据表（前 3 行）
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full text-[11px]">
                <thead className="bg-slate-50 border-b border-slate-200">
                  <tr>
                    <th className="px-2 py-2 text-left font-medium text-slate-600 w-10">#</th>
                    {sampleGrid.cols.length ? (
                      sampleGrid.cols.map((c) => (
                        <th key={c.standardKey} className={cn(
                          "px-2 py-2 text-left font-medium text-slate-700 whitespace-nowrap",
                          c.standardKey === activeFieldKey ? "bg-indigo-50 text-indigo-800" : ""
                        )}>
                          <div className="flex flex-col leading-tight">
                            <span>{c.standardName}</span>
                            <span className="font-mono text-[10px] text-slate-500">{c.physicalColumn}</span>
                          </div>
                        </th>
                      ))
                    ) : (
                      <th className="px-2 py-2 text-left font-medium text-slate-600">暂无可展示列（请选择有表头的文件并完成映射）</th>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {sampleGrid.rows.map((r, idx) => (
                    <tr key={idx} className="hover:bg-slate-50/60">
                      <td className="px-2 py-2 font-mono text-slate-400">{idx + 1}</td>
                      {sampleGrid.cols.length ? (
                        sampleGrid.cols.map((c) => (
                          <td key={c.standardKey} className={cn(
                            "px-2 py-2 font-mono text-slate-800 whitespace-nowrap",
                            c.standardKey === activeFieldKey ? "bg-indigo-50/50" : ""
                          )}>
                            {r[c.standardKey] || <span className="text-slate-300">-</span>}
                          </td>
                        ))
                      ) : (
                        <td className="px-2 py-2 text-slate-400">-</td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
    {aiReportOpen && (
      <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm flex items-center justify-center p-4">
        <div className="w-[min(960px,95vw)] bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden">
          <div className="px-5 py-4 bg-slate-900 text-white flex items-center justify-between">
            <div>
              <div className="text-sm font-semibold">AI 发现报告</div>
              <div className="text-[11px] text-slate-300 mt-0.5">映射与 Join 已自动写入左侧标准字段，可关闭后继续沙盒测试与认证</div>
            </div>
            <button
              type="button"
              onClick={() => setAiReportOpen(false)}
              className="px-3 py-1.5 text-[11px] rounded-lg bg-white/10 border border-white/15 hover:bg-white/15"
            >
              关闭
            </button>
          </div>
          <div className="p-5 grid grid-cols-12 gap-4">
            <div className="col-span-12 md:col-span-7">
              <div className="text-xs font-semibold text-slate-900 mb-2">标准字段映射（smartSuggestions）</div>
              <div className="border border-slate-200 rounded-xl overflow-hidden">
                <div className="max-h-56 overflow-y-auto divide-y divide-slate-100">
                  {(aiSuggestions || []).length ? (
                    aiSuggestions.map((s, idx) => (
                      <div key={idx} className="px-4 py-3 text-[11px] flex flex-wrap items-center gap-2">
                        <span className="px-2 py-1 rounded-md bg-indigo-50 border border-indigo-200 text-indigo-800 font-mono">{s.standardKey}</span>
                        <span className="text-slate-400">←</span>
                        <span className="px-2 py-1 rounded-md bg-slate-900 text-white border border-slate-800 font-mono">{s.sourceTable}.{s.sourceField}</span>
                      </div>
                    ))
                  ) : (
                    <div className="px-4 py-6 text-[11px] text-slate-500">暂无 smartSuggestions</div>
                  )}
                </div>
              </div>
            </div>
            <div className="col-span-12 md:col-span-5">
              <div className="text-xs font-semibold text-slate-900 mb-2">Join 链路（joinPathSuggestions）</div>
              <div className="border border-slate-200 rounded-xl overflow-hidden">
                <div className="max-h-56 overflow-y-auto divide-y divide-slate-100">
                  {(aiJoinPaths || []).length ? (
                    aiJoinPaths.map((j, idx) => (
                      <div key={idx} className="px-4 py-3 text-[11px]">
                        <div className="flex items-center gap-2 mb-2">
                          <span className="px-2 py-1 rounded-md bg-amber-50 border border-amber-200 text-amber-800 font-mono">{j.targetStandardKey}</span>
                        </div>
                        <div className="text-[10px] text-slate-600 font-mono break-all">
                          {j.path.join(' -> ')}
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="px-4 py-6 text-[11px] text-slate-500">暂无 joinPathSuggestions</div>
                  )}
                </div>
              </div>
            </div>

            <div className="col-span-12 flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
              <button
                type="button"
                onClick={() => setAiReportOpen(false)}
                className="px-4 py-2 text-sm font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 shadow-sm"
              >
                知道了
              </button>
            </div>
          </div>
        </div>
      </div>
    )}
    {sandboxDetailOpen && sandboxChecks && (
      <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm flex items-center justify-center p-4">
        <div className="w-[min(520px,95vw)] bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden">
          <div className="px-4 py-3 bg-slate-900 text-white flex items-center justify-between">
            <div className="text-sm font-semibold">沙盒测试明细（7 维）</div>
            <button
              type="button"
              onClick={() => setSandboxDetailOpen(false)}
              className="px-2.5 py-1 text-[11px] rounded-lg bg-white/10 border border-white/15 hover:bg-white/15"
            >
              关闭
            </button>
          </div>
          <div className="p-4 max-h-[min(70vh,480px)] overflow-y-auto space-y-2">
            {SANDBOX_DIM_KEYS.map((k) => {
              const c = sandboxChecks[k];
              if (!c) return null;
              const label = SANDBOX_DIM_LABEL[k];
              const skipped = !c.expected && !c.actual;
              return (
                <div
                  key={k}
                  className={cn(
                    'rounded-lg border px-3 py-2 text-[11px]',
                    skipped ? 'border-slate-100 bg-slate-50 text-slate-400' : c.pass ? 'border-emerald-200 bg-emerald-50/50' : 'border-amber-200 bg-amber-50/50'
                  )}
                >
                  <div className="flex items-center gap-2 font-medium text-slate-800">
                    {skipped ? null : c.pass ? (
                      <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" />
                    ) : (
                      <AlertCircle className="w-4 h-4 text-amber-600 shrink-0" />
                    )}
                    <span className="font-mono">{k}</span>
                    <span className="text-slate-500 font-normal">({label})</span>
                  </div>
                  <div className="mt-1 text-slate-600">
                    {skipped ? '未填写期望且无实际值（跳过）' : `期望：${c.expected || '—'} · 实际：${c.actual || '—'}`}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    )}
    </>
  );
}
