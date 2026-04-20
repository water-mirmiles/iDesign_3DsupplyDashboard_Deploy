import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
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

type ListSandboxResponse = {
  ok: boolean;
  dir?: string;
  files?: string[];
  error?: string;
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
type AiJoinPath = { targetStandardKey: string; path?: string[]; joinPath?: any };
type AiConcatPart = { sourceField: string; sourceTable: string; joinPath?: string[] };
type AiConcatSuggestion = { standardKey: string; parts: AiConcatPart[]; operator?: string };

/** 将 AI 返回的直连 / Join / CONCAT 合并进标准字段（纯函数，便于 flushSync 后立即预览） */
function mergeAiSuggestionsIntoStandardFields(
  prev: GlobalSchemaField[],
  sug: AiSuggestion[],
  jp: AiJoinPath[],
  concatSuggestions: AiConcatSuggestion[],
  lockedStandardKeys: Set<string>
): GlobalSchemaField[] {
  const next = prev.map((f) => ({ ...f, mappedSources: [...(f.mappedSources || [])] }));
  for (const s of sug) {
    const target = next.find((f) => f.standardKey === s.standardKey);
    if (!target) continue;
    if (lockedStandardKeys.has(s.standardKey)) continue;
    target.mappedSources = [`${s.sourceField}@${s.sourceTable}`];
  }
  const normalizeStructuredToLegacyPath = (joinPath: any): string[] => {
    if (!Array.isArray(joinPath) || joinPath.length < 2) return [];
    // hop: {sourceTable,sourceField,targetTable,targetField} ... terminal: {targetTable,valueField}
    const out: string[] = [];
    for (let i = 0; i < joinPath.length; i++) {
      const seg = joinPath[i];
      if (!seg || typeof seg !== 'object') return [];
      const isLast = i === joinPath.length - 1;
      if (!isLast) {
        const st = String((seg as any).sourceTable || '').trim();
        const sf = String((seg as any).sourceField || '').trim();
        const tt = String((seg as any).targetTable || '').trim();
        const tf = String((seg as any).targetField || '').trim();
        if (!st || !sf || !tt || !tf) return [];
        if (out.length === 0) out.push(`${st}.${sf}`, `${tt}.${tf}`);
        else out.push(`${tt}.${tf}`);
      } else {
        const tt = String((seg as any).targetTable || '').trim();
        const vf = String((seg as any).valueField || '').trim();
        if (!tt || !vf) return [];
        out.push(`${tt}.${vf}`);
      }
    }
    return out;
  };
  for (const j of jp) {
    const target = next.find((f) => f.standardKey === j.targetStandardKey);
    if (!target) continue;
    if (lockedStandardKeys.has(j.targetStandardKey)) continue;
    const pathArr =
      Array.isArray(j.path) && j.path.length >= 2 ? j.path : j.joinPath ? normalizeStructuredToLegacyPath(j.joinPath) : [];
    if (pathArr.length >= 2) target.mappedSources = [`CHAIN|${pathArr.join('->')}`];
  }
  for (const c of concatSuggestions) {
    const target = next.find((f) => f.standardKey === c.standardKey);
    if (!target || !Array.isArray(c.parts) || c.parts.length < 2) continue;
    if (lockedStandardKeys.has(c.standardKey)) continue;
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
}

function normalize(s: string) {
  return (s || '').trim().toLowerCase();
}

/** 与后端 buildTableNameIndex 一致：数据文件名 → 逻辑表名 */
function logicalTableFromDataFile(fileName: string) {
  const base = String(fileName || '').replace(/\.(xlsx|xls)$/i, '');
  return base.replace(/^\d{8}_/, '');
}

/** 非主表 CHAIN 首段：主表外键列候选（brand 优先用列名 brand） */
function fkColumnCandidatesForStandardKey(standardKey: string): string[] {
  const sk = String(standardKey || '').trim();
  const out: string[] = [];
  if (sk === 'brand') out.push('brand', 'brand_id', 'base_brand_id', 'wms_brand_id');
  if (sk === 'lastCode') out.push('associated_last', 'associated_last_type', 'last_id', 'last_type_id');
  if (sk === 'soleCode') out.push('associated_sole_info', 'sole_id', 'associated_sole');
  if (sk === 'colorCode') out.push('initial_sample_color_id', 'color_id');
  if (sk === 'materialCode') out.push('material_id', 'main_material');
  if (sk) out.push(`${sk}_id`, sk);
  return [...new Set(out.filter(Boolean))];
}

function pickFkColumnOnMaster(masterCols: string[], standardKey: string): string {
  const set = new Set(masterCols.map((c) => String(c)));
  const lowerToOrig = new Map(masterCols.map((c) => [String(c).trim().toLowerCase(), c]));
  for (const cand of fkColumnCandidatesForStandardKey(standardKey)) {
    if (set.has(cand)) return cand;
    const lo = lowerToOrig.get(cand.toLowerCase());
    if (lo) return lo;
  }
  return '';
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

/** Step2 按维度：多行样本，每行可有逻辑备注 + 多段物理字段（拼接） */
type GoldenDimensionRow = {
  id: string;
  notes: string;
  segments: GoldenSampleSegment[];
};

/** Step2 AI 学习区：按标准维度分组的样本（持久化字段名 dimensionSamples） */
type DimensionSamplesState = Record<string, { targetGoal: string; rows: GoldenDimensionRow[] }>;

type DraftDimensionSamples = Record<
  string,
  {
    targetValue: string;
    samples: Array<{ tableName: string; fieldName: string; sampleValue: string; notes: string; rowId?: string }>;
  }
>;

function createEmptyDimensionSamples(): DimensionSamplesState {
  const o: DimensionSamplesState = {};
  for (const f of initialStandardFields) {
    o[f.standardKey] = { targetGoal: '', rows: [] };
  }
  return o;
}

function newGoldenRowId() {
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function emptyGoldenDimensionRow(): GoldenDimensionRow {
  return { id: newGoldenRowId(), notes: '', segments: [{ tableName: '', fieldName: '', value: '' }] };
}

function migrateLegacyGoldenSamplesToByDimension(gs: unknown[]): DimensionSamplesState {
  const next = createEmptyDimensionSamples();
  gs.forEach((x: unknown, idx: number) => {
    if (!x || typeof x !== 'object' || Array.isArray(x)) return;
    const o = x as { standardKey?: unknown; segments?: unknown; tableName?: unknown; fieldName?: unknown; value?: unknown; id?: unknown };
    const sk = typeof o.standardKey === 'string' ? o.standardKey : 'materialCode';
    if (!next[sk]) next[sk] = { targetGoal: '', rows: [] };
    let segments: GoldenSampleSegment[];
    if (Array.isArray(o.segments) && o.segments.length) {
      segments = o.segments.map((seg: unknown) => {
        if (!seg || typeof seg !== 'object' || Array.isArray(seg)) {
          return { tableName: '', fieldName: '', value: '' };
        }
        const s = seg as { tableName?: unknown; fieldName?: unknown; value?: unknown };
        return {
          tableName: typeof s.tableName === 'string' ? s.tableName : '',
          fieldName: typeof s.fieldName === 'string' ? s.fieldName : '',
          value: typeof s.value === 'string' ? s.value : '',
        };
      });
    } else {
      segments = [
        {
          tableName: typeof o.tableName === 'string' ? o.tableName : '',
          fieldName: typeof o.fieldName === 'string' ? o.fieldName : '',
          value: typeof o.value === 'string' ? o.value : '',
        },
      ];
    }
    if (!segments.some((s) => s.tableName || s.fieldName || s.value)) return;
    next[sk].rows.push({
      id: typeof o.id === 'string' && o.id ? o.id : `m_${idx}_${newGoldenRowId()}`,
      notes: '',
      segments,
    });
  });
  return next;
}

function flattenDimensionSamplesForLegacySave(g: DimensionSamplesState) {
  const out: { standardKey: string; segments: GoldenSampleSegment[] }[] = [];
  for (const f of initialStandardFields) {
    const block = g[f.standardKey];
    if (!block) continue;
    for (const row of block.rows) {
      if (row.segments.some((s) => s.tableName && s.fieldName)) {
        out.push({ standardKey: f.standardKey, segments: row.segments });
      }
    }
  }
  return out;
}

/** legacy/AI payload：targetGoal + rows[].segments（保留 rowId 与 notes） */
function serializeDimensionSamplesForAiLegacy(g: DimensionSamplesState) {
  const o: Record<
    string,
    { targetGoal: string; rows: { id: string; notes: string; segments: GoldenSampleSegment[] }[] }
  > = {};
  for (const f of initialStandardFields) {
    const b = g[f.standardKey] || { targetGoal: '', rows: [] };
    o[f.standardKey] = {
      targetGoal: b.targetGoal,
      rows: b.rows.map((r) => ({
        id: r.id,
        notes: r.notes,
        segments: r.segments.map((s) => ({
          tableName: s.tableName,
          fieldName: s.fieldName,
          value: s.value,
        })),
      })),
    };
  }
  return o;
}

/** draft payload（所见即所得最高优先级）：targetValue + samples[] */
function serializeDimensionSamplesForDraft(g: DimensionSamplesState): DraftDimensionSamples {
  const out: DraftDimensionSamples = {};
  for (const f of initialStandardFields) {
    const b = g[f.standardKey] || { targetGoal: '', rows: [] };
    out[f.standardKey] = {
      targetValue: String(b.targetGoal || ''),
      samples: (b.rows || []).flatMap((r) => {
        const notes = String(r?.notes || '');
        const rowId = String(r?.id || '');
        const segs = Array.isArray(r?.segments) ? r.segments : [];
        return segs.map((s) => ({
          tableName: String(s?.tableName || ''),
          fieldName: String(s?.fieldName || ''),
          sampleValue: String(s?.value || ''),
          notes,
          rowId,
        }));
      }),
    };
  }
  return out;
}

function suggestTableFieldFromDdl(parsedTableMap: Record<string, Column[]>, standardKey: string): { tableName: string; fieldName: string } {
  const sk = String(standardKey || '').trim();
  const keywords: string[] =
    sk === 'styleCode'
      ? ['style', 'style_wms', '款号', 'style_no']
      : sk === 'brand'
        ? ['brand', '品牌', 'brand_name', 'name']
        : sk === 'lastCode'
          ? ['last', '楦', 'code', 'last_code']
          : sk === 'soleCode'
            ? ['sole', '底', 'heel', 'code', 'sole_code']
            : sk === 'colorCode'
              ? ['color', '颜色', 'colour', 'code', 'color_code']
              : sk === 'materialCode'
                ? ['material', '材质', 'category', 'code', 'name', 'material_code', 'category_code']
                : sk === 'status'
                  ? ['status', '状态', 'data_status', '生效', '有效']
                  : [];

  let best: { tableName: string; fieldName: string; score: number } | null = null;
  for (const [tn, cols] of Object.entries(parsedTableMap || {})) {
    for (const c of cols || []) {
      const name = String(c?.name || '').trim();
      const comment = String((c as any)?.comment || '').trim();
      if (!name) continue;
      const hay = `${name} ${comment}`.toLowerCase();
      let score = 0;
      for (const kw of keywords) {
        const k = String(kw).toLowerCase();
        if (!k) continue;
        if (hay.includes(k)) score += 10;
      }
      if (sk.endsWith('Code') && name.toLowerCase() === 'code') score += 8;
      if (sk === 'brand' && name.toLowerCase() === 'brand_name') score += 12;
      if (score <= 0) continue;
      if (!best || score > best.score) best = { tableName: tn, fieldName: name, score };
    }
  }
  return best ? { tableName: best.tableName, fieldName: best.fieldName } : { tableName: '', fieldName: '' };
}

function normalizeDraftDimensionSamples(raw: unknown): DimensionSamplesState {
  // New draft shape: { targetValue, samples[] }
  const base = createEmptyDimensionSamples();
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return base;
  const rec = raw as Record<string, unknown>;
  for (const f of initialStandardFields) {
    const g = rec[f.standardKey];
    if (!g || typeof g !== 'object' || Array.isArray(g)) continue;
    const go = g as any;
    // preferred
    if (Array.isArray(go.samples)) {
      const targetGoal = typeof go.targetValue === 'string' ? String(go.targetValue) : '';
      const samples = go.samples as any[];
      const byRow = new Map<string, GoldenDimensionRow>();
      samples.forEach((s, idx) => {
        const rowId = typeof s?.rowId === 'string' && s.rowId.trim() ? s.rowId.trim() : `r_${idx}_${newGoldenRowId()}`;
        const row = byRow.get(rowId) || { id: rowId, notes: typeof s?.notes === 'string' ? s.notes : '', segments: [] };
        // keep last non-empty notes
        const nextNotes = typeof s?.notes === 'string' ? s.notes : '';
        if (nextNotes) row.notes = nextNotes;
        row.segments.push({
          tableName: typeof s?.tableName === 'string' ? s.tableName : '',
          fieldName: typeof s?.fieldName === 'string' ? s.fieldName : '',
          value: typeof s?.sampleValue === 'string' ? s.sampleValue : '',
        });
        byRow.set(rowId, row);
      });
      base[f.standardKey] = { targetGoal, rows: Array.from(byRow.values()).filter((r) => r.segments.length) };
      continue;
    }
  }
  // fallback to legacy shape if new shape absent
  return normalizeDraftDimensionSamplesLegacy(raw, base);
}

function normalizeDraftDimensionSamplesLegacy(raw: unknown, baseIn?: DimensionSamplesState): DimensionSamplesState {
  const base = baseIn || createEmptyDimensionSamples();
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return base;
  const rec = raw as Record<string, unknown>;
  for (const f of initialStandardFields) {
    const g = rec[f.standardKey];
    if (!g || typeof g !== 'object' || Array.isArray(g)) continue;
    const go = g as { targetGoal?: unknown; rows?: unknown };
    const targetGoal = typeof go.targetGoal === 'string' ? go.targetGoal : '';
    const rowsRaw = Array.isArray(go.rows) ? go.rows : [];
    const rows: GoldenDimensionRow[] = [];
    rowsRaw.forEach((r: unknown, idx: number) => {
      if (!r || typeof r !== 'object' || Array.isArray(r)) return;
      const ro = r as { id?: unknown; notes?: unknown; segments?: unknown };
      const segments = Array.isArray(ro.segments)
        ? ro.segments.map((seg: unknown) => {
            if (!seg || typeof seg !== 'object' || Array.isArray(seg)) {
              return { tableName: '', fieldName: '', value: '' };
            }
            const s = seg as { tableName?: unknown; fieldName?: unknown; value?: unknown };
            return {
              tableName: typeof s.tableName === 'string' ? s.tableName : '',
              fieldName: typeof s.fieldName === 'string' ? s.fieldName : '',
              value: typeof s.value === 'string' ? s.value : '',
            };
          })
        : [];
      rows.push({
        id: typeof ro.id === 'string' && ro.id.trim() ? ro.id.trim() : `d_${idx}_${newGoldenRowId()}`,
        notes: typeof ro.notes === 'string' ? ro.notes : '',
        segments: segments.length ? segments : [{ tableName: '', fieldName: '', value: '' }],
      });
    });
    base[f.standardKey] = { targetGoal, rows };
  }
  return base;
}

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
  const [dimensionSamples, setDimensionSamples] = useState<DimensionSamplesState>(() => createEmptyDimensionSamples());
  /** 避免草稿尚未从服务器恢复前用户编辑被后续 setState 覆盖 */
  const [schemaDraftLoadState, setSchemaDraftLoadState] = useState<'loading' | 'ready'>('loading');
  const [draftSaveFeedback, setDraftSaveFeedback] = useState(false);
  const [draftSaving, setDraftSaving] = useState(false);
  const draftSaveFeedbackTimerRef = useRef<number | null>(null);
  const step2DimGroupRef = useRef<Record<string, HTMLDivElement | null>>({});
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

  const [isUploadingSandbox, setIsUploadingSandbox] = useState(false);
  const [sandboxFiles, setSandboxFiles] = useState<string[]>([]);
  const [isSyncingSandboxFiles, setIsSyncingSandboxFiles] = useState(false);
  const [uploadCounter, setUploadCounter] = useState(0);

  const [authSyncing, setAuthSyncing] = useState(false);
  const [certifyEngineRunning, setCertifyEngineRunning] = useState(false);
  /** Step4：最近一次 AI 建模成功后仍为空的维度（用于提示「尽力了」而非笼统「未回填」） */
  const [aiUnfilledAfterRun, setAiUnfilledAfterRun] = useState<Record<string, boolean>>({});
  /** 手动纠偏锁定：一旦用户手动点选字段，该维度不允许被 AI 覆盖 */
  const [lockedStandardKeys, setLockedStandardKeys] = useState<Set<string>>(() => new Set());
  const [conversationalInput, setConversationalInput] = useState('');
  const [chatToConfigBusy, setChatToConfigBusy] = useState(false);
  const [reviewTweakOpen, setReviewTweakOpen] = useState(false);
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
  const expectedByStandardKey = useMemo(() => {
    const out: Record<string, string> = {};
    for (const f of initialStandardFields) {
      const b = dimensionSamples[f.standardKey] || { targetGoal: '', rows: [] };
      const goal = String(b.targetGoal || '').trim();
      if (goal) {
        out[f.standardKey] = goal;
        continue;
      }
      const rows = Array.isArray(b.rows) ? b.rows : [];
      const joined = rows
        .map((r) => (Array.isArray(r?.segments) ? r.segments.map((s) => String(s?.value ?? '')).join('') : ''))
        .join('');
      out[f.standardKey] = String(joined || '').trim();
    }
    return out;
  }, [dimensionSamples]);

  const actualByStandardKey = useMemo(() => {
    const row = resolvedRow || {};
    const out: Record<string, string> = {};
    for (const f of initialStandardFields) {
      const v =
        f.standardKey === 'styleCode' ? String((row as any).style_wms ?? '') : String((row as any)[f.standardKey] ?? '');
      out[f.standardKey] = String(v || '').trim();
    }
    return out;
  }, [resolvedRow]);

  const previewMatchByStandardKey = useMemo(() => {
    const out: Record<string, { expected: string; actual: string; ok: boolean; hasExpectation: boolean }> = {};
    for (const f of initialStandardFields) {
      const expected = String(expectedByStandardKey[f.standardKey] ?? '').trim();
      const actual = String(actualByStandardKey[f.standardKey] ?? '').trim();
      const hasExpectation = Boolean(expected);
      out[f.standardKey] = {
        expected,
        actual,
        hasExpectation,
        ok: hasExpectation && expected === actual,
      };
    }
    return out;
  }, [actualByStandardKey, expectedByStandardKey]);

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
      const dimensionSamplesDraft = serializeDimensionSamplesForDraft(dimensionSamples);
      const legacy = serializeDimensionSamplesForAiLegacy(dimensionSamples);
      const data = {
        ddlText,
        masterTable,
        dimensionSamples: dimensionSamplesDraft,
        goldenByDimension: legacy,
        goldenSamples: flattenDimensionSamplesForLegacySave(dimensionSamples), // 兼容旧后端/Prompt
      };
      // eslint-disable-next-line no-console
      console.log('💾 正在保存全量配置:', data);

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
  }, [ddlText, dimensionSamples, masterTable]);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/load-schema-draft`);
        if (!alive) return;
        const json = (await resp.json()) as { ok?: boolean; draft?: Record<string, unknown> };
        if (!alive) return;
        const d = json.draft && typeof json.draft === 'object' && !Array.isArray(json.draft) ? json.draft : {};
        if (typeof d.ddlText === 'string') setDdlText(d.ddlText);
        if (typeof (d as any).masterTable === 'string') setMasterTable((d as any).masterTable);

        const ds = (d as any).dimensionSamples;
        const gbd = (d as any).goldenByDimension;
        if (ds != null && typeof ds === 'object' && !Array.isArray(ds)) {
          setDimensionSamples(normalizeDraftDimensionSamples(ds));
        } else if (gbd != null && typeof gbd === 'object' && !Array.isArray(gbd)) {
          setDimensionSamples(normalizeDraftDimensionSamples(gbd));
        } else {
          const gs = (d as any).goldenSamples;
          if (Array.isArray(gs) && gs.length) {
            setDimensionSamples(migrateLegacyGoldenSamplesToByDimension(gs));
          }
        }
      } catch {
        // ignore load errors — 草稿可选
      } finally {
        if (alive) setSchemaDraftLoadState('ready');
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    return () => {
      if (authSyncHintTimerRef.current) window.clearTimeout(authSyncHintTimerRef.current);
      if (draftSaveFeedbackTimerRef.current) window.clearTimeout(draftSaveFeedbackTimerRef.current);
    };
  }, []);

  useEffect(() => {
    const shouldPoll = isAiModeling;
    if (!shouldPoll) {
      setAiStatusText('');
      return;
    }
    let alive = true;
    let lastPartialRevision = 0;
    const tick = async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/ai-status`);
        if (!resp.ok) return;
        const json = (await resp.json()) as any;
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

        const rev = Number(json.partialRevision || 0);
        const partial = json.partial && typeof json.partial === 'object' ? json.partial : null;
        if (rev && partial && rev > lastPartialRevision) {
          lastPartialRevision = rev;
          const sug = Array.isArray(partial.smartSuggestions) ? partial.smartSuggestions : [];
          const jp = Array.isArray(partial.joinPathSuggestions) ? partial.joinPathSuggestions : [];
          const cc = Array.isArray(partial.concatMappingSuggestions) ? partial.concatMappingSuggestions : [];
          let mergedFields: GlobalSchemaField[] = [];
          flushSync(() => {
            setStandardFields((prev) => {
              mergedFields = mergeAiSuggestionsIntoStandardFields(prev, sug, jp, cc, lockedStandardKeys);
              return mergedFields;
            });
          });
          const mappingNow = buildCertifiedMapping(mergedFields);
          void runPreviewMappingRow(mappingNow);
        }
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

  const detectedTableCount = useMemo(() => sandboxFiles.length, [sandboxFiles]);

  const refreshSandboxFiles = useCallback(async () => {
    setIsSyncingSandboxFiles(true);
    try {
      const resp = await fetch(`${API_BASE}/api/list-sandbox`);
      if (!resp.ok) throw new Error(`获取沙盒列表失败（HTTP ${resp.status}）`);
      const json = (await resp.json().catch(() => null)) as ListSandboxResponse | null;
      if (!json?.ok) throw new Error(json?.error || '获取沙盒列表失败');
      const files = Array.isArray(json.files) ? json.files.map((x) => String(x || '').trim()).filter(Boolean) : [];
      setSandboxFiles(files);
      // 绕过缓存：强制 React 重建列表 DOM
      setUploadCounter((c) => c + 1);
      setSelectedFile((prev) => {
        if (prev && files.includes(prev)) return prev;
        return files[0] || '';
      });
      if (!files.length) {
        setTables({});
        setSamples({});
        setLatestDir('sandbox');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : '获取沙盒列表失败');
    } finally {
      setIsSyncingSandboxFiles(false);
    }
  }, []);

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
      const resp = await fetch(`${API_BASE}/api/table-headers?scope=sandbox`);
      if (!resp.ok) throw new Error(`获取表头失败（HTTP ${resp.status}）`);
      const json = (await resp.json()) as TableHeadersResponse;
      if (!json.ok) throw new Error('获取表头失败');
      setTables(json.tables || {});
      setLatestDir(json.latestDir || '');
      const firstFile = sandboxFiles[0] || Object.keys(json.tables || {})[0] || '';
      setSelectedFile((prev) => (prev ? prev : firstFile));
    } catch (e) {
      setError(e instanceof Error ? e.message : '获取表头失败');
    }
  };

  const handleSandboxUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    setError(null);
    const files = e.target.files;
    if (!files?.length) return;
    const all = Array.from(files) as File[];
    const fd = new FormData();
    // 强制对账：与后端 upload-sandbox 的字段名保持一致
    for (const f of all) fd.append('file', f);
    try {
      setIsUploadingSandbox(true);
      const resp = await fetch(`${API_BASE}/api/upload-sandbox`, { method: 'POST', body: fd });
      if (!resp.ok) throw new Error(`上传失败（HTTP ${resp.status}）`);
      const json = (await resp.json().catch(() => null)) as any;
      if (!json?.ok) throw new Error(json?.error || '上传失败');
      // 上传成功后必须立即物理刷新一次沙盒列表（实时同步）
      await refreshSandboxFiles();
      await refreshHeaders();
      // 再刷新一次列表，避免“表头刷新/选中切换”导致 UI 短暂不一致
      await refreshSandboxFiles();
    } catch (err) {
      setError(err instanceof Error ? err.message : '上传失败');
    } finally {
      setIsUploadingSandbox(false);
    }
    e.target.value = '';
  };

  const loadSamples = async (fileName: string) => {
    if (!fileName) return;
    try {
      const resp = await fetch(`${API_BASE}/api/table-samples?scope=sandbox&fileName=${encodeURIComponent(fileName)}`);
      if (!resp.ok) return;
      const json = (await resp.json()) as TableSamplesResponse;
      if (!json.ok) return;
      setSamples(json.samples || {});
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    void refreshSandboxFiles();
    void refreshHeaders();
  }, []);

  useEffect(() => {
    void loadSamples(selectedFile);
  }, [selectedFile]);

  const runPreviewMappingRow = useCallback(async (mapping: MappingEntry[], signal?: AbortSignal) => {
    const hasAny = mapping.some((m) => mappingEntryIsPublishable(m));
    if (!hasAny) {
      if (!signal?.aborted) {
        setResolvedRow(null);
        setResolvedMeta(null);
      }
      return;
    }
    setResolvingRow(true);
    try {
      const targetStyle = String(expectedByStandardKey?.styleCode || '').trim();
      const resp = await fetch(`${API_BASE}/api/preview-mapping-row`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal,
        body: JSON.stringify({ mapping, targetStyle, scope: 'sandbox' }),
      });
      const json = (await resp.json().catch(() => null)) as any;
      if (signal?.aborted) return;
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
    } catch (e) {
      if (signal?.aborted || String((e as any)?.name || '').includes('Abort')) return;
      setResolvedRow(null);
      setResolvedMeta(null);
    } finally {
      if (!signal?.aborted) setResolvingRow(false);
    }
  }, [expectedByStandardKey]);

  // Step 4：mapping 变化时拉取真实预览行（含 AI 回填后）
  useEffect(() => {
    const ac = new AbortController();
    void runPreviewMappingRow(mappingPreview, ac.signal);
    return () => ac.abort();
  }, [mappingPreview, runPreviewMappingRow]);

  const handleAiLogicModeling = async (): Promise<{ ok: true } | { ok: false; error: string }> => {
    setError(null);
    setAiBusy503(false);
    setAiQueueHint(true);
    setIsAiModeling(true);
    setAiUnfilledAfterRun({});
    try {
      setAiTables([]);
      const controller = new AbortController();
      const t = window.setTimeout(() => controller.abort(), 180000);
      const payload = {
        sqlText: ddlText,
        masterTable: masterTable || undefined,
        conversationalInput: conversationalInput || undefined,
        // AI 仅消费 legacy 结构（targetGoal/rows/segments）；dimensionSamples 用于草稿持久化
        goldenByDimension: serializeDimensionSamplesForAiLegacy(dimensionSamples),
        // 兼容旧 Prompt：扁平化样本（每一行保留用户选定的 tableName/fieldName）
        goldenSamples: flattenDimensionSamplesForLegacySave(dimensionSamples),
      };
      // eslint-disable-next-line no-console
      console.log('[ai-parse-multi-sql] payload:', payload);
      const resp = await fetch(`${API_BASE}/api/ai-parse-multi-sql`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify(payload),
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

      let mergedFields: GlobalSchemaField[] = [];
      flushSync(() => {
        setStandardFields((prev) => {
          mergedFields = mergeAiSuggestionsIntoStandardFields(prev, sug, jp, concatSug, lockedStandardKeys);
          return mergedFields;
        });
      });
      setIsSuccess(true);

      const mappingAfterAi = buildCertifiedMapping(mergedFields);
      const firstHit = mappingAfterAi.find((m) => mappingEntryIsPublishable(m));
      if (firstHit) setActiveFieldKey(firstHit.standardKey);

      const unfilled: Record<string, boolean> = {};
      for (const f of initialStandardFields) {
        const ent = mappingAfterAi.find((x) => x.standardKey === f.standardKey);
        if (!ent || !mappingEntryIsPublishable(ent)) unfilled[f.standardKey] = true;
      }
      setAiUnfilledAfterRun(unfilled);

      void runPreviewMappingRow(mappingAfterAi);

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
    const append = concatPickNext;
    if (append) setConcatPickNext(false);

    const masterLogical = String(masterTable || '').trim();
    const targetLogical = logicalTableFromDataFile(sourceFile);
    const buildToken = (): string => {
      if (append) return `${physicalColumn}@${sourceFile}`;
      if (masterLogical && targetLogical && targetLogical !== masterLogical) {
        const masterFile =
          Object.keys(tables || {}).find((fn) => logicalTableFromDataFile(fn) === masterLogical) || '';
        const masterCols = masterFile ? tables[masterFile] || [] : [];
        const fk = pickFkColumnOnMaster(masterCols, activeFieldKey);
        const fkUse = fk || `${activeFieldKey}_id`;
        return `CHAIN|${masterLogical}.${fkUse}->${targetLogical}.id->${targetLogical}.${physicalColumn}`;
      }
      return `${physicalColumn}@${sourceFile}`;
    };
    const tok = buildToken();

    let nextFields: GlobalSchemaField[] = [];
    flushSync(() => {
      setStandardFields((prev) => {
        nextFields = prev.map((f) => {
          if (f.standardKey !== activeFieldKey) return f;
          const cur = (f.mappedSources || []).map((s) => String(s).trim()).filter(Boolean);
          if (append) {
            if (!cur.length) return { ...f, mappedSources: [tok] };
            return { ...f, mappedSources: [...cur, tok] };
          }
          return { ...f, mappedSources: [tok] };
        });
        return nextFields;
      });
    });
    // 手动纠偏后锁定该维度：后续 AI 建模不允许覆盖
    setLockedStandardKeys((prev) => {
      const next = new Set(prev);
      next.add(activeFieldKey);
      return next;
    });
    if (nextFields.length) void runPreviewMappingRow(buildCertifiedMapping(nextFields));
  };

  const handleSaveMappingAuthenticated = async (): Promise<{ ok: true } | { ok: false; error: string }> => {
    setError(null);
    try {
      const payload = {
        latestDir,
        mapping: mappingPreview,
        mappingAuthenticated: true,
      };
      const resp = await fetch(`${API_BASE}/api/save-mapping`, {
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
      const syncPayload = {
        mappingConfig: {
          latestDir,
          mapping: mappingPreview,
          mappingAuthenticated: true,
        },
      };
      const syncResp = await fetch(`${API_BASE}/api/certify-and-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(syncPayload),
      });
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
  const busy = isAiModeling || authSyncing || certifyEngineRunning || isUploadingSandbox || resolvingRow;
  const draftHydrating = schemaDraftLoadState === 'loading';
  const step12Disabled = busy || draftHydrating;
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
        <div className="relative flex flex-col gap-4">
          {draftHydrating && (
            <div
              className="absolute inset-0 z-30 flex items-start justify-center pt-[min(28vh,12rem)] rounded-xl bg-white/75 backdrop-blur-[1px] pointer-events-auto"
              aria-busy
              aria-label="加载草稿"
            >
              <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-800 shadow-md">
                正在从服务器加载草稿，请稍候…
              </div>
            </div>
          )}
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
            {/* Left: Sandbox sample files (storage/sandbox) */}
            <div className="col-span-12 lg:col-span-6 border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-2">
                <div className="space-y-1">
                  <div className="text-[11px] font-semibold text-slate-800">沙盒样本区（Sandbox - 仅用于配置验证）</div>
                  <div className="text-[11px] text-slate-500">此处文件仅供逻辑推导使用，建议上传包含 5-10 行典型数据的极简表。</div>
                </div>
                <label className="inline-flex items-center gap-2 px-3 py-1.5 text-[11px] font-medium rounded-lg bg-white border border-emerald-400 text-emerald-900 cursor-pointer hover:bg-emerald-50">
                  <input
                    type="file"
                    accept=".xlsx,.xls"
                    multiple
                    className="hidden"
                    disabled={step12Disabled}
                    onChange={(e) => void handleSandboxUpload(e)}
                  />
                  {isUploadingSandbox ? '上传中…' : '上传样本 XLSX'}
                </label>
              </div>
              <div className="p-3 space-y-2">
                <div className="text-[11px] text-slate-600">
                  最新目录：<span className="font-mono text-slate-800">{latestDir || '-'}</span>
                  <span className="mx-2 text-slate-300">|</span>
                  文件数：<span className="font-mono text-slate-800">{sandboxFiles.length}</span>
                </div>
                <div
                  key={`sandbox_list_${uploadCounter}`}
                  className="max-h-[260px] overflow-y-auto border border-slate-200 rounded-lg bg-white"
                >
                  {isUploadingSandbox || isSyncingSandboxFiles ? (
                    <div className="p-3 text-[11px] text-slate-600">正在同步文件列表...</div>
                  ) : sandboxFiles.length ? (
                    <div className="divide-y divide-slate-100">
                      {sandboxFiles.map((fileName) => (
                        <button
                          key={fileName}
                          type="button"
                          disabled={step12Disabled}
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
                    <div className="p-3 text-[11px] text-slate-600">⚠️ 沙盒目录为空，请上传样本 XLSX</div>
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
                  disabled={step12Disabled}
                />
                {isParsingDdl && <div className="text-[11px] text-slate-500">正在解析 DDL…</div>}
              </div>
            </div>
          </div>
        </div>

        {/* Step 2: 动态样本配置（核心重构） */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-3 py-2 bg-slate-900 text-white flex items-center justify-between gap-2">
            <div className="text-sm font-semibold">Step 2 · 对话驱动型智能配置中心（Chat-first）</div>
            <div className="text-[11px] text-slate-200">描述业务案例 → 一键智能建模 → 结果自动验证</div>
          </div>
          <div className="p-3 space-y-3">
            <div className="border border-slate-200 rounded-xl overflow-hidden bg-slate-50/40">
              <div className="px-3 py-2 bg-white border-b border-slate-200 flex items-center justify-between gap-2 flex-wrap">
                <div className="text-[11px] font-semibold text-slate-800">业务场景描述 (Conversational Input)</div>
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    disabled={step12Disabled || chatToConfigBusy || !String(conversationalInput || '').trim()}
                    onClick={async () => {
                      setChatToConfigBusy(true);
                      setError(null);
                      try {
                        // 若 Step1 DDL 尚未解析进内存：先强制解析，保证下拉数据源不为空
                        if (String(ddlText || '').trim() && Object.keys(parsedTableMap || {}).length === 0) {
                          await runParseDdlSchema(ddlText, 'manual');
                        }

                        const resp = await fetch(`${API_BASE}/api/parse-chat-to-samples`, {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({
                            text: conversationalInput,
                            masterTable: masterTable || undefined,
                            ddlTableNames,
                            ddlText,
                          }),
                        });
                        const json = (await resp.json().catch(() => null)) as any;
                        if (!resp.ok || !json?.ok) throw new Error(json?.error || `HTTP ${resp.status}`);
                        const ds = json.dimensionSamples && typeof json.dimensionSamples === 'object' ? (json.dimensionSamples as any) : null;
                        if (!ds) throw new Error('AI 返回的 dimensionSamples 为空');

                        // 若 AI 提取的 tableName 在 DDL 中存在，但当前 parsedTableMap 缺失：再强制刷新一次 DDL 解析
                        try {
                          const mentioned = new Set<string>();
                          for (const f of initialStandardFields) {
                            const k = f.standardKey;
                            const blk = ds[k] && typeof ds[k] === 'object' ? ds[k] : null;
                            const rowsRaw = Array.isArray(blk?.rows) ? blk.rows : [];
                            for (const r of rowsRaw) {
                              const segs = Array.isArray(r?.segments) ? r.segments : [];
                              for (const s of segs) {
                                const tn = String(s?.tableName || '').trim();
                                if (tn) mentioned.add(tn);
                              }
                            }
                          }
                          const missing = Array.from(mentioned).filter((tn) => !(tn in (parsedTableMap || {})));
                          if (missing.length && String(ddlText || '').trim()) {
                            await runParseDdlSchema(ddlText, 'manual');
                          }
                        } catch {
                          // ignore
                        }

                        // 自动补全 tableName/fieldName：基于 Step1 解析出的 DDL 字段做快速启发式匹配
                        setDimensionSamples((prev) => {
                          const next: DimensionSamplesState = { ...(prev || createEmptyDimensionSamples()) };
                          for (const f of initialStandardFields) {
                            const k = f.standardKey;
                            const blk = ds[k] && typeof ds[k] === 'object' ? ds[k] : null;
                            const targetGoal = typeof blk?.targetGoal === 'string' ? String(blk.targetGoal || '') : '';
                            const rowsRaw = Array.isArray(blk?.rows) ? blk.rows : [];
                            const suggestion = suggestTableFieldFromDdl(parsedTableMap, k);
                            const rows = rowsRaw
                              .map((r: any) => {
                                const notes = String(r?.notes || '');
                                const id = String(r?.id || '') || newGoldenRowId();
                                const segs = Array.isArray(r?.segments) ? r.segments : [];
                                const segments = segs.map((s: any) => {
                                  const value = String(s?.value ?? '');
                                  const tableName = String(s?.tableName || '').trim() || suggestion.tableName;
                                  const fieldName = String(s?.fieldName || '').trim() || suggestion.fieldName;
                                  return { tableName, fieldName, value };
                                });
                                return {
                                  id,
                                  notes,
                                  segments: segments.length
                                    ? segments
                                    : [{ tableName: suggestion.tableName, fieldName: suggestion.fieldName, value: '' }],
                                };
                              })
                              .filter(
                                (r: any) =>
                                  Array.isArray(r.segments) && r.segments.some((s: any) => String(s?.value || '').trim() !== '')
                              );

                            // 坐标级回填：只覆盖 AI 明确提取到的维度；未提及的维度保持用户原状态（严禁重置）
                            if (rows.length || String(targetGoal || '').trim()) {
                              next[k] = { targetGoal, rows };
                            }
                          }
                          return next;
                        });
                        setReviewTweakOpen(false);

                        // 直接触发 AI 建模（串行维度 + 回测闭环）
                        const r = await handleAiLogicModeling();
                        if (!r.ok) throw new Error(r.error);
                      } catch (e) {
                        const msg = e instanceof Error ? e.message : String(e);
                        setError(msg);
                      } finally {
                        setChatToConfigBusy(false);
                      }
                    }}
                    className="px-4 py-2 text-[12px] font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                    title="一键：对话解析 → 建模推导 → 物理回测 → Step4 实时变绿"
                  >
                    {chatToConfigBusy ? '一键全自动建模中…' : '一键全自动建模 (AI Auto-Modeling)'}
                  </button>
                  <button
                    type="button"
                    disabled={step12Disabled}
                    onClick={() => setReviewTweakOpen((v) => !v)}
                    className="px-3 py-2 text-[11px] font-semibold rounded-lg bg-white border border-slate-200 text-slate-700 hover:bg-slate-50 disabled:opacity-50"
                    title="展开后可查看 AI 识别详情并手动微调"
                  >
                    {reviewTweakOpen ? '收起：识别详情与微调' : '展开：识别详情与微调'}
                  </button>
                </div>
              </div>
              <div className="p-3 space-y-2 bg-white">
                <div className="text-[11px] text-slate-600">
                  请直接描述一个真实的业务数据案例。例如：‘款号 A 对应品牌 B，楦头是 C，材质是由 D 和 E 拼成的’。
                </div>
                <textarea
                  value={conversationalInput}
                  onChange={(e) => setConversationalInput(e.target.value)}
                  placeholder="请直接描述一个真实的业务数据案例。例如：‘款号 A 对应品牌 B，楦头是 C，材质是由 D 和 E 拼成的’。"
                  className="w-full min-h-[260px] p-3 text-xs border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent resize-y text-slate-800 bg-white"
                  disabled={step12Disabled || chatToConfigBusy}
                />
              </div>
            </div>

            <div className="grid grid-cols-12 gap-2 items-end">
              <div className="col-span-12 lg:col-span-6">
                <div className="text-[11px] font-medium text-slate-700 mb-1">业务主表（Master Table）</div>
                <select
                  value={masterTable}
                  onChange={(e) => setMasterTable(e.target.value)}
                  disabled={ddlTableNames.length === 0 || step12Disabled}
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
                  onClick={async () => {
                    setDraftSaving(true);
                    const r = await saveSchemaDraft();
                    setDraftSaving(false);
                    if (r.ok) {
                      setDraftSaveFeedback(true);
                      if (draftSaveFeedbackTimerRef.current) window.clearTimeout(draftSaveFeedbackTimerRef.current);
                      draftSaveFeedbackTimerRef.current = window.setTimeout(() => setDraftSaveFeedback(false), 2000);
                    }
                  }}
                  disabled={step12Disabled}
                  className="px-3 py-2 text-[11px] font-semibold rounded-lg bg-slate-900 text-white hover:bg-slate-800 disabled:opacity-50"
                  title="写入 server/storage/schema_draft.json"
                >
                  {draftSaving ? '正在保存...' : draftSaveFeedback ? '✅ 成功存至服务器(持久化)' : '保存当前配置草稿'}
                </button>
                <button
                  type="button"
                  disabled={step12Disabled}
                  onClick={() => {
                    setError(null);
                    setHasAttemptedAuth(false);
                    setAuthSyncHint('');
                    setIsSuccess(false);
                    setDdlText('');
                    setParsedTables([]);
                    setMasterTable('');
                    setDimensionSamples(createEmptyDimensionSamples());
                    setConcatPickNext(false);
                    setStandardFields(initialStandardFields);
                    setAiUnfilledAfterRun({});
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

            {!reviewTweakOpen ? (
              <div className="border border-slate-200 rounded-xl bg-slate-50/40 px-3 py-3 text-[11px] text-slate-600">
                <span className="font-semibold text-slate-800">AI 识别详情与手动微调 (Review &amp; Tweak)</span>
                <span className="ml-2 text-slate-500">默认已折叠。点击上方按钮展开后，可查看维度分组样本并进行微调。</span>
              </div>
            ) : (
              <div className="border border-slate-200 rounded-xl">
                <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-2 flex-wrap">
                  <div className="text-[11px] font-semibold text-slate-800">AI 识别详情与手动微调 (Review &amp; Tweak)</div>
                </div>
                <div className="p-3 space-y-3">
                <div className="text-[11px] text-slate-600">
                  每个维度独立配置<strong>目标期望值</strong>与若干<strong>样本行</strong>。行内可用「+ 组合」为多段物理字段（拼接），并在右侧填写<strong>逻辑备注/线索</strong>告知
                  AI（例如父类/子类）。例：材质目标 <code className="font-mono">LT04</code>，两行分别备注「这是父类」「这是子类，需拼接」。
                </div>
                {Object.keys(parsedTableMap || {}).length === 0 ? (
                  <div className="text-[11px] text-slate-500">请先解析 SQL 或等待解析完成…</div>
                ) : (
                  <div className="space-y-4">
                    {initialStandardFields.map((dim) => {
                      const block = dimensionSamples[dim.standardKey] || { targetGoal: '', rows: [] };
                      const dimActive = activeFieldKey === dim.standardKey;
                      return (
                        <div
                          key={dim.standardKey}
                          ref={(el) => {
                            step2DimGroupRef.current[dim.standardKey] = el;
                          }}
                          className={cn(
                            'rounded-xl border p-3 space-y-3 bg-white transition-shadow',
                            dimActive ? 'border-indigo-500 ring-2 ring-indigo-100 shadow-sm' : 'border-slate-200'
                          )}
                        >
                          <div className="flex flex-col lg:flex-row lg:items-end gap-2">
                            <div className="min-w-0 flex-1">
                              <div className="text-[12px] font-semibold text-slate-900">
                                {dim.standardName}{' '}
                                <span className="font-mono text-[10px] font-normal text-slate-500">({dim.standardKey})</span>
                              </div>
                              <div className="text-[10px] text-slate-500 mt-0.5">该维度下的样本行与备注仅作用于本维度</div>
                            </div>
                            <div className="w-full lg:w-[min(100%,20rem)] shrink-0">
                              <div className="text-[10px] font-medium text-slate-600 mb-1">该维度最终期望值 (Target Value)</div>
                              <input
                                value={block.targetGoal}
                                disabled={step12Disabled}
                                onChange={(e) => {
                                  const v = e.target.value;
                                  setDimensionSamples((prev) => {
                                    const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                    return { ...prev, [dim.standardKey]: { ...b, targetGoal: v } };
                                  });
                                }}
                                placeholder="例：LT04"
                                className="w-full text-[11px] border border-slate-200 rounded-lg px-2 py-2 bg-white text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                              />
                            </div>
                            <button
                              type="button"
                              disabled={Object.keys(parsedTableMap || {}).length === 0 || step12Disabled}
                              onClick={() =>
                                setDimensionSamples((prev) => {
                                  const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                  return {
                                    ...prev,
                                    [dim.standardKey]: { ...b, rows: [...b.rows, emptyGoldenDimensionRow()] },
                                  };
                                })
                              }
                              className="shrink-0 px-2.5 py-2 text-[11px] font-semibold rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                            >
                              + 本维度样本行
                            </button>
                          </div>

                          {block.rows.length === 0 ? (
                            <div className="text-[11px] text-slate-400 border border-dashed border-slate-200 rounded-lg px-3 py-2">
                              暂无样本行。点击「+ 本维度样本行」添加。
                            </div>
                          ) : (
                            block.rows.map((row) => (
                              <div
                                key={row.id}
                                className="border border-slate-100 rounded-lg p-2 space-y-2 bg-slate-50/40"
                              >
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="text-[10px] text-slate-500 font-mono truncate max-w-[12rem]" title={row.id}>
                                    行 {row.id.slice(-8)}
                                  </span>
                                  <button
                                    type="button"
                                    disabled={step12Disabled}
                                    onClick={() =>
                                      setDimensionSamples((prev) => {
                                        const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                        return {
                                          ...prev,
                                          [dim.standardKey]: {
                                            ...b,
                                            rows: b.rows.filter((r) => r.id !== row.id),
                                          },
                                        };
                                      })
                                    }
                                    className="ml-auto text-[11px] px-2 py-1 rounded-lg border border-slate-200 text-slate-600 hover:bg-white bg-white"
                                  >
                                    删除本行
                                  </button>
                                </div>
                                <div className="flex flex-col xl:flex-row gap-2 xl:items-start">
                                  <div className="flex-1 min-w-0 space-y-2">
                                    {row.segments.map((seg, segIdx) => {
                                      const colsRaw = seg.tableName ? parsedTableMap[seg.tableName] || [] : [];
                                      const cols = sortDdlColumnsForGolden(colsRaw, masterTable, seg.tableName);
                                      return (
                                        <div key={`${row.id}_seg_${segIdx}`} className="grid grid-cols-12 gap-2 items-center">
                                          <select
                                            value={seg.tableName}
                                            disabled={step12Disabled}
                                            onChange={(e) => {
                                              const nextTable = e.target.value;
                                              setDimensionSamples((prev) => {
                                                const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                                return {
                                                  ...prev,
                                                  [dim.standardKey]: {
                                                    ...b,
                                                    rows: b.rows.map((r) => {
                                                      if (r.id !== row.id) return r;
                                                      const nextSegs = r.segments.map((s, i) =>
                                                        i === segIdx ? { ...s, tableName: nextTable, fieldName: '' } : s
                                                      );
                                                      return { ...r, segments: nextSegs };
                                                    }),
                                                  },
                                                };
                                              });
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
                                            disabled={!seg.tableName || step12Disabled}
                                            onChange={(e) => {
                                              const nextField = e.target.value;
                                              setDimensionSamples((prev) => {
                                                const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                                return {
                                                  ...prev,
                                                  [dim.standardKey]: {
                                                    ...b,
                                                    rows: b.rows.map((r) => {
                                                      if (r.id !== row.id) return r;
                                                      const nextSegs = r.segments.map((s, i) =>
                                                        i === segIdx ? { ...s, fieldName: nextField } : s
                                                      );
                                                      return { ...r, segments: nextSegs };
                                                    }),
                                                  },
                                                };
                                              });
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
                                            disabled={step12Disabled}
                                            onChange={(e) => {
                                              const v = e.target.value;
                                              setDimensionSamples((prev) => {
                                                const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                                return {
                                                  ...prev,
                                                  [dim.standardKey]: {
                                                    ...b,
                                                    rows: b.rows.map((r) => {
                                                      if (r.id !== row.id) return r;
                                                      const nextSegs = r.segments.map((s, i) =>
                                                        i === segIdx ? { ...s, value: v } : s
                                                      );
                                                      return { ...r, segments: nextSegs };
                                                    }),
                                                  },
                                                };
                                              });
                                            }}
                                            placeholder="该段样本值（如 LT / 04）"
                                            className="col-span-12 lg:col-span-3 text-[11px] border border-slate-200 rounded-lg px-2 py-2 bg-white text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                                          />
                                          <button
                                            type="button"
                                            disabled={step12Disabled}
                                            onClick={() =>
                                              setDimensionSamples((prev) => {
                                                const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                                return {
                                                  ...prev,
                                                  [dim.standardKey]: {
                                                    ...b,
                                                    rows: b.rows.map((r) => {
                                                      if (r.id !== row.id) return r;
                                                      return {
                                                        ...r,
                                                        segments: [
                                                          ...r.segments.slice(0, segIdx + 1),
                                                          { tableName: '', fieldName: '', value: '' },
                                                          ...r.segments.slice(segIdx + 1),
                                                        ],
                                                      };
                                                    }),
                                                  },
                                                };
                                              })
                                            }
                                            className="col-span-6 sm:col-span-6 lg:col-span-1 text-[11px] px-2 py-2 rounded-lg bg-indigo-50 border border-indigo-200 text-indigo-800 hover:bg-indigo-100"
                                            title="本行内追加一段物理字段（同一样本行的拼接）"
                                          >
                                            + 组合
                                          </button>
                                          <button
                                            type="button"
                                            disabled={step12Disabled || row.segments.length <= 1}
                                            onClick={() =>
                                              setDimensionSamples((prev) => {
                                                const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                                return {
                                                  ...prev,
                                                  [dim.standardKey]: {
                                                    ...b,
                                                    rows: b.rows.map((r) => {
                                                      if (r.id !== row.id) return r;
                                                      if (r.segments.length <= 1) return r;
                                                      return { ...r, segments: r.segments.filter((_, i) => i !== segIdx) };
                                                    }),
                                                  },
                                                };
                                              })
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
                                  <div className="w-full xl:w-52 shrink-0 xl:pt-0">
                                    <div className="text-[10px] font-medium text-slate-600 mb-1">逻辑备注/线索</div>
                                    <textarea
                                      value={row.notes}
                                      disabled={step12Disabled}
                                      onChange={(e) => {
                                        const v = e.target.value;
                                        setDimensionSamples((prev) => {
                                          const b = prev[dim.standardKey] || { targetGoal: '', rows: [] };
                                          return {
                                            ...prev,
                                            [dim.standardKey]: {
                                              ...b,
                                              rows: b.rows.map((r) => (r.id === row.id ? { ...r, notes: v } : r)),
                                            },
                                          };
                                        });
                                      }}
                                      placeholder="例：这是父类 / 子类需拼接"
                                      rows={3}
                                      className="w-full text-[11px] border border-slate-200 rounded-lg px-2 py-2 bg-white text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500 resize-y min-h-[4.5rem]"
                                    />
                                  </div>
                                </div>
                              </div>
                            ))
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
                </div>
              </div>
            )}
          </div>
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
                const pm = previewMatchByStandardKey[m.standardKey] || { expected: '', actual: '', ok: false, hasExpectation: false };
                const hasPreview = Boolean(pm.actual);
                const isMismatch = has && pm.hasExpectation && !pm.ok;
                const isMatched = has && pm.hasExpectation && pm.ok;
                const brandIdWarning =
                  m.standardKey === 'brand' && hasPreview && /^\d+$/.test(String(pm.actual).trim());
                return (
                  <button
                    key={m.standardKey}
                    type="button"
                    onClick={() => {
                      const sk = m.standardKey;
                      setActiveFieldKey(sk);
                      window.requestAnimationFrame(() => {
                        step2DimGroupRef.current[sk]?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                      });
                    }}
                    className={cn(
                      "w-full text-left rounded-xl border px-3 py-2 transition-all",
                      isActive ? "border-indigo-500 ring-2 ring-indigo-100" : "border-slate-200 hover:bg-slate-50",
                      !has
                        ? "bg-slate-50"
                        : brandIdWarning
                          ? "bg-amber-50/70 border-amber-300"
                          : isMismatch
                            ? "bg-orange-50/60 border-orange-200"
                            : isMatched
                              ? "bg-emerald-50/60 border-emerald-200"
                              : isConcat
                                ? "bg-violet-50/50"
                                : isJoin
                                  ? "bg-amber-50/40"
                                  : "bg-emerald-50/40"
                    )}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-slate-900 flex items-center gap-2 min-w-0">
                        <span className="truncate">{m.standardName}</span>
                        {isMatched ? (
                          <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" aria-hidden />
                        ) : isMismatch ? (
                          <AlertCircle className="w-4 h-4 text-orange-600 shrink-0" aria-hidden />
                        ) : null}
                      </div>
                      <span className="text-[10px] font-mono text-slate-500 bg-white border border-slate-200 px-1.5 py-0.5 rounded">
                        {m.standardKey}
                      </span>
                    </div>
                    <div className="mt-1 text-[11px]">
                      {!has ? (
                        aiUnfilledAfterRun[m.standardKey] ? (
                          <span className="text-amber-800">AI 尽力了，请手动在右侧纠偏</span>
                        ) : (
                          <span className="text-slate-400">未回填</span>
                        )
                      ) : isMismatch ? (
                        <span className="text-orange-800 font-medium">路径偏移，请检查终点字段</span>
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
                    {has && pm.hasExpectation && (
                      <div className="mt-1 text-[10px] text-slate-500">
                        期望：<span className="font-mono text-slate-700">{pm.expected || '-'}</span>
                        <span className="mx-2 text-slate-300">|</span>
                        预览：<span className={cn('font-mono', isMatched ? 'text-emerald-700' : isMismatch ? 'text-orange-700' : 'text-slate-700')}>{pm.actual || '-'}</span>
                      </div>
                    )}
                    {brandIdWarning && (
                      <div className="mt-1 text-[10px] font-medium text-amber-700 bg-amber-50/80 border border-amber-200/80 rounded px-1.5 py-0.5">
                        检测到 ID，请检查是否需要关联品牌名称表
                      </div>
                    )}
                  </button>
                );
              })}
            </div>

            <button
              type="button"
              disabled={busy}
              onClick={async () => {
                try {
                  const resp = await fetch(`${API_BASE}/api/ai-trace-log`);
                  const json = (await resp.json().catch(() => null)) as any;
                  if (!resp.ok || !json?.ok) throw new Error(json?.error || `HTTP ${resp.status}`);
                  const text = String(json.text || '');
                  const w = window.open('', '_blank');
                  if (!w) return;
                  w.document.open();
                  w.document.write(
                    `<pre style="white-space:pre-wrap;word-break:break-word;font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;font-size:12px;margin:16px;">${text
                      .replace(/&/g, '&amp;')
                      .replace(/</g, '&lt;')
                      .replace(/>/g, '&gt;')}</pre>`
                  );
                  w.document.close();
                } catch (e) {
                  // eslint-disable-next-line no-console
                  console.warn('[ai-trace-log] open failed', e);
                }
              }}
              className="w-full mt-1 px-3 py-2 text-[11px] font-semibold rounded-lg border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 disabled:opacity-50"
              title="打开 server/storage/ai_trace.log（Prompt/Raw/Join Trace/Verdict）"
            >
              打开深度审计日志 (Debug Info)
            </button>
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
                  disabled={busy}
                  onClick={async () => {
                    try {
                      const resp = await fetch(`${API_BASE}/api/engine-audit-log`);
                      const json = (await resp.json().catch(() => null)) as any;
                      if (!resp.ok || !json?.ok) throw new Error(json?.error || `HTTP ${resp.status}`);
                      const text = String(json.text || '');
                      const w = window.open('', '_blank');
                      if (!w) return;
                      w.document.open();
                      w.document.write(
                        `<pre style="white-space:pre-wrap;word-break:break-word;font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;font-size:12px;margin:16px;">${text
                          .replace(/&/g, '&amp;')
                          .replace(/</g, '&lt;')
                          .replace(/>/g, '&gt;')}</pre>`
                      );
                      w.document.close();
                    } catch (e) {
                      // eslint-disable-next-line no-console
                      console.warn('[engine-audit-log] open failed', e);
                    }
                  }}
                  className="px-3 py-1.5 text-[11px] font-semibold rounded-lg border disabled:opacity-50 bg-white border-slate-200 text-slate-700 hover:bg-slate-50"
                  title="打开 server/storage/engine_audit.log"
                >
                  下载/查看完整逻辑审计日志
                </button>
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
                        表（{sandboxFiles.length}）
                      </div>
                      <div key={`sandbox_pick_${uploadCounter}`} className="max-h-[240px] overflow-y-auto p-2 space-y-1">
                        {isUploadingSandbox || isSyncingSandboxFiles ? (
                          <div className="text-[11px] text-slate-600 p-2">正在同步文件列表...</div>
                        ) : sandboxFiles.length ? (
                          sandboxFiles.map((fileName) => (
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
                          <div className="text-[11px] text-slate-600 p-2">⚠️ 沙盒目录为空，请上传样本 XLSX</div>
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
