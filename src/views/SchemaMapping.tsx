import React, { useState } from 'react';
import { Database, Save, Wand2, FileText, CheckCircle2, Link as LinkIcon, Code2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { GlobalSchemaField } from '@/types';

const initialStandardFields: GlobalSchemaField[] = [
  { id: 'f1', standardName: '款号', standardKey: 'styleCode', mappedSources: [], description: '产品的唯一标识符' },
  { id: 'f2', standardName: '品牌', standardKey: 'brand', mappedSources: [], description: '所属品牌名称' },
  { id: 'f3', standardName: '楦编号', standardKey: 'lastCode', mappedSources: [], description: '关联的 3D 楦头编号' },
  { id: 'f4', standardName: '大底编号', standardKey: 'soleCode', mappedSources: [], description: '关联的 3D 大底编号' },
  { id: 'f5', standardName: '颜色', standardKey: 'colorCode', mappedSources: [], description: '产品颜色编码' },
  { id: 'f6', standardName: '材质', standardKey: 'materialCode', mappedSources: [], description: '产品材质编码' },
  { id: 'f7', standardName: '状态', standardKey: 'status', mappedSources: [], description: '当前生命周期状态' },
];

export default function SchemaMapping() {
  const [standardFields, setStandardFields] = useState<GlobalSchemaField[]>(initialStandardFields);
  const [inputText, setInputText] = useState('');
  const [isOrganizing, setIsOrganizing] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);
  const [extractedFields, setExtractedFields] = useState<string[]>([]);

  const handleAutoOrganize = () => {
    if (!inputText.trim()) return;
    setIsOrganizing(true);
    setIsSuccess(false);
    setExtractedFields([]);
    
    // Simulate AI parsing and mapping
    setTimeout(() => {
      const updatedFields = [...initialStandardFields];
      const text = inputText.toLowerCase();
      const newExtracted: string[] = [];
      
      if (text.includes('style_wms') || text.includes('款号')) {
        updatedFields[0].mappedSources = ['style_wms', 'ods_pdm_style_no'];
        newExtracted.push('style_wms', 'ods_pdm_style_no');
      }
      if (text.includes('brand') || text.includes('品牌')) {
        updatedFields[1].mappedSources = ['brand_name', 'brand_id'];
        newExtracted.push('brand_name', 'brand_id');
      }
      if (text.includes('last') || text.includes('楦')) {
        updatedFields[2].mappedSources = ['associated_last_type', 'last_id'];
        newExtracted.push('associated_last_type', 'last_id');
      }
      if (text.includes('sole') || text.includes('底')) {
        updatedFields[3].mappedSources = ['sole_id', 'outsole_code'];
        newExtracted.push('sole_id', 'outsole_code');
      }
      if (text.includes('color') || text.includes('颜色')) {
        updatedFields[4].mappedSources = ['color_code', 'color_way'];
        newExtracted.push('color_code', 'color_way');
      }
      if (text.includes('material') || text.includes('材质')) {
        updatedFields[5].mappedSources = ['material_code', 'upper_material'];
        newExtracted.push('material_code', 'upper_material');
      }
      if (text.includes('status') || text.includes('状态')) {
        updatedFields[6].mappedSources = ['data_status', 'lifecycle_status'];
        newExtracted.push('data_status', 'lifecycle_status');
      }

      // If no specific match, just mock some
      if (newExtracted.length === 0) {
        newExtracted.push('unknown_field_1', 'unknown_field_2');
      }

      setExtractedFields(newExtracted);
      setStandardFields(updatedFields);
      setIsOrganizing(false);
      setIsSuccess(true);
      
      // Hide success message after a few seconds
      setTimeout(() => setIsSuccess(false), 3000);
    }, 2000);
  };

  return (
    <div className="flex h-[calc(100vh-8rem)] gap-6 animate-in fade-in duration-500">
      {/* Left Sidebar: Standard Fields List */}
      <div className="w-80 bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col overflow-hidden shrink-0">
        <div className="p-4 border-b border-slate-200 bg-slate-50/50">
          <h2 className="font-semibold text-slate-900">全局标准字段模型</h2>
          <p className="text-xs text-slate-500 mt-1">系统核心业务实体属性</p>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-3">
          {standardFields.map(field => (
            <div key={field.id} className="p-3 rounded-lg border border-slate-100 bg-white shadow-sm hover:border-blue-200 transition-colors">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-blue-500" />
                  <span className="font-medium text-slate-900">{field.standardName}</span>
                </div>
                <span className="text-xs font-mono text-slate-400 bg-slate-50 px-1.5 py-0.5 rounded">{field.standardKey}</span>
              </div>
              
              {/* Mapped Sources Tags */}
              <div className="mt-3">
                {field.mappedSources.length > 0 ? (
                  <div className="flex flex-wrap gap-1.5">
                    {field.mappedSources.map((source, idx) => (
                      <span key={idx} className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium bg-indigo-50 text-indigo-700 border border-indigo-100 animate-in zoom-in duration-300" style={{ animationDelay: `${idx * 100}ms` }}>
                        <LinkIcon className="w-3 h-3" />
                        {source}
                      </span>
                    ))}
                  </div>
                ) : (
                  <div className="text-xs text-slate-400 italic flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-slate-200"></span>
                    暂无映射字段
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Right Main Area: Parsing Area */}
      <div className="flex-1 flex flex-col gap-4 min-w-0">
        {/* Header */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">智能字段映射引擎</h2>
            <p className="text-sm text-slate-500 mt-1">支持多表 DDL 或数据字典混合解析，自动建立多对一映射关系。</p>
          </div>
          <button className="flex items-center gap-2 px-4 py-2 bg-slate-900 text-white text-sm font-medium rounded-lg hover:bg-slate-800 transition-colors shadow-sm disabled:opacity-50">
            <Save className="w-4 h-4" />
            保存全局映射
          </button>
        </div>

        {/* Input Area */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col flex-1 min-h-0 relative">
          <div className="p-4 border-b border-slate-200 bg-slate-50/50 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <FileText className="w-4 h-4 text-slate-500" />
              <h3 className="text-sm font-medium text-slate-700">数据源解析区</h3>
            </div>
            {isSuccess && (
              <div className="flex items-center gap-1.5 text-emerald-600 text-sm font-medium animate-in fade-in slide-in-from-right-4">
                <CheckCircle2 className="w-4 h-4" />
                解析并映射成功
              </div>
            )}
          </div>
          <div className="p-4 flex-1 flex flex-col gap-4 overflow-hidden">
            <textarea
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
              placeholder="请在此处粘贴多张表的 DDL 建表语句或字段描述信息...&#10;&#10;例如：&#10;CREATE TABLE ods_pdm_style (&#10;  style_wms VARCHAR(50) COMMENT '款号',&#10;  brand_name VARCHAR(50) COMMENT '品牌',&#10;  associated_last_type VARCHAR(50) COMMENT '关联楦头'&#10;);&#10;&#10;CREATE TABLE erp_inventory (&#10;  style_no VARCHAR(50) COMMENT '产品款号',&#10;  color_code VARCHAR(20) COMMENT '颜色编码'&#10;);"
              className="flex-1 w-full p-4 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none font-mono text-slate-600 leading-relaxed bg-slate-50/30"
            />
            
            {/* Extracted Fields Display */}
            {extractedFields.length > 0 && (
              <div className="bg-slate-50 border border-slate-200 rounded-lg p-4 animate-in slide-in-from-bottom-4 duration-500">
                <div className="flex items-center gap-2 mb-3">
                  <Code2 className="w-4 h-4 text-indigo-500" />
                  <h4 className="text-sm font-medium text-slate-700">已提取的物理字段 ({extractedFields.length})</h4>
                </div>
                <div className="flex flex-wrap gap-2">
                  {extractedFields.map((field, idx) => (
                    <span 
                      key={idx} 
                      className="px-2.5 py-1 bg-white border border-slate-200 text-slate-600 text-xs font-mono rounded-md shadow-sm animate-in zoom-in duration-300"
                      style={{ animationDelay: `${idx * 50}ms` }}
                    >
                      {field}
                    </span>
                  ))}
                </div>
              </div>
            )}

            <div className="flex justify-center shrink-0">
              <button 
                onClick={handleAutoOrganize}
                disabled={isOrganizing || !inputText.trim()}
                className="flex items-center gap-2 px-8 py-3.5 bg-indigo-600 text-white text-sm font-medium rounded-xl hover:bg-indigo-700 transition-all shadow-sm disabled:opacity-50 disabled:hover:bg-indigo-600 hover:shadow-md"
              >
                <Wand2 className={cn("w-5 h-5", isOrganizing && "animate-spin")} />
                {isOrganizing ? 'AI 引擎深度解析中...' : '一键智能化梳理全局关系'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
