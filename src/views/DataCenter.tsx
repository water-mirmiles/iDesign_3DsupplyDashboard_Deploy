import React, { useState } from 'react';
import { UploadCloud, FileSpreadsheet, Box, CheckCircle2, Clock, FileWarning, Search, Info, Calendar } from 'lucide-react';
import { cn } from '@/lib/utils';
import { ImportHistory } from '@/types';

const mockHistory: ImportHistory[] = [
  { id: '1', fileName: '20240520_StyleList.xlsx', type: 'xlsx', status: 'success', uploadTime: '2024-05-20 14:30', snapshotDate: '2024-05-20', operator: 'Admin', matchedCount: 1250, version: 'v1.2', updateType: 'overwrite', targetTable: '款号主表' },
  { id: '2', fileName: 'NK_Lasts_Batch1.zip', type: '3d_model', status: 'success', uploadTime: '2024-05-20 11:15', operator: 'Admin', matchedCount: 45, version: 'v1.0', updateType: 'retain', targetTable: '3D 楦头库' },
  { id: '3', fileName: '20240519_AD_Soles.xlsx', type: 'xlsx', status: 'processing', uploadTime: '2024-05-20 09:00', snapshotDate: '2024-05-19', operator: 'Admin', version: 'v1.1', updateType: 'overwrite', targetTable: '大底关联表' },
  { id: '4', fileName: 'Invalid_Data.xlsx', type: 'xlsx', status: 'failed', uploadTime: '2024-05-19 16:45', operator: 'Admin', targetTable: '未知' },
];

export default function DataCenter() {
  const [activeTab, setActiveTab] = useState<'xlsx' | '3d'>('xlsx');

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">数据导入中心</h1>
        <p className="text-sm text-slate-500 mt-1">上传业务表格与 3D 资产文件，系统将自动进行匹配</p>
      </div>

      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
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
          <div className="border-2 border-dashed border-slate-300 rounded-xl bg-slate-50 hover:bg-slate-100 transition-colors cursor-pointer flex flex-col items-center justify-center py-16 px-4 text-center">
            <div className="p-4 bg-white rounded-full shadow-sm mb-4">
              <UploadCloud className="w-8 h-8 text-blue-500" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">
              点击或拖拽文件到此处上传
            </h3>
            <p className="text-sm text-slate-500 max-w-md mb-4">
              {activeTab === 'xlsx' 
                ? "支持 .xlsx, .xls 格式。请确保表格包含款号、品牌等关键字段以便系统解析。"
                : "支持 .obj, .stl, .zip 格式。系统将尝试通过文件名自动匹配对应的款号或资产编号。"}
            </p>
            
            <div className="flex items-center gap-2 text-xs text-amber-600 bg-amber-50 px-3 py-1.5 rounded-md border border-amber-100">
              <Info className="w-3.5 h-3.5" />
              <span>建议命名规范：<strong>日期前缀_文件名.xlsx</strong>（例如：20240414_product_info.xlsx，系统将自动提取为快照时间点）</span>
            </div>

            <button className="mt-6 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
              选择文件
            </button>
          </div>
        </div>
      </div>

      {/* History List */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
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
              {mockHistory.map((item) => (
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
