import React, { useState } from 'react';
import { Search, Filter, Download, MoreHorizontal, CheckCircle2, XCircle, Database, Box, Layers, X, DownloadCloud, ChevronDown } from 'lucide-react';
import { cn } from '@/lib/utils';
import { InventoryItem } from '@/types';

const mockInventory: InventoryItem[] = [
  { id: '1', style_wms: 'NK-RUN-2024-001', brand: 'Nike', colorCode: 'C01-BLK', colorHex: '#000000', materialCode: 'M-MESH-01', materialThumb: 'https://picsum.photos/seed/m1/24/16', lastCode: 'LST-NK-2024-A1', lastStatus: 'matched', soleCode: 'SOL-NK-A1-01', soleStatus: 'matched', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-20', updatedBy: 'Admin' },
  { id: '2', style_wms: 'NK-RUN-2024-002', brand: 'Nike', colorCode: 'C02-WHT', colorHex: '#FFFFFF', materialCode: 'M-MESH-01', materialThumb: 'https://picsum.photos/seed/m1/24/16', lastCode: 'LST-NK-2024-A1', lastStatus: 'matched', soleCode: 'SOL-NK-A1-02', soleStatus: 'matched', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-20', updatedBy: 'Admin' },
  { id: '3', style_wms: 'AD-CAS-2024-X1', brand: 'Adidas', colorCode: 'C15-RED', colorHex: '#EF4444', materialCode: 'M-LTHR-05', materialThumb: 'https://picsum.photos/seed/m2/24/16', lastCode: 'LST-AD-CAS-X', lastStatus: 'missing', soleCode: 'SOL-AD-CX-1', soleStatus: 'matched', data_status: 'draft', sourceTable: 'erp_inventory', lastUpdated: '2024-05-19', updatedBy: 'System' },
  { id: '4', style_wms: 'PM-SNEAK-V2', brand: 'Puma', colorCode: 'C08-BLU', colorHex: '#3B82F6', materialCode: 'M-SUED-02', materialThumb: 'https://picsum.photos/seed/m3/24/16', lastCode: 'LST-PM-SNK-V2', lastStatus: 'matched', soleCode: 'SOL-PM-SV2', soleStatus: 'missing', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-18', updatedBy: 'User_A' },
  { id: '5', style_wms: 'NB-WALK-500', brand: 'New Balance', colorCode: 'C04-GRY', colorHex: '#9CA3AF', materialCode: 'M-MESH-03', materialThumb: 'https://picsum.photos/seed/m4/24/16', lastCode: 'LST-NB-W500', lastStatus: 'matched', soleCode: 'SOL-NB-W500', soleStatus: 'matched', data_status: 'obsolete', sourceTable: 'legacy_db', lastUpdated: '2024-05-15', updatedBy: 'Admin' },
  { id: '6', style_wms: 'UA-TRAIN-X', brand: 'Under Armour', colorCode: 'C11-GRN', colorHex: '#10B981', materialCode: 'M-KNIT-01', materialThumb: 'https://picsum.photos/seed/m5/24/16', lastCode: 'LST-UA-TRX', lastStatus: 'missing', soleCode: 'SOL-UA-TRX', soleStatus: 'missing', data_status: 'draft', sourceTable: 'erp_inventory', lastUpdated: '2024-05-21', updatedBy: 'User_B' },
  { id: '7', style_wms: 'RBK-CLASSIC-1', brand: 'Reebok', colorCode: 'C02-WHT', colorHex: '#FFFFFF', materialCode: 'M-LTHR-01', materialThumb: 'https://picsum.photos/seed/m6/24/16', lastCode: 'LST-RBK-CL1', lastStatus: 'matched', soleCode: 'SOL-RBK-CL1', soleStatus: 'matched', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-21', updatedBy: 'Admin' },
  { id: '8', style_wms: 'AS-GEL-90', brand: 'Asics', colorCode: 'C22-NVY', colorHex: '#1E3A8A', materialCode: 'M-MESH-05', materialThumb: 'https://picsum.photos/seed/m7/24/16', lastCode: 'LST-AS-G90', lastStatus: 'matched', soleCode: 'SOL-AS-G90', soleStatus: 'missing', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-20', updatedBy: 'System' },
  { id: '9', style_wms: 'VN-SK8-HI', brand: 'Vans', colorCode: 'C01-BLK', colorHex: '#000000', materialCode: 'M-CNVS-01', materialThumb: 'https://picsum.photos/seed/m8/24/16', lastCode: 'LST-VN-SK8', lastStatus: 'missing', soleCode: 'SOL-VN-SK8', soleStatus: 'matched', data_status: 'active', sourceTable: 'erp_inventory', lastUpdated: '2024-05-19', updatedBy: 'User_A' },
  { id: '10', style_wms: 'NK-BBALL-Z', brand: 'Nike', colorCode: 'C33-PUR', colorHex: '#9333EA', materialCode: 'M-SYNT-02', materialThumb: 'https://picsum.photos/seed/m9/24/16', lastCode: 'LST-NK-BBZ', lastStatus: 'matched', soleCode: 'SOL-NK-BBZ', soleStatus: 'matched', data_status: 'draft', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-22', updatedBy: 'Admin' },
  { id: '11', style_wms: 'AD-TERREX-1', brand: 'Adidas', colorCode: 'C44-BRN', colorHex: '#78350F', materialCode: 'M-GTEX-01', materialThumb: 'https://picsum.photos/seed/m10/24/16', lastCode: 'LST-AD-TX1', lastStatus: 'matched', soleCode: 'SOL-AD-TX1', soleStatus: 'matched', data_status: 'active', sourceTable: 'ods_pdm_style', lastUpdated: '2024-05-22', updatedBy: 'System' },
];

const DataStatusBadge = ({ status }: { status: InventoryItem['data_status'] }) => {
  switch (status) {
    case 'active':
      return <span className="px-2 py-1 bg-emerald-50 text-emerald-700 border border-emerald-200 rounded-md text-xs font-medium">生效</span>;
    case 'draft':
      return <span className="px-2 py-1 bg-slate-100 text-slate-600 border border-slate-200 rounded-md text-xs font-medium">草稿</span>;
    case 'obsolete':
      return <span className="px-2 py-1 bg-red-50 text-red-700 border border-red-200 rounded-md text-xs font-medium">作废</span>;
    default:
      return null;
  }
};

interface PreviewModalProps {
  isOpen: boolean;
  onClose: () => void;
  assetCode: string;
  assetType: 'last' | 'sole';
}

const PreviewModal = ({ isOpen, onClose, assetCode, assetType }: PreviewModalProps) => {
  const [showAllStyles, setShowAllStyles] = useState(false);
  const [showAllSoles, setShowAllSoles] = useState(false);

  if (!isOpen) return null;

  const mockStyles = ['NK-RUN-2024-001', 'NK-RUN-2024-002', 'NK-RUN-2024-005', 'NK-TRAIN-X1', 'NK-CASUAL-09', 'NK-CASUAL-10', 'NK-CASUAL-11', 'NK-CASUAL-12', 'NK-CASUAL-13', 'NK-CASUAL-14'];
  const mockSoles = ['SOL-NK-A1-01', 'SOL-NK-A1-02', 'SOL-NK-A1-03', 'SOL-NK-A1-04', 'SOL-NK-A1-05', 'SOL-NK-A1-06'];

  const displayedStyles = showAllStyles ? mockStyles : mockStyles.slice(0, 5);
  const displayedSoles = showAllSoles ? mockSoles : mockSoles.slice(0, 3);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="bg-white w-[90vw] h-[85vh] rounded-2xl shadow-2xl overflow-hidden flex animate-in zoom-in-95 duration-200">
        
        {/* Left: 3D Canvas Area (70%) */}
        <div className="w-[70%] bg-slate-900 relative flex items-center justify-center">
          <div className="absolute top-4 left-4 flex items-center gap-2 bg-black/40 backdrop-blur-md px-3 py-1.5 rounded-lg text-white/80 text-sm">
            {assetType === 'last' ? <Box className="w-4 h-4" /> : <Layers className="w-4 h-4" />}
            <span className="font-mono">{assetCode}</span>
          </div>
          
          <div className="text-center">
            <div className="w-16 h-16 border-4 border-slate-700 border-t-blue-500 rounded-full animate-spin mx-auto mb-4"></div>
            <p className="text-slate-400 font-medium tracking-widest">3D Viewer 加载中...</p>
            <p className="text-slate-600 text-sm mt-2">Three.js Engine Initialization</p>
          </div>
        </div>

        {/* Right: Asset Details (30%) */}
        <div className="w-[30%] bg-white flex flex-col border-l border-slate-200">
          <div className="p-6 border-b border-slate-100 flex justify-between items-start">
            <div>
              <h2 className="text-xl font-bold text-slate-900">资产详情</h2>
              <p className="text-sm text-slate-500 mt-1">{assetType === 'last' ? '3D 楦头模型' : '3D 大底模型'}</p>
            </div>
            <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full text-slate-400 hover:text-slate-600 transition-colors">
              <X className="w-5 h-5" />
            </button>
          </div>

          <div className="p-6 flex-1 overflow-y-auto space-y-6">
            <div className="space-y-4">
              <div>
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">资产编号</label>
                <p className="text-sm font-mono text-slate-900 mt-1 bg-slate-50 px-2 py-1 rounded border border-slate-100">{assetCode}</p>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">源文件名</label>
                <p className="text-sm text-slate-900 mt-1">{assetCode}_v1.2.obj</p>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">上传日期</label>
                  <p className="text-sm text-slate-900 mt-1">2024-05-20</p>
                </div>
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">上传用户</label>
                  <p className="text-sm text-slate-900 mt-1">Admin</p>
                </div>
                <div>
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">文件大小</label>
                  <p className="text-sm text-slate-900 mt-1">12.4 MB</p>
                </div>
              </div>
            </div>

            <div className="pt-6 border-t border-slate-100">
              <div className="flex justify-between items-center mb-3">
                <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block">关联款号列表</label>
                <button 
                  onClick={() => setShowAllStyles(!showAllStyles)}
                  className="text-xs text-blue-600 hover:text-blue-700 font-medium transition-colors"
                >
                  {showAllStyles ? '收起' : `查看全部 (${mockStyles.length})`}
                </button>
              </div>
              <div className="space-y-2">
                {displayedStyles.map((style, idx) => (
                  <div key={idx} className="flex items-center justify-between p-2 rounded-lg bg-slate-50 border border-slate-100">
                    <span className="text-sm font-mono text-slate-700">{style}</span>
                    <span className="text-xs text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded font-medium">Active</span>
                  </div>
                ))}
              </div>
            </div>

            {assetType === 'last' && (
              <div className="pt-6 border-t border-slate-100">
                <div className="flex justify-between items-center mb-3">
                  <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block">关联大底列表</label>
                  <button 
                    onClick={() => setShowAllSoles(!showAllSoles)}
                    className="text-xs text-blue-600 hover:text-blue-700 font-medium transition-colors"
                  >
                    {showAllSoles ? '收起' : `查看全部 (${mockSoles.length})`}
                  </button>
                </div>
                <div className="space-y-2">
                  {displayedSoles.map((sole, idx) => (
                    <div key={idx} className="flex items-center justify-between p-2 rounded-lg bg-slate-50 border border-slate-100">
                      <span className="text-sm font-mono text-slate-700">{sole}</span>
                      <span className="text-xs text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded font-medium flex items-center gap-1">
                        <CheckCircle2 className="w-3 h-3" /> 3D 已匹配
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="p-6 border-t border-slate-100 bg-slate-50">
            <button className="w-full flex items-center justify-center gap-2 py-3 bg-blue-600 text-white rounded-xl font-medium hover:bg-blue-700 transition-colors shadow-sm">
              <DownloadCloud className="w-5 h-5" />
              下载 .obj 源文件
            </button>
          </div>
        </div>

      </div>
    </div>
  );
};

export default function InventoryList() {
  const [previewModal, setPreviewModal] = useState<{isOpen: boolean, assetCode: string, type: 'last'|'sole'}>({ isOpen: false, assetCode: '', type: 'last' });
  const [has3DFilter, setHas3DFilter] = useState<string>('all');

  const openPreview = (assetCode: string | undefined, type: 'last' | 'sole') => {
    if (assetCode) {
      setPreviewModal({ isOpen: true, assetCode, type });
    }
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500 pb-10">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">款号详细清单</h1>
          <p className="text-sm text-slate-500 mt-1">管理所有款号及其 3D 资产的匹配状态</p>
        </div>
        <div className="flex gap-3">
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-50 transition-colors shadow-sm">
            <Filter className="w-4 h-4" />
            高级筛选
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 text-slate-700 text-sm font-medium rounded-lg hover:bg-slate-50 transition-colors shadow-sm">
            <Download className="w-4 h-4" />
            导出报表
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        {/* Toolbar */}
        <div className="p-4 border-b border-slate-200 flex flex-wrap gap-4 justify-between items-center bg-slate-50/50">
          <div className="flex flex-wrap items-center gap-3">
            <div className="relative">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input 
                type="text" 
                placeholder="搜索款号或品牌..." 
                className="pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent w-64"
              />
            </div>
            
            {/* Brand Filter Mock */}
            <div className="relative">
              <select className="appearance-none pl-3 pr-8 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white text-slate-600 font-medium">
                <option value="all">所有品牌</option>
                <option value="nike">Nike</option>
                <option value="adidas">Adidas</option>
              </select>
              <ChevronDown className="w-4 h-4 absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
            </div>

            {/* Has 3D Filter */}
            <div className="relative">
              <select 
                value={has3DFilter}
                onChange={(e) => setHas3DFilter(e.target.value)}
                className="appearance-none pl-3 pr-8 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white text-slate-600 font-medium"
              >
                <option value="all">3D 文件: 全部</option>
                <option value="yes">有 3D 文件</option>
                <option value="no">无 3D 文件</option>
              </select>
              <ChevronDown className="w-4 h-4 absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
            </div>
          </div>
          <div className="text-sm text-slate-500">
            共 <span className="font-medium text-slate-900">{mockInventory.length}</span> 条记录
          </div>
        </div>

        {/* Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left whitespace-nowrap">
            <thead className="text-xs text-slate-500 bg-slate-50 uppercase border-b border-slate-200">
              <tr>
                <th className="px-5 py-3 font-medium">款号 (Style_WMS)</th>
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
              {mockInventory.map((item) => (
                <tr key={item.id} className="hover:bg-slate-50/50 transition-colors group">
                  <td className="px-5 py-4 font-medium text-slate-900">{item.style_wms}</td>
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
                        onClick={() => openPreview(item.lastCode, 'last')}
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
                        onClick={() => openPreview(item.soleCode, 'sole')}
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
          <span className="text-sm text-slate-500">显示 1 到 11 条，共 11 条</span>
          <div className="flex gap-1">
            <button className="px-3 py-1 border border-slate-200 rounded text-sm text-slate-600 bg-white hover:bg-slate-50 disabled:opacity-50" disabled>上一页</button>
            <button className="px-3 py-1 border border-blue-600 rounded text-sm text-white bg-blue-600">1</button>
            <button className="px-3 py-1 border border-slate-200 rounded text-sm text-slate-600 bg-white hover:bg-slate-50 disabled:opacity-50" disabled>下一页</button>
          </div>
        </div>
      </div>

      <PreviewModal 
        isOpen={previewModal.isOpen} 
        onClose={() => setPreviewModal({ ...previewModal, isOpen: false })} 
        assetCode={previewModal.assetCode}
        assetType={previewModal.type}
      />
    </div>
  );
}
