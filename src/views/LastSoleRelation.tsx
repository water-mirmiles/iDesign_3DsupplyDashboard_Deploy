import React, { useState } from 'react';
import { Search, Box, Layers, Hash, CheckCircle2, XCircle, AlertCircle, ChevronRight, Package } from 'lucide-react';
import { cn } from '@/lib/utils';
import { LastSoleRelation } from '@/types';

const mockRelations: LastSoleRelation[] = [
  {
    id: '1',
    lastCode: 'LST-NK-2024-A1',
    associatedSoles: [
      {
        soleCode: 'SOL-NK-A1-01',
        sole3DStatus: 'matched',
        styles: [
          { styleCode: 'NK-RUN-2024-001', brand: 'Nike', image: 'https://picsum.photos/seed/s1/60/60' },
          { styleCode: 'NK-RUN-2024-002', brand: 'Nike', image: 'https://picsum.photos/seed/s2/60/60' }
        ]
      },
      {
        soleCode: 'SOL-NK-A1-02',
        sole3DStatus: 'matched',
        styles: [
          { styleCode: 'NK-TRAIN-X1', brand: 'Nike', image: 'https://picsum.photos/seed/s3/60/60' }
        ]
      },
      {
        soleCode: 'SOL-NK-A1-03',
        sole3DStatus: 'missing',
        styles: [
          { styleCode: 'NK-CASUAL-09', brand: 'Jordan', image: 'https://picsum.photos/seed/s4/60/60' },
          { styleCode: 'NK-CASUAL-10', brand: 'Jordan', image: 'https://picsum.photos/seed/s5/60/60' }
        ]
      }
    ],
    associatedStylesCount: 5,
    brands: ['Nike', 'Jordan'],
    last3DStatus: 'matched',
    soles3DStatus: 'partial'
  },
  {
    id: '2',
    lastCode: 'LST-AD-RUN-X',
    associatedSoles: [
      {
        soleCode: 'SOL-AD-RX-1',
        sole3DStatus: 'matched',
        styles: [
          { styleCode: 'AD-CAS-2024-X1', brand: 'Adidas', image: 'https://picsum.photos/seed/s6/60/60' },
          { styleCode: 'AD-CAS-2024-X2', brand: 'Adidas', image: 'https://picsum.photos/seed/s7/60/60' }
        ]
      }
    ],
    associatedStylesCount: 2,
    brands: ['Adidas'],
    last3DStatus: 'matched',
    soles3DStatus: 'matched'
  }
];

export default function LastSoleRelationView() {
  const [searchQuery, setSearchQuery] = useState('');
  const [hasSearched, setHasSearched] = useState(false);
  const [results, setResults] = useState<LastSoleRelation[]>([]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!searchQuery.trim()) {
      setHasSearched(false);
      setResults([]);
      return;
    }
    
    setHasSearched(true);
    const filtered = mockRelations.filter(r => 
      r.lastCode.toLowerCase().includes(searchQuery.toLowerCase())
    );
    setResults(filtered);
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500 pb-10">
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">楦底关联查询</h1>
        <p className="text-sm text-slate-500 mt-1">通过 3D 楦头编号，反查与其绑定的所有大底及款号信息 (1:N 视图)</p>
      </div>

      {/* Search Bar */}
      <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
        <form onSubmit={handleSearch} className="flex gap-4 max-w-3xl">
          <div className="relative flex-1">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Search className="h-5 w-5 text-slate-400" />
            </div>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="block w-full pl-10 pr-3 py-3 border border-slate-300 rounded-lg bg-slate-50 text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all text-sm"
              placeholder="输入楦头编号查询，如 LST-NK-2024-A1..."
            />
          </div>
          <button
            type="submit"
            className="px-6 py-3 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm whitespace-nowrap"
          >
            查询关联
          </button>
        </form>
      </div>

      {/* Results Area */}
      {!hasSearched ? (
        <div className="bg-white rounded-xl border border-slate-200 border-dashed p-12 flex flex-col items-center justify-center text-center min-h-[400px]">
          <div className="w-16 h-16 bg-slate-50 rounded-full flex items-center justify-center mb-4">
            <Search className="w-8 h-8 text-slate-300" />
          </div>
          <h3 className="text-lg font-medium text-slate-900 mb-2">请输入楦头编号查询资产关联关系</h3>
          <p className="text-sm text-slate-500 max-w-md">
            支持模糊搜索。查询结果将以 1:N 卡片形式展示该楦头绑定的所有大底及对应的款号快照。
          </p>
        </div>
      ) : results.length === 0 ? (
        <div className="bg-white rounded-xl border border-slate-200 p-12 flex flex-col items-center justify-center text-center min-h-[400px]">
          <div className="w-16 h-16 bg-amber-50 rounded-full flex items-center justify-center mb-4">
            <AlertCircle className="w-8 h-8 text-amber-500" />
          </div>
          <h3 className="text-lg font-medium text-slate-900 mb-2">未找到匹配的关联记录</h3>
          <p className="text-sm text-slate-500">
            请检查输入的楦头编号是否正确，或尝试使用其他关键字。
          </p>
        </div>
      ) : (
        <div className="space-y-8">
          {results.map((relation) => (
            <div key={relation.id} className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              {/* Last Header (The "1") */}
              <div className="p-6 bg-slate-900 text-white flex flex-wrap gap-6 justify-between items-center">
                <div className="flex items-center gap-4">
                  <div className="w-12 h-12 bg-blue-600/20 rounded-xl flex items-center justify-center border border-blue-500/30">
                    <Box className="w-6 h-6 text-blue-400" />
                  </div>
                  <div>
                    <div className="text-sm text-slate-400 mb-1">楦头编号</div>
                    <div className="text-xl font-bold font-mono tracking-tight">{relation.lastCode}</div>
                  </div>
                </div>
                
                <div className="flex gap-8">
                  <div>
                    <div className="text-xs text-slate-400 mb-1 uppercase tracking-wider">3D 楦头状态</div>
                    {relation.last3DStatus === 'matched' ? (
                      <div className="flex items-center gap-1.5 text-emerald-400">
                        <CheckCircle2 className="w-4 h-4" />
                        <span className="font-medium text-sm">已匹配</span>
                      </div>
                    ) : (
                      <div className="flex items-center gap-1.5 text-red-400">
                        <XCircle className="w-4 h-4" />
                        <span className="font-medium text-sm">缺失</span>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="text-xs text-slate-400 mb-1 uppercase tracking-wider">关联大底数</div>
                    <div className="text-lg font-semibold">{relation.associatedSoles.length}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-400 mb-1 uppercase tracking-wider">关联款号数</div>
                    <div className="text-lg font-semibold">{relation.associatedStylesCount}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-400 mb-1 uppercase tracking-wider">涉及品牌</div>
                    <div className="flex gap-2">
                      {relation.brands.map(b => (
                        <span key={b} className="px-2 py-0.5 bg-slate-800 rounded text-sm">{b}</span>
                      ))}
                    </div>
                  </div>
                </div>
              </div>

              {/* Soles List (The "N") */}
              <div className="p-6 bg-slate-50">
                <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wider mb-4 flex items-center gap-2">
                  <Layers className="w-4 h-4" />
                  配套大底及款号快照
                </h3>
                
                <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
                  {relation.associatedSoles.map((sole, idx) => (
                    <div key={idx} className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm hover:shadow-md transition-shadow">
                      <div className="flex justify-between items-start mb-4 pb-4 border-b border-slate-100">
                        <div>
                          <div className="text-xs text-slate-500 mb-1">大底编号</div>
                          <div className="font-mono font-semibold text-slate-900 text-base">{sole.soleCode}</div>
                        </div>
                        {sole.sole3DStatus === 'matched' ? (
                          <span className="px-2 py-1 bg-emerald-50 text-emerald-700 border border-emerald-200 rounded-md text-xs font-medium flex items-center gap-1">
                            <CheckCircle2 className="w-3 h-3" /> 3D 已匹配
                          </span>
                        ) : (
                          <span className="px-2 py-1 bg-red-50 text-red-700 border border-red-200 rounded-md text-xs font-medium flex items-center gap-1">
                            <XCircle className="w-3 h-3" /> 3D 缺失
                          </span>
                        )}
                      </div>
                      
                      <div>
                        <div className="text-xs text-slate-500 mb-3 flex items-center justify-between">
                          <span>应用此大底的款号 ({sole.styles.length})</span>
                        </div>
                        <div className="space-y-3">
                          {sole.styles.map((style, sIdx) => (
                            <div key={sIdx} className="flex items-center gap-3 group cursor-pointer">
                              <div className="w-10 h-10 rounded-lg bg-slate-100 border border-slate-200 overflow-hidden shrink-0">
                                {style.image ? (
                                  <img src={style.image} alt={style.styleCode} className="w-full h-full object-cover" referrerPolicy="no-referrer" />
                                ) : (
                                  <Package className="w-5 h-5 text-slate-400 m-2.5" />
                                )}
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="font-mono text-sm font-medium text-slate-900 truncate group-hover:text-blue-600 transition-colors">{style.styleCode}</div>
                                <div className="text-xs text-slate-500">{style.brand}</div>
                              </div>
                              <ChevronRight className="w-4 h-4 text-slate-300 group-hover:text-blue-500 transition-colors" />
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
