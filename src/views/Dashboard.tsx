import React, { useState, useEffect } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer,
  AreaChart, Area
} from 'recharts';
import { Box, Layers, Hash, Factory, Loader2, Activity, TrendingUp } from 'lucide-react';
import { cn } from '@/lib/utils';
import { DashboardKPIs, AssetTrendStats, BrandCoverageStats } from '@/types';

// --- Mock Data ---
const mockKPIs: DashboardKPIs = {
  totalBrands: 24,
  newBrandsLastMonth: 2,
  totalStyles: 4200,
  activeStyles: 3450,
  newActiveStylesLastMonth: 150,
  matched3DLasts: 2100,
  newMatched3DLastsLastMonth: 45,
  totalLastIDs: 2890,
  matched3DSoles: 1850,
  newMatched3DSolesLastMonth: 32,
  totalSoleIDs: 2100,
  overallCoverage: 78,
  coverageIncreaseLastMonth: 2.5,
};

const mockBrandCoverage: BrandCoverageStats[] = [
  { brand: 'Nike', linked: 850, unlinked: 150 },
  { brand: 'Adidas', linked: 620, unlinked: 280 },
  { brand: 'Puma', linked: 450, unlinked: 300 },
  { brand: 'New Balance', linked: 380, unlinked: 420 },
  { brand: 'Under Armour', linked: 290, unlinked: 510 },
  { brand: 'Reebok', linked: 210, unlinked: 390 },
  { brand: 'Asics', linked: 180, unlinked: 220 },
  { brand: 'Vans', linked: 150, unlinked: 150 },
];

const trendDataMap: Record<string, AssetTrendStats[]> = {
  day: [
    { date: '08:00', newLasts: 5, newSoles: 2 },
    { date: '10:00', newLasts: 12, newSoles: 8 },
    { date: '12:00', newLasts: 8, newSoles: 5 },
    { date: '14:00', newLasts: 15, newSoles: 10 },
    { date: '16:00', newLasts: 20, newSoles: 18 },
    { date: '18:00', newLasts: 10, newSoles: 7 },
  ],
  week: [
    { date: 'Mon', newLasts: 45, newSoles: 30 },
    { date: 'Tue', newLasts: 52, newSoles: 38 },
    { date: 'Wed', newLasts: 38, newSoles: 45 },
    { date: 'Thu', newLasts: 65, newSoles: 50 },
    { date: 'Fri', newLasts: 48, newSoles: 42 },
    { date: 'Sat', newLasts: 15, newSoles: 10 },
    { date: 'Sun', newLasts: 20, newSoles: 15 },
  ],
  month: [
    { date: 'Week 1', newLasts: 150, newSoles: 120 },
    { date: 'Week 2', newLasts: 180, newSoles: 160 },
    { date: 'Week 3', newLasts: 210, newSoles: 190 },
    { date: 'Week 4', newLasts: 170, newSoles: 140 },
  ],
  quarter: [
    { date: 'Jan', newLasts: 600, newSoles: 500 },
    { date: 'Feb', newLasts: 750, newSoles: 680 },
    { date: 'Mar', newLasts: 820, newSoles: 710 },
  ],
  year: [
    { date: 'Jan', newLasts: 600, newSoles: 500 },
    { date: 'Feb', newLasts: 750, newSoles: 680 },
    { date: 'Mar', newLasts: 820, newSoles: 710 },
    { date: 'Apr', newLasts: 900, newSoles: 850 },
    { date: 'May', newLasts: 850, newSoles: 800 },
    { date: 'Jun', newLasts: 950, newSoles: 900 },
    { date: 'Jul', newLasts: 1100, newSoles: 1050 },
    { date: 'Aug', newLasts: 1050, newSoles: 980 },
    { date: 'Sep', newLasts: 1200, newSoles: 1150 },
    { date: 'Oct', newLasts: 1300, newSoles: 1250 },
    { date: 'Nov', newLasts: 1400, newSoles: 1350 },
    { date: 'Dec', newLasts: 1500, newSoles: 1450 },
  ],
};

type TimePeriod = 'day' | 'week' | 'month' | 'quarter' | 'year';

export default function Dashboard() {
  const [trendPeriod, setTrendPeriod] = useState<TimePeriod>('week');
  const [isChartLoading, setIsChartLoading] = useState(false);
  const [chartData, setChartData] = useState<AssetTrendStats[]>(trendDataMap['week']);

  const handlePeriodChange = (period: TimePeriod) => {
    if (period === trendPeriod) return;
    setTrendPeriod(period);
    setIsChartLoading(true);
    // Simulate network request
    setTimeout(() => {
      setChartData(trendDataMap[period]);
      setIsChartLoading(false);
    }, 600);
  };

  const getProgressColor = (percent: number) => {
    if (percent >= 80) return 'bg-emerald-500';
    if (percent >= 50) return 'bg-amber-500';
    return 'bg-red-500';
  };

  const getTrendBadge = (value: number, isPercent = false) => {
    if (value > 0) {
      return (
        <div className="flex items-center text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded mb-1">
          <TrendingUp className="w-3 h-3 mr-1" />
          较上月 +{value}{isPercent ? '%' : ''}
        </div>
      );
    } else if (value < 0) {
      return (
        <div className="flex items-center text-xs font-medium text-red-600 bg-red-50 px-1.5 py-0.5 rounded mb-1">
          <TrendingUp className="w-3 h-3 mr-1 rotate-180" />
          较上月 {value}{isPercent ? '%' : ''}
        </div>
      );
    } else {
      return (
        <div className="flex items-center text-xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded mb-1">
          <Activity className="w-3 h-3 mr-1" />
          较上月持平
        </div>
      );
    }
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500 pb-10">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">概览看板</h1>
        <p className="text-sm text-slate-500 mt-1">全局 3D 资产覆盖率与新增趋势</p>
      </div>

      {/* KPI Cards - 5 Cards Layout */}
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {/* Card 1: Total Brands */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">品牌总数</h3>
            <div className="p-2 rounded-lg bg-blue-50">
              <Factory className="w-4 h-4 text-blue-600" />
            </div>
          </div>
          <div className="mt-4">
            <span className="text-3xl font-bold text-slate-900">{mockKPIs.totalBrands.toLocaleString()}</span>
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
              <span className="text-3xl font-bold text-slate-900">{mockKPIs.activeStyles.toLocaleString()}</span>
              {getTrendBadge(mockKPIs.newActiveStylesLastMonth)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              总款号参考: <span className="font-medium text-slate-700">{mockKPIs.totalStyles.toLocaleString()}</span>
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
              <span className="text-3xl font-bold text-slate-900">{mockKPIs.matched3DLasts.toLocaleString()}</span>
              {getTrendBadge(mockKPIs.newMatched3DLastsLastMonth)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              / {mockKPIs.totalLastIDs.toLocaleString()} ({Math.round((mockKPIs.matched3DLasts / mockKPIs.totalLastIDs) * 100)}%)
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
              <span className="text-3xl font-bold text-slate-900">{mockKPIs.matched3DSoles.toLocaleString()}</span>
              {getTrendBadge(mockKPIs.newMatched3DSolesLastMonth)}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              / {mockKPIs.totalSoleIDs.toLocaleString()} ({Math.round((mockKPIs.matched3DSoles / mockKPIs.totalSoleIDs) * 100)}%)
            </div>
          </div>
        </div>

        {/* Card 5: Overall Coverage */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm flex flex-col justify-between">
          <div className="flex justify-between items-start">
            <h3 className="text-sm font-medium text-slate-500">整体覆盖率</h3>
            <div className="p-2 rounded-lg bg-emerald-50">
              <Activity className="w-4 h-4 text-emerald-600" />
            </div>
          </div>
          <div className="mt-4">
            <div className="flex items-end gap-2">
              <span className="text-3xl font-bold text-slate-900">{mockKPIs.overallCoverage}%</span>
              {getTrendBadge(mockKPIs.coverageIncreaseLastMonth, true)}
            </div>
            <div className="mt-2 h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
              <div 
                className={cn("h-full rounded-full transition-all duration-1000", getProgressColor(mockKPIs.overallCoverage))} 
                style={{ width: `${mockKPIs.overallCoverage}%` }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Left: Stacked Bar Chart for Brand Coverage */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">各品牌 3D 覆盖统计</h3>
            <span className="text-xs text-slate-500">按款号数量统计</span>
          </div>
          <div className="flex-1 min-h-[350px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart 
                data={mockBrandCoverage} 
                margin={{ top: 10, right: 10, left: -20, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="brand" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} dy={10} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip 
                  cursor={{ fill: '#f8fafc' }}
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '13px', paddingTop: '20px' }} />
                <Bar dataKey="linked" name="已关联 3D 资产" stackId="a" fill="#10b981" radius={[0, 0, 4, 4]} maxBarSize={40} />
                <Bar dataKey="unlinked" name="未关联/缺失" stackId="a" fill="#f59e0b" radius={[4, 4, 0, 0]} maxBarSize={40} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Right: Trend Area Chart */}
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-base font-semibold text-slate-900">3D 资产新增趋势</h3>
            <div className="flex bg-slate-100 p-1 rounded-lg">
              {(['day', 'week', 'month', 'quarter', 'year'] as const).map((period) => (
                <button
                  key={period}
                  onClick={() => handlePeriodChange(period)}
                  className={cn(
                    "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
                    trendPeriod === period ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
                  )}
                >
                  {period === 'day' ? '日' : period === 'week' ? '周' : period === 'month' ? '月' : period === 'quarter' ? '季' : '年'}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 min-h-[350px] w-full relative">
            {isChartLoading && (
              <div className="absolute inset-0 z-10 flex items-center justify-center bg-white/50 backdrop-blur-sm rounded-lg">
                <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
              </div>
            )}
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                <defs>
                  <linearGradient id="colorLasts" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorSoles" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} dy={10} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip 
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '13px', paddingTop: '20px' }} />
                <Area type="monotone" dataKey="newLasts" name="新增 3D 楦" stroke="#0ea5e9" strokeWidth={2} fillOpacity={1} fill="url(#colorLasts)" />
                <Area type="monotone" dataKey="newSoles" name="新增 3D 底" stroke="#8b5cf6" strokeWidth={2} fillOpacity={1} fill="url(#colorSoles)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

      </div>
    </div>
  );
}

