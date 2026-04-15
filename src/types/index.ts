/**
 * 业务模型定义 (Business Models)
 * 这些接口定义了前端所需的数据结构，方便后续与后端 API 对接。
 */

// 概览看板 KPI 数据
export interface DashboardKPIs {
  totalBrands: number; // 品牌总数
  newBrandsLastMonth: number; // 较上月新增品牌数
  totalStyles: number; // 总款号数
  activeStyles: number; // 生效款号总数 (基于 data_status)
  newActiveStylesLastMonth: number; // 较上月新增生效款号数
  matched3DLasts: number; // 已匹配 3D 楦头数
  newMatched3DLastsLastMonth: number; // 较上月新增匹配 3D 楦头数
  totalLastIDs: number; // 总楦头 ID 数
  matched3DSoles: number; // 已匹配 3D 大底数
  newMatched3DSolesLastMonth: number; // 较上月新增匹配 3D 大底数
  totalSoleIDs: number; // 总大底 ID 数
  overallCoverage: number; // 整体覆盖率 0-100
  coverageIncreaseLastMonth: number; // 较上月覆盖率增长
}

// 品牌覆盖率统计 (堆叠柱状图)
export interface BrandCoverageStats {
  brand: string;
  linked: number;   // 已关联 3D 资产的款号数
  unlinked: number; // 未关联/缺失的款号数
}

// 楦底关联查询结果
export interface LastSoleRelation {
  id: string;
  lastCode: string;
  associatedSoles: {
    soleCode: string;
    sole3DStatus: 'matched' | 'missing';
    styles: {
      styleCode: string;
      brand: string;
      image?: string;
    }[];
  }[];
  associatedStylesCount: number;
  brands: string[];
  last3DStatus: 'matched' | 'missing';
  soles3DStatus: 'matched' | 'partial' | 'missing';
}

// 品牌 3D 化进度排行榜
export interface BrandProgressStats {
  brand: string;
  progress: number; // 0-100
}

// 3D 资产新增趋势 (折线图)
export interface AssetTrendStats {
  date: string;
  newLasts: number; // 新增楦头数
  newSoles: number; // 新增底数
}

// 异常警告信息
export interface AlertItem {
  id: string;
  type: 'missing_last' | 'missing_sole';
  styleCode: string;
  brand: string;
  message: string;
  date: string;
}

// 导入历史记录
export interface ImportHistory {
  id: string;
  fileName: string;
  type: 'xlsx' | '3d_model';
  status: 'success' | 'processing' | 'failed';
  uploadTime: string;
  snapshotDate?: string; // 快照时间点
  operator: string;
  matchedCount?: number;
  version?: string;
  updateType?: 'overwrite' | 'retain';
  targetTable?: string;
}

// 全局字段映射定义
export interface GlobalSchemaField {
  id: string;
  standardName: string; // 标准系统字段名 (如: 款号)
  standardKey: string;  // 标准系统字段Key (如: styleCode)
  mappedSources: string[]; // 映射到的物理字段 (如: ['style_wms', 'style_no'])
  description: string;
}

// 详细清单项
export interface InventoryItem {
  id: string;
  style_wms: string; // 款号
  brand: string; // 品牌
  colorCode: string; // 颜色编码
  colorHex?: string; // 颜色色块
  materialCode: string; // 材质编码
  materialThumb?: string; // 材质缩略图
  lastCode?: string; // 楦头编号
  lastStatus: 'matched' | 'missing'; // 3D楦头状态
  soleCode?: string; // 大底编号
  soleStatus: 'matched' | 'missing'; // 3D大底状态
  data_status: 'active' | 'draft' | 'obsolete'; // 状态
  lastUpdated: string; // 最后更新
  updatedBy: string; // 更新人
  sourceTable: string; // 数据源说明
}
