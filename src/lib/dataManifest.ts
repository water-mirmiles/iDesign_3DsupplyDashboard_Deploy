export type MandatoryDataFileRole = 'core-main' | 'last-relation' | 'sole-relation';

export type MandatoryDataFile = {
  role: MandatoryDataFileRole;
  requiredLabel: string;
  tableName: string;
  expectedFileName: string;
  title: string;
  description: string;
};

export const CORE_MAIN_TABLE_NAME = 'ods_pdm_pdm_product_info_df';

export const MANDATORY_DATA_FILES: MandatoryDataFile[] = [
  {
    role: 'core-main',
    requiredLabel: '必填 A',
    tableName: CORE_MAIN_TABLE_NAME,
    expectedFileName: `${CORE_MAIN_TABLE_NAME}.xlsx`,
    title: '核心主表',
    description: '提供款号、品牌、状态、及关联 ID，是看板统计和资产匹配的主数据入口。',
  },
  {
    role: 'last-relation',
    requiredLabel: '必填 B',
    tableName: 'ods_pdm_pdm_base_last_df',
    expectedFileName: 'ods_pdm_pdm_base_last_df.xlsx',
    title: '楦头关系表',
    description: '将主表的 Last ID 换算为可匹配 3D 文件的 Last Code。',
  },
  {
    role: 'sole-relation',
    requiredLabel: '必填 C',
    tableName: 'ods_pdm_pdm_base_heel_df',
    expectedFileName: 'ods_pdm_pdm_base_heel_df.xlsx',
    title: '大底关系表',
    description: '将主表的 Sole ID 换算为可匹配 3D 文件的 Sole Code。',
  },
];
