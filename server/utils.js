import path from 'path';

/**
 * 从物理文件名提取“逻辑表名”（用于跨 Sandbox / data_tables 日期目录稳定索引）。
 *
 * 目标：剔除 UID/时间戳/日期等前缀，以及 .xlsx/.xls 后缀。
 *
 * 示例：
 * - 1776341880504_20260415_ods_pdm_pdm_product_info_df.xlsx -> ods_pdm_pdm_product_info_df
 */
export function getLogicalTableName(fileName) {
  const base = path.basename(String(fileName || ''));
  let s = base.replace(/\.(xlsx|xls)$/i, '');
  const parts = s.split('_').filter(Boolean);
  if (!parts.length) return s;

  // 去掉前缀 token（纯数字/纯十六进制 UID 等）
  const isDigits = (x) => /^[0-9]+$/.test(x);
  const isHexUid = (x) => /^[a-f0-9]{8,}$/i.test(x);

  let i = 0;
  while (i < parts.length) {
    const t = parts[i];
    // 时间戳/UID：10~17 位数字
    if (isDigits(t) && t.length >= 10 && t.length <= 17) {
      i += 1;
      continue;
    }
    // 日期：YYYYMMDD
    if (isDigits(t) && t.length === 8) {
      i += 1;
      continue;
    }
    // 其他短数字前缀（例如随机 UID 切片）
    if (isDigits(t) && t.length >= 4) {
      i += 1;
      continue;
    }
    // 十六进制 UID（例如 hash）
    if (isHexUid(t)) {
      i += 1;
      continue;
    }
    break;
  }

  const rest = parts.slice(i).join('_');
  return rest || s;
}

