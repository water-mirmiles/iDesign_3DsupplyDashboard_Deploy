import assert from 'node:assert/strict';
import { loadExcelFolderAsTablesMap } from './dataEngine.js';

// 这是一条“可复现实验式单测”：验证 sandbox 下不会把数据行误判为表头
// 运行方式：node server/excel_header_alignment.test.mjs

const sandboxDir = new URL('./storage/sandbox', import.meta.url).pathname;
const { tablesMap } = await loadExcelFolderAsTablesMap(sandboxDir);

const brand = tablesMap.get('ods_wms_base_brand_df');
assert.ok(brand, 'ods_wms_base_brand_df should be loaded into tablesMap');
assert.equal(brand.headers?.[0], 'id', 'brand table header row must be detected as DDL header (id...)');
assert.ok((brand.rows?.length || 0) >= 1, 'brand table must have at least 1 data row (e.g., Bruno Marc)');
assert.equal(String(brand.rows?.[0]?.brand_name || ''), 'Bruno Marc', 'brand first row should preserve business value');

console.log('[PASS] excel header alignment for ods_wms_base_brand_df');

