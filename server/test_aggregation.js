import path from 'node:path';
import { fileURLToPath } from 'node:url';
import XLSX from 'xlsx';
import fse from 'fs-extra';
import {
  aggregateProjectData,
  matchesMandatoryDataTableFileName,
} from './dataEngine.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const storageRoot = path.join(__dirname, 'storage');
const targetDir = path.join(storageRoot, 'data_tables', '2026-04-26');

function normalizeKey(v) {
  return String(v ?? '').trim().toLowerCase();
}

function getLoose(row, wanted) {
  const want = normalizeKey(wanted);
  const key = Object.keys(row || {}).find((k) => normalizeKey(k) === want);
  return key ? row[key] : undefined;
}

function getFirstExisting(row, candidates) {
  for (const candidate of candidates) {
    const value = getLoose(row, candidate);
    if (value !== undefined && String(value).trim() !== '') return { column: candidate, value };
  }
  return { column: '', value: undefined };
}

function normalizeProductLevel(v) {
  const s = String(v ?? '').trim().toUpperCase().replace(/\s+/g, '');
  if (!s) return '未定级';
  return s.endsWith('级') ? s.slice(0, -1) || '未定级' : s;
}

function readWorkbookRows(filePath) {
  const workbook = XLSX.readFile(filePath, { cellText: false, cellDates: true });
  const sheetName = workbook.SheetNames?.[0];
  if (!sheetName) return { headers: [], rawRows: [] };
  const sheet = workbook.Sheets[sheetName];
  const grid = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: '', blankrows: false });
  const headers = (Array.isArray(grid?.[0]) ? grid[0] : []).map((h) => String(h ?? ''));
  const rawRows = XLSX.utils.sheet_to_json(sheet, { raw: false, defval: '' });
  return { headers, rawRows };
}

async function main() {
  console.log('[Diagnostic] targetDir =', targetDir);
  const exists = await fse.pathExists(targetDir);
  console.log('[Diagnostic] targetDir exists =', exists);
  if (!exists) {
    process.exitCode = 1;
    return;
  }

  const fileNames = (await fse.readdir(targetDir)).filter((name) => path.extname(name).toLowerCase() === '.xlsx');
  console.log('[Diagnostic] xlsx files =', fileNames);

  const mainFileName = fileNames.find((name) => matchesMandatoryDataTableFileName(name, 'ods_pdm_pdm_product_info_df'));
  console.log('[Diagnostic] main table file =', mainFileName || '(not found)');
  if (!mainFileName) {
    process.exitCode = 1;
    return;
  }

  const mainPath = path.join(targetDir, mainFileName);
  console.log('[Diagnostic] main physical path =', mainPath);

  const { headers, rawRows } = readWorkbookRows(mainPath);
  console.log('[Diagnostic] main headers raw =', headers);
  console.log('[Diagnostic] main headers cleaned =', headers.map((h) => h.trim()));
  console.log('[Diagnostic] main row count =', rawRows.length);
  console.log('[Diagnostic] main first 2 rows raw JSON =');
  console.log(JSON.stringify(rawRows.slice(0, 2), null, 2));

  const firstRows = rawRows.slice(0, 5);
  const fieldAudit = firstRows.map((row, index) => {
    const style = getFirstExisting(row, ['style_wms']);
    const brand = getFirstExisting(row, ['brand', 'brand_name']);
    const status = getFirstExisting(row, ['data_status', 'status']);
    const productActualPosition = getFirstExisting(row, ['product_actual_position']);
    return {
      row: index + 1,
      style_wms: style.value ?? null,
      brand_column_used: brand.column || null,
      brand: brand.value ?? null,
      data_status_column_used: status.column || null,
      data_status: status.value ?? null,
      product_actual_position_column_used: productActualPosition.column || null,
      product_actual_position_raw: productActualPosition.value ?? null,
      product_actual_position_cleaned: normalizeProductLevel(productActualPosition.value),
    };
  });
  console.log('[Diagnostic] first 5 key-field audit =');
  console.log(JSON.stringify(fieldAudit, null, 2));

  console.log('[Diagnostic] calling aggregateProjectData(storageRoot)...');
  const agg = await aggregateProjectData({ storageRoot });
  const latest = agg?.latest || {};
  const inventory = Array.isArray(latest.inventory) ? latest.inventory : [];
  const brands = Array.from(new Set(inventory.map((row) => String(row?.brand || '').trim()).filter(Boolean))).sort();
  console.log('[Diagnostic] aggregate dates =', agg?.dates);
  console.log('[Diagnostic] inventoryRows =', inventory.length);
  console.log('[Diagnostic] uniqueBrandCount =', brands.length);
  console.log('[Diagnostic] brands =', brands);
  console.log('[Diagnostic] filterOptions =');
  console.log(JSON.stringify(latest.filterOptions || {}, null, 2));
  console.log('[Diagnostic] styleMetadata first 12 =');
  console.log(JSON.stringify((latest.styleMetadata || []).slice(0, 12), null, 2));
  console.log('[Diagnostic] first 12 product level audit =');
  console.log(
    JSON.stringify(
      inventory.slice(0, 12).map((row) => ({
        style_wms: row?.style_wms,
        status: row?.data_status,
        brand: row?.brand,
        product_actual_position: row?.product_actual_position,
        hasId: Boolean(String(row?.lastCode || row?.soleCode || '').trim()),
        has3D: row?.has3DLast === true || row?.has3DSole === true,
      })),
      null,
      2
    )
  );
  console.log('[Diagnostic] brandBindingStats length =', Array.isArray(latest.brandBindingStats) ? latest.brandBindingStats.length : 0);
  console.log('[Diagnostic] brandBindingStats first 12 =');
  console.log(JSON.stringify((latest.brandBindingStats || []).slice(0, 12), null, 2));
  console.log('[Diagnostic] kpis =');
  console.log(JSON.stringify(latest.kpis || {}, null, 2));
}

main().catch((error) => {
  console.error('[Diagnostic] failed:', error);
  process.exitCode = 1;
});
