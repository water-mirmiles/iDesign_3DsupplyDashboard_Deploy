/**
 * 供 Three.js / fetch 直链静态资源使用（与 Vite 代理、后端 /storage 对齐）。
 * 生产默认同源（空串 + 以 / 开头的路径），避免请求打到用户本机 localhost；开发默认 3001。
 */
export function getStorageBaseUrl() {
  const v = (import.meta as any)?.env?.VITE_STORAGE_ORIGIN as string | undefined;
  if (v && String(v).trim()) return String(v).trim().replace(/\/$/, '');
  if (import.meta.env.PROD) return '';
  return 'http://localhost:3001';
}
