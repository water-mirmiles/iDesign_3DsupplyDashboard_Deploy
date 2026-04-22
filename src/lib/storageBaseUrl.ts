/**
 * 供 Three.js / fetch 直链静态资源使用（与 Vite 代理、后端 /storage 对齐）。
 * 默认指向本地后端端口 3001；生产可通过 VITE_STORAGE_ORIGIN 覆盖。
 */
export function getStorageBaseUrl() {
  const v = (import.meta as any)?.env?.VITE_STORAGE_ORIGIN as string | undefined;
  const raw = (v && String(v).trim()) || 'http://localhost:3001';
  return raw.replace(/\/$/, '');
}
