/**
 * 与后端 getRequestOperator 对齐：已登录则返回 trim 后的用户名；未登录返回空串（服务端再降级为 System）。
 * 禁止在客户端用字面量 "System" 冒充「已传操作人」，避免误覆盖审计。
 */
export function getApiOperatorUsername(): string {
  try {
    const parsed = JSON.parse(localStorage.getItem('currentUser') || 'null');
    const u = parsed?.username != null ? String(parsed.username).trim() : '';
    if (u) return u;
  } catch {
    // ignore
  }
  const legacy = localStorage.getItem('username');
  return legacy != null ? String(legacy).trim() : '';
}
