// 旧 sole 与关系适配共用现有五个只读入口；PUBLIC/Host/CORS 边界继续由中央路由持有。
export function quotaHttpRoute(method: string | undefined, pathname: string) {
  if (method !== 'GET') {
    return null;
  }
  if (pathname === '/api/me/quota') {
    return 'quota';
  }
  return /^(?:\/v1)?(?:\/ai)?\/user\/subscription$/.test(pathname) ? 'subscription' : null;
}
