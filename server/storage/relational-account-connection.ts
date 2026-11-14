import type Database from 'better-sqlite3';
import type { RelationalEvent } from '../events/relational-events.ts';
import { normalizeSocksProxyUrl } from '../protocols/novelai-client.ts';

export function assertRelationalAccountConnections(sqlite: Database.Database): void {
  for (const row of sqlite
    .prepare<[], { accountId: string; enabled: number; proxyUrl: string | null }>(
      'SELECT a.id AS accountId,a.enabled,c.proxy_url AS proxyUrl FROM upstream_credentials c JOIN upstream_accounts a ON a.id=c.account_id',
    )
    .iterate()) {
    if (
      relationalAccountConnectionSource(sqlite, row.accountId) === null ||
      (row.proxyUrl === null && row.enabled === 1) ||
      (row.proxyUrl !== null && normalizeSocksProxyUrl(row.proxyUrl) !== row.proxyUrl)
    ) {
      throw new Error('上游私有连接缺少获选的规范来源，不能默认直连。');
    }
  }
}

// 连接身份取真正改变连接的事实；名称/权重修改不使在途查询失效，A→B→A 也不复用旧身份。
export function relationalAccountConnectionSource(
  sqlite: Database.Database,
  accountId: string,
  before?: Pick<RelationalEvent, 'commitSeq' | 'eventIndex'>,
): string | null {
  return (
    sqlite
      .prepare<
        [string, string, string | null, string | null, string | null, number | null],
        { id: string }
      >(
        `SELECT event_id AS id FROM domain_events WHERE account_id=? AND stream_id=? AND member_id IS NULL
        AND (event_type IN ('AccountRegistered','OpeningAccountRecorded')
          OR (event_type='AccountConfigurationChanged' AND json_extract(payload,'$.connectionChanged')=1))
        AND (? IS NULL OR commit_seq<CAST(? AS INTEGER) OR (commit_seq=CAST(? AS INTEGER) AND event_index<?))
        ORDER BY stream_version DESC LIMIT 1`,
      )
      .get(
        accountId,
        `account:${accountId}`,
        before?.commitSeq ?? null,
        before?.commitSeq ?? null,
        before?.commitSeq ?? null,
        before?.eventIndex ?? null,
      )?.id ?? null
  );
}
