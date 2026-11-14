import test from 'node:test';
import assert from 'node:assert/strict';
import { managementCommand } from '../../server/http/management-command.ts';
import { domainDigest } from '../../server/events/domain-events.ts';

const request = (key = 'same-key', rawHeaders = ['Idempotency-Key', key]) => ({
  headers: { 'idempotency-key': key },
  rawHeaders,
});
test('管理命令范围含实际kind与稳定ID，不保留local-admin身份名，完整正文仍决定指纹', () => {
  const body = { action: 'groups', actorKind: 'admin', actorId: 'forged-client', groups: [] },
    operation = 'member-management:groups',
    member = managementCommand(request(), 'local-admin', operation, body, 'member'),
    admin = managementCommand(request(), 'local-admin', operation, body);
  assert.notEqual(member.commandId, admin.commandId);
  assert.equal(member.actorKind, 'member');
  assert.equal(member.actorId, 'local-admin');
  assert.equal(
    member.commandId,
    'management:' + domainDigest(['member', 'local-admin', operation, 'same-key']),
  );
  assert.equal(
    admin.commandId,
    'management:' + domainDigest(['admin', 'local-admin', operation, 'same-key']),
  );
  assert.deepEqual(admin, managementCommand(request(), 'local-admin', operation, body, 'admin'));
  assert.equal(member.fingerprint, admin.fingerprint);
  assert.notEqual(
    member.fingerprint,
    managementCommand(
      request(),
      'local-admin',
      operation,
      { ...body, actorId: 'another-client-field' },
      'member',
    ).fingerprint,
  );
});
test('政策/账户caller保留原operation字面值，角色和操作互相隔离，legacy缺省仍admin', () => {
  for (const operation of [
    'member-management:fallback-policy',
    'PATCH:/api/member/owner/accounts/a',
    'POST:/api/member/owner/accounts/quota',
  ]) {
    const body = { enabled: true },
      member = managementCommand(request(), 'local-admin', operation, body, 'member'),
      admin = managementCommand(request(), 'local-admin', operation, body);
    assert.notEqual(member.commandId, admin.commandId, operation);
    assert.equal(admin.actorKind, 'admin');
    assert.notEqual(
      admin.commandId,
      managementCommand(request(), 'local-admin', 'member-management:groups', body).commandId,
    );
  }
  assert.deepEqual(managementCommand({ headers: {}, rawHeaders: [] }, 'actual', 'op', {}), {
    actorId: 'actual',
    actorKind: 'admin',
  });
  assert.deepEqual(
    managementCommand({ headers: {}, rawHeaders: [] }, 'actual', 'op', {}, 'member'),
    { actorId: 'actual', actorKind: 'member' },
  );
});
test('加入角色不放宽原ASCII单头边界或改变无key行为', () => {
  for (const key of ['', 'x'.repeat(129), 'has space', 'é', ['same', 'same']]) {
    assert.throws(() => managementCommand(request(key), 'actor', 'op', {}), { statusCode: 400 });
  }
  assert.throws(
    () =>
      managementCommand(
        request('same', ['Idempotency-Key', 'same', 'idempotency-key', 'same']),
        'actor',
        'op',
        {},
      ),
    { statusCode: 400 },
  );
  assert.throws(() => managementCommand(request('same', []), 'actor', 'op', {}), {
    statusCode: 400,
  });
  for (const key of ['!', '~', 'x'.repeat(128)])
    assert.match(
      managementCommand(request(key), 'actor', 'op', {}).commandId,
      /^management:[a-f0-9]{64}$/,
    );
});
