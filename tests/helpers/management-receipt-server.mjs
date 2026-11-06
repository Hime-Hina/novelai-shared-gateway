import http from 'node:http';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { handleMemberRoutes } from '../../server/members/http-routes.ts';
import { createMemberSession } from '../../server/members/authentication.ts';

const env = {
  OWNER_USER_ID: 'owner-a',
  DISCORD_GUILD_ID: '1134557553011998840',
  DISCORD_CLIENT_ID: '123456789012345678',
  DISCORD_CLIENT_SECRET: 'synthetic-receipt-secret',
  PUBLIC_BASE_URL: 'https://nai.example',
};
const store = new JsonStore(process.argv[2]);
await store.init();
let db = await store.readRaw();
if (!db.users.length) {
  db.users = ['owner-a', 'other-b', 'other-c', 'other-d'].map((id, index) => ({
    id,
    token: `STA1N-synthetic-${id}`,
    enabled: true,
    memberAuth: { username: id, version: id },
    discord: {
      id: String(222222222222222220n + BigInt(index)),
      guildId: env.DISCORD_GUILD_ID,
      checkedAt: Date.now(),
      member: true,
    },
  }));
  db.accounts = [
    {
      id: 'opus',
      enabled: true,
      quotaTier: 3,
      subscriptionActive: true,
      subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000,
      quotaPurchased: 100,
      v5UsagePercent: 80,
      quotaCheckedAt: Date.now(),
    },
  ];
  configureFairQuota(
    db,
    db.users.map((user) => user.id),
  );
  syncFairQuota(db);
  await store.write(db);
  await store.enableEventAuthority();
}
db = await store.readRaw();
const session = await createMemberSession(store, db.users[0], env);
const server = http.createServer(async (req, res) => {
  try {
    if (req.headers['x-test-drop-response'] === '1')
      res.end = () => {
        res.destroy();
        process.send({ dropped: true });
        return res;
      };
    const handled = await handleMemberRoutes(req, res, new URL(req.url, 'https://nai.example'), {
      env,
      store,
      assertAdmin() {
        throw Object.assign(new Error('admin required'), { statusCode: 401 });
      },
    });
    if (!handled) {
      res.writeHead(404);
      res.end('{}');
    }
  } catch (error) {
    res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: error.message }));
  }
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
process.send({ port: server.address().port, cookie: `nai_member_session=${session.token}` });
process.on('message', async (message) => {
  if (message?.stop) {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await store.close();
    process.exit(0);
  }
  if (message?.revoke) {
    env.OWNER_USER_ID = 'revoked';
    process.send({ revoked: true });
  }
});
