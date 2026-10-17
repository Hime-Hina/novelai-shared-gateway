FROM node:22.23.3-bookworm-slim AS dependencies
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++
RUN corepack enable pnpm
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY patches ./patches
RUN pnpm install --prod --frozen-lockfile
RUN node -e "const db = require('better-sqlite3')(':memory:'); if (db.pragma('integrity_check', {simple:true}) !== 'ok') process.exit(1); db.close();"

FROM dependencies AS build
COPY executor/package.json ./executor/package.json
RUN CI=true pnpm --filter @novelai-gateway/executor... install --frozen-lockfile
COPY executor ./executor
COPY server ./server
COPY public ./public
COPY scripts ./scripts
COPY LICENSE NOTICE.md UPSTREAM-LICENSE ./
RUN pnpm --filter @novelai-gateway/executor build

FROM node:22.23.3-bookworm-slim
WORKDIR /app
COPY --from=dependencies /app/node_modules ./node_modules
COPY --from=build /app/executor/.output ./.output
ENV NODE_ENV=production DATA_DIR=/data HOST=0.0.0.0 PORT=8080
USER 1000:1000
EXPOSE 8080
CMD ["node", ".output/server/start.mjs"]
