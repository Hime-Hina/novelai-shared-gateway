FROM node:22.23.3-bookworm-slim AS dependencies
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++
RUN corepack enable pnpm
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY patches ./patches
RUN pnpm install --prod --frozen-lockfile

FROM dependencies AS build
RUN CI=true pnpm install --frozen-lockfile
COPY tsconfig.backend.json ./
COPY server ./server
COPY public ./public
COPY scripts ./scripts
COPY tests ./tests
RUN pnpm run check && pnpm run build && node -e "const db = require('better-sqlite3')(':memory:'); if (db.pragma('integrity_check', {simple:true}) !== 'ok') process.exit(1); db.close();"

FROM node:22.23.3-bookworm-slim
WORKDIR /app
COPY --from=dependencies /app/node_modules ./node_modules
COPY --from=build /app/.output/backend/server ./server
COPY --from=build /app/server/migrations ./server/migrations
COPY --from=build /app/public ./public
COPY --from=build /app/scripts ./scripts
COPY --from=build /app/.output/backend/scripts ./scripts
COPY package.json pnpm-lock.yaml LICENSE NOTICE.md UPSTREAM-LICENSE ./
ENV NODE_ENV=production DATA_DIR=/data HOST=0.0.0.0 PORT=8080
USER 1000:1000
EXPOSE 8080
CMD ["node", "server/index.js"]
