FROM node:22.23.3-bookworm-slim AS build
WORKDIR /app
ENV NUXT_TELEMETRY_DISABLED=1
RUN corepack enable pnpm
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY patches ./patches
COPY web/package.json ./web/package.json
RUN pnpm --filter @novelai-gateway/web install --frozen-lockfile
COPY web ./web
COPY public/member-events.js ./public/member-events.js
COPY server/generation-request.js server/queue-policy.ts server/shared-surface.js server/shared-status.ts server/shared-capacity.ts server/state-events.ts server/activity-heatmap.js ./server/
RUN pnpm --filter @novelai-gateway/web build
RUN mkdir -p /app/dependency-licenses && find node_modules -type f \( -iname 'license*' -o -iname 'copying*' -o -iname 'notice*' \) -exec cp --parents --target-directory=/app/dependency-licenses -- {} +

FROM node:22.23.3-bookworm-slim
WORKDIR /app
COPY --from=build /app/web/.output ./.output
COPY --from=build /app/dependency-licenses ./dependency-licenses
COPY LICENSE NOTICE.md UPSTREAM-LICENSE ./
COPY web/THIRD_PARTY_NOTICES.md ./web/THIRD_PARTY_NOTICES.md
ENV NODE_ENV=production HOST=0.0.0.0 PORT=3000
USER 1000:1000
EXPOSE 3000
CMD ["node", ".output/server/index.mjs"]
