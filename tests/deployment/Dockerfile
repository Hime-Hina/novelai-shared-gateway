FROM caddy:2.11.7@sha256:685ae3d7c34d9b5f8caeddc5d14051db7bbd88ff1f5a2fa0c16bd8ca328fb7af AS caddy
FROM node:22.23.4-bookworm-slim
COPY --from=caddy /usr/bin/caddy /usr/bin/caddy
WORKDIR /checks
COPY Caddyfile.example ./Caddyfile.example
COPY tests/deployment/caddy-routing.test.mjs ./tests/deployment/caddy-routing.test.mjs
USER 1000:1000
CMD ["node", "--test", "tests/deployment/caddy-routing.test.mjs"]
