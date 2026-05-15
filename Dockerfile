# Backend Dockerfile - multi-stage (build + runtime)
FROM node:20-bullseye-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install build tools needed for native modules (bcrypt, node-gyp)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends python3 build-essential git make g++ \
	&& rm -rf /var/lib/apt/lists/*

# Set python for node-gyp to use
ENV npm_config_python=/usr/bin/python3

# Install dependencies (including dev) and build
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# Production image (only production deps + dist)
FROM node:20-bullseye-slim AS runner

ENV NODE_ENV=production
WORKDIR /app

# Create app user and group (use non-root for better security)
RUN groupadd -r app && useradd -r -g app -s /sbin/nologin app || true

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Install production dependencies only
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Copy built output and scripts from builder
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/scripts ./scripts
COPY --from=builder /app/update-merchant-categories.js ./
COPY --from=builder /app/check-merchants.js ./

# Adjust ownership and switch to unprivileged user
RUN chown -R app:app /app
USER app

EXPOSE 3002

# Simple healthcheck (uses curl installed above)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl -f http://localhost:3002/ || exit 1

CMD ["node", "dist/main.js"]