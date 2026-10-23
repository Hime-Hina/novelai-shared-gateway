-- Frozen schema from the pre-Drizzle JsonStore.createSchema; synthetic tables only.
CREATE TABLE accounts (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        );

CREATE TABLE app_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

CREATE TABLE app_records (
        collection TEXT NOT NULL,
        id TEXT NOT NULL,
        order_value REAL NOT NULL,
        data TEXT NOT NULL,
        PRIMARY KEY (collection, id)
      );

CREATE TABLE app_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

CREATE TABLE cards (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        );

CREATE TABLE images (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        );

CREATE TABLE jobs (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        , cost REAL NOT NULL DEFAULT 0, stats_excluded INTEGER NOT NULL DEFAULT 0, error_loggable INTEGER NOT NULL DEFAULT 0);

CREATE TABLE ledger (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        );

CREATE TABLE users (
          id TEXT PRIMARY KEY,
          order_value REAL NOT NULL,
          data TEXT NOT NULL,
          token TEXT,
          user_token TEXT,
          account_id TEXT,
          status TEXT,
          source TEXT,
          code TEXT,
          cache_key TEXT,
          image_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          at TEXT,
          enabled INTEGER,
          balance REAL,
          route_id INTEGER,
          width INTEGER,
          height INTEGER,
          mock INTEGER,
          mime_type TEXT,
          model TEXT,
          prompt TEXT,
          full_prompt TEXT,
          resolution_tier TEXT,
          search_text TEXT
        );

CREATE INDEX idx_accounts_order
          ON accounts (order_value DESC);

CREATE INDEX idx_accounts_route_id
        ON accounts (route_id);

CREATE INDEX idx_app_records_collection_order
        ON app_records (collection, order_value DESC);

CREATE INDEX idx_cards_order
          ON cards (order_value DESC);

CREATE INDEX idx_images_cache_key
        ON images (cache_key);

CREATE INDEX idx_images_model_order
        ON images (model, order_value DESC);

CREATE INDEX idx_images_order
          ON images (order_value DESC);

CREATE INDEX idx_images_tier_model_order
        ON images (resolution_tier, model, order_value DESC);

CREATE INDEX idx_images_tier_order
        ON images (resolution_tier, order_value DESC);

CREATE INDEX idx_jobs_active_created
        ON jobs (created_at) WHERE status IN ('queued', 'running');

CREATE INDEX idx_jobs_created_stats
        ON jobs (created_at, status, stats_excluded, account_id);

CREATE INDEX idx_jobs_error_log_updated
        ON jobs (updated_at DESC) WHERE error_loggable = 1;

CREATE INDEX idx_jobs_failed_order
        ON jobs (order_value DESC) WHERE status = 'failed';

CREATE INDEX idx_jobs_image_id
        ON jobs (image_id);

CREATE INDEX idx_jobs_order
          ON jobs (order_value DESC);

CREATE INDEX idx_jobs_updated_stats
        ON jobs (updated_at, status, model, created_at, cost);

CREATE INDEX idx_jobs_user_token
        ON jobs (user_token);

CREATE INDEX idx_ledger_order
          ON ledger (order_value DESC);

CREATE INDEX idx_users_order
          ON users (order_value DESC);

CREATE INDEX idx_users_token
        ON users (token);
