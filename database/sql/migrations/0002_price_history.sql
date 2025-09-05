CREATE TABLE IF NOT EXISTS price_history (
  id TEXT PRIMARY KEY,                    -- e.g., {vehicle_id}-{epoch} or UUID
  vehicle_id TEXT NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
  price NUMERIC(10,2) NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_price_history_vehicle ON price_history (vehicle_id);
CREATE INDEX IF NOT EXISTS ix_price_history_observed ON price_history (observed_at DESC);
