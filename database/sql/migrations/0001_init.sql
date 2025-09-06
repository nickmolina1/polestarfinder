CREATE TABLE IF NOT EXISTS _migrations (
  id TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicles (
  id TEXT PRIMARY KEY,                    -- provider vehicle id
  vin TEXT,
  model TEXT NOT NULL,
  year INT NOT NULL,
  partner_location TEXT,
  state TEXT,                             -- 'Preowned','Certified Preowned','New'
  mileage INT,
  first_time_registration DATE,
  retail_price NUMERIC(10,2),
  dealer_price NUMERIC(10,2),
  exterior TEXT,
  interior TEXT,
  wheels TEXT,
  motor TEXT,
  edition TEXT,
  performance BOOLEAN NOT NULL DEFAULT FALSE,
  pilot BOOLEAN NOT NULL DEFAULT FALSE,
  plus BOOLEAN NOT NULL DEFAULT FALSE,
  available BOOLEAN NOT NULL DEFAULT TRUE,
  stock_images JSONB,                     -- array of URLs
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- optional helper index if you filter by last_seen_at
CREATE INDEX IF NOT EXISTS vehicles_last_seen_at_idx ON vehicles (last_seen_at);


-- VIN unique only when present
CREATE UNIQUE INDEX IF NOT EXISTS uq_vehicles_vin_not_null
ON vehicles (vin) WHERE vin IS NOT NULL;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_vehicles_available ON vehicles (available);
CREATE INDEX IF NOT EXISTS ix_vehicles_state ON vehicles (state);
CREATE INDEX IF NOT EXISTS ix_vehicles_year ON vehicles (year DESC);
CREATE INDEX IF NOT EXISTS ix_vehicles_price ON vehicles (retail_price);
