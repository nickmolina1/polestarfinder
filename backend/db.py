from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Numeric,
    Date,
    DateTime,
    Boolean,
    func,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Vehicle(Base):
    __tablename__ = "vehicles"

    # Identifiers
    id = Column(String, primary_key=True)  # provider vehicle id
    vin = Column(
        String, nullable=True, unique=False
    )  # partial unique index added below

    # Core fields
    model = Column(String, nullable=False)
    year = Column(Integer, nullable=False)  # or SmallInteger if you prefer
    partner_location = Column(String, nullable=True)
    state = Column(String, nullable=True)  # e.g. 'Preowned','CPO','New'

    mileage = Column(Integer, nullable=True)
    first_time_registration = Column(Date, nullable=True)

    # Prices (money -> NUMERIC)
    retail_price = Column(Numeric(10, 2), nullable=True)
    dealer_price = Column(Numeric(10, 2), nullable=True)

    # Features
    exterior = Column(String, nullable=True)
    interior = Column(String, nullable=True)
    wheels = Column(String, nullable=True)
    motor = Column(String, nullable=True)
    edition = Column(String, nullable=True)

    performance = Column(Boolean, default=False, nullable=False)
    pilot = Column(Boolean, default=False, nullable=False)
    plus = Column(Boolean, default=False, nullable=False)

    # Availability
    available = Column(Boolean, default=True, nullable=False)

    # Images as JSON array instead of comma-separated text
    stock_images = Column(
        JSONB, nullable=True
    )  # e.g. ["https://.../1.jpg", "https://.../2.jpg"]

    # Timestamps (server-managed, UTC)
    date_added = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_scan = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


# ---- Engine/session (use env vars!) ----
# postgresql+psycopg (SQLAlchemy 2.x + psycopg3) or postgresql+psycopg2 if using psycopg2
# import os; DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = "postgresql+psycopg://user:pass@host:5432/polestarfinder"
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)

Base.metadata.create_all(engine)

# ---- Postgres partial unique index for vin when not null ----
# You can add with Alembic migration (recommended). Raw SQL example:
with engine.begin() as conn:
    conn.exec_driver_sql(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE tablename = 'vehicles' AND indexname = 'uq_vehicles_vin_not_null'
          ) THEN
            CREATE UNIQUE INDEX uq_vehicles_vin_not_null ON vehicles (vin) WHERE vin IS NOT NULL;
          END IF;
        END$$;
    """
    )
    # Helpful indexes for typical queries
    conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_vehicles_available ON vehicles (available);"
    )
    conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_vehicles_state ON vehicles (state);"
    )
    conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_vehicles_year ON vehicles (year DESC);"
    )
    conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_vehicles_price ON vehicles (retail_price);"
    )
