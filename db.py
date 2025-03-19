from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

Base = declarative_base()


class Vehicle(Base):
    __tablename__ = "vehicles"

    id = Column(String, primary_key=True)  # Unique vehicle ID
    model = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    partner_location = Column(String, nullable=True)
    retail_price = Column(Float, nullable=True)
    dealer_price = Column(Float, nullable=True)
    mileage = Column(Float, nullable=True)
    first_time_registration = Column(String, nullable=True)
    vin = Column(String, nullable=True, unique=True)
    stock_images = Column(String, nullable=True)  # Comma-separated image URLs

    # Timestamps
    date_added = Column(DateTime, default=datetime.datetime.now)  # When first seen
    last_scan = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )  # Last updated

    # Feature columns (default to None for text and False for booleans)
    exterior = Column(String, nullable=True)
    interior = Column(String, nullable=True)
    wheels = Column(String, nullable=True)
    motor = Column(String, nullable=True)
    edition = Column(String, nullable=True)
    state = Column(String, nullable=True)  # 'Preowned', 'Certified Preowned', 'New'

    # Package booleans (default False)
    performance = Column(Boolean, default=False)
    pilot = Column(Boolean, default=False)
    plus = Column(Boolean, default=False)


# Initialize SQLite (switch to PostgreSQL later)
DATABASE_URL = "sqlite:///vehicles.db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)

# Create tables
Base.metadata.create_all(engine)
