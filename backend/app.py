from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime


from fastapi.middleware.cors import CORSMiddleware


# Enable CORS (Cross-Origin Resource Sharing)


# Import your database session and model
from backend.db import SessionLocal, Vehicle

app = FastAPI(
    title="Polestar Vehicle API",
    description="Read-only API for querying vehicles.",
    version="1.0.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],  # Allow all origins (you can replace "*" with specific frontend URLs)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)


# Dependency for SQLAlchemy session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Pydantic model to represent a vehicle. Using orm_mode so that SQLAlchemy objects can be automatically converted.
class VehicleSchema(BaseModel):
    id: str
    model: str
    year: int
    partner_location: Optional[str] = None
    retail_price: Optional[float] = None
    dealer_price: Optional[float] = None
    mileage: Optional[float] = None
    first_time_registration: Optional[str] = None
    vin: Optional[str] = None
    stock_images: Optional[str] = None
    date_added: Optional[datetime] = None
    last_scan: Optional[datetime] = None
    exterior: Optional[str] = None
    interior: Optional[str] = None
    wheels: Optional[str] = None
    motor: Optional[str] = None
    edition: Optional[str] = None
    state: Optional[str] = None
    performance: bool = False
    pilot: bool = False
    plus: bool = False
    available: bool = True  # New availability field advertised

    class Config:
        orm_mode = True


# Endpoint to query vehicles, with optional filters.
@app.get("/vehicles/", response_model=List[VehicleSchema])
def get_vehicles(
    partner_location: Optional[str] = Query(
        None, description="Filter by partner location."
    ),
    model: Optional[str] = Query(None, description="Filter by vehicle model."),
    state: Optional[str] = Query(
        None, description="Filter by vehicle state (e.g., Preowned, New)."
    ),
    exterior: Optional[str] = Query(None, description="Filter by exterior color."),
    interior: Optional[str] = Query(None, description="Filter by interior option."),
    wheels: Optional[str] = Query(None, description="Filter by wheels option."),
    motor: Optional[str] = Query(None, description="Filter by motor type."),
    edition: Optional[str] = Query(None, description="Filter by edition."),
    min_price: Optional[float] = Query(None, description="Filter by minimum price."),
    max_price: Optional[float] = Query(None, description="Filter by maximum price."),
    min_mileage: Optional[float] = Query(
        None, description="Filter by minimum mileage."
    ),
    max_mileage: Optional[float] = Query(
        None, description="Filter by maximum mileage."
    ),
    performance: Optional[bool] = Query(
        None, description="Filter by Performance Pack."
    ),
    pilot: Optional[bool] = Query(None, description="Filter by Pilot Pack."),
    plus: Optional[bool] = Query(None, description="Filter by Plus Pack."),
    available: Optional[bool] = Query(
        None, description="Filter by availability status."
    ),
    date_added: Optional[datetime] = Query(None, description="Filter by date added."),
    first_time_registration: Optional[str] = Query(
        None, description="Filter by first time registration."
    ),
    db: Session = Depends(get_db),
):

    query = db.query(Vehicle)

    # Apply filters
    if partner_location:
        query = query.filter(Vehicle.partner_location == partner_location)
    if model:
        query = query.filter(Vehicle.model == model)
    if state:
        query = query.filter(Vehicle.state == state)
    if exterior:
        query = query.filter(Vehicle.exterior == exterior)
    if interior:
        query = query.filter(Vehicle.interior == interior)
    if wheels:
        query = query.filter(Vehicle.wheels == wheels)
    if motor:
        query = query.filter(Vehicle.motor == motor)
    if edition:
        query = query.filter(Vehicle.edition == edition)

    # Apply numeric filters
    if min_price is not None:
        query = query.filter(Vehicle.retail_price >= min_price)
    if max_price is not None:
        query = query.filter(Vehicle.retail_price <= max_price)
    if min_mileage is not None:
        query = query.filter(Vehicle.mileage >= min_mileage)
    if max_mileage is not None:
        query = query.filter(Vehicle.mileage <= max_mileage)

    # Apply pack filters
    if performance is not None:
        query = query.filter(Vehicle.performance == performance)
    if pilot is not None:
        query = query.filter(Vehicle.pilot == pilot)
    if plus is not None:
        query = query.filter(Vehicle.plus == plus)

    if available is not None:
        query = query.filter(Vehicle.available == available)

    if date_added is not None:
        query = query.filter(Vehicle.date_added == date_added)

    if first_time_registration is not None:
        query = query.filter(Vehicle.first_time_registration == first_time_registration)

    vehicles = query.all()
    return vehicles
