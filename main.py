from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Numeric, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# ---------- Налаштування бази ----------
DATABASE_URL = "postgresql://admin:admin@db:5432/test_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ---------- Модель БД ----------
class Lot(Base):
    __tablename__ = "lots"

    id = Column(Integer, primary_key=True, index=True)
    price = Column(Numeric, nullable=False, default=0)
    name = Column(String, nullable=False)
    status = Column(String, default="active")

Base.metadata.create_all(bind=engine)
# ---------- Pydantic схеми ----------
class LotCreate(BaseModel):
    name: str
    price: float = 0

class LotOut(BaseModel):
    id: int
    name: str
    price: float
    status: str

    class Config:
        orm_mode = True

class BetRequest(BaseModel):
    bet: float

# ---------- FastAPI ----------
app = FastAPI()

# Dependency для сесії
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Ендпоінти ----------
@app.post("/lots/", response_model=LotOut)
def create_lot(lot: LotCreate, db: Session = Depends(get_db)):
    new_lot = Lot(name=lot.name, price=lot.price, status="active")
    db.add(new_lot)
    db.commit()
    db.refresh(new_lot)
    return new_lot

@app.post("/lots/{lot_id}/bet", response_model=LotOut)
def make_bet(lot_id: int, bet_req: BetRequest, db: Session = Depends(get_db)):
    lot = db.query(Lot).filter(Lot.id == lot_id).first()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")
    if lot.status != "active":
        raise HTTPException(status_code=400, detail="Lot is not active")
    if bet_req.bet <= lot.price:
        raise HTTPException(status_code=400, detail="Bet must be higher than current price")

    lot.price = bet_req.bet
    db.commit()
    db.refresh(lot)
    return lot

@app.get("/lots/{lot_id}", response_model=LotOut)
def get_lot(lot_id: int, db: Session = Depends(get_db)):
    lot = db.query(Lot).filter(Lot.id == lot_id).first()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")
    return lot
