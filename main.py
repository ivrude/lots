from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Numeric, create_engine, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from typing import Dict, Set, List
import asyncio
from datetime import datetime, timedelta


DATABASE_URL = "postgresql://admin:admin@db:5432/test_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ---------- Модель ----------
class Lot(Base):
    __tablename__ = "lots"
    id = Column(Integer, primary_key=True, index=True)
    price = Column(Numeric, nullable=False, default=0)
    name = Column(String, nullable=False)
    status = Column(String, default="active")
    end_time = Column(DateTime, default=lambda: datetime.utcnow() + timedelta(minutes=182))

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

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()
# ---------- Dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def lot_timer(lot_id: int, db: Session):
    while True:
        lot = db.query(Lot).filter(Lot.id == lot_id).first()
        if not lot or lot.status != "active":
            break
        now = datetime.utcnow()
        if lot.end_time <= now:
            lot.status = "closed"
            db.commit()
            await manager.broadcast(f"Lot {lot_id} closed!")
            break
        await asyncio.sleep(1)
# ---------- WebSocket ----------
lot_connections: Dict[int, Set[WebSocket]] = {}

@app.websocket("/ws/lots/{lot_id}")
async def websocket_lot(websocket: WebSocket, lot_id: int):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ---------- Ендпоінти ----------
@app.post("/lots/", response_model=LotOut)
async def create_lot(lot: LotCreate, db: Session = Depends(get_db)):
    new_lot = Lot(name=lot.name, price=lot.price, status="active")
    db.add(new_lot)
    db.commit()
    db.refresh(new_lot)
    asyncio.create_task(lot_timer(new_lot.id, db))
    return new_lot

@app.post("/lots/{lot_id}/bet", response_model=LotOut)
async def make_bet(lot_id: int, bet_req: BetRequest, db: Session = Depends(get_db)):
    lot = db.query(Lot).filter(Lot.id == lot_id).first()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")
    if lot.status != "active":
        raise HTTPException(status_code=400, detail="Lot is not active")
    if bet_req.bet <= lot.price:
        raise HTTPException(status_code=400, detail="Bet must be higher than current price")

    lot.price = bet_req.bet
    lot.end_time = datetime.utcnow() + timedelta(seconds=30, hours=3)
    db.commit()
    db.refresh(lot)

    # надсилаємо оновлення WebSocket

    await manager.broadcast(f"Lot {lot_id} new price: {lot.price}, ends at {lot.end_time.isoformat()}")

    return lot

@app.get("/lots/{lot_id}", response_model=LotOut)
def get_lot(lot_id: int, db: Session = Depends(get_db)):
    lot = db.query(Lot).filter(Lot.id == lot_id).first()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")
    return lot
