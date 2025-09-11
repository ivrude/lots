import json
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


class Lot(Base):
    __tablename__ = "lots"
    id = Column(Integer, primary_key=True, index=True)
    price = Column(Numeric, nullable=False, default=0)
    name = Column(String, nullable=False)
    status = Column(String, default="active")
    end_time = Column(DateTime, default=lambda: datetime.utcnow() + timedelta(minutes=2))

Base.metadata.create_all(bind=engine)


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
    username : str


app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.lot_connections: Dict[int, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, lot_id: int):
        await websocket.accept()
        if lot_id not in self.lot_connections:
            self.lot_connections[lot_id] = set()
        self.lot_connections[lot_id].add(websocket)

    def disconnect(self, websocket: WebSocket, lot_id: int):
        if lot_id in self.lot_connections:
            self.lot_connections[lot_id].discard(websocket)
            if not self.lot_connections[lot_id]:
                del self.lot_connections[lot_id]

    async def broadcast(self, lot_id: int, message: str):
        if lot_id in self.lot_connections:
            for connection in self.lot_connections[lot_id]:
                await connection.send_text(message)


manager = ConnectionManager()



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
            message = {
                "type": "lot_closed",
                "lot_id": lot_id,
            }
            await manager.broadcast(lot_id, json.dumps(message))
            break
        await asyncio.sleep(1)


# Websocket
lot_connections: Dict[int, Set[WebSocket]] = {}

@app.websocket("/ws/lots/{lot_id}")
async def websocket_lot(websocket: WebSocket, lot_id: int):
    await manager.connect(websocket, lot_id)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, lot_id)

#Endpoints
@app.post("/lots/", response_model=LotOut)
async def create_lot(lot: LotCreate, db: Session = Depends(get_db)):
    new_lot = Lot(name=lot.name, price=lot.price, status="active")
    db.add(new_lot)
    db.commit()
    db.refresh(new_lot)
    asyncio.create_task(lot_timer(new_lot.id, db))
    return new_lot

@app.post("/lots/{lot_id}/bids", response_model=LotOut)
async def make_bet(lot_id: int, bet_req: BetRequest, db: Session = Depends(get_db)):
    lot = db.query(Lot).filter(Lot.id == lot_id).first()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")
    if lot.status != "active":
        raise HTTPException(status_code=400, detail="Lot is not active")
    if bet_req.bet <= lot.price:
        raise HTTPException(status_code=400, detail="Bet must be higher than current price")

    lot.price = bet_req.bet
    lot.end_time += timedelta(seconds=30)
    db.commit()
    db.refresh(lot)

    # надсилаємо оновлення WebSocket
    ends_at = (lot.end_time + timedelta(hours=3)).isoformat()
    message = {
        "type": "bid_placed",
        "lot_id": lot_id,
        "amount": float(lot.price),
        "username" : bet_req.username,
        "ends_at": ends_at,
    }
    await manager.broadcast(lot_id, json.dumps(message))

    return lot

@app.get("/lots", response_model=List[LotOut])
def get_active_lots(db: Session = Depends(get_db)):
    lots = db.query(Lot).filter(Lot.status == "active").all()
    if not lots:
        raise HTTPException(status_code=404, detail="Lots not found")
    return lots
