from typing import Optional
from pydantic import BaseModel, Field

class StockInfo(BaseModel):
    symbol: str
    reference_price: Optional[float] = Field(None, alias="referencePrice")
    ceiling_price: Optional[float] = Field(None, alias="ceilingPrice")
    floor_price: Optional[float] = Field(None, alias="floorPrice")
    open_price: Optional[float] = Field(None, alias="openPrice")
    high_price: Optional[float] = Field(None, alias="highPrice")
    low_price: Optional[float] = Field(None, alias="lowPrice")
    close_price: Optional[float] = Field(None, alias="closePrice")
    current_room: Optional[int] = Field(None, alias="currentRoom")
    total_volume_traded: Optional[int] = Field(None, alias="totalVolumeTraded")
    
    class Config:
        populate_by_name = True

class TopPrice(BaseModel):
    symbol: str
    best_bid_price: Optional[float] = Field(None, alias="bestBidPrice")
    best_bid_volume: Optional[int] = Field(None, alias="bestBidVolume")
    best_offer_price: Optional[float] = Field(None, alias="bestOfferPrice")
    best_offer_volume: Optional[int] = Field(None, alias="bestOfferVolume")

    class Config:
        populate_by_name = True

class Tick(BaseModel):
    symbol: str
    match_price: float = Field(..., alias="matchPrice")
    match_quantity: int = Field(..., alias="matchQtty")
    match_value: Optional[float] = Field(None, alias="matchValue")
    side: Optional[str] = None
    sending_time: Optional[str] = Field(None, alias="sendingTime")

    class Config:
        populate_by_name = True

class BoardEvent(BaseModel):
    board_event_id: str = Field(..., alias="boardEventID")
    board_event_name: str = Field(..., alias="boardEventName")
    description: Optional[str] = None

    class Config:
        populate_by_name = True
