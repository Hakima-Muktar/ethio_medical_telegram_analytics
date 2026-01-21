from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ChannelActivity(BaseModel):
    channel_name: str
    message_count: int

class TopProduct(BaseModel):
    product_name: str
    count: int

class MessageResponse(BaseModel):
    message_id: int
    channel_name: str
    message_text: Optional[str]
    views: Optional[int]
    message_date: datetime

class VisualStat(BaseModel):
    category: str
    count: int