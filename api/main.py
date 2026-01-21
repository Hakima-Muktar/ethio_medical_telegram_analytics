from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from .database import get_db
from .schemas import ChannelActivity, TopProduct, MessageResponse, VisualStat

app = FastAPI(
    title="Medical Data Warehouse API",
    description="API for accessing Telegram medical channel analytics",
    version="1.0.0"
)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/channels/activity", response_model=List[ChannelActivity])
def get_channel_activity(db: Session = Depends(get_db)):
    """
    Get message count per channel
    """
    try:
        query = text("""
            SELECT c.channel_name, COUNT(m.message_id) as message_count
            FROM dbt_dev.fct_messages m
            JOIN dbt_dev.dim_channels c ON m.channel_key = c.channel_key
            GROUP BY c.channel_name
            ORDER BY message_count DESC
        """)
        result = db.execute(query).fetchall()
        return [{"channel_name": row[0], "message_count": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/products/top", response_model=List[TopProduct])
def get_top_products(db: Session = Depends(get_db)):
    """
    Get top detected products (heuristic based on YOLO detections)
    """
    try:
        query = text("""
            SELECT detection_class, COUNT(*) as count
            FROM dbt_dev.fct_image_detections
            WHERE image_class = 'Product Display'
            GROUP BY detection_class
            ORDER BY count DESC
            LIMIT 10
        """)
        result = db.execute(query).fetchall()
        return [{"product_name": row[0], "count": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/messages/search", response_model=List[MessageResponse])
def search_messages(keyword: str, db: Session = Depends(get_db)):
    """
    Search messages by keyword
    """
    try:
        query = text("""
            SELECT m.message_id, c.channel_name, m.message_text, m.view_count as views, d.full_date
            FROM dbt_dev.fct_messages m
            JOIN dbt_dev.dim_channels c ON m.channel_key = c.channel_key
            JOIN dbt_dev.dim_dates d ON m.date_key = d.date_key
            WHERE m.message_text ILIKE :keyword
            LIMIT 50
        """)
        result = db.execute(query, {"keyword": f"%{keyword}%"}).fetchall()
        return [
            {
                "message_id": row[0],
                "channel_name": row[1],
                "message_text": row[2],
                "views": row[3],
                "message_date": row[4]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/visual/stats", response_model=List[VisualStat])
def get_visual_stats(db: Session = Depends(get_db)):
    """
    Get statistics on visual content types
    """
    try:
        query = text("""
            SELECT image_class, COUNT(*) as count
            FROM dbt_dev.fct_image_detections
            GROUP BY image_class
            ORDER BY count DESC
        """)
        result = db.execute(query).fetchall()
        return [{"category": row[0], "count": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)