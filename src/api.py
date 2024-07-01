from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, Column, String, LargeBinary, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import uuid, json, redis, io, os, time


app = FastAPI()
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

redis_client = redis.Redis(host="redis", port=os.getenv("REDIS_PORT"), password=os.getenv("REDIS_PASSWORD"), db=os.getenv("REDIS_DBNUMBER"))

class Task(Base):
    __tablename__ = 'tasks'
    task_id = Column(String(36), primary_key=True, index=True)
    text = Column(Text, nullable=False)
    status = Column(String(20), nullable=False)
    wordcloud_image = Column(LargeBinary)

Base.metadata.create_all(bind=engine)

@app.post("/api/wordcloud/generate")
async def generate_wordcloud(text: str):
    if not text:
        raise HTTPException(status_code=400, detail="Text is required")
    
    task_id = str(uuid.uuid4())
    task = Task(task_id=task_id, text=text, status='queued')
    
    db = SessionLocal()
    db.add(task)
    db.commit()
    db.close()
    
    task_data = {'task_id': task_id, 'text': text}
    redis_client.rpush('wordcloud_tasks', json.dumps(task_data))
    
    return JSONResponse(content={'status': 'queued', 'task_id': task_id})

@app.get("/api/wordcloud/status/{task_id}")
async def get_wordcloud_status(task_id: str):
    db = SessionLocal()
    task = db.query(Task).filter(Task.task_id == task_id).first()
    db.close()
    
    if task:
        return JSONResponse(content={'task_id': task.task_id, 'status': task.status})
    else:
        raise HTTPException(status_code=404, detail="Task not found")

@app.get("/api/wordcloud/{task_id}")
async def download_wordcloud(task_id: str):
    db = SessionLocal()
    task = db.query(Task).filter(Task.task_id == task_id).first()
    db.close()
    
    if task and task.wordcloud_image:
        return StreamingResponse(
            io.BytesIO(task.wordcloud_image),
            media_type="image/png",
            headers={"Content-Disposition": f"attachment; filename={task_id}.png"}
        )
    elif task:
        return JSONResponse(content={'status': task.status, 'message': 'Word cloud not yet generated'}, status_code=202)
    else:
        raise HTTPException(status_code=404, detail="Task not found")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)