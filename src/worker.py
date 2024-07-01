from sqlalchemy import create_engine, Column, String, LargeBinary, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from wordcloud import WordCloud

import json, redis, time, io, os


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

def process_task(task_data):
    session = SessionLocal()
    task = json.loads(task_data)
    task_id = task['task_id']
    text = task['text']
    
    # Генерация облака слов
    wordcloud = WordCloud(width=800, height=400).generate(text)
    image_io = io.BytesIO()
    wordcloud.to_image().save(image_io, format='PNG')
    image_bytes = image_io.getvalue()
    
    # Обновление задачи в базе данных
    db_task = session.query(Task).filter(Task.task_id == task_id).first()
    if db_task:
        db_task.status = 'completed'
        db_task.wordcloud_image = image_bytes
        session.commit()
    
    session.close()

while True:
    _, task_data = redis_client.blpop('wordcloud_tasks')
    process_task(task_data)