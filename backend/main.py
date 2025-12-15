from fastapi import FastAPI
from pydantic import BaseModel
import os
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from db import get_connection

app = FastAPI()

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)

QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "task_queue")

class TaskCreate(BaseModel):
    title: str

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def on_startup():
    init_db()

@app.post("/api/tasks")
def create_task(payload: TaskCreate):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "INSERT INTO tasks (title) VALUES (%s) RETURNING id, title, status, created_at, updated_at;",
        (payload.title,)
    )
    task = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    redis_client.lpush(QUEUE_NAME, task["id"])

    return {"message": "Task created", "task": task}

@app.get("/api/tasks")
def list_tasks():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM tasks ORDER BY id DESC;")
    tasks = cur.fetchall()
    cur.close()
    conn.close()
    return {"tasks": tasks}

