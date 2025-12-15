import os
import time
import redis
import psycopg2

QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "task_queue")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
    )

def process_task(task_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tasks SET status = 'completed', updated_at = NOW() WHERE id = %s;",
        (task_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"[worker] Processed task {task_id}")

def main_loop():
    print("[worker] Starting worker loop...")
    while True:
        result = redis_client.brpop(QUEUE_NAME, timeout=5)
        if result:
            queue_name, task_id = result
            try:
                process_task(int(task_id))
            except Exception as e:
                print(f"[worker] Error processing task {task_id}: {e}")
        else:
            time.sleep(1)

if __name__ == "__main__":
    main_loop()

