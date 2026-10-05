import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

ROUTER_IP = "192.168.142.157"

TOPIC_PREFIX = "router-command-"


# ============================================================
# Kafka
# ============================================================

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

admin = AdminClient({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


# ============================================================
# FastAPI
# ============================================================

app = FastAPI(title="Cisco Command Backend")


class CommandRequest(BaseModel):
    command: str


# ============================================================
# Kafka delivery callback
# ============================================================

def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(
            f"[Kafka] Message delivered to "
            f"{msg.topic()} [{msg.partition()}] @ {msg.offset()}"
        )


# ============================================================
# Health check
# ============================================================

@app.get("/")
def root():
    return {
        "service": "backend1",
        "status": "running"
    }


# ============================================================
# Send Cisco command
# ============================================================

@app.post("/command")
def send_command(request: CommandRequest):

    command = request.command.strip()

    if not command:
        raise HTTPException(
            status_code=400,
            detail="Command cannot be empty"
        )

    # Generate unique request ID
    request_id = str(uuid.uuid4())

    # Create a new Kafka topic for this command
    topic_name = f"{TOPIC_PREFIX}{request_id}"

    print()
    print("=" * 60)
    print(f"[Backend1] Request ID : {request_id}")
    print(f"[Backend1] Command    : {command}")
    print(f"[Backend1] Topic      : {topic_name}")
    print("=" * 60)

    # --------------------------------------------------------
    # Create topic
    # --------------------------------------------------------

    topic = NewTopic(
        topic_name,
        num_partitions=1,
        replication_factor=1
    )

    futures = admin.create_topics([topic])

    try:
        futures[topic_name].result()
        print(f"[Kafka] Created topic: {topic_name}")

    except Exception as e:

        # Topic may already exist, although UUID makes this unlikely
        if "TopicExistsError" in str(e):
            print(f"[Kafka] Topic already exists: {topic_name}")
        else:
            print(f"[Kafka] Topic creation failed: {e}")

            raise HTTPException(
                status_code=500,
                detail=f"Kafka topic creation failed: {e}"
            )

    # --------------------------------------------------------
    # Create event
    # --------------------------------------------------------

    event = {
        "request_id": request_id,
        "router_ip": ROUTER_IP,
        "command": command,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    message = json.dumps(event)

    print(f"[Backend1] Producing:")
    print(message)

    # --------------------------------------------------------
    # Produce message
    # --------------------------------------------------------

    try:

        producer.produce(
            topic=topic_name,
            key=request_id,
            value=message,
            callback=delivery_report
        )

        producer.flush()

    except Exception as e:

        print(f"[Kafka] Produce failed: {e}")

        raise HTTPException(
            status_code=500,
            detail=f"Kafka produce failed: {e}"
        )

    return {
        "status": "queued",
        "request_id": request_id,
        "topic": topic_name,
        "router_ip": ROUTER_IP,
        "command": command
    }
