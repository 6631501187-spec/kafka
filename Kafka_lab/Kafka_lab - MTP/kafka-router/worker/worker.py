import json
import os
import socket
import time

import paramiko
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

CONSUMER_GROUP = os.getenv(
    "CONSUMER_GROUP",
    "cisco-workers"
)

ROUTER_USERNAME = os.getenv(
    "ROUTER_USERNAME",
    "admin"
)

ROUTER_PASSWORD = os.getenv(
    "ROUTER_PASSWORD",
    "cisco"
)

WORKER_NAME = os.getenv(
    "WORKER_NAME",
    socket.gethostname()
)


# ============================================================
# Legacy Cisco SSH Transport
# ============================================================

def legacy_transport_factory(sock, **kwargs):

    transport = paramiko.Transport(sock, **kwargs)

    options = transport.get_security_options()

    # Cisco router KEX
    options.kex = (
        "diffie-hellman-group14-sha1",
    )

    # Cisco router host key
    options.key_types = (
        "ssh-rsa",
    )

    # Use the exact cipher that worked with OpenSSH
    options.ciphers = (
        "aes128-cbc",
    )

    # Cisco router MAC
    options.macs = (
        "hmac-sha1",
    )

    return transport


# ============================================================
# Kafka
# ============================================================

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

admin_client = AdminClient({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


# ============================================================
# Discover Topics
# ============================================================

def get_command_topics():

    metadata = admin_client.list_topics(timeout=5)

    return sorted(
        topic
        for topic in metadata.topics.keys()
        if topic.startswith("router-command-")
    )


# ============================================================
# Execute Cisco Command
# ============================================================

def execute_command(router_ip, command):

    print("-" * 60)
    print(f"[{WORKER_NAME}] Connecting to Cisco router")
    print(f"[{WORKER_NAME}] Router  : {router_ip}")
    print(f"[{WORKER_NAME}] Command : {command}")
    print("-" * 60)

    client = paramiko.SSHClient()

    client.set_missing_host_key_policy(
        paramiko.AutoAddPolicy()
    )

    try:

        client.connect(
            hostname=router_ip,
            username=ROUTER_USERNAME,
            password=ROUTER_PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
            auth_timeout=10,
            banner_timeout=10,
            transport_factory=legacy_transport_factory
        )

        print(
            f"[{WORKER_NAME}] SSH connection successful"
        )

        shell = client.invoke_shell()

        time.sleep(1)

        # Clear initial output
        if shell.recv_ready():
            shell.recv(65535)

        # Disable Cisco paging
        shell.send("terminal length 0\n")

        time.sleep(0.5)

        if shell.recv_ready():
            shell.recv(65535)

        # Execute requested command
        shell.send(command + "\n")

        time.sleep(2)

        output = b""

        while shell.recv_ready():
            output += shell.recv(65535)

        output = output.decode(
            "utf-8",
            errors="replace"
        )

        print()
        print("=" * 60)
        print("Cisco Router Output")
        print("=" * 60)
        print(output)
        print("=" * 60)

        return output

    except Exception as e:

        print(
            f"[{WORKER_NAME}] SSH connection failed: {e}"
        )

        raise

    finally:

        client.close()

        print(
            f"[{WORKER_NAME}] SSH connection closed"
        )


# ============================================================
# Process Kafka Message
# ============================================================

def process_message(msg):

    try:

        data = json.loads(
            msg.value().decode("utf-8")
        )

        request_id = data.get("request_id")
        router_ip = data.get("router_ip")
        command = data.get("command")

        print()
        print("=" * 60)
        print(f"[{WORKER_NAME}] Kafka message received")
        print("=" * 60)

        print(f"Topic     : {msg.topic()}")
        print(f"Partition : {msg.partition()}")
        print(f"Offset    : {msg.offset()}")
        print(f"Request ID : {request_id}")
        print(f"Router     : {router_ip}")
        print(f"Command    : {command}")
        print()

        execute_command(
            router_ip,
            command
        )

        print(
            f"[{WORKER_NAME}] "
            "Command execution successful"
        )

        return True

    except Exception as e:

        print(
            f"[{WORKER_NAME}] "
            f"Command execution failed: {e}"
        )

        return False


# ============================================================
# Main Worker
# ============================================================

def main():

    print("=" * 60)
    print(f"Worker started: {WORKER_NAME}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print("=" * 60)

    subscribed_topics = set()

    while True:

        try:

            topics = get_command_topics()

            new_topics = set(topics) - subscribed_topics

            if new_topics:

                print()
                print(
                    f"[{WORKER_NAME}] "
                    "New command topic(s) found:"
                )

                for topic in sorted(new_topics):
                    print(f"  + {topic}")

                subscribed_topics.update(new_topics)

                consumer.subscribe(
                    sorted(subscribed_topics)
                )

            elif not subscribed_topics:

                print(
                    f"[{WORKER_NAME}] "
                    "No command topics yet. Waiting..."
                )

                time.sleep(2)
                continue

            msg = consumer.poll(
                timeout=2.0
            )

            if msg is None:
                continue

            if msg.error():

                print(
                    f"[{WORKER_NAME}] "
                    f"Kafka error: {msg.error()}"
                )

                continue

            success = process_message(msg)

            if success:

                consumer.commit(
                    message=msg,
                    asynchronous=False
                )

                print(
                    f"[{WORKER_NAME}] "
                    "Message committed."
                )

            else:

                print(
                    f"[{WORKER_NAME}] "
                    "Message NOT committed."
                )

                print(
                    f"[{WORKER_NAME}] "
                    "It can be retried."
                )

        except KeyboardInterrupt:

            print()
            print(
                f"[{WORKER_NAME}] "
                "Worker stopping..."
            )

            break

        except Exception as e:

            print(
                f"[{WORKER_NAME}] "
                f"Worker error: {e}"
            )

            time.sleep(3)

    consumer.close()

    print(
        f"[{WORKER_NAME}] Worker stopped."
    )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()
