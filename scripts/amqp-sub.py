#!/usr/bin/env python3
"""Subscribe to all vlinder messages on a local AMQP broker.

Equivalent of `nats sub "vlinder.>"` for AMQP 0-9-1 brokers.
Requires: pip3 install pika
"""

import json
import signal
import sys

try:
    import pika
except ImportError:
    print("Install pika: pip3 install pika", file=sys.stderr)
    sys.exit(1)

signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

url = sys.argv[1] if len(sys.argv) > 1 else "amqp://guest:guest@localhost:5672/%2f"
conn = pika.BlockingConnection(pika.URLParameters(url))
ch = conn.channel()
ch.exchange_declare(exchange="vlinder.topic", exchange_type="topic", durable=True)
result = ch.queue_declare(queue="", exclusive=True, auto_delete=True)
queue = result.method.queue
ch.queue_bind(exchange="vlinder.topic", queue=queue, routing_key="vlinder.#")
print(f"Subscribed to vlinder.# on vlinder.topic exchange ({url}). Ctrl-C to quit.\n")


def on_message(_ch, method, _properties, body):
    rk = method.routing_key
    try:
        payload = json.loads(body)
        print(f"[{rk}] {json.dumps(payload, indent=2)[:500]}")
    except Exception:
        print(f"[{rk}] {body[:500]}")
    print()


ch.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=True)
ch.start_consuming()
