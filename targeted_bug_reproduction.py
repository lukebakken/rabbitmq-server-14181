#!/usr/bin/env python3
"""
Fixed RabbitMQ message store bug reproduction with proper threading
"""

import pika
import threading
import time
import random
import sys
import functools
import argparse
from datetime import datetime, timedelta

# Global connection parameters
CONNECTION_PARAMS = None

# Pre-computed message bodies
MESSAGE_BODIES = []

def generate_message_bodies():
    """Pre-generate message bodies with specific characteristics"""
    global MESSAGE_BODIES

    # Small messages (<4KB)
    for i in range(50):
        size = random.randint(100, 4000)
        body = bytes([255] * size)
        MESSAGE_BODIES.append(body)

    # Large messages (4KB-16KB)
    for i in range(50):
        size = random.randint(4096, 16384)
        body = bytes([255] * size)
        MESSAGE_BODIES.append(body)

    print(f"Generated {len(MESSAGE_BODIES)} pre-computed message bodies")

class DelayedAckConsumer(threading.Thread):
    def __init__(self, queue_name, consumer_id):
        super().__init__()
        self.queue_name = queue_name
        self.consumer_tag = f"delayed-ack-consumer-{consumer_id}"
        self.connection = None
        self.channel = None
        self.pending_acks = {}
        self.running = True

    def callback(self, ch, method, properties, body):
        delivery_tag = method.delivery_tag

        # Random delay with shorter times for faster ack rate
        # 1% chance of very long delay (20-35 min), rest are much shorter
        if random.random() < 0.01:
            delay_seconds = random.randint(1200, 2100)  # 20-35 minutes
        else:
            delay_seconds = random.uniform(0, 300)  # 0-5 minutes

        ack_time = datetime.now() + timedelta(seconds=delay_seconds)
        self.pending_acks[delivery_tag] = ack_time

    def open_channel_and_consume(self):
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=100)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, consumer_tag=self.consumer_tag)
        print(f"Consumer {self.consumer_tag} started")

    def reconnect(self):
        """Reconnect after channel failure"""
        try:
            self.pending_acks.clear()  # Clear old delivery tags
            if self.connection is None or self.connection.is_closed:
                # Connection is bad, recreate
                self.connection = pika.BlockingConnection(CONNECTION_PARAMS)
                print(f"Consumer {self.consumer_tag} reconnected")
            self.open_channel_and_consume()
            print(f"Consumer {self.consumer_tag} recreated channel")
            return True
        except Exception as e:
            print(f"Consumer {self.consumer_tag} reconnect failed: {e}")
            return False

    def process_pending_acks(self):
        """Process pending acks - called periodically"""
        now = datetime.now()
        to_ack = []

        for delivery_tag, ack_time in self.pending_acks.items():
            if now >= ack_time:
                to_ack.append(delivery_tag)

        for delivery_tag in to_ack:
            try:
                if self.channel and self.channel.is_open:
                    self.channel.basic_ack(delivery_tag=delivery_tag)
                    del self.pending_acks[delivery_tag]
                else:
                    # Channel closed, clear this ack (message will be redelivered)
                    del self.pending_acks[delivery_tag]
            except Exception as e:
                print(f"Consumer {self.consumer_tag} ack error: {e}")
                # Clear the ack on error
                if delivery_tag in self.pending_acks:
                    del self.pending_acks[delivery_tag]

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()

    def run(self):
        try:
            self.connection = pika.BlockingConnection(CONNECTION_PARAMS)
            self.open_channel_and_consume()

            # Process messages and acks in same thread
            while self.running:
                try:
                    if not self.channel or not self.channel.is_open:
                        print(f"Consumer {self.consumer_tag} channel closed, reconnecting...")
                        if not self.reconnect():
                            self.connection.process_data_events(5)
                            continue

                    self.connection.process_data_events(time_limit=1)
                    self.process_pending_acks()

                except Exception as e:
                    print(f"Consumer {self.consumer_tag} processing error: {e}")
                    # Try to reconnect on error
                    if not self.reconnect():
                        self.connection.process_data_events(5)

        except Exception as e:
            print(f"Consumer {self.consumer_tag} error: {e}")
        finally:
            self.running = False
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()

def publisher_worker(queue_name, publisher_id, runtime_hours=4):
    """Publisher with priority messages that runs for specified hours"""
    try:
        connection = pika.BlockingConnection(CONNECTION_PARAMS)
        channel = connection.channel()

        end_time = time.time() + (runtime_hours * 3600)  # 4 hours
        message_count = 0

        while time.time() < end_time:
            # Check queue depth every 100 messages
            if message_count % 100 == 0:
                try:
                    method = channel.queue_declare(queue=queue_name, passive=True)
                    queue_depth = method.method.message_count

                    # Adjust publishing rate based on queue depth
                    if queue_depth < 5000:
                        # Fast publishing to build backlog
                        batch_size = 20
                        sleep_time = 0.5
                    elif queue_depth < 10000:
                        # Medium publishing
                        batch_size = 10
                        sleep_time = 1.0
                    else:
                        # Slow maintenance publishing
                        batch_size = 2
                        sleep_time = 3.0
                except:
                    # Default to medium rate if can't check queue
                    batch_size = 10
                    sleep_time = 1.0

            # Publish batch
            for i in range(batch_size):
                # 90% priority 1, 10% priorities 2-10
                if random.random() < 0.9:
                    priority = 1
                else:
                    priority = random.randint(2, 10)

                message_body = random.choice(MESSAGE_BODIES)

                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        priority=priority
                    )
                )
                message_count += 1

                if message_count % 1000 == 0:
                    print(f"Publisher {publisher_id}: {message_count} messages sent")

            connection.process_data_events(sleep_time)

        connection.close()
        print(f"Publisher {publisher_id} completed after {runtime_hours} hours")
    except Exception as e:
        print(f"Publisher {publisher_id} error: {e}")

def monitor_progress(queue_name, consumers, runtime_hours=4):
    """Monitor and report progress every minute"""
    end_time = time.time() + (runtime_hours * 3600)

    while time.time() < end_time:
        try:
            connection = pika.BlockingConnection(CONNECTION_PARAMS)
            channel = connection.channel()
            method = channel.queue_declare(queue=queue_name, passive=True)
            queue_depth = method.method.message_count
            connection.close()

            active_consumers = sum(1 for c in consumers if c.is_alive())
            elapsed_hours = (time.time() - (end_time - runtime_hours * 3600)) / 3600

            print(f"[{elapsed_hours:.1f}h] Queue depth: {queue_depth}, Active consumers: {active_consumers}")

        except Exception as e:
            print(f"Monitor error: {e}")

        time.sleep(60)  # Report every minute

def create_initial_backlog(queue_name, target_backlog=10000):
    """Create initial 10K message backlog"""
    print(f"Creating initial backlog of {target_backlog} messages...")

    connection = pika.BlockingConnection(CONNECTION_PARAMS)
    channel = connection.channel()

    # Declare priority queue
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments={
            'x-queue-type': 'classic',
            'x-max-priority': 10
        }
    )

    for i in range(target_backlog):
        priority = 1 if random.random() < 0.9 else random.randint(2, 10)
        message_body = random.choice(MESSAGE_BODIES)

        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                priority=priority
            )
        )

        if i % 1000 == 0:
            print(f"Backlog progress: {i}/{target_backlog}")

    connection.close()
    print("Initial backlog created")

def main():
    global CONNECTION_PARAMS

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='RabbitMQ message store bug reproduction')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host (default: localhost)')
    args = parser.parse_args()

    # Set up connection parameters
    CONNECTION_PARAMS = pika.ConnectionParameters(host=args.host)

    print("Fixed RabbitMQ Message Store Bug Reproduction")
    print(f"Connecting to RabbitMQ at {args.host}")
    print("Generating message bodies...")
    generate_message_bodies()

    queue_name = f"priority_bug_test_{int(time.time())}"

    # Create initial backlog
    create_initial_backlog(queue_name)

    print("Starting publishers and consumers...")

    # Start 15 publishers for 4-hour runtime
    publishers = []
    for i in range(15):
        t = threading.Thread(target=publisher_worker, args=(queue_name, i, 4))  # 4 hours
        publishers.append(t)
        t.start()

    # Start 20 consumers with delayed acks
    consumers = []
    for i in range(20):
        consumer = DelayedAckConsumer(queue_name, i)
        consumers.append(consumer)
        consumer.start()
        time.sleep(0.2)  # Stagger consumer starts

    # Start progress monitor
    monitor_thread = threading.Thread(target=monitor_progress, args=(queue_name, consumers, 4))
    monitor_thread.daemon = True
    monitor_thread.start()

    print("4-hour test running... Monitor RabbitMQ logs for function_clause errors")
    print("Press Ctrl+C to stop gracefully")

    try:
        # Wait for publishers (4 hours)
        for t in publishers:
            t.join()
        print("All publishers completed after 4 hours")

        # Wait a bit more for final consumer processing
        time.sleep(300)  # 5 minutes

    except KeyboardInterrupt:
        print("\nGracefully stopping test...")
    finally:
        # Stop consumers
        for consumer in consumers:
            consumer.stop()

        # Wait for consumer threads to finish
        for consumer in consumers:
            consumer.join(timeout=5)

        # Cleanup
        try:
            connection = pika.BlockingConnection(CONNECTION_PARAMS)
            channel = connection.channel()
            channel.queue_delete(queue=queue_name)
            connection.close()
            print(f"Cleaned up queue: {queue_name}")
        except Exception as e:
            print(f"Cleanup error: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
