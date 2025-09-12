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
    def __init__(self, queue_name, consumer_id, consumer_timeout_minutes):
        super().__init__()
        self.queue_name = queue_name
        self.consumer_tag = f"delayed-ack-consumer-{consumer_id}"
        self.connection = None
        self.channel = None
        self.pending_acks = {}
        self.running = True
        self.consumer_timeout_minutes = consumer_timeout_minutes

    def callback(self, ch, method, properties, body):
        delivery_tag = method.delivery_tag

        # Random delay with 1% chance of timeout, rest are short for good flow (original pattern)
        # Safe range: 0-5 minutes (for message flow), Timeout range: (timeout_minutes + 1) to (timeout_minutes + 3)
        if random.random() < 0.01:
            delay_seconds = random.randint(
                (self.consumer_timeout_minutes + 1) * 60,
                (self.consumer_timeout_minutes + 3) * 60
            )  # Will timeout
        else:
            delay_seconds = random.uniform(0, 300)  # Keep at 0-5 minutes for good flow

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
            print(f"Consumer {self.consumer_tag} recreating channel and starting")
            self.open_channel_and_consume()
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

def perftest_workload(base_queue_name, consumer_timeout_ms, runtime_hours=2):
    """PerfTest-like concurrent workload with separate queue and bidirectional I/O"""
    perftest_queue = f"perftest_{base_queue_name}"
    print(f"Starting PerfTest-like workload on queue: {perftest_queue}")

    # Declare the PerfTest queue
    try:
        setup_connection = pika.BlockingConnection(CONNECTION_PARAMS)
        setup_channel = setup_connection.channel()
        setup_channel.queue_declare(
            queue=perftest_queue,
            durable=True,
            arguments={
                'x-queue-type': 'classic',
                'x-max-priority': 10,
                'x-consumer-timeout': consumer_timeout_ms
            }
        )
        setup_connection.close()
        print(f"PerfTest queue {perftest_queue} declared")
    except Exception as e:
        print(f"PerfTest queue setup error: {e}")
        return

    def perftest_producer():
        """PerfTest producer with cycling pattern"""
        try:
            connection = pika.BlockingConnection(CONNECTION_PARAMS)
            channel = connection.channel()

            end_time = time.time() + (runtime_hours * 3600)
            cycle_count = 0

            while time.time() < end_time:
                cycle_count += 1
                print(f"PerfTest producer cycle {cycle_count}: Starting 2-minute burst...")

                # 2-minute active period
                burst_end = time.time() + 120
                message_count = 0

                while time.time() < burst_end:
                    # Smaller variable message sizes (4KB-16KB) to avoid memory alarms
                    if random.random() < 0.3:
                        # Use medium pre-computed messages
                        base_body = random.choice(MESSAGE_BODIES[50:70])  # Medium messages
                        target_size = random.randint(4096, 8192)  # 4-8KB
                    elif random.random() < 0.6:
                        base_body = random.choice(MESSAGE_BODIES[70:85])  # Large messages
                        target_size = random.randint(8192, 12288)  # 8-12KB
                    else:
                        base_body = random.choice(MESSAGE_BODIES[85:])  # Largest messages
                        target_size = random.randint(12288, 16384)  # 12-16KB

                    # Pad to target size efficiently
                    if len(base_body) < target_size:
                        padding_needed = target_size - len(base_body)
                        message_body = base_body + bytes([255] * padding_needed)
                    else:
                        message_body = base_body[:target_size]

                    # Priority distribution similar to main workload (90% priority 1, 10% mixed)
                    if random.random() < 0.9:
                        priority = 1
                    else:
                        priority = random.randint(2, 10)

                    channel.basic_publish(
                        exchange='',
                        routing_key=perftest_queue,
                        body=message_body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent
                            priority=priority
                        )
                    )
                    message_count += 1

                print(f"PerfTest producer cycle {cycle_count}: Sent {message_count} messages, pausing...")

                # 5-minute pause period
                connection.process_data_events(300)

            connection.close()
            print("PerfTest producer completed")

        except Exception as e:
            print(f"PerfTest producer error: {e}")

    def perftest_consumer():
        """PerfTest consumer with multi-ack"""
        try:
            connection = pika.BlockingConnection(CONNECTION_PARAMS)
            channel = connection.channel()
            channel.basic_qos(prefetch_count=100)  # Same as main consumers

            ack_batch = []
            batch_size = 10  # Multi-ack every 10 messages

            def callback(ch, method, properties, body):
                ack_batch.append(method.delivery_tag)

                # Multi-ack when batch is full
                if len(ack_batch) >= batch_size:
                    try:
                        # Ack the highest delivery tag with multiple=True
                        ch.basic_ack(delivery_tag=max(ack_batch), multiple=True)
                        ack_batch.clear()
                    except Exception as e:
                        print(f"PerfTest consumer ack error: {e}")
                        ack_batch.clear()

            channel.basic_consume(queue=perftest_queue, on_message_callback=callback)

            end_time = time.time() + (runtime_hours * 3600)
            while time.time() < end_time:
                try:
                    connection.process_data_events(time_limit=1)

                    # Periodic cleanup of remaining acks
                    if ack_batch and random.random() < 0.1:  # 10% chance per second
                        try:
                            channel.basic_ack(delivery_tag=max(ack_batch), multiple=True)
                            ack_batch.clear()
                        except:
                            ack_batch.clear()

                except Exception as e:
                    print(f"PerfTest consumer processing error: {e}")
                    break

            # Final ack cleanup
            if ack_batch:
                try:
                    channel.basic_ack(delivery_tag=max(ack_batch), multiple=True)
                except:
                    pass

            connection.close()
            print("PerfTest consumer completed")

        except Exception as e:
            print(f"PerfTest consumer error: {e}")

    # Start both producer and consumer threads
    producer_thread = threading.Thread(target=perftest_producer)
    consumer_thread = threading.Thread(target=perftest_consumer)

    producer_thread.start()
    consumer_thread.start()

    # Wait for both to complete
    producer_thread.join()
    consumer_thread.join()

    print("PerfTest workload completed")

def create_initial_backlog(queue_name, consumer_timeout_ms, target_backlog=10000):
    """Create initial 10K message backlog"""
    print(f"Creating initial backlog of {target_backlog} messages...")

    connection = pika.BlockingConnection(CONNECTION_PARAMS)
    channel = connection.channel()

    # Declare priority queue with consumer timeout
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments={
            'x-queue-type': 'classic',
            'x-max-priority': 10,
            'x-consumer-timeout': consumer_timeout_ms
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
    parser.add_argument('--consumer-timeout', type=int, default=30,
                       help='Consumer timeout in minutes (minimum: 5, default: 30)')
    parser.add_argument('--enable-perftest', action='store_true', default=False,
                       help='Enable PerfTest concurrent workload (default: disabled)')
    args = parser.parse_args()

    # Validate consumer timeout
    if args.consumer_timeout < 5:
        print("Error: --consumer-timeout must be at least 5 minutes")
        sys.exit(1)

    consumer_timeout_minutes = args.consumer_timeout
    consumer_timeout_ms = consumer_timeout_minutes * 60 * 1000

    # Set up connection parameters
    CONNECTION_PARAMS = pika.ConnectionParameters(host=args.host)

    print("Fixed RabbitMQ Message Store Bug Reproduction")
    print(f"Connecting to RabbitMQ at {args.host}")
    print(f"Consumer timeout: {consumer_timeout_minutes} minutes")
    print("Generating message bodies...")
    generate_message_bodies()

    queue_name = f"priority_bug_test_{int(time.time())}"

    # Create initial backlog
    create_initial_backlog(queue_name, consumer_timeout_ms)

    print("Starting publishers and consumers...")

    # Start 15 publishers for 2-hour runtime
    publishers = []
    for i in range(15):
        t = threading.Thread(target=publisher_worker, args=(queue_name, i, 2))  # 2 hours
        publishers.append(t)
        t.start()

    # Start 20 consumers with delayed acks
    consumers = []
    for i in range(20):
        consumer = DelayedAckConsumer(queue_name, i, consumer_timeout_minutes)
        consumers.append(consumer)
        consumer.start()
        time.sleep(0.2)  # Stagger consumer starts

    # Start progress monitor
    monitor_thread = threading.Thread(target=monitor_progress, args=(queue_name, consumers, 2))
    monitor_thread.start()

    # Conditionally start PerfTest workload if enabled
    if args.enable_perftest:
        # Start PerfTest workload after 3 consumer timeout cycles plus buffer (matches original 1.5h test)
        # Each cycle is (timeout_minutes + 1) since timeouts start at timeout+1 minutes
        # Add 2 minutes buffer to ensure all three cycles complete
        perftest_delay_seconds = (3 * (consumer_timeout_minutes + 1) + 2) * 60
        def start_perftest_delayed():
            print(f"Waiting {perftest_delay_seconds//60} minutes for consumer timeouts before starting PerfTest workload...")
            time.sleep(perftest_delay_seconds)
            perftest_workload(queue_name, consumer_timeout_ms, 2)

        perftest_thread = threading.Thread(target=start_perftest_delayed)
        perftest_thread.start()
        perftest_status = f"PerfTest workload: Starts after {perftest_delay_seconds//60} minutes (3 timeout cycles + buffer)"
    else:
        perftest_status = "PerfTest workload: DISABLED (use --enable-perftest to enable)"

    print("Optimized reproduction test running...")
    print(f"- Consumer timeouts: 1% of acks will timeout after {consumer_timeout_minutes + 1}+ minutes (original pattern)")
    print(f"- {perftest_status}")
    if args.enable_perftest:
        print("- Fragmentation period: 95+ minutes (matches original 1.5+ hour successful test)")
    print("Monitor RabbitMQ logs for function_clause errors")
    print("Press Ctrl+C to stop gracefully")

    try:
        # Wait for publishers (2 hours)
        for t in publishers:
            t.join()
        print("All publishers completed after 2 hours")

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
