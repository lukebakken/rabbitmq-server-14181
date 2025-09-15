#!/usr/bin/env python3
"""
RabbitMQ message store bug reproduction - Cyclic workload pattern
Mimics user workload: fast publishes followed by slow consumption cycles
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

# Cycle configuration
CYCLE_DURATION = 2 * 3600  # 2 hours per cycle
FAST_PUBLISH_PHASE = 45 * 60  # 45 minutes
SLOW_CONSUME_PHASE = 75 * 60  # 75 minutes
TOTAL_CYCLES = 4  # 8 hours total runtime
TARGET_BACKLOG = 16200  # 15k ready + 1.2k unacked
BACKLOG_GROWTH_TOLERANCE = 1.1  # 10% growth over 8 hours

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
        self.start_time = time.time()
        self.last_backlog_check = 0
        self.current_backlog = 0

    def get_current_backlog(self):
        """Get current queue backlog (ready + unacked messages)"""
        try:
            if self.channel and self.channel.is_open:
                method = self.channel.queue_declare(queue=self.queue_name, passive=True)
                ready_messages = method.method.message_count
                # Estimate unacked from our pending acks
                unacked_messages = len(self.pending_acks)
                return ready_messages + unacked_messages
        except Exception:
            pass
        return self.current_backlog  # Return cached value on error

    def callback(self, ch, method, properties, body):
        delivery_tag = method.delivery_tag
        current_time = time.time()

        # Update backlog periodically (every 30 seconds)
        if current_time - self.last_backlog_check > 30:
            self.current_backlog = self.get_current_backlog()
            self.last_backlog_check = current_time

        # Determine cycle phase
        cycle_elapsed = (current_time - self.start_time) % CYCLE_DURATION
        cycle_num = int((current_time - self.start_time) // CYCLE_DURATION) + 1

        if cycle_elapsed < FAST_PUBLISH_PHASE:
            # Fast consumption phase - prepare for slow phase
            delay_seconds = random.uniform(0, 30)
        else:
            # Slow consumption phase - adapt based on backlog
            target_with_growth = TARGET_BACKLOG * (1 + 0.1 * (current_time - self.start_time) / (8 * 3600))

            if self.current_backlog > target_with_growth * 1.2:
                # Backlog too high, process faster
                if random.random() < 0.6:  # 60% faster processing
                    delay_seconds = random.randint(1800, 3600)  # 30min-1hr
                else:
                    delay_seconds = random.randint(3600, 5400)  # 1-1.5hr
            else:
                # Normal slow processing (1+ hour as per user requirement)
                if random.random() < 0.8:  # 80% long processing
                    delay_seconds = random.randint(3600, 5400)  # 1-1.5hr
                else:
                    delay_seconds = random.uniform(0, 300)  # Some fast acks

        ack_time = datetime.now() + timedelta(seconds=delay_seconds)
        self.pending_acks[delivery_tag] = ack_time

    def open_channel_and_consume(self):
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=100)

        # Ensure queue exists with proper arguments (consumer may start before publisher)
        queue_args = {
            'x-max-priority': 10,
            'x-consumer-timeout': 86400000  # 24 hours in milliseconds
        }
        self.channel.queue_declare(queue=self.queue_name, durable=True, arguments=queue_args)

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

def publisher_worker(queue_name, publisher_id, runtime_hours=8):
    """Cyclic publisher: fast publishes followed by slow consumption phases"""
    try:
        connection = pika.BlockingConnection(CONNECTION_PARAMS)
        channel = connection.channel()

        start_time = time.time()
        end_time = start_time + (runtime_hours * 3600)
        message_count = 0

        print(f"Publisher {publisher_id} starting cyclic workload for {runtime_hours} hours")

        while time.time() < end_time:
            current_time = time.time()
            cycle_elapsed = (current_time - start_time) % CYCLE_DURATION
            cycle_num = int((current_time - start_time) // CYCLE_DURATION) + 1

            # Check queue depth every 50 messages
            if message_count % 50 == 0:
                try:
                    method = channel.queue_declare(queue=queue_name, passive=True)
                    ready_messages = method.method.message_count

                    # Calculate target backlog with growth allowance
                    elapsed_hours = (current_time - start_time) / 3600
                    target_with_growth = TARGET_BACKLOG * (1 + 0.1 * elapsed_hours / 8)

                except Exception:
                    ready_messages = TARGET_BACKLOG  # Default assumption
                    target_with_growth = TARGET_BACKLOG

            # Determine publishing behavior based on cycle phase
            if cycle_elapsed < FAST_PUBLISH_PHASE:
                # Fast publishing phase (45 minutes)
                if ready_messages < target_with_growth:
                    batch_size = 25  # ~200 msg/sec with 0.125s sleep
                    sleep_time = 0.125
                else:
                    batch_size = 5   # Moderate rate
                    sleep_time = 1.0
            else:
                # Slow consumption phase (75 minutes) - maintain backlog
                if ready_messages < target_with_growth * 0.8:
                    batch_size = 10  # Moderate publishing
                    sleep_time = 2.0
                elif ready_messages > target_with_growth * 1.2:
                    batch_size = 1   # Very slow publishing
                    sleep_time = 10.0
                else:
                    batch_size = 3   # Maintenance publishing
                    sleep_time = 5.0

            # Publish batch
            for i in range(batch_size):
                # Priority distribution: 70% priority 1, 30% priorities 2-10
                if random.random() < 0.7:
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
                    phase = "FAST_PUBLISH" if cycle_elapsed < FAST_PUBLISH_PHASE else "SLOW_CONSUME"
                    print(f"Publisher {publisher_id} [CYCLE_{cycle_num}_{phase}]: {message_count} messages sent")

            connection.process_data_events(sleep_time)

        connection.close()
        print(f"Publisher {publisher_id} completed {message_count} messages in {runtime_hours} hours")
    except Exception as e:
        print(f"Publisher {publisher_id} error: {e}")

def monitor_progress(queue_name, consumers, runtime_hours=8):
    """Monitor and report progress with cycle phase information"""
    start_time = time.time()
    end_time = start_time + (runtime_hours * 3600)

    while time.time() < end_time:
        connection = pika.BlockingConnection(CONNECTION_PARAMS)
        channel = connection.channel()
        try:
            method = channel.queue_declare(queue=queue_name, passive=True)
            ready_messages = method.method.message_count
            connection.close()

            current_time = time.time()
            elapsed_time = current_time - start_time
            elapsed_hours = elapsed_time / 3600

            # Determine cycle phase
            cycle_elapsed = elapsed_time % CYCLE_DURATION
            cycle_num = int(elapsed_time // CYCLE_DURATION) + 1

            if cycle_elapsed < FAST_PUBLISH_PHASE:
                phase = f"CYCLE_{cycle_num}_FAST_PUBLISH"
                phase_remaining = (FAST_PUBLISH_PHASE - cycle_elapsed) / 60
            else:
                phase = f"CYCLE_{cycle_num}_SLOW_CONSUME"
                phase_remaining = (CYCLE_DURATION - cycle_elapsed) / 60

            # Calculate target backlog with growth
            target_with_growth = TARGET_BACKLOG * (1 + 0.1 * elapsed_hours / 8)

            # Count active consumers and estimate unacked
            active_consumers = sum(1 for c in consumers if c.is_alive())
            total_pending_acks = sum(len(c.pending_acks) for c in consumers if hasattr(c, 'pending_acks'))

            print(f"[{phase}] Time: {elapsed_hours:.1f}h | Phase remaining: {phase_remaining:.1f}min")
            print(f"  Queue: {ready_messages} ready, ~{total_pending_acks} unacked (target: {target_with_growth:.0f})")
            print(f"  Consumers: {active_consumers} active")
            print()

        except Exception as e:
            print(f"Monitor error: {e}")

        connection.process_data_events(60)  # Check every minute

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

    print("Optimized reproduction test running...")
    print(f"- Consumer timeouts: 1% of acks will timeout after {consumer_timeout_minutes + 1}+ minutes")
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
