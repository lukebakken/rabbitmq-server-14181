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
from datetime import datetime, timedelta

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

def safe_ack(channel, delivery_tag):
    """Thread-safe ack function"""
    if channel.is_open:
        channel.basic_ack(delivery_tag)

class DelayedAckConsumer:
    def __init__(self, queue_name, consumer_id):
        self.queue_name = queue_name
        self.consumer_id = consumer_id
        self.connection = None
        self.channel = None
        self.pending_acks = {}
        self.running = True
        self.ack_thread = None
        
    def callback(self, ch, method, properties, body):
        delivery_tag = method.delivery_tag
        
        # Random delay from 0 to 30 minutes, with 1% chance of >30 min timeout
        if random.random() < 0.01:
            delay_seconds = random.randint(1800, 2100)  # 30-35 minutes
        else:
            delay_seconds = random.uniform(0, 1800)  # 0-30 minutes
            
        ack_time = datetime.now() + timedelta(seconds=delay_seconds)
        self.pending_acks[delivery_tag] = ack_time
        
    def process_delayed_acks(self):
        """Process pending acks using add_callback_threadsafe"""
        while self.running:
            now = datetime.now()
            to_ack = []
            
            for delivery_tag, ack_time in self.pending_acks.items():
                if now >= ack_time:
                    to_ack.append(delivery_tag)
            
            for delivery_tag in to_ack:
                try:
                    # Use thread-safe callback
                    cb = functools.partial(safe_ack, self.channel, delivery_tag)
                    self.connection.add_callback_threadsafe(cb)
                    del self.pending_acks[delivery_tag]
                except Exception as e:
                    print(f"Consumer {self.consumer_id} ack error: {e}")
            
            self.connection.process_data_events(1) # Check every second
                
    def start_consuming(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=100)
            
            # Start ack processor thread
            self.ack_thread = threading.Thread(target=self.process_delayed_acks)
            self.ack_thread.start()
            
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
            print(f"Consumer {self.consumer_id} started")
            
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"Consumer {self.consumer_id} error: {e}")
        finally:
            self.running = False
            if self.ack_thread:
                self.ack_thread.join()
            if self.connection and not self.connection.is_closed:
                self.connection.close()

def publisher_worker(queue_name, publisher_id, message_count):
    """Publisher with priority messages"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        
        for i in range(message_count):
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
            
            if i % 1000 == 0:
                print(f"Publisher {publisher_id}: {i} messages sent")
        
        connection.close()
        print(f"Publisher {publisher_id} completed")
    except Exception as e:
        print(f"Publisher {publisher_id} error: {e}")

def create_initial_backlog(queue_name, target_backlog=10000):
    """Create initial 10K message backlog"""
    print(f"Creating initial backlog of {target_backlog} messages...")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
    print("Fixed RabbitMQ Message Store Bug Reproduction")
    print("Generating message bodies...")
    generate_message_bodies()
    
    queue_name = f"priority_bug_test_{int(time.time())}"
    
    # Create initial backlog
    create_initial_backlog(queue_name)
    
    print("Starting publishers and consumers...")
    
    # Start 15 publishers with more messages to maintain backlog
    publishers = []
    for i in range(15):
        t = threading.Thread(target=publisher_worker, args=(queue_name, i, 5000))  # Increased from 2000
        publishers.append(t)
        t.start()
    
    # Start 20 consumers with delayed acks
    consumers = []
    for i in range(20):
        consumer = DelayedAckConsumer(queue_name, i)
        t = threading.Thread(target=consumer.start_consuming)
        consumers.append((t, consumer))
        t.start()
        time.sleep(0.2)  # Stagger consumer starts
    
    print("Test running... Monitor RabbitMQ logs for function_clause errors")
    print("Press Ctrl+C to stop")
    
    try:
        # Wait for publishers
        for t in publishers:
            t.join()
        print("All publishers completed")
        
        # Let consumers run
        time.sleep(600)  # 10 minutes
        
    except KeyboardInterrupt:
        print("\nStopping test...")
    finally:
        # Stop consumers
        for t, consumer in consumers:
            consumer.running = False
        
        # Wait for consumer threads to finish
        for t, consumer in consumers:
            t.join(timeout=5)
        
        # Cleanup
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
