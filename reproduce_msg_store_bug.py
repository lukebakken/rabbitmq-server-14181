#!/usr/bin/env python3
"""
Reproduce RabbitMQ message store bug where reader_pread_parse receives
a list of 'eof' atoms instead of binary data, causing function_clause exception.
"""

import pika
import threading
import time
import random
import sys

def create_connection():
    """Create RabbitMQ connection"""
    return pika.BlockingConnection(pika.ConnectionParameters('localhost'))

def publisher_worker(queue_name, message_count):
    """Publish messages rapidly to trigger message store operations"""
    connection = create_connection()
    channel = connection.channel()
    
    # Declare classic queue (not quorum)
    channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'classic'})
    
    for i in range(message_count):
        # Vary message sizes to stress the message store
        message_size = random.randint(100, 10000)
        message = f"Message {i}: " + "x" * message_size
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )
        
        if i % 100 == 0:
            print(f"Published {i} messages")
    
    connection.close()

def consumer_worker(queue_name, consume_count):
    """Consume messages with acknowledgments to trigger queue operations"""
    connection = create_connection()
    channel = connection.channel()
    
    consumed = 0
    
    def callback(ch, method, properties, body):
        nonlocal consumed
        consumed += 1
        
        # Randomly ack/nack to create mixed states
        if random.random() < 0.8:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        if consumed >= consume_count:
            ch.stop_consuming()
    
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    
    connection.close()

def stress_test_scenario():
    """Create conditions that may trigger the message store bug"""
    queue_name = f"test_queue_{int(time.time())}"
    
    print(f"Starting stress test on queue: {queue_name}")
    
    # Start multiple publishers
    publishers = []
    for i in range(3):
        t = threading.Thread(target=publisher_worker, args=(queue_name, 1000))
        publishers.append(t)
        t.start()
    
    # Start consumers after a delay
    time.sleep(2)
    consumers = []
    for i in range(2):
        t = threading.Thread(target=consumer_worker, args=(queue_name, 500))
        consumers.append(t)
        t.start()
    
    # Wait for publishers to finish
    for t in publishers:
        t.join()
    
    print("Publishers finished, waiting for consumers...")
    
    # Wait for consumers
    for t in consumers:
        t.join()
    
    print("Test completed")
    
    # Clean up
    try:
        connection = create_connection()
        channel = connection.channel()
        channel.queue_delete(queue=queue_name)
        connection.close()
        print(f"Cleaned up queue: {queue_name}")
    except Exception as e:
        print(f"Cleanup error: {e}")

if __name__ == "__main__":
    print("RabbitMQ Message Store Bug Reproduction Script")
    print("This script attempts to reproduce the reader_pread_parse function_clause bug")
    print("Monitor RabbitMQ logs for the specific error pattern")
    print()
    
    try:
        stress_test_scenario()
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error during test: {e}")
        sys.exit(1)
