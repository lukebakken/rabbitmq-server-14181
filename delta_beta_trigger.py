#!/usr/bin/env python3
"""
Target the specific queue operation that triggers the bug:
maybe_deltas_to_betas -> read_many_file2 -> reader_pread_parse
"""

import pika
import time
import random

def create_memory_pressure_scenario():
    """Create conditions that force queue to move messages between memory segments"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    queue_name = f"delta_beta_test_{int(time.time())}"
    
    # Classic queue with specific settings to trigger delta/beta operations
    channel.queue_declare(
        queue=queue_name, 
        durable=True,
        arguments={
            'x-queue-type': 'classic',
            'x-max-length': 10000  # Limit to force memory management
        }
    )
    
    print(f"Created queue: {queue_name}")
    
    # Phase 1: Fill queue beyond memory limits to create deltas
    print("Phase 1: Creating delta messages...")
    for i in range(15000):  # Exceed max-length to force deltas
        message = f"Delta message {i}: " + "x" * random.randint(1000, 5000)
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        if i % 1000 == 0:
            print(f"Published {i} messages")
    
    print("Phase 2: Triggering delta-to-beta conversion...")
    
    # Phase 2: Consume some messages to trigger delta-to-beta conversion
    consumed = 0
    def callback(ch, method, properties, body):
        nonlocal consumed
        consumed += 1
        
        # Acknowledge to trigger queue state changes
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        if consumed >= 5000:  # Consume enough to trigger conversions
            ch.stop_consuming()
    
    channel.basic_qos(prefetch_count=100)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    try:
        channel.start_consuming()
    except Exception as e:
        print(f"Consumer error: {e}")
    
    print(f"Consumed {consumed} messages")
    
    # Phase 3: Rapid consume/nack to stress the queue state machine
    print("Phase 3: Stressing queue state with nacks...")
    
    consumed = 0
    def stress_callback(ch, method, properties, body):
        nonlocal consumed
        consumed += 1
        
        # Mix of ack/nack to create complex queue states
        if random.random() < 0.5:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        if consumed >= 2000:
            ch.stop_consuming()
    
    channel.basic_consume(queue=queue_name, on_message_callback=stress_callback)
    
    try:
        channel.start_consuming()
    except Exception as e:
        print(f"Stress consumer error: {e}")
    
    print("Test completed - check RabbitMQ logs for errors")
    
    # Cleanup
    try:
        channel.queue_delete(queue=queue_name)
        print(f"Cleaned up queue: {queue_name}")
    except Exception as e:
        print(f"Cleanup error: {e}")
    
    connection.close()

if __name__ == "__main__":
    print("RabbitMQ Delta-to-Beta Conversion Bug Trigger")
    print("Targeting maybe_deltas_to_betas function that leads to the bug")
    print()
    
    try:
        create_memory_pressure_scenario()
    except Exception as e:
        print(f"Error: {e}")
