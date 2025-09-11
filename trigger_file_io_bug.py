#!/usr/bin/env python3
"""
Targeted reproduction of RabbitMQ message store bug by creating conditions
that lead to file I/O issues during message store operations.
"""

import pika
import threading
import time
import random
import signal
import sys

class MessageStoreStresser:
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self.running = True
        
    def stop(self):
        self.running = False
        
    def rapid_publish_consume_cycle(self):
        """Rapidly publish and consume to stress message store file operations"""
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        
        # Ensure classic queue
        channel.queue_declare(queue=self.queue_name, durable=True, 
                            arguments={'x-queue-type': 'classic'})
        
        cycle = 0
        while self.running and cycle < 50:
            # Burst publish
            for i in range(100):
                message = f"Cycle {cycle} Message {i}: " + "data" * random.randint(50, 500)
                channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
            # Immediate consume with mixed ack/nack
            consumed = 0
            def callback(ch, method, properties, body):
                nonlocal consumed
                consumed += 1
                
                # Create requeue scenarios
                if random.random() < 0.3:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                
                if consumed >= 80:  # Don't consume all to leave messages
                    ch.stop_consuming()
            
            channel.basic_qos(prefetch_count=20)
            channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            
            try:
                channel.start_consuming()
            except:
                pass
                
            cycle += 1
            if cycle % 10 == 0:
                print(f"Completed {cycle} cycles")
        
        connection.close()

def signal_handler(signum, frame):
    print("\nReceived interrupt signal, stopping...")
    global stresser
    if stresser:
        stresser.stop()

def main():
    global stresser
    
    print("RabbitMQ Message Store File I/O Bug Trigger")
    print("This creates rapid publish/consume cycles to stress file operations")
    print("Monitor RabbitMQ logs for reader_pread_parse function_clause errors")
    print()
    
    signal.signal(signal.SIGINT, signal_handler)
    
    queue_name = f"file_io_stress_{int(time.time())}"
    stresser = MessageStoreStresser(queue_name)
    
    # Run multiple concurrent stress operations
    threads = []
    for i in range(4):
        t = threading.Thread(target=stresser.rapid_publish_consume_cycle)
        threads.append(t)
        t.start()
        time.sleep(0.5)  # Stagger starts
    
    # Wait for completion
    for t in threads:
        t.join()
    
    print("Stress test completed")
    
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
    stresser = None
    main()
