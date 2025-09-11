#!/usr/bin/env python3
"""
Runner script for RabbitMQ message store bug reproduction tests
"""

import subprocess
import sys
import time

def run_test(script_name, description):
    """Run a test script and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Script: {script_name}")
    print('='*60)
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True, 
                              timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            print(f"✓ {script_name} completed successfully")
        else:
            print(f"✗ {script_name} failed with return code {result.returncode}")
            
    except subprocess.TimeoutExpired:
        print(f"⚠ {script_name} timed out after 5 minutes")
    except KeyboardInterrupt:
        print(f"⚠ {script_name} interrupted by user")
        return False
    except Exception as e:
        print(f"✗ Error running {script_name}: {e}")
    
    return True

def main():
    print("RabbitMQ Message Store Bug Reproduction Test Suite")
    print("Make sure RabbitMQ is running before starting these tests")
    print()
    
    input("Press Enter to continue or Ctrl+C to abort...")
    
    tests = [
        ("delta_beta_trigger.py", "Delta-to-Beta conversion trigger (most likely to reproduce bug)"),
        ("trigger_file_io_bug.py", "File I/O stress test"),
        ("reproduce_msg_store_bug.py", "General message store stress test")
    ]
    
    for script, description in tests:
        if not run_test(script, description):
            break
        
        print("\nWaiting 10 seconds before next test...")
        time.sleep(10)
    
    print("\n" + "="*60)
    print("All tests completed!")
    print("Check RabbitMQ logs for the following error pattern:")
    print("  exception exit: {function_clause,")
    print("  [{rabbit_msg_store,reader_pread_parse,")
    print("   [[eof,eof,eof,eof,eof,eof,eof]],")
    print("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest suite interrupted by user")
        sys.exit(1)
