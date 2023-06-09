import sys
import socket
import time
from datetime import datetime

address = "localhost"
port = 9998

start_time = time.time()  # get start time

with socket.socket() as s:
    s.bind((address, port))
    s.listen(1)

    print(f"Server is running on {address}:{port}")
    
    conn, addr = s.accept()
    with conn:
        try:
            with open("stream-data.csv", 'rt') as f:
                f.readline()
                while True:
                    line = f.readline()

                    if not line or time.time() - start_time > 60:  # break after 60 seconds or if end of file is reached
                        break
                    
                    raw_timestamp = line.split()[1].split(",")[0] 
                    print("Timestamp= ", raw_timestamp)
                    
                    conn.sendall(line.encode('utf-8'))
                    time.sleep(1)  # sleep for a second

        except Exception as e:
            print(f"Error occurred: {e}")
            print("Closing gracefully")
            sys.exit(1)
