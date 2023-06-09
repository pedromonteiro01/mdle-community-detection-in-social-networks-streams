import socket
import time
import random
import sys

# Define the host and port
HOST = 'localhost'
PORT = 9999

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
server_socket.bind((HOST, PORT))

# Listen for incoming connections
server_socket.listen(1)
print('Server is listening on {}:{}'.format(HOST, PORT))

# Accept a connection from a client
client_socket, client_address = server_socket.accept()
print('Accepted connection from {}:{}'.format(client_address[0], client_address[1]))

stream_size = 200 # stream size
stop_counter = 1 # stop server counter

# Send data to client
while True:
    try:
        rnd = random.uniform(0, 1) # Generate random 0 or 1
        data = '0'
        if rnd < 0.3: # probability of being one
            data = '1'

        client_socket.send((data + "\n").encode("utf-8"))
        time.sleep(0.1)  # Delay to simulate streaming

        stop_counter += 1 # increase iteration counter

        if stop_counter > stream_size:
            break

    except KeyboardInterrupt:
        break

# Close the connection
client_socket.close()
server_socket.close()
