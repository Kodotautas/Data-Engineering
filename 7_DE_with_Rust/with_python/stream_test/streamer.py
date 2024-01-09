# combined.py
import socket
import time
import threading

def start_server(stop_flag):
    host = 'localhost'
    port = 12345
    buffer_size = 1024

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(1)
    print("Server started, waiting for connections...")

    while not stop_flag[0]:
        client, addr = server.accept()
        print(f"Connection from {addr}")
        while True:
            data = client.recv(buffer_size)
            if data:
                print(f"Received data: {data.decode()}")
                client.send("Received".encode())  # Send a response back to the client
            else:
                break
        client.close()

    print("Server stopped")

def start_client():
    host = 'localhost'
    port = 12345
    buffer_size = 1024
    message = "Hello, Server!"
    run_duration = 30  # Duration
    latencies = []  # List to store latency values

    time.sleep(1)  # Ensure server is up

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))

    start_time = time.time()
    while time.time() - start_time < run_duration:
        send_time = time.time()
        client.send(message.encode())
        client.recv(buffer_size)
        receive_time = time.time()

        latency = receive_time - send_time
        latencies.append(latency)

        print(f"Latency: {latency} seconds")

        time.sleep(1)  # Delay for 1 second

    client.close()

    # Calculate and print average latency in milliseconds
    average_latency = sum(latencies) / len(latencies) * 1000
    print(f"Average Latency: {average_latency} ms")

if __name__ == "__main__":
    stop_flag = [False]  # Shared flag

    server_thread = threading.Thread(target=start_server, args=(stop_flag,))
    client_thread = threading.Thread(target=start_client)

    server_thread.start()
    client_thread.start()

    client_thread.join()
    stop_flag[0] = True  # Signal the server to stop

    server_thread.join()