import time
import queue as Q
from threading import Thread
import socket
import select
import random
from collections import defaultdict

class Network:
    def __init__(self, ip, port, maxConnections ):
        q = lambda: Q.Queue()

        self.CONNECTION_LIST = []
        self.ip = ip
        self.port = port
        self.maxConnections = maxConnections
        self.channelDictionary = defaultdict( q )

    def random_interval(self):
        value = random.random()
        scaled_value = 1 + int((value * (4 - 1)))
        time.sleep(scaled_value)

    def putChannelQueue(self, data_received):
        TO, MESSAGE, TIME, FROM, TRANSACTION = data_received.split(";")
        ip_to, port_to = TO.split(",")
        ip_from, port_from = FROM.split(",")
        self.channelDictionary[(port_to, port_from)].put(data_received)  # put message into required queue
        print("Message queued: " , data_received)
#        print("Starting the threads for connection")
        t = Thread(target=self.send_Message_To_Target_Server, args=( (port_to, port_from), ))  # it starts the thread
        # once data is received by network process, it will process where to forward this address
        # so, we do not need to have this connection open anymore !!
        t.start()

    def start(self):
        # Network process is listening to this port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen( self.maxConnections)
        self.CONNECTION_LIST.append(self.server_socket)

        print("Network running ...")
        count = 0
        while 1:
            count = count + 1
            if(count > 1000):
                print("Waiting for connection ...")
                count = 0

            read_sockets, write_sockets, error_sockets = select.select(self.CONNECTION_LIST, [], [])
            for sock in read_sockets:
                if sock == self.server_socket:
                    # Handle the case in which there is a new connection recieved
                    # through server_socket
                    sockfd, addr = self.server_socket.accept()
                    self.CONNECTION_LIST.append(sockfd)
                else:
                    # we got some information over the sockets.
                    # We will simply delegate the data and addr information to the thread.
                    # It will handle the message forwarding to required server.
                    try:
                        data = sock.recv(1024)
                        addr = sock.getsockname()
                    except:
                        print("Client (%s, %s) is offline" % addr)
                        sock.close()
                        self.CONNECTION_LIST.remove(sock)
                        continue
                    if data:
#                        print("Client (%s, %s) is sending data to network" % addr)
                        print("Message received: ", data.decode("utf-8"))
                        self.putChannelQueue(data.decode("utf-8"))
                        sock.close()
                        self.CONNECTION_LIST.remove(sock)


    def send_Message_To_Target_Server(self, channelQueue):
        # wait before asking the server
        self.random_interval()
        Message = self.channelDictionary[channelQueue].get()

        TO, MESSAGE, TIME, FROM, TRANSACTION = Message.split(";")
        ip_to, port_to = TO.split(",")
        ip_from, port_from = FROM.split(",")

#        print("Inside thread to forward the message to the required server")
#        print("Connecting to server (%s, %s)" % (ip_to, port_to))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ((ip_to, int(port_to)))
        sock.connect(server_address)
        try:
            sock.sendall ( Message.encode() )
            print("sent: ", Message.encode())
        finally:
            sock.close()
            self.channelDictionary[channelQueue].task_done()
        return

    def stop_Network_Process(self):
        try:
            for conn in self.CONNECTION_LIST:
                conn.close()
            self.server_socket.close()
        finally:
            print("Network process closed")

def main():
    print("Hello World!\nNetwork process is started ...\n")
    network_port = 8081
    network_ip = 'localhost'
    nw = Network( network_ip, network_port, 10 )
    nw.start()


if __name__ == "__main__":
    main()
