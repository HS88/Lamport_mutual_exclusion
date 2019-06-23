import queue as Q
import socket
import select

class Client:
    def __init__(self, id, ip, port, server_ip, server_port, maxConnections):
        self.ID = id
        self.ip = ip
        self.port = port
        self.server_connection = (server_ip, server_port)
        self.pendingTransactions = Q.Queue()
        self.CONNECTION_LIST = []
        self.maxConnections = maxConnections
        self.message_from_server=""

        self.CONNECTION_LIST = []
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.ip, self.port))  # I am listening to this port for all messages
        self.server_socket.listen(10)
        print("Client listening at", self.ip, self.port)
        self.CONNECTION_LIST.append(self.server_socket)

        return

    def validTransaction(self, transaction):
        try:
            sender, receiver, amount = transaction.split(" ")
            result = int(amount) > 0 and sender==self.ID
        except:
            result = False
        finally:
            return result

    def composeMessage(self, transaction):
        message = "{},{};INITIATE;NA;{},{};{}".format(self.server_connection[0],self.server_connection[1], self.ip, self.port, transaction)
        return message

    def sendMessageToServer(self, transaction):
        print("A thread will send the data to the server")
        print("Start the thread that listens to the server response")
        Message = self.composeMessage(transaction)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ( self.server_connection )
        sock.connect(server_address)

#client listening port

        self.message_from_server = ""
        try:
            sock.sendall(Message.encode())
        except:
            print("Exception occurred while sending data to server")
            return "Exception occurred while sending data to server"
        finally:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            waitReply = True
            while (waitReply == True):
                read_sockets, write_sockets, error_sockets = select.select(self.CONNECTION_LIST, [], [])
                for sock in read_sockets:
                    if sock == self.server_socket:
                        sockfd, addr = self.server_socket.accept()
                        self.CONNECTION_LIST.append(sockfd)
                    else:
                        try:
                            data = sock.recv(4096)
                            addr = sock.getsockname()
                        except:
                            print("Client (%s, %s) is disconnected" % addr)
                            sock.close()
                            self.CONNECTION_LIST.remove(sock)
                            continue
                        if data:
                            self.message_from_server = data.decode("utf-8")
                            self.CONNECTION_LIST.remove(sock)
                            waitReply = False
                            break
            sock.shutdown(socket.SHUT_RDWR)
#            server_socket.close()
        print("Client closing socket after response from server")
        return self.message_from_server


    def listenToServerResponse(self):
        print("A thread will start listening to the server response")
        print("Waiting for server response")

    def startClient(self):
        while (True):
            # wait for previous transaction to end
            if( self.pendingTransactions.empty() == False):
                continue
            text = input("Enter Transaction: ")
            if (client.validTransaction(text)):
                print("Put this transaction on the Queue.")
                print("Send transaction to server...")
                message_from_server = self.sendMessageToServer(text)
                #TO, MESSAGE, TIME, FROM, TRANSACTION = message_from_server.split(",")
                print(message_from_server)
            else:
                print("Enter valid transaction ...")

if __name__ == "__main__":
    server_ip = 'localhost'
    server_port = 8092
    ip = 'localhost'
    port = 8093
    client = Client('B', ip, port, server_ip, server_port, 3)
    client.startClient()