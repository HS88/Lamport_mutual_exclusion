import time
import queue as Q
from threading import Thread
import socket
import select
import random
from collections import defaultdict
import threading
from ast import literal_eval as make_tuple

class Request(object):
    def __init__(self, time, transaction):
        self.time = time
        self.transaction = transaction
        return

    @staticmethod
    def cmp(x, y):
        c1,p1 = x.time
        c2,p2 = y.time
        if(c1 < c2 ):
            return -1
        elif(c1==c2 and p1< p2):
            return -1
        else:
            return 1


    def __lt__(self, other):
        c1, p1 = self.time
        c2, p2 = other.time
        if (c1 < c2):
            return True
        elif (c1 == c2 and p1 < p2):
            return True
        else:
            return False

    def __cmp__(self, other):
        return Request.cmp(self.time, other.time)

class Server:

    def __init__(self, ip, port, maxConnections, id, client_ID,ledger, network_ip, network_port, otherServers = {} ):
        self.CONNECTION_LIST = []
        self.ip = ip
        self.port = port
        self.maxConnections = maxConnections
        self.otherServers = otherServers
        self.ID = id
        self.client_ID = client_ID
        self.COUNTER = 0
        self.ledger = ledger
        self.network_port = 8081
        self.network_ip = 'localhost'

        self.pendingTransactionsRequestQueue = Q.PriorityQueue() # waiting for these messages to be replied back to Q1
        self.pendingSendMessagesFromServerQueue = Q.PriorityQueue()        # messages that I need to send to network Q2
        self.pendingReleaseMessagesToProcessQueue = Q.PriorityQueue()     # release messages received that are not processed yet Q3
        self.pendingReplyForMessagesQueue = Q.PriorityQueue()  # messages initiated by myself Q4
        self.ReplyWaiting = 2

        self.lock_Counter = threading.RLock()
        self.lock_ReplyWaiting = threading.RLock()
        self.lock_QueueManagement = threading.RLock()
        self.lock_LedgerManagement = threading.RLock()
        self.lock_SendMessageQueue = threading.RLock()
        self.address = self.ip+','+str(self.port)
        self.network_address = (network_ip, network_port)

    def checkQueues(self):
        print("Q1 ",
              self.pendingTransactionsRequestQueue.qsize())  # waiting for these messages to be replied back to Q1
        print("Q2 ", self.pendingSendMessagesFromServerQueue.qsize())  # messages that I need to send to network Q2
        print("Q3 ",
              self.pendingReleaseMessagesToProcessQueue.qsize())  # release messages received that are not processed yet Q3
        print("Q4 ", self.pendingReplyForMessagesQueue.qsize())  # messages initiated by myself Q4

    def insertRequest(self, request, queue=None):
        if(queue == None):
            queue = self.pendingTransactionsRequestQueue
        queue.put(request)

    def peekQueue(self, queue=None):
        if(queue == None):
            queue = self.pendingTransactionsRequestQueue
        if(not queue.empty()):
            temp = queue.get()
            self.insertRequest(temp, queue)
            return temp

    def updateCOUNTER(self, new_time):# it gets tuple Time,process
        if(self.ID == new_time[1]):
            self.COUNTER = self.COUNTER + 1
        else:
            self.COUNTER = max(self.COUNTER, new_time[0]) + 1

    def getLogicalTime(self):
        logical_time = (self.COUNTER, self.ID)
        return logical_time

    def checkValidityOfTransaction(self, transaction):
        client_sender, client_receiver, transfer_amount = transaction.split(' ')

        f = open(self.ledger, 'r')
        lines = f.readlines()
        available_credit = 0

        for transaction in lines:
            sender,receiver,amount = transaction.split(' ')
            if(sender == client_sender ):
                available_credit = available_credit - int(amount)
            if(receiver == client_sender ):
                available_credit = available_credit + int(amount)
        f.close()
        return int(available_credit) > int(transfer_amount)

    def appendTransactionToLedger(self, transaction, parse=False):
        # assumed transaction is validated already
        print("Appending to ledger")
        if( transaction=="INVALID"):
            return
        print("lock_LedgerManagement acquire")
        self.lock_LedgerManagement.acquire()

        f = open(self.ledger, 'a+')
        f.write(transaction+"\n")
        f.close()
        self.lock_LedgerManagement.release()
        print("lock_LedgerManagement released")
        return

    def sendMessageThread(self,dummy):# always waiting on message to be sent queue
        print("Waiting for message to be sent ... ")

        while True:
            if(self.pendingSendMessagesFromServerQueue.empty() == False ):
                request = self.pendingSendMessagesFromServerQueue.get( )
                print("Server will send the message", request.transaction )
                Message = request.transaction
                TO, MESSAGE, TIME, FROM, TRANSACTION = Message.split(";")
                ip_to, port_to = TO.split(",")
                ip_from, port_from = FROM.split(",")

                print("Connecting to server (%s, %s)" % (ip_to, port_to))
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_address = ((ip_to, int(port_to)))
            #    sock.connect(server_address)
                sock.connect(self.network_address)
                try:
                    sock.sendall(Message.encode())
                finally:
                    sock.close()
                    print("Message sent: ", Message)
            else:
                break

    def replyClient(self, reply):
        request = self.pendingReplyForMessagesQueue.get()
        message = request.transaction
        TO, MESSAGE, TIME, FROM, TRANSACTION = message.split(";")
        client_address = (FROM.split(",")[0], int(FROM.split(",")[1]))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(client_address)

        print("Server sending data to " , client_address)
        try:
            sock.sendall(reply.encode())
        except:
            print("Exception occurred while sending reply to client")
        finally:
            sock.close()
        print("Data sent to client")

    def startSendMessageThread(self):
        t = Thread(target=self.sendMessageThread, args=([1]))  # it starts the thread
        t.start()

    def processReleaseMessageFromQueueThread(self,dummy):# always waiting on message to be sent queue
        print("Inside thread:  processReleaseMessageFromQueueThread" )
        if( self.pendingReleaseMessagesToProcessQueue.empty() ):
            print("Empty return")
            return
        while True:
            request = self.pendingReleaseMessagesToProcessQueue.get()
            Message = request.transaction
            TO, MESSAGE, TIME, FROM, TRANSACTION = Message.split(";")
            print(" append ledhger processReleaseMessageFromQueueThread")
            if ( self.checkIfThisRequestIsOnTop(Message) == True ):#on top, append ledger
                self.appendTransactionToLedger(TRANSACTION)
            else:# put it back on queue
                self.insertRequest(request,self.pendingReleaseMessagesToProcessQueue)
            if(self.pendingReleaseMessagesToProcessQueue.empty()):
                break

    def createMessageForSending(self, TO, MESSAGE, TIME, FROM, TRANSACTION):
        TO = TO.split(",")
        FROM = FROM.split(",")
        message = "{},{};{};{};{},{};{}".format(TO[0],TO[1], MESSAGE, TIME, FROM[0],FROM[1], TRANSACTION )
        return message

    def checkIfThisRequestIsOnTop(self, message_to_be_checked):
        print(message_to_be_checked)
        topRequest = self.peekQueue()
        print(topRequest)
        message = topRequest.transaction
        TO_q, MESSAGE_q, TIME_q, FROM_q, TRANSACTION_q = message.split(";")
        TO_r, MESSAGE_r, TIME_r, FROM_r, TRANSACTION_r = message_to_be_checked.split(";")

        R = (TIME_r.split(",")[0], TIME_r.split(",")[1])
        Q = (TIME_q.split(",")[0], TIME_q.split(",")[1])

        if( Q == R ):
            return True
        return False


        # case-0 Message is from client to inititiate the transaction
        # case-1 Message is REQUEST from other server
        # case-2 Message is REPLY from other server
        # case-3 Message is RELEASE from other server

    def checkAfterReplyMessageWasProcessed(self):
        # I Must check if my own message is the next message to act upon !
        # I must also check if release queue's top message is next message to act upon !
        # These are internal checks, I do not need to communicate with anyone for these
        if(self.pendingReplyForMessagesQueue.empty() == True or self.ReplyWaiting > 0):#harmeet
            return

        request = self.peekQueue( self.pendingReplyForMessagesQueue )
        message = request.transaction
        TO, MESSAGE, TIME, FROM, TRANSACTION = message.split(";")

        if ( self.checkIfThisRequestIsOnTop(message) == True ):
            if (self.checkValidityOfTransaction(TRANSACTION)):
                print("Append ledger inside checkAfterReplyMessageWasProcessed")
                self.appendTransactionToLedger(TRANSACTION)
                self.checkQueues()
                if(TRANSACTION[0]=='B'):
                    self.replyClient("Success")
            else:
                print("Transaction is not valid")
                TRANSACTION = "INVALID"
                if(TRANSACTION[0]=='B'):
                    self.replyClient("Transaction Not Valid")
            print("lock_QueueManagement acquire")
            self.checkQueues()
#            self.lock_QueueManagement.acquire()
            self.pendingTransactionsRequestQueue.get()
#            self.pendingReplyForMessagesQueue.get() # reply to client
            tt = make_tuple(TIME)
            for key, value in self.otherServers.items():
                message_release = self.createMessageForSending(value, "RELEASE", TIME, self.address, TRANSACTION)
                self.insertRequest(Request(time=tt, transaction= message_release), self.pendingSendMessagesFromServerQueue)
#            self.lock_QueueManagement.release()
            print("lock_QueueManagement released")
            self.startSendMessageThread()

        return False

    def processReceivedMessage(self, Message_Received):
        TO, MESSAGE, TIME, FROM, TRANSACTION = Message_Received.split(";")
#        assert(TO == self.address)
        if (MESSAGE == "INITIATE"):
            print("Inside INITIATE")
            print("New transaction received from client")
            print("lock_Counter acquire")
            self.lock_Counter.acquire()
            print("Update Time")
            self.updateCOUNTER((self.COUNTER, self.ID))
            Time = (self.COUNTER, self.ID)
            self.lock_Counter.release()
            print("lock_Counter released")

            print("lock_ReplyWaiting acquire")
            self.lock_ReplyWaiting.acquire()
            self.ReplyWaiting = len(self.otherServers)
            print(self.ReplyWaiting)
            message = self.createMessageForSending(TO, MESSAGE, Time, FROM, TRANSACTION )

            print("lock_QueueManagement acquire")
            self.lock_QueueManagement.acquire()

            self.insertRequest( Request(time=Time, transaction=message),self.pendingTransactionsRequestQueue ) # updated Q1
            self.insertRequest( Request(time=Time, transaction=message),self.pendingReplyForMessagesQueue )# updated Q4

            for key,value in self.otherServers.items():
                message_request = self.createMessageForSending(value, "REQUEST", Time, self.address, TRANSACTION)
                self.pendingSendMessagesFromServerQueue.put   ( Request(time=Time, transaction=message_request) )

            self.lock_QueueManagement.release()
            print("lock_QueueManagement released")
            self.lock_ReplyWaiting.release()
            print("lock_ReplyWaiting released")

            if( self.ReplyWaiting == 0):
                message = self.createMessageForSending(TO, "RELEASE", Time, FROM, TRANSACTION)
                self.processReceivedMessage(message)
            self.startSendMessageThread()
            self.checkQueues()
            print("Exiting INITIATE")

        elif (MESSAGE == "REQUEST"):
            print("Inside REQUEST")
            print("REQUEST received from server")
            print("lock_Counter acquire")
            self.lock_Counter.acquire()
            print("Update Time")
            self.updateCOUNTER( make_tuple(TIME) )
            Time = (self.COUNTER, self.ID)
            self.lock_Counter.release()
            print("lock_Counter released")
            self.lock_QueueManagement.acquire()
            message = self.createMessageForSending(TO, MESSAGE, TIME, FROM, TRANSACTION)
            self.insertRequest( Request(time=make_tuple(TIME), transaction=message), self.pendingTransactionsRequestQueue) # updated Q1

            message_request = self.createMessageForSending( FROM, "REPLY", TIME, self.address, TRANSACTION)
            self.insertRequest   ( Request(time=make_tuple(TIME), transaction = message_request ),self.pendingSendMessagesFromServerQueue)
            self.lock_QueueManagement.release()
            print("lock_QueueManagement released")
            self.checkQueues()
            print("Exiting REQUEST")
            self.startSendMessageThread()

        elif (MESSAGE == "REPLY"):
            print("Inside REPLY")
            print("REPLY received from server")
            print("lock_Counter acquire")
            self.lock_Counter.acquire()
            print("Update Time")
            self.updateCOUNTER( make_tuple(TIME) )
            self.lock_Counter.release()
            print("lock_Counter released")
            print("lock_ReplyWaiting acquire")
            self.lock_ReplyWaiting.acquire()
            if (self.ReplyWaiting >= 1):
                self.ReplyWaiting = self.ReplyWaiting - 1
            if (self.ReplyWaiting == 0):
                if(self.checkIfThisRequestIsOnTop(Message_Received) == True):
                    if(self.checkValidityOfTransaction( TRANSACTION  )):
                        print("Both replies received")
                        self.appendTransactionToLedger(TRANSACTION)

                        self.replyClient("Success")   # reply client
                    else:
                        self.replyClient("Transaction Not Valid")

                        TRANSACTION = "INVALID"

                    print("lock_QueueManagement acquire")
                    self.lock_QueueManagement.acquire()
                    self.pendingTransactionsRequestQueue.get() # remove from Q1

                    for key, value in self.otherServers.items():
                        message_release = self.createMessageForSending(value,"RELEASE",TIME,self.address,TRANSACTION)
                        self.insertRequest(Request(time=make_tuple(TIME), transaction= message_release ), self.pendingSendMessagesFromServerQueue)
                    self.lock_QueueManagement.release()
                    print("lock_QueueManagement released")
                else:
                    print("Not on top")
            else:
                print("More replies needed")

            self.lock_ReplyWaiting.release()
            print("lock_ReplyWaiting released")
#            self.checkAfterReplyMessageWasProcessed()
            self.startSendMessageThread()
            print("Exiting REPLY")
            self.checkQueues()

        elif (MESSAGE == "RELEASE"):
            print("Inside RELEASE")
            print("RELEASE received from server", Message_Received)
            self.lock_Counter.acquire()
            print("lock_Counter acquire")
            print("Update Time")
            self.updateCOUNTER(make_tuple(TIME))
            self.lock_Counter.release()
            print("lock_Counter released")

            print("lock_QueueManagement acquire")
            self.lock_QueueManagement.acquire()
            if (self.checkIfThisRequestIsOnTop(Message_Received) == True):
                #if(self.checkValidityOfTransaction( TRANSACTION  )):
                print("RELEASE: appending transaction: ",TRANSACTION)
                self.appendTransactionToLedger(TRANSACTION)
#                    self.replyClient("Success")
                #else:
#                    self.replyClient("Transaction Not Valid")
                self.pendingTransactionsRequestQueue.get()

            else:
                self.insertRequest( Request(time=make_tuple(TIME), transaction= Message_Received), self.pendingReleaseMessagesToProcessQueue )
                #processReleaseMessageFromQueueThread# start this thread
                t = Thread(target=self.processReleaseMessageFromQueueThread, args=((None)))  # it starts the thread
                t.start()


            self.lock_QueueManagement.release()
            self.checkAfterReplyMessageWasProcessed()
            print("lock_QueueManagement released")
            print("Exiting RELEASE")
            self.checkQueues()

    def startNetworkListenerThread(self):
        CONNECTION_LIST = []  # all connections, it can be from client also
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Server Listening at:  ", self.ip, self.port)
        server_socket.bind((self.ip, self.port))  # I am listening to this port for all messages
        server_socket.listen(10)
        CONNECTION_LIST.append(server_socket)
        print("Ledger server started ...")
        while 1:
            read_sockets, write_sockets, error_sockets = select.select(CONNECTION_LIST, [], [])
            for sock in read_sockets:
                if sock == server_socket:
                    sockfd, addr = server_socket.accept()
                    CONNECTION_LIST.append(sockfd)
                else:
                    try:
                        data = sock.recv(4096)
                        addr = sock.getsockname()
                    except:
                        print("Client (%s, %s) is disconnected" % addr)
                        sock.close()
                        CONNECTION_LIST.remove(sock)
                        continue
                    if data:
                        Message_Received = data.decode("utf-8")
                        t = Thread(target=self.processReceivedMessage, args=(Message_Received,))  # it starts the thread
                        t.start()
                        CONNECTION_LIST.remove(sock)
        server_socket.close()

    def startServer(self):
        print("Start the server ...")
        # My server needs listens to the messages
        # Client sends TRANSACTION
        # Other servers send PROTOCOL messages

        print("Started and sending")
        self.startSendMessageThread()
        self.startNetworkListenerThread()

def main():
    print("Hello World!\nServer process is started ...\n")
    network_ip = 'localhost'
    network_port = 8081
    ip = 'localhost'
    port = 8092
    otherServers = {'A':"localhost,8090",'C':"localhost,8094"}
    id = 2
    client_ID = 'B'
    ledger = 'ledger_serverB.txt'
    f = open( ledger , "w+") # always start from initial state of ledger
    f.write("Init A 100\n")
    f.write("Init B 100\n")
    f.write("Init C 100\n")
    f.close()
    sv = Server(  ip, port, 10, id, client_ID ,ledger, network_ip, network_port, otherServers = otherServers  )
    sv.startServer()



if __name__ == "__main__":
    main()
