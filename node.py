"""
BIG PICTURE
- We are Implementing a Distributed Hash Table (DHT).
- We are going to be using Datagrams for this. 
    - Sending messages using JSON.


REQUIREMENTS (Things to ensure the Node is able to do)
--------------------------------------------------------
1. Will randomly generate an ID between 1 and (2 ^ 16) - 2 for use when joinig the ring and managing queries.
2. Upon startup, the node should join the ring based on the algorithms discussed in class. 
3. The node should perform stabilization and error detection/recovery using the algotithms discussed in class.
4. 2 seconds should be used as the timeout value.
5. Parsing JSON strings to extract the data from them.

Node Behavious (triggered via stdin):
- On receiving an empty string, the node will close it's socket and exit immediately.
- On receiving an integer value (Which is a key to query) the node will send query to it's successor.
    - Query will then proceed around the ring until a node can respond indicating ownership of the ID.
    - Upon receiving this ownership message, the receiving node will log the result.
        - Loging should be done to stdout or maybe a JSON file?
        - Log will include
            * timestamp
            * log message
- The query value entered in stdin must be greter than the node's ID (since all other keys can be handeled automatically?).
    - If the entered value is less than or equal to your ID (or anything invalid), randomly generate key values until the 
    value that is generated is greater than the node's ID.
    - Proceed to perform the query using the generated value.


Protocol Requirements:
- Messages are sent between nodes using datagrams that contain JSON objects.
    - Contents of JSON objects depend on the object being sent.
        - Every message must contain:
            - 'cmd' field which identifies the type of message being sent/received.

                - 'pred?'   : query for a node's predecessor. A node that receieves this message will respond with the message 'myPred'. 
                              The sender of this message will need a timeout waiting for a response. When timeout occurs, log what you were trying to do and how 
                              you will recovver.

                                If none arives:
                                    a. (On trying to join the ring) Join the ring (using setPred) at the last node from which a predecessor was sucessfully obtained.
                                    b. On attempting to stablilize the ring, initiate a new join at the bootstrap node.


                - 'myPred'  : response to the predecessor request. The message contains two sub-dictionaries for identification purposes. "me" and "thePred". These
                              contain the the 'ID', 'hostname' and 'port' for the node sending the message and it's predecessor, respectively. 
                              
                              * THIS IS HE ONLY MESSAGE WHERE YOU SHOULDN'T EXPLICITLY INDICATE 'port', 'ID', and 'hostname' like in the other message types


                - 'setPred' : message telling a node to update its predecessor. Is sent to finalize your node's joining the ring. Receiver updates their pred without
                              giving a response. When a node joins the rind and sends this message, it will log its current successor and predecessor information. Receiver
                              will log that that it's pred has changed (and what is has changed to).


                - 'find'    : ititiates a query. Should always contain the itiiating node's 'ID', 'hostname' and 'port' (indicating the node to which the resp should be sent).
                              Initial sender includes "hops" set to 0 and "query" set to the ID being queried, and sends the message to its (the sender) successor.

                              Upon receiving a find, a node increments "hops" and will either:
                                a. Pass the query to its (the receiver) sucessor if it doesn't own the queried ID, or
                                b. Send owner to the initiating node if it does own the ID.

                - 'owner'   : response to a query. 'ID', 'hostname', and 'port' in this message will contain the information for the owner of the queried ID. The message will
                              also contain the "query" ID and the final "hops" value. Upon receiving the message, log the owner and number of hops required to obtain the owner 
                              of the queried ID.                                                             

            - 'port', 'ID', and 'hostname' for identifying the sender of the message



IDEAS:
- Consider having a function/class for ring management
    - This is handle joining the ring
    - Taking in input from stdin
"""



import socket
import os
import sys

# BOOTSTRAP_ID
# BOOTSTRAP_ PORT
# BOOTSTRAP_HOSTNAME

class Node:
    def __init__(self):
        self.id = self.generateID()


    def generateID(self):
        return 1    



class RingManager:
# join ring
# leave ring
# handle messages
# should initialize when the program loads? Data grams are just ways for peers to talk to each other. Remember that
#join ring first, the start taking messages.


if __name__ == "__main__":
    HOST = socket.getfqdn()
    PORT = 15030
    address = (HOST, PORT)

    finished = False

    nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    try:
        # This says "I want to receive messages on this port".
        nodeSocket.bind(address) 
    except socket.error as e:
        print "Unable to bind with '{0}'".format(e.strerror)
        nodeSocket.close()
        finished = True        

    while not finished:
        try:
            data, address = nodeSocket.recvfrom(4096)    

            #process the message receieved here.
            nodeSocket.send