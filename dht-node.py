
"""
Distributed Hash Table
Programmed by Manasseh Banda
"""
import socket
import os
import sys
import random
import select
import json
import datetime
import pprint

MAX_BUFFER_SIZE = 4096
BOOTSTRAP_ID = (2 ** 16) - 1
BOOTSTRAP_PORT = 15000
BOOTSTRAP_HOSTNAME = 'silicon.cs.umanitoba.ca'
HOST = socket.getfqdn()
PORT = 15030
ADDRESS = (HOST, PORT)

class Node:
    def __init__(self):
        self.id = self.fetchID()
        self.pred = {
          "port": None,
          "ID": 0,
          "hostname": ""
        }
        self.succ = {
          "port": None,
          "ID": 0,
          "hostname": None
        }
        self.nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
          self.nodeSocket.bind(ADDRESS)
          print("Socket running at {0}. ID = {1}\n".format(HOST + ':' + str(PORT), str(self.id)))
        except socket.error as e:
          print ("Unable to bind with '{0}'".format(e.strerror))
          self.nodeSocket.close()

    def fetchID(self):
      nodeID = None
      try:
        f = open("id.txt", "r")
        nodeID = int(f.readline())
      except:
        nodeID = self.generateID()
        f = open("id.txt", "w")
        f.write(str(nodeID))
        f.close()

      return nodeID

    def generateID(self):
      min = 1
      max = (2 ** 16) - 2
      return random.randrange(min, max)    

    def getSocket(self):
      return self.nodeSocket

    def getID(self):
      return self.id

    def getPred(self):
      return self.pred

    def setPred(self, pred):
      self.pred = pred

    def getSucc(self):
      return self.succ

    def setSucc(self, succ):
      self.succ = succ  


class RingManager:
  def __init__(self):
    self.node = Node()
    
    # This containts the lists of the known predecessors. It is used for joining and is emptitied every time there is a successful join. 
    self.predList = []

  def pred(self):
    request = {
      "port": PORT,
      "ID": self.node.getID(),
      "hostname": HOST,
      "cmd": "pred?"
    }
    return json.dumps(request)

  def myPred(self):
    pred = self.node.getPred()
    request = {
      "cmd": "myPred",
      "me": {
        "port": PORT,
        "ID": self.node.getID(),
        "hostname": HOST
      },
      "thePred": {
        "port": pred["port"],
        "ID": pred["ID"],
        "hostname": pred["hostname"] 
      }  
    }
    return json.dumps(request)

  def setPred(self):
    request = {
      "port": PORT,
      "ID": self.node.getID(),
      "hostname": HOST,
      "cmd": "setPred"
    }
    return json.dumps(request)

  def find(self, originHostname, originID, originPort, hops, query):
    request = {
      "port": originPort,
      "ID": originID,
      "hostname": originHostname,
      "cmd": "find",
      "hops": hops,
      "query": query
    }
    return json.dumps(request)    

  def owner(self, hops, query):
    request = {
      "port": PORT,
      "ID": self.node.getID(),
      "hostname": HOST,
      "cmd": "owner",
      "hops": hops,
      "query": query
    }
    return json.dumps(request) 
    
  def joinRing(self, peerHostname, peerPort, peerID):
    nodeSocket = self.node.getSocket()
    nodeSocket.sendto(self.pred(), (peerHostname, peerPort))
    nodeSocket.settimeout(2)

    try:
      data, address = nodeSocket.recvfrom(MAX_BUFFER_SIZE)
      print(data)
      message = json.loads(data)

      if message["cmd"] == "myPred":
        if message["me"] and message["thePred"]:
          peer = message["me"]
          peerPred = message["thePred"]
          if int(peerPred["ID"]) > self.node.getID():
            self.predList.append((peer["hostname"], peer["port"], peer["ID"]))
            self.joinRing(peerPred["hostname"], peerPred["port"], peerPred["ID"])

          else:
            succ = {
              "port": int(peer["port"]),
              "hostname": peer["hostname"],
              "ID": int(peer["ID"])
            }
            self.setNodeSucc(succ)

            if int(peerPred["ID"]) != self.node.getID():
              nodeSocket.sendto(self.setPred(), (peer["hostname"], peer["port"]))

            nodeSocket.settimeout(None)
            self.predList = []

    except socket.timeout:
      print("Timeout occured while trying to join the Ring. Joining at last known predecessor.")
      succHost = self.predList[-1][0]
      succID = int(self.predList[-1][2])
      succPort = int(self.predList[-1][1])

      succ = {
        "port": succPort,
        "hostname": succHost,
        "ID": succID
      }
      self.setNodeSucc(succ)
      nodeSocket.sendto(self.setPred(), (succHost, succPort))
      nodeSocket.settimeout(None)
      self.predList = []

  def stabilizeRing(self, peerHostname, peerPort, peerID):
    nodeSocket = self.node.getSocket()
    nodeSocket.sendto(self.pred(), (peerHostname, peerPort))
    nodeSocket.settimeout(2)
    try:
      data, address = nodeSocket.recvfrom(MAX_BUFFER_SIZE)
      message = json.loads(data)

      if message["cmd"] == "myPred":
        if message["me"] and message["thePred"]:
          peer = message["me"]
          peerPred = message["thePred"]

          if int(peerPred["ID"]) > self.node.getID():
            self.stabilizeRing(peerPred["hostname"], peerPred["port"], peerPred["ID"])

          elif int(peerPred["ID"]) < self.node.getID():
            succ = {
              "port": int(peer["port"]),
              "hostname": peer["hostname"],
              "ID": int(peer["ID"])
            }
            self.setNodeSucc(succ)
            nodeSocket.sendto(self.setPred(), (peer["hostname"], peer["port"]))
            nodeSocket.settimeout(None)

    except socket.timeout:
      print("Timeout occured while trying to stabilize ring. Initiating new join at the bootstrap node.")
      self.joinRing(BOOTSTRAP_HOSTNAME, BOOTSTRAP_PORT, BOOTSTRAP_ID)
    
  def updatePred(self, hostname, predID, port):
    pred = {
      "port": int(port),
      "ID": int(predID),
      "hostname": hostname
    }
    self.node.setPred(pred)
    print("Predecessor has been changed to {0}\n".format(hostname + ':' + str(port) + ' with ID ' + str(predID)))

  def getNode(self):
    return self.node

  def getNodeID(self):
    return self.node.getID()

  def getNodeSucc(self):
    return self.node.getSucc()

  def setNodeSucc(self, succ):
    self.node.setSucc(succ)

  def getSocket(self):
    return self.node.getSocket()

  def generateNewID(self):
    nodeID = self.node.getID()
    while True:
      newID = self.node.generateID()
      if newID > nodeID:
        return newID


if __name__ == "__main__":
  try:
    # The only way the main method can interact with the Node is through the RingManager
    manager = RingManager()
    manager.joinRing(BOOTSTRAP_HOSTNAME, BOOTSTRAP_PORT, BOOTSTRAP_ID)
    nodeSocket = manager.getSocket()
    finished = False
    inputs = [nodeSocket, sys.stdin]

    while not finished:
      (readFDs, writeFDs, errorFDs) = select.select(inputs, [], []) 

      for desciptor in readFDs:
        if desciptor == sys.stdin:
          stdInput = sys.stdin.readline()
          if stdInput.strip() == "":
            finished = True
            break
          
          try:
            keyToQuery = int(stdInput)

            if keyToQuery <= manager.getNodeID():
              keyToQuery = manager.generateNewID()

            succ = manager.getNodeSucc()
            manager.stabilizeRing(succ["hostname"], succ["port"], succ["ID"])

            # Get node successor again just in case it changed after stabilization. 
            succ = manager.getNodeSucc()
            nodeSocket.sendto(manager.find(HOST, manager.getNodeID(), PORT, 0, keyToQuery), (succ["hostname"], succ["port"]))

          except Exception as e:
            print ("Error! - '{0}'".format(e))

        elif desciptor == nodeSocket:
          data, address = nodeSocket.recvfrom(MAX_BUFFER_SIZE)  
          message = json.loads(data)
          cmd = message["cmd"]
          
          # I am constantly checking if the message specifications have been met as a precautionary measure.
          if cmd == "pred?":
            if message["port"] and message["hostname"]:
              nodeSocket.sendto(manager.myPred(), (message["hostname"], int(message["port"])))

          if cmd == "setPred":
            if message["ID"] and message["port"] and message["hostname"]:
              if int(message["ID"]) < manager.getNodeID():
                manager.updatePred(message["hostname"], int(message["ID"]), int(message["port"]))

          if cmd == "find":
            if message["ID"] and message["port"] and message["hostname"] and message["hops"] and message["query"]:
              if int(message["query"]) == manager.getNodeID():
                nodeSocket.sendto(manager.owner(message["hops"], int(message["query"])), (message["hostname"], message["port"]))
              else:
                succ = manager.getNodeSucc()
                manager.stabilizeRing(succ["hostname"], succ["port"], succ["ID"])

                # Get node successor again just in case it changed after stabilization. 
                succ = manager.getNodeSucc()
                nodeSocket.sendto(manager.find(message["hostname"], int(message["ID"]), int(message["port"]), (int(message["hops"]) + 1), int(message["query"])), (succ["hostname"], succ["port"]))

          if cmd == "owner":
            if message["ID"] and message["port"] and message["hostname"] and message["hops"] and message["query"]:
              timestamp = datetime.datetime.now()
              print("{3} - The owner of {2} is {0}. Result was found in {1} hops.".format(message["hostname"] + ':' + str(message["port"]), str(message["hops"]), str(message["ID"]), timestamp))      

  except Exception as e:
    print ("Error '{0}'".format(e))
    
  finally:
    nodeSocket.close()
    finished = True    
