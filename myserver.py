# twisted imports
from twisted.protocols import basic
from twisted.words.protocols import irc
from twisted.internet import reactor, protocol, defer
from twisted.python import log

# system imports
import sys, time, re, logging, httplib
from time import time

# global variables
PORT = 12811
LOC_PATTERN = re.compile(r'([\+\-]\d+\.\d+)([\+\-]\d+\.\d+)')

# set up logging
logging.basicConfig(filename='hw6log.log', level=logging.DEBUG)

class Bryant(basic.LineReceiver):
	name = "Bryant"
	database = {}

	def connectionMade(self):
		print "Got new client!"
		self.factory.clients.append(self)
		logging.info("Connection made. ")

	def connectionLost(self, reason):
		print "Lost a client!"
		self.factory.clients.remove(self)
		logging.info("Connection lost: " + str(reason))

	def lineReceived(self, line):
		print "received", repr(line)
		logging.info("Line received: " + line)
		try:
			msg = line.split()
			if msg[0] == "IAMAT":
				self.processIAMAT(msg)
			elif msg[0] == "WHATSAT":
				self.processWHATSAT(msg)
			else:
				outmsg = "Unrecognized command or format\n"
				self.transport.write(outmsg)
				logging.info(outmsg)
		except IndexError:
			self.transport.write("Empty messages are not accepted.\n")
			logging.info("Empty Message. Response: " + outmsg)
			
    # Process 'AT' message
	def processIAMAT(self, msg):
		logging.info("Processing AT message. ")
        
		# get message components
		try:
			clientID = msg[1]
			coordinates = msg[2]
			time_stamp = msg[3]
		except IndexError:
			outmsg = "Syntax Error: \"AT <userID> <coordinates> <time>\"\n"
			self.transport.write(outmsg)
			logging.warning("IndexError. Response: " + outmsg)
			return

        # compute time difference, latitude/longitude
		try:
			lat, long = self.splitCoords(coordinates)
			timediff = time() - float(time_stamp)            
		except:
			outmsg = "Error computing time difference or coordinates.\n"
			self.transport.write(outmsg)
			logging.warning("Exception caught. Reponse: " + outmsg)
			return
        
        # update database with client info
		outmsg = "AT " + self.name + " " + "%+f" %(timediff) + " " + clientID + " " + str(coordinates) + " " + str(time_stamp) + "\n"
		client_dict = {'loc-lat': lat, 'loc-long': long, 'time_in': time_stamp, 'at_msg': outmsg}
		self.database[clientID] = client_dict

        # output response message
		self.transport.write(outmsg)
		logging.info("IAMAT command processed succesfully. Response: " + outmsg)
    
	# split coordinates into latitude and longitude
	def splitCoords(self, coordinates):
		try:
			m = re.search(LOC_PATTERN, coordinates)
			latitude = m.group(1)
			longitude = m.group(2)
			return latitude, longitude
		except:
			self.transport.write("Error parsing coordinates\n")
			logging.warning("Response: Error parsing coordinates")
			
	def processWHATSAT(self, msg):
		try:
			clientID = msg[1]
			radius = msg[2]
			upper_bound = msg[3]
			if int(upper_bound) > 100:
				upper_bound = str(100)
				self.transport.write("Upper bound cannot exceed 100; changing upper_bound to 100.\n")
		except IndexError:
			outmsg = "Syntax Error: \"WHATSAT <userID> <radius> <upper_bound>\"\n"
			self.transport.write(outmsg)
			logging.warning("IndexError. Response: " + outmsg)
			return
			
		if self.database.has_key(clientID):
			self.transport.write(self.database[clientID]['at_msg'])
			dfrd = self._queryTwitter(clientID, radius, upper_bound)
			dfrd.addCallback(self._handleQueryResult)
			dfrd.addErrback(self.handleQueryFailure)
		else:
			outmsg = "No information found for client %s\n" %clientID
			self.transport.write(outmsg)
			logging.warning("Queried client not found. Response: " + outmsg)
			
	def _queryTwitter(self, clientID, radius, upper_bound):
		d = defer.Deferred()
		
		lat = str(float(self.database[clientID]['loc-lat']))
		long = str(float(self.database[clientID]['loc-long']))
		query = "http://search.twitter.com/search.json?q=&geocode=" + lat + "." + long + "." + radius + "km&rpp=" + upper_bound + "&page=1"
		conn = httplib.HTTPConnection("www.twitter.com")
		conn.request("GET", query)
		response = conn.getresponse()
		query_result = response.read()
		
		d.callback(query_result)
		return d
		
		def _handleQueryResult(self, query_result):
			self.transport.write(query_result)
			logging.info("Query successful. Response: " + query_result)
			
		def _handleQueryFailure(self, query_failure):
			self.transport.write("Error in Twitter query.\n")
			logging.error("Errback. Response: Error in Twittery query.\n")
			query_failure.trap(RuntimeError)
		
from twisted.internet import protocol
from twisted.application import service, internet

factory = protocol.ServerFactory()
factory.protocol = Bryant
factory.clients = []

application = service.Application("chatserver")
internet.TCPServer(PORT, factory).setServiceParent(application)

reactor.connectTCP("lnxsrv.seas.ucla.edu", 12810, EchoClientFactory())
reactor.connectTCP("lnxsrv.seas.ucla.edu", 12812, EchoClientFactory())
reactor.connectTCP("lnxsrv.seas.ucla.edu", 12814, EchoClientFactory())
reactor.run()