from twisted.internet.protocol import Protocol, ServerFactory 
from twisted.protocols.basic import LineReceiver 
from twisted.internet import reactor, defer 
import re 
from time import time 
import httplib 
import logging 

# Global Variables 
PORT = 12610 
LOC_PATTERN = re.compile(r'([\+\-]\d+\.\d+)([\+\-]\d+\.\d+)') 

# Set up logging 
logging.basicConfig(filename='hw6log.log', level=logging.DEBUG) 

class MessageReceiver(LineReceiver): 
    """ Protocol for server to process incoming messages (one line is a message) """ 
    name = "HERMES" # made-up server name 
    database = {} # Database of client info including time and location 
  
    # Log connection made 
    def connectionMade(self): 
        logging.info("Connection made.  ") 
   
    # Log connection dropped 
    def connectionLost(self, reason): 
        logging.info("Connection dropped: " + str(reason)) 
   
    # Process incoming client message 
    def lineReceived(self, line): 
        logging.info("Input message: " + line) 
        try: 
            msg = line.split() 
            if msg[0] == "AT": 
                self.processAT(msg) 
            elif msg[0] == "WHATSAT": 
                self.processWHATSAT(msg) 
            else: 
                outmsg = "?" + msg[0] + "\n" 
                self.transport.write(outmsg) 
                logging.info("Command unrecogized.  OUTPUT MSG: " + outmsg) 
        except IndexError: 
            self.transport.write("?\n") 
            logging.info("Empty Message.  OUTPUT MSG: ?\n") 
   
    # Process 'AT' message 
    def processAT(self, data):  
        logging.info("Processing AT message. ") 
    
	    # get message components 
        try: 
            clientID = data[1] 
            time_stamp = data[2] 
            location = data[3]  
        except IndexError: 
            outmsg = "Error: \"AT\" command syntax: \"AT <userID> <time> <location>\"\n" 
            self.transport.write(outmsg) 
            logging.warning("Index Error.  OUTPUT MSG: " + outmsg) 
            return
			
		# compute time difference, latitude/longitude 
        try: 
            timediff = time() - float(time_stamp) 
            lat, long = self._getLatLong(location) 
        except: 
            outmsg = "Error computing time difference or latitude/longitude coordinates.   
                Check that your command is in the proper format.\n"
            self.transport.write(outmsg) 
            logging.warning("Exception caught.  OUTPUT MSG: " + outmsg) 
            return 
   
        # update database with client info 
        outmsg = "AT " + self.name + " " + "%+f" %(timediff) + " " + clientID + " " + str(time_stamp)  
            + " " + str(location) + "\n"  
        client_dict = {'time_in': time_stamp, 'loc-lat': lat, 'loc-long': long, 'at_msg': outmsg} 
        self.database[clientID] = client_dict 
   
        # output response message  
        self.transport.write(outmsg) 
        logging.info("AT msg processed succesfully.  OUTPUT MSG: " + outmsg)  
   
    # Process 'WHATSAT' message 
    def processWHATSAT(self, data): 
        logging.info("Processing WHATSAT message. ") 
        
		# get message components 
        try: 
            clientID = data[1] 
            radius = data[2] 
            max_tweets = data[3] 
            if int(max_tweets) > 100: 
                max_tweets = str(100) 
                self.transport.write("Max results = 100, setting max_tweets to 100.\n") 
        except IndexError: 
            self.transport.write("Error: \"WHATSAT\" command syntax: \"WHATSAT  
            <userID> <radius> <max_tweets>\"\n") 
            return 
   
        # if client exists, query twitter 
        if self.database.has_key(clientID): 
            self.transport.write(self.database[clientID]['at_msg']) 
            dfrd = self._queryTwitter(clientID, radius, max_tweets) 
            dfrd.addCallback(self._handleQueryResult) 
            dfrd.addErrback(self._handleQueryFailure) 
        # client does not exist in database 
        else: 
            outmsg = "No information found for client %s\n" %clientID 
            self.transport.write(outmsg) 
            logging.warning("Queried client not found.  OUTUT MSG: " + outmsg) 
    
    # Split location coordinates into latitude and longitude 
    def _getLatLong(self, location): 
        try: 
            m = re.search(LOC_PATTERN, location) 
            latitude = m.group(1)  
            longitude = m.group(2)  
            return latitude, longitude 
        except: 
            self.transport.write("Error parsing location coordinates") 
            logging.warning("OUTPUT MSG: Error parsing location coordinates") 
    # Query twitter for [max_tweets] number of tweets within [radius] km  
    def _queryTwitter(self, clientID, radius, max_tweets): 
        d = defer.Deferred() 
  
        lat = str( float(self.database[clientID]['loc-lat']) ) 
        long = str( float(self.database[clientID]['loc-long']) ) 
        query = "http://search.twitter.com/search.json?q=&geocode=" + lat + "," + long + "," +  
            radius + "km&rpp=" + max_tweets + "&page=1" 
        conn = httplib.HTTPConnection("www.twitter.com") 
        conn.request("GET", query) 
        response = conn.getresponse() 
        query_result = response.read() 
  
        d.callback(query_result) 
        return d 
 
    # Callback to output Twitter's query response 
    def _handleQueryResult(self, query_result): 
        self.transport.write(query_result) 
        logging.info("Query successful.  OUTPUT: " + query_result) 
   
    # Errback to handle error as a result of Twitter query 
    def _handleQueryFailure(self, query_failure): 
        self.transport.write("Error in Twitter query.\n")
        logging.error("Errback.  OUTPUT: Error in Twitter query.\n") 
        query_failure.trap(RuntimeError) 

		### END class MessageReceiver 

def main(): 
    serverFactory = ServerFactory() 
    serverFactory.protocol = MessageReceiver 
    reactor.listenTCP(PORT, serverFactory) 
    reactor.run() 
  
if __name__ == '__main__': 
    main()