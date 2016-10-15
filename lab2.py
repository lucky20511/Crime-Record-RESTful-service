import logging
logging.basicConfig(level=logging.DEBUG)
from spyne import Application, srpc, ServiceBase, \
    Integer, Unicode
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication

import urllib2
import json
from collections import OrderedDict
import operator
from datetime import datetime
import re

class HelloWorldService(ServiceBase):
    @srpc(float, float, float, _returns=Iterable(Unicode))
    def checkcrime(lat, lon, radius):
        #for i in range(times):
        content = urllib2.urlopen('https://api.spotcrime.com/crimes.json?lat=%f&lon=%f&radius=%f&key=.'%(lat, lon, radius)).read()
        #print 'https://api.spotcrime.com/crimes.json?lat=%f&lon=%f&radius=%f&key=.'%(lat, lon, radius)


        type_count = dict()
        location_count = dict()
        count = 0
        time_interval = dict()
        time_interval["12:01am-3am"] = 0
        time_interval["3:01am-6am"] = 0
        time_interval["6:01am-9am"] = 0
        time_interval["9:01am-12noon"] = 0
        time_interval["12:01pm-3pm"] = 0
        time_interval["3:01pm-6pm"] = 0
        time_interval["6:01pm-9pm"] = 0
        time_interval["9:01pm-12midnight"] = 0

        #print content
        parsed_json = json.loads(content)
        ##print parsed_json

        #parsed_json = content
        #print parsed_json
        #print parsed_json["crimes"]


        for row in parsed_json["crimes"]:
            
            # total crimes
            count += 1

            # time    
            date_object = datetime.strptime(row["date"], '%m/%d/%y %I:%M %p')
            
            hour = date_object.hour
            minute = date_object.minute
            
            print row["date"] + " " + str(hour) + " " + str(minute)  

            if (hour >=0 and minute >= 1) and ((hour <= 2) or (hour==3 and minute == 0)):
                time_interval["12:01am-3am"] += 1
                print "12:01am-3am"
            elif (hour >=3 and minute >= 1) and ((hour <= 5) or (hour==6 and minute == 0)):
                time_interval["3:01am-6am"] += 1
                print "3:01am-6am"
            elif (hour >=6 and minute >= 1) and ((hour <= 8) or (hour==9 and minute == 0)):
                time_interval["6:01am-9am"] += 1
                print "6:01am-9am"
            elif (hour >=9 and minute >= 1) and ((hour <= 11) or (hour==12 and minute == 0)):
                time_interval["9:01am-12noon"] += 1
                print "9:01am-12noon"
            elif (hour >=12 and minute >= 1) and ((hour <= 14) or (hour==15 and minute == 0)):
                time_interval["12:01pm-3pm"] += 1
                print "12:01pm-3pm"
            elif (hour >=15 and minute >= 1) and ((hour <= 17) or (hour==18 and minute == 0)):
                time_interval["3:01pm-6pm"] += 1
                print "3:01pm-6pm"
            elif (hour >=18 and minute >= 1) and ((hour <= 20) or (hour==21 and minute == 0)):
                time_interval["6:01pm-9pm"] += 1
                print "6:01pm-9pm"
            elif (hour >=21 and minute >= 1) and ((hour <= 23) or (hour==24 and minute == 0)):
                time_interval["9:01pm-12midnight"] += 1
                print "9:01pm-12midnight"
            else:
                time_interval["9:01pm-12midnight"] += 1
                print "9:01pm-12midnight"


            address =  row["address"].split('&');


            for addr in address:
                searchObj = re.search( r'(.*)(block|of)(.*)', addr, re.M|re.I)
                if searchObj:
                    addr = searchObj.group(3)
                    searchObj = re.search( r'(.*)(#)(.*)', addr, re.M|re.I)
                    if searchObj:
                        addr = searchObj.group(1)
                addr = re.sub(r'^\s+|\s+\Z', '', addr)

                # location    
                if addr in location_count:
                    location_count[addr] += 1
                else:
                    location_count[addr] = 1

            # type
            if row["type"] in type_count:
                type_count[row["type"]] += 1
            else:
                type_count[row["type"]] = 1
            
            
        sorted_type_count = OrderedDict(sorted(type_count.items(), key=operator.itemgetter(1), reverse=True))
        sorted_location_count = OrderedDict(sorted(location_count.items(), key=operator.itemgetter(1), reverse=True))    

        ret = { "total_crime":count , 
        "the_most_dangerous_streets": sorted_location_count.keys()[:3], #[:3]
        "crime_type_count":sorted_type_count,
        "event_time_count":time_interval
        }


        yield ret




application = Application([HelloWorldService],
    tns='cmpe_273_lab2',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)
if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    #print "!!!!!!!!!!!!"
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()