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
        print parsed_json

        #parsed_json = content
        #print parsed_json
        #print parsed_json["crimes"]


        for row in parsed_json["crimes"]:
            #print row
            count += 1

            date_object = datetime.strptime(row["date"], '%m/%d/%y %I:%M %p')
            #date_object_1 = datetime.strptime('10/08/16 4:00 AM', '%m/%d/%y %I:%M %p')
            #print date_object.hour
            #print date_object.minute
            hour = date_object.hour
            minute = date_object.minute
            #print hour
            #print minute
            #date_object = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
            if (hour >=0 and minute > 1) and hour <= 2:
                time_interval["12:01am-3am"] += 1
            elif (hour >=3 and minute > 1) and hour <= 5:
                time_interval["3:01am-6am"] += 1
            elif (hour >=6 and minute > 1) and hour <= 8:
                time_interval["6:01am-9am"] += 1
            elif (hour >=9 and minute > 1) and hour <= 11:
                time_interval["9:01am-12noon"] += 1
            elif (hour >=12 and minute > 1) and hour <= 14:
                time_interval["12:01pm-3pm"] += 1
            elif (hour >=15 and minute > 1) and hour <= 17:
                time_interval["3:01pm-6pm"] += 1
            elif (hour >=18 and minute > 1) and hour <= 20:
                time_interval["6:01pm-9pm"] += 1
            elif (hour >=21 and minute > 1) and hour <= 23:
                time_interval["9:01pm-12midnight"] += 1
            else:
                time_interval["9:01pm-12midnight"] += 1

            # location    
            if row["address"] in location_count:
                location_count[row["address"]] += 1
            else:
                location_count[row["address"]] = 1
            # type
            if row["type"] in type_count:
                type_count[row["type"]] += 1
            else:
                type_count[row["type"]] = 1
            
            #print "========="
        sorted_type_count = OrderedDict(sorted(type_count.items(), key=operator.itemgetter(1), reverse=True))
        sorted_location_count = OrderedDict(sorted(location_count.items(), key=operator.itemgetter(1), reverse=True))    
        #print json.dumps(sorted_type_count)
        #print json.dumps(sorted_location_count)

        ret = { "total_crime":count , 
        "the_most_dangerous_streets": sorted_location_count.keys()[:3],
        "crime_type_count":sorted_type_count,
        "event_time_count":time_interval
        }
        #print json.dumps(ret, sort_keys=False)

        #yield parsed_json

        yield ret

        #print json.dumps( sorted_location_count.keys())
        #print json.dumps( sorted_location_count.keys()+ sorted_type_count)
        #yield sorted_type_count+sorted_location_count
        #yield 'Hello, lat = %f  lon = %f  radius = %f ' % (lat, lon, radius)




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