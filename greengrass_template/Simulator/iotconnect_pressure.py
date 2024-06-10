import sys
import os
import os.path
import time
import json
import datetime
import random
import pickle
import numpy as np
from sklearn.svm import OneClassSVM
import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToTopicRequest,
    PublishMessage,
    BinaryMessage
)

frequency = int(sys.argv[1])
print(frequency)
TIMEOUT = 10
threshold = -1

@property
def _time(self):
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")

ipc_client = awsiot.greengrasscoreipc.connect()


def Publish_client_data_to_core(topic,messages):
    print("Publish sending message {}".format(messages))
    print("sending to topic {}".format(topic))
    try:
        msgstring = json.dumps(messages)
    
        request = PublishToTopicRequest()
        request.topic = topic
        publish_message = PublishMessage()
        publish_message.binary_message = BinaryMessage()
        publish_message.binary_message.message = bytes(msgstring, "utf-8")
        request.publish_message = publish_message
        operation = ipc_client.new_publish_to_topic()
        operation.activate(request)
        try:
            future_response = operation.get_response()
            print("Future value : :  ", future_response)
    
            future_response.result(TIMEOUT)
        except Exception as error:
            print("Error in future()", str(error))        
    except Exception as ex:
        print("Publish error...! ",str(ex))


   
while True:
    topi = "iotc/rpt/d2gg/sub"
    #data = {"Temperature":random.randint(30, 50)}

    data = {"Pressure":random.randint(100, 500)}
    obj_data = [{
       
        "time": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "data": data
        }]
    Publish_client_data_to_core(topi,obj_data)
    print(frequency)
    time.sleep(15)
    pass