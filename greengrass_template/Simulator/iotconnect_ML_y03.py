"""Module import sys."""
import sys
import time
import json
import datetime
import random
import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToTopicRequest,
    PublishMessage,
    BinaryMessage
)

frequency = int(sys.argv[1])
TIMEOUT = 10

ipc_client = awsiot.greengrasscoreipc.connect()


def publish_client_data_to_core(topic,messages):
    """Function publish message to Core."""
    print(">>> Local Publish topic : {} <<<".format(topic))
    print(">>> Local Publish message : {} <<<".format(messages))
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
            print(">>> Local Publish response :: {} <<<".format(future_response))

            future_response.result(TIMEOUT)
        except ImportError:
            print(">>> Error :: Local Publish response <<<")
    except ImportError:
        print(">>> Error :: publish data to IOT Core <<<")


while True:
    IPC_TOPIC = "iotc/rpt/d2gg/sub"

    data = {"Temperature":random.randint(0, 80),"Anomaly":0}
    obj_data = [{
        # "uniqueId": "iotconnect_ML",
        "time": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "data": data
        }]
    publish_client_data_to_core(IPC_TOPIC, obj_data)
    time.sleep(frequency)
