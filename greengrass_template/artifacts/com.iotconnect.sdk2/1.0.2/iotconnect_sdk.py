import sys
import json
import os.path
import time
import copy
import datetime
import traceback
import random
import os
import ssl
import urllib.request as urllib
from urllib.parse import urlparse, quote_plus, urlencode
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.clientv2 as clientv2
import awsiot.greengrasscoreipc.client as client
import  awsiot.greengrasscoreipc.model
from awsiot.greengrasscoreipc.model import (
    SubscribeToTopicRequest,
    SubscriptionResponseMessage,
    UnauthorizedError,
    PublishToIoTCoreRequest,
    IoTCoreMessage,
    QOS,
    SubscribeToIoTCoreRequest,
    PublishToTopicRequest,
    PublishMessage,
    BinaryMessage

)

OPTION = {
    "attribute": "att",
    "setting": "set",
    "protocol": "p",
    "device": "d",
    "sdkConfig": "sc",
    "rule": "r"
}

CMDTYPE = {
    "DCOMM": 0,
    "FIRMWARE": 1,
    "MODULE": 2,
    "U_ATTRIBUTE": 101,
    "U_SETTING": 102,
    "U_RULE": 103,
    "U_DEVICE": 104,
    "DATA_FRQ": 105,
    "U_barred": 106,
    "D_Disabled": 107,
    "D_Released": 108,
    "STOP": 109,
    "Start_Hr_beat": 110,
    "Stop_Hr_beat": 111,
    "is_connect": 116,
    "SYNC": "sync",
    "RESETPWD": "resetpwd",
    "UCART": "updatecrt"
}

subtopic = "iotc/rpt/d2gg/sub"
publishtopic = "my/topic/pub1"

SId = "ODkwODBjOWVmNmE3NDZmYTg5NDI3OGRlZDMwYWY3ODE=UDE6MTI6MTYuNTc="
cpid = os.environ['CPID']
env = os.environ['ENV']
Instance = os.environ['Instance']
UniqueId = os.environ['AWS_IOT_THING_NAME']
Discovery_url = os.environ['URL']

if(Instance == "S"):
    cpid = UniqueId.split("-")[0]
    UniqueId = UniqueId.replace(cpid+"-", "", 1)

print("uniqueId : " + UniqueId)
print("CPID : " +cpid)

message = "b4pressed"
TIMEOUT = 30

subqos = QOS.AT_MOST_ONCE
qos = QOS.AT_LEAST_ONCE

ipc_client_v2 =  clientv2.GreengrassCoreIPCClientV2()

class IoTConnectSDK:
    _property = None
    _config = None
    _cpId = None
    _env = None
    _sId = None
    _uniqueId = None
    _listner_callback = None
    _listner_device_callback = None
    _listner_attchng_callback = None
    _listner_module_callback = None
    _listner_devicechng_callback = None
    _listner_rulechng_callback = None
    _listner_creatchild_callback = None
    _listner_twin_callback = None
    _data_json = None
    _client = None
    _is_process_started = False
    _base_url = ""
    _pf = None
    _dip = None
    _thread = None
    _ruleEval = None
    _offlineClient = None
    _lock = None
    _dispose = False
    _live_device = []
    _debug = False
    _data_frequency = 60
    _debug_error_path = None
    _debug_output_path = None
    _dftime = None
    _offlineflag = False
    _time_s = None
    _heartbeat_timer = None
    deletechild = None
    _listner_deletechild_callback = None
    _validation = True
    _getattribute_callback = None
    _subTopic = None
    _pubRpt = None
    _ditopic = None
    _pubACK = None
    _pubFlt = None

    @property
    def _time(self):
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")

    @property
    def protocol(self):
        try:
            key = OPTION["protocol"]
            if self._data_json != None and self.has_key(self._data_json, key) and self._data_json[key] != None:
                return self._data_json[key]
            else:
                return None
        except:
            print("protocol not initialized")

    @property
    def _timestamp(self):
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")

    @property
    def _data_template(self):
        print("fetching current time")
        try:
            data = {
                "d": [],
                "dt": ""
            }
            data["dt"] = self._timestamp
            return data
        except Exception as ex:
            print("_data_template ", ex)

    @property
    def _Ack_data_template(self):
        try:
            data = {
                "dt": "",
                "d": {
                    "ack": "",
                    "type": 0,
                    "st": 0,
                    "msg": "",
                }
            }
            data["dt"] = self._timestamp
            return data
        except Exception as ex:
            print("_Ack_data_template ", ex)

    def has_key(self, data, key):
        try:
            return key in data
        except:
            return False

    def init_protocol(self):
        try:
            protocol_cofig = self.protocol
            name = protocol_cofig["n"]
            protocol_cofig["pf"] = self._pf
            self._subTopic = protocol_cofig["topics"]["c2d"]
            self.subscribe_to_core_v2(self._subTopic)
            self._pubRpt = protocol_cofig["topics"]["rpt"]
            print(self._pubRpt)
            self._ditopic = protocol_cofig["topics"]["di"]
            self._pubACK = protocol_cofig["topics"]["ack"]
            self._pubFlt = protocol_cofig["topics"]["flt"]
        except Exception as ex:
            print("init_protocol", ex)

    def _hello_handsake(self, data):
        self.Send(data, "Di")

    def Send(self, data, msgtype):
        try:
            _obj = None
            pubtopic = None

            if msgtype == "Di":
                pubtopic = self._ditopic
            elif msgtype == "CMD_ACK":
                pubtopic = self._pubACK
            elif msgtype == "RPT":
                pubtopic = self._pubRpt

            else:
                pubtopic = self._pubFlt

            if pubtopic != None:
                if pubtopic == self._ditopic:
                    obj = self.publish_to_iot_core_v2(
                        pubtopic, json.dumps(data))
                else:
                    _obj = self.publish_to_iot_core_v2(
                        pubtopic, json.dumps(data))

        except Exception as ex:
            print("send error...! ", ex)

    
    def publish_to_iot_core_v2(self, topic, messages):
        print("publishing to iot using clientv2()...... ")
        iot_core_topic = topic
        
        try:
            ipc_client_v2.publish_to_iot_core(topic_name = iot_core_topic, qos = '1', payload = bytes(messages, 'utf-8'))
            print(f'Published message to AWS IoT Core: {messages}')
        except Exception as e:
            print(f'Failed to publish message to AWS IoT Core: {e}')


   
    def subscribe_to_core_v2(self, topic):
        print("Subscribe_to_core {}".format(topic))
        handler_core = SubHandler()
        try:
            resp, operation = ipc_client_v2.subscribe_to_iot_core(
                topic_name=topic,
                qos=qos, 
                on_stream_event=handler_core.on_stream_event,
                on_stream_error=handler_core.on_stream_error,
                on_stream_closed=handler_core.on_stream_closed
            )
            print("RRESP AND OPERATION" , resp, operation)
        except Exception as ex:
            print("subscribe error...! ", ex)

    def onDeviceCommand(self, callback):
        if callback:
            self._listner_device_callback = callback

    def DeviceCallback(self, msg):
        print("\n--- Command Message Received in Firmware ---")
        print(json.dumps(msg))
        cmdType = None
        if msg != None:
            cmdType = msg["ct"] if "ct" in msg else None
        if cmdType == 0:
            """
            * Type    : Public Method "sendAck()"
            * Usage   : Send device command received acknowledgment to cloud
            * 
            * - status Type
            *     st = 6; // Device command Ack status 
            *     st = 4; // Failed Ack
            * - Message Type
            *     msgType = 5; // for "0x01" device command 
            """
            data = msg
            if data != None:
                if "id" in data:
                    if "ack" in data and data["ack"]:
                        print("\n---  if ack in data in Firmware ---")
                        # SDK.sendAckCmd(data["ack"],7,"sucessfull",data["id"])  #fail=4,executed= 5,sucess=7,6=executedack
                else:
                    if "ack" in data and data["ack"]:
                        print("\n---  if ack in data Received in Firmware ---")
                        # fail=4,executed= 5,sucess=7,6=executedack
                        self.sendAckCmd(data["ack"], 7, "sucessfull")
        else:
            print("rule command", msg)

    def sendAckCmd(self, ackGuid, status, msg):
        try:
            template = self._Ack_data_template
            template["d"]["type"] = 0
            template["d"]["st"] = status
            template["d"]["msg"] = msg
            template["d"]["ack"] = ackGuid
            print("template", template)
            self.send_msg_to_broker(template)
        except Exception as ex:
            raise (ex)

    def onMessage(self, msg):
        # print("\n====================>>>>>>>>>>>>>>>>>>>>>>>\n")
        # print ("Cloud To Device Message Received::\n",msg)
        # print("\n<<<<<<<<<<<<<<<<<<<<<<<====================\n")
        try:
            if msg == None:
                return

            if "ct" not in msg:
                print("Command Received : " + json.dumps(msg))
                return

            if msg["ct"] == CMDTYPE["DCOMM"]:
                print(str(CMDTYPE["DCOMM"])+" DCOMM command received...")
                print(msg)
                self.DeviceCallback(msg)
                # if self._listner_device_callback != None:
                # self._listner_device_callback(msg)

        except Exception as ex:
            print("Message process failed..." + str(ex))

    def send_data_to_SDK2(self,jsonArray):
        rpt_topic = self._pubRpt
        jsonArray = json.loads(jsonArray)
        try:
            for obj in jsonArray:
                unId = self._uniqueId
                time_v = obj["time"]
                sensorData = obj["data"]
            rpt_data = self._data_template    
            d_object = {}
            d_object["id"] = unId
            d_object["tg"] = ""
            d_object["dt"] = time_v
            d_object["d"] = sensorData
            rpt_data["d"].append(d_object)  
            print(rpt_data)  
            self.publish_to_iot_core_v2(rpt_topic, json.dumps(rpt_data))
            return True
        except Exception as ex:
            print("Send data error ", ex)  


    def send_msg_to_broker(self, data):
        print("Sending ACK to Core {}".format(data))
        data1 = {
            "Neerav": random.randint(30, 50)
        }

        time.sleep(20)
        ack_topic = self._pubACK
        self.publish_to_iot_core_v2(ack_topic, json.dumps(data))
        return True

    def get_base_url(self, cpid, env):
        base_url = "/api/v2.1/dsdk/cpid/" + cpid + "/env/" + env + "?pf=aws"
        base_url = Discovery_url + base_url
        print(base_url)
        res = urllib.urlopen(base_url).read().decode("utf-8")
        print(res)
        data = json.loads(res)
        return data['d']["bu"], data['d']["pf"], data['d']["dip"]

    def get_call(self, url, uniqueId):
        url = url+"/uid/"+uniqueId
        res = urllib.urlopen(url).read().decode("utf-8")
        data = json.loads(res)
        return data

    def process_sync(self, base_url, dip, uniqueid):
        try:
            response = self.get_call(base_url, uniqueid)
            if self.has_key(response, "d"):
                response = response["d"]
                print('[INFO_IN01] '+'[' + str(self._sId)+'_' + str(self._uniqueId) +
                      "] Device information received successfully: " + self._time, 0)
            else:
                print('[error01] '+'[' + str(self._sId)+'_' + str(self._uniqueId) +
                      "] Device information no received : " + self._time, 0)

            self._data_json = response
            self.init_protocol()

            if self.has_key(self._data_json, "has") and self._data_json["has"]["attr"]:
                self._hello_handsake({"mt": 201})
            if self.has_key(self._data_json, "has") and self._data_json["has"]["d"]:
                self._hello_handsake({"mt": 204})

        except Exception as ex:
            print("sync call... ", ex)

    def __init__(self, uniqueId, sId, cpid, env):

        self._sId = sId
        self._cpId = cpid
        self._env = env
        self._uniqueId = uniqueId

        self._base_url, self._pf, self._dip = self.get_base_url(cpid, env)
        print(self._pf)
        print(self._dip)
        if self._base_url != None:
            print('[INFO_IN07] '+'[' + str(self._sId or cpid)+'_' + str(self._uniqueId) +
                  "] BaseUrl received to sync the device information: " + self._time, 0)
            self.process_sync(self._base_url, self._dip, self._uniqueId)


class SubHandler(client.SubscribeToIoTCoreStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        try:
            message = str(event.message.payload, "utf-8")
            print("payload received from client dev :", event.message.payload)
            topic_name = event.message.topic_name
            print("payload topic from client dev :", topic_name)
            # Handle message.
            jsonmsg = json.loads(message)
            SDK.onMessage(jsonmsg)
        except:
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        # Handle error.
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        pass


class StreamHandler(client.SubscribeToTopicStreamHandler):
    def __init__(self, SDK):
        super().__init__(SDK)

    def on_stream_event(self, event: SubscriptionResponseMessage) -> None:
        try:
            data = str(event.binary_message.message, "utf-8")
            print("Received new message: " + data)
            print("TOPIC  : :   " + event.binary_message.context.topic)
            print("type of data ============================: ", type(data))
            SDK.send_data_to_SDK2(data)
            # Handle message.
        except:
            print("Error in receiving stream event message.....")
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        # Handle error.
        print("ON STREAM ERROR : : : ", str(error))
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        print('Subscribe to topic stream closed.')
        pass


try:
    handler = StreamHandler(SDK)
    _, operation = ipc_client_v2.subscribe_to_topic(topic=subtopic, on_stream_event=handler.on_stream_event,
                                                        on_stream_error=handler.on_stream_error, on_stream_closed=handler.on_stream_closed)
    print('Successfully subscribed to topic: ' + subtopic)
except UnauthorizedError:
        print('Unauthorized error while subscribing to topic: ' +
              subtopic)
        traceback.print_exc()
      
except Exception:
        print('Exception occurred', file=sys.stderr)
        traceback.print_exc()


def main():
    global SId, cpid, env, UniqueId, SDK
    SDK = IoTConnectSDK(UniqueId, SId, cpid, env)
    while True:
        print("In WHILE True")
        time.sleep(10)
 

if __name__ == "__main__":
    main()
