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
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    SubscribeToTopicRequest,
    SubscriptionResponseMessage,
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

ipc_client = awsiot.greengrasscoreipc.connect()


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
        return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.000")

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
        return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    @property
    def _data_template(self):
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
            self.subscribe_to_core(self._subTopic)
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
                    obj = self.Publish_client_data_to_core(
                        pubtopic, json.dumps(data))
                else:
                    _obj = self.Publish_client_data_to_core(
                        pubtopic, json.dumps(data))

        except Exception as ex:
            print("send error...! ", ex)

    def Publish_client_data_to_core(self, topic, messages):
        print("Publish sending message {}".format(messages))
        print("sending to topic {}".format(topic))
        try:
            msgstring = json.dumps(messages)

            pubrequest = PublishToIoTCoreRequest()
            pubrequest.topic_name = topic
            pubrequest.payload = bytes(messages, "utf-8")
            pubrequest.qos = qos
            operation = ipc_client.new_publish_to_iot_core()
            operation.activate(pubrequest)
            try:
                future = operation.get_response()
                print("Future value : :  ", future)

                future.result(25)
                # future.result(TIMEOUT)
            except Exception as error:
                print("Error in future()", str(error))
        except Exception as ex:
            print("Publish error...! ", str(ex))

    def subscribe_to_core(self, topic):
        print("Subscribe_to_core {}".format(topic))
        try:
            subrequest_core = SubscribeToIoTCoreRequest()
            subrequest_core.topic_name = topic
            subrequest_core.qos = subqos
            handler_core = SubHandler()
            operation_core = ipc_client.new_subscribe_to_iot_core(handler_core)
            future_core = operation_core.activate(subrequest_core)
            future_core.result(TIMEOUT)
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

    def send_data_to_SDK(self, data):
        print("Resived data from firmware {}".format(data))
        rpt_topic = publishtopic
        # rpt_topic = self._pubRpt
        self.Publish_client_data_to_core(rpt_topic, json.dumps(data))
        return True

    #def send_data_to_SDK2(self, jsonArray):
    #    print("Resived data from firmware {}".format(jsonArray))
    #    rpt_topic = publishtopic
    #    #rpt_topic = self._pubRpt
    #    self.Publish_client_data_to_core(rpt_topic, json.dumps(jsonArray))
    #    return True

    def send_data_to_SDK2(self,jsonArray):
        print("Resived data from firmware {}".format(jsonArray))
        #rpt_topic = publishtopic
        rpt_topic = self._pubRpt
        print("type of received object : : : :", type(jsonArray))
        jsonArray = json.loads(jsonArray)
        try:
            for obj in jsonArray:
                print(obj)
                #unId = obj["uniqueId"]
                unId = self._uniqueId
                print(unId)
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
            self.Publish_client_data_to_core(rpt_topic, json.dumps(rpt_data))
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
        self.Publish_client_data_to_core(ack_topic, json.dumps(data))
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
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: SubscriptionResponseMessage) -> None:
        try:
            data = str(event.binary_message.message, "utf-8")
            print("Received new message: " + data)
            print("type of data ============================: ", type(data))
            SDK.send_data_to_SDK2(data)
            # Handle message.
        except:
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        # Handle error.
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        pass


request = SubscribeToTopicRequest()
request.topic = subtopic
handler = StreamHandler()
operation = ipc_client.new_subscribe_to_topic(handler)
operation.activate(request)
future_response = operation.get_response()
future_response.result(TIMEOUT)


def main():
    global SId, cpid, env, UniqueId, SDK
    SDK = IoTConnectSDK(UniqueId, SId, cpid, env)
    while True:
        # dObj = [{
        #        "uniqueId":UniqueId,
        #        "time":datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        #        "data": {
        #            "Temperature": -2147483649
        #        }
        #    }]
        dObj = {
            "dt": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "d": [
                {
                    "id": UniqueId,
                    "tg": "parent",
                    "dt": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "d": {
                        "Temperature": random.randint(30, 50),
                        "PBit": 1,
                        "PBoolean": True,
                        "PDate": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                        "PDateTime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "PDecimal": 2.555,
                        "PInteger": random.randint(30, 50),
                        "PLong": 123456789,
                        "PString": "Green Grass parent",
                        "PTime": "11:44:22",
                        "PObject": {
                            "pbit": 0,
                            "pboolean": True,
                            "pdate": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                            "pdatetime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "pdecimal": 2.555,
                            "pinteger": 884,
                            "plong": 999,
                            "pstring": "green",
                            "ptime": "11:44:22"
                        }
                    }
                },
                {
                    "id": UniqueId+"c1",
                    "tg": "child1",
                    "dt": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "d": {
                        "Temperature": random.randint(30, 50),
                        "cBit": 1,
                        "cBoolean": True,
                        "cDate": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                        "cDateTime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "cDecimal": 2.555,
                        "cInteger": random.randint(30, 50),
                        "cLong": 123456789,
                        "cString": "Green Grass parent",
                        "cTime": "1:44:22",
                        "cObject": {
                            "cbit": 0,
                            "cboolean": True,
                            "cdate": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                            "cdatetime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "cdecimal": 2.555,
                            "cinteger": 884,
                            "clong": 999,
                            "cstring": "green",
                            "ctime": "2:44:22"
                        }
                    }
                }  # ,
                # {
                #     "id":UniqueId+"c2",
                #     "tg": "child1",
                #     "dt": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                #     "d": {
                #             "Temperature":random.randint(10, 80)
                #    }
                # }
            ]
        }
        # result = SDK.send_data_to_SDK(dObj)
        # result = SDK.send_data_to_SDK2(dObj)
        # time.sleep(10)
        pass


if __name__ == "__main__":
    main()
