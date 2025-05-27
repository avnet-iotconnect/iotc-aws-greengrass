'''AWS Green Grass IoTConnect SDK Code'''
import sys
import json
import time
import datetime
import traceback
import os
import os.path
import urllib.request as urllib
from awsiot.greengrasscoreipc import clientv2
from awsiot.greengrasscoreipc import client
from awsiot.greengrasscoreipc.model import (
    SubscriptionResponseMessage,
    UnauthorizedError,
    IoTCoreMessage,
    QOS,
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

LOCAL_SUBTOPIC = "iotc/rpt/d2gg/sub"

SID = "ODkwODBjOWVmNmE3NDZmYTg5NDI3OGRlZDMwYWY3ODE=UDE6MTI6MTYuNTc="
cpid = os.environ['CPID']
env = os.environ['ENV']
Instance = os.environ['Instance']
UniqueId = os.environ['AWS_IOT_THING_NAME']
Discovery_url = os.environ['URL']

if Instance == "S":
    cpid = UniqueId.split("-")[0]
    UniqueId = UniqueId.replace(cpid + "-", "", 1)

print("uniqueId : " + UniqueId)
print("CPID : " +cpid)

TIMEOUT = 10

ipc_client_v2 = clientv2.GreengrassCoreIPCClientV2()


class IoTConnectSDK:
    '''Class IOTCONNECTSDK'''
    _cpid = None
    _env = None
    _sid = None
    _uniqueid = None
    _data_json = None
    _base_url = ""
    _pf = None
    _dip = None
    _debug = False
    _data_frequency = 60
    _sub_topic = None
    _pub_rpt = None
    _ditopic = None
    _pub_ack = None
    _pub_flt = None

    @property
    def protocol(self):
        '''def protocol(self):'''
        try:
            key = OPTION["protocol"]
            if self._data_json is not None and self.has_key(self._data_json,
                                    key) and self._data_json[key] is not None:
                return self._data_json[key]
        except ImportError as ex:
            print("protocol not initialized", ex)
        return None


    @property
    def _timestamp(self):
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")

    @property
    def _data_template(self):
        try:
            data = {
                "d" : [],
                "dt" : ""
            }
            data["dt"] = self._timestamp
            return data
        except ImportError as ex:
            print("_data_template ", ex)
        return None

    @property
    def ack_data_template(self):
        '''def ack_data_template()'''
        try:
            data = {
                "dt": "",
                "d": {
                    "ack" : "",
                    "type" : 0,
                    "st" : 0,
                    "msg" : "",
                }
            }
            data["dt"] = self._timestamp
            return data
        except ImportError as ex:
            print("ack_data_template ", ex)
        return None

    def has_key(self, data, key):
        '''def has_key(self):'''
        try:
            return key in data
        except ImportError:
            return False

    def init_protocol(self):
        '''def init_protocol(self):'''
        try:
            protocol_cofig = self.protocol
            protocol_cofig["pf"] = self._pf
            self._sub_topic = protocol_cofig["topics"]["c2d"]
            self.subscribe_to_core_v2(self._sub_topic)
            self._pub_rpt = protocol_cofig["topics"]["rpt"]
            print(self._pub_rpt)
            self._ditopic = protocol_cofig["topics"]["di"]
            self._pub_ack = protocol_cofig["topics"]["ack"]
            self._pub_flt = protocol_cofig["topics"]["flt"]
        except ImportError as ex:
            print("init_protocol", ex)

    def _hello_handsake(self, data):
        '''def _hello_handsake():'''
        self.send_data(data, "Di")

    def send_data(self, data, msgtype):
        '''def send_data():'''
        try:
            _obj = None
            pubtopic = None

            if msgtype == "Di":
                pubtopic = self._ditopic
            elif msgtype == "CMD_ACK":
                pubtopic = self._pub_ack
            elif msgtype == "RPT":
                pubtopic = self._pub_rpt

            else:
                pubtopic = self._pub_flt

            if pubtopic is not None:
                if pubtopic == self._ditopic:
                    self.publish_to_iot_core_v2(
                        pubtopic, json.dumps(data))
                else:
                    self.publish_to_iot_core_v2(
                        pubtopic, json.dumps(data))

        except ImportError as ex:
            print("send error...! ", ex)

    def publish_to_iot_core_v2(self, topic, messages):
        '''def publish_to_iot_core_v2():'''
        iot_core_topic = topic
        try:
            ipc_client_v2.publish_to_iot_core(topic_name = iot_core_topic,
                                              qos = QOS.AT_LEAST_ONCE,
                                              payload = bytes(messages, 'utf-8'))
            print(f'Published message to AWS IoT Core: {messages}')
        except ImportError as ex:
            print(f'Failed to publish message to AWS IoT Core: {ex}')


    def subscribe_to_core_v2(self, topic):
        '''def subscribe_to_core_v2():'''
        print(f"Subscribe_to_core {topic}")
        handler_core = SubHandler()
        try:
            ipc_client_v2.subscribe_to_iot_core(
                topic_name = topic,
                qos = QOS.AT_LEAST_ONCE,
                on_stream_event = handler_core.on_stream_event,
                on_stream_error = handler_core.on_stream_error,
                on_stream_closed = handler_core.on_stream_closed
            )
        except ImportError as ex:
            print("subscribe error...!" , ex)


    def device_callback(self, msg):
        '''def device_callback():'''
        print("\n--- Command Message Received in Firmware ---")
        print(json.dumps(msg))
        cmd_type = None
        if msg is not None:
            cmd_type = msg["ct"] if "ct" in msg else None
        if cmd_type == 0:
            # * Type    : Public Method "sendAck()"
            # * Usage   : Send device command received acknowledgment to cloud
            # *
            # * - status Type
            # *     st = 6; // Device command Ack status
            # *     st = 4; // Failed Ack
            # * - Message Type
            # *     msgType = 5; // for "0x01" device command
            data = msg
            if data is not None:
                if "id" in data:
                    if "ack" in data and data["ack"]:
                        print("\n---  if ack in data in Firmware ---")
                        # SDK.send_ack_cmd(data["ack"],7,"sucessfull",data["id"])
                        # #fail=4,executed= 5,sucess=7,6=executedack
                else:
                    if "ack" in data and data["ack"]:
                        print("\n---  if ack in data Received in Firmware ---")
                        # fail=4,executed= 5,sucess=7,6=executedack
                        self.send_ack_cmd(data["ack"], 7, "sucessfull")
        else:
            print(f"rule command : {msg}")

    def send_ack_cmd(self, ack_guid, status, msg):
        '''def send_ack_cmd():'''
        try:
            template = self.ack_data_template
            template["d"]["type"] = 0
            template["d"]["st"] = status
            template["d"]["msg"] = msg
            template["d"]["ack"] = ack_guid
            print(f"ACK Template : {template}")
            self.send_msg_to_broker(template)
        except Exception as ex:
            raise ex

    def on_message(self, msg):
        '''def on_message():'''
        try:
            if msg is None:
                return

            if "ct" not in msg:
                print(f"Command Received : {json.dumps(msg)}")
                return

            if msg["ct"] == CMDTYPE["DCOMM"]:
                print(str(CMDTYPE["DCOMM"])+" DCOMM command received...")
                print(msg)
                self.device_callback(msg)
                # if self._listner_device_callback != None:
                # self._listner_device_callback(msg)

        except ImportError as ex:
            print("Message process failed..." + str(ex))


    def send_data_to_sdk2(self,json_array):
        '''def send_data_to_sdk2():'''
        rpt_topic = self._pub_rpt
        json_array = json.loads(json_array)
        try:
            for obj in json_array:
                unid = self._uniqueid
                time_v = obj["time"]
                sensor_data = obj["data"]
            rpt_data = self._data_template
            d_object = {}
            d_object["id"] = unid
            d_object["tg"] = ""
            d_object["dt"] = time_v
            d_object["d"] = sensor_data
            rpt_data["d"].append(d_object)
            print("Publishing data to IOT Core")
            self.publish_to_iot_core_v2(rpt_topic, json.dumps(rpt_data))
            return True
        except ImportError as ex:
            print("Send data error ", ex)
        return None


    def send_msg_to_broker(self, data):
        '''def send_msg_to_broker():'''
        print(f"Sending ACK to Core {data}")
        time.sleep(20)
        ack_topic = self._pub_ack
        self.publish_to_iot_core_v2(ack_topic, json.dumps(data))
        return True

    def get_base_url(self, cpid, env):
        '''def get_base_url():'''
        base_url = "/api/v2.1/dsdk/cpid/" + cpid + "/env/" + env + "?pf=aws"
        base_url = Discovery_url + base_url
        print(base_url)
        with urllib.urlopen(base_url) as response:
            res = response.read().decode("utf-8")
        print(res)
        data = json.loads(res)
        return data['d']["bu"], data['d']["pf"], data['d']["dip"]

    def get_call(self, url, uniqueid):
        '''def get_call():'''
        url = url + "/uid/" + uniqueid
        with urllib.urlopen(url) as response:
            res = response.read().decode("utf-8")
        data = json.loads(res)
        return data

    def process_sync(self, base_url, uniqueid):
        '''def process_sync():'''
        try:
            response = self.get_call(base_url, uniqueid)
            if self.has_key(response, "d"):
                response = response["d"]
                print('[INFO_IN01] '+'[' + str(self._sid)+'_' + str(self._uniqueid) +
                      "] Device information received successfully: " + self._timestamp, 0)
            else:
                print('[error01] '+'[' + str(self._sid)+'_' + str(self._uniqueid) +
                      "] Device information no received : " + self._timestamp, 0)

            self._data_json = response
            self.init_protocol()

            if self.has_key(self._data_json, "has") and self._data_json["has"]["attr"]:
                self._hello_handsake({"mt": 201})
            if self.has_key(self._data_json, "has") and self._data_json["has"]["d"]:
                self._hello_handsake({"mt": 204})

        except ImportError as ex:
            print("sync call... ", ex)

    def __init__(self, uniqueid, sid, cpid, env):

        self._sid = sid
        self._cpid = cpid
        self._env = env
        self._uniqueid = uniqueid

        self._base_url, self._pf, self._dip = self.get_base_url(cpid, env)
        print(self._pf)
        if self._base_url is not None:
            print('[INFO_IN07] '+'[' + str(self._sid or cpid)+'_' + str(self._uniqueid) +
                  "] BaseUrl received to sync the device information: " + self._timestamp, 0)
            self.process_sync(self._base_url, self._uniqueid)


class SubHandler(client.SubscribeToIoTCoreStreamHandler):
    '''class SubHandler'''

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        try:
            message = str(event.message.payload, "utf-8")
            print("payload received from client dev :", event.message.payload)
            topic_name = event.message.topic_name
            print("payload topic from client dev :", topic_name)
            # Handle message.
            jsonmsg = json.loads(message)
            SDK.on_message(jsonmsg)
        except ImportError:
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        return True

    def on_stream_closed(self) -> None:
        pass


class StreamHandler(client.SubscribeToTopicStreamHandler):
    '''class StreamHandler'''

    def on_stream_event(self, event: SubscriptionResponseMessage) -> None:
        try:
            data = str(event.binary_message.message, "utf-8")
            print(f"Local Topic :: {event.binary_message.context.topic}")
            print(f"Received message :: {data}")
            SDK.send_data_to_sdk2(data)
        except ImportError:
            print("Error in receiving stream event message.....")
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        print("ON STREAM ERROR :: ", str(error))
        return True

    def on_stream_closed(self) -> None:
        print("Subscribe to topic stream closed.")


def sub_local_topic():
    '''def sub_local_topic():'''
    try:
        handler = StreamHandler()
        ipc_client_v2.subscribe_to_topic(topic=LOCAL_SUBTOPIC,
                            on_stream_event=handler.on_stream_event,
                            on_stream_error=handler.on_stream_error,
                            on_stream_closed=handler.on_stream_closed)
        print(f"Successfully Subscribed to Local topic: {LOCAL_SUBTOPIC}")

    except UnauthorizedError:
        print(f"Unauthorized error while subscribing to topic: {LOCAL_SUBTOPIC}")
        traceback.print_exc()

    except ImportError:
        print("Exception occurred", file=sys.stderr)
        traceback.print_exc()

def main():
    '''main'''
    global SID, cpid, env, UniqueId, SDK

    sub_local_topic()

    # Init SDK
    SDK = IoTConnectSDK(UniqueId, SID, cpid, env)
    while True:
        print("IoTConnectSDK Live...")
        time.sleep(5)

if __name__ == "__main__":
    main()
