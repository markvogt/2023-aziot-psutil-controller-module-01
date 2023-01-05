# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import asyncio
import sys
import signal
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device.aio import IoTHubDeviceClient
# IMPORT psutil in order to access SYSTEM PROPERITES (these make for a COOL demo with REAL time-varying DATA)...
import psutil
# IMPORT os in order to be able to read-in "environment variables" configured up in AzureIoTHub during creation a device's Deployment Plan...
import os
# IMPORT time in order to add TIMESTAMPS (handy!) onto module logging entries...
from datetime import datetime

# INITIALIZE a few global variables (in some situations this is preferred over passing them as parameters)...
# SET a default DeviceID in case can't get one from AzureIoTHub (this will ensure the module at least WORKS)...
DEFAULT_IOTHUB_DEVICE_CONNECTION_STRING = "HostName=iot-2022-VOGTLAND-demo-01.azure-devices.net;DeviceId=ED-60542-VM-COYOTE-02;SharedAccessKey=CqkGX2z3UjgFiWcWYSZSNevtJBm6QHkDzRZkbmCGtuo="


# Event indicating client stop
stop_event = threading.Event()


def create_IoTHubModuleClient(_IOTHUB_DEVICE_CONNECTION_STRING):
    print(f"ENTERING create_IoTHubModuleClient()... ")
    print(f"IN create_IoTHubModuleClient() > _IOTHUB_DEVICE_CONNECTION_STRING = {_IOTHUB_DEVICE_CONNECTION_STRING}... ")
    # SET this Device-to-IoT ConnectionString to the value of this VM-as-AzIoTEdge Device... 
    try:
        # INSTANTIATE an IoTHubModule "client" for interacting with the on-device HubModule which acts as a "proxy" to the actual IoTHub...
        # newIoTHubModuleClient = IoTHubModuleClient.create_from_edge_environment()
        newIoTHubModuleClient = IoTHubModuleClient.create_from_connection_string(_IOTHUB_DEVICE_CONNECTION_STRING)
        print(f"IN create_IoTHubModuleClient() > AZIOTE Device 'newIoTHubModuleClient' SUCCESSFULLY created... ")
    except Exception as ex: 
        print(f"IN create_IoTHubModuleClient() > ERROR creating newIoTHubModuleClient > ex = {ex}")

    # Define function for handling received messages
    async def receive_message_handler(message):
        # NOTE: This function only handles messages sent to "input1".
        # Messages sent to other inputs, or to the default, will be discarded
        if message.input_name == "input1":
            print("IN receive_message_handler() > MV custom module module_Controller...")
            print("IN receive_message_handler() > the data in the C2D message received on input1 was ")
            print(message.data)
            print("IN receive_message_handler() > custom properties are")
            print(message.custom_properties)
            print("IN receive_message_handler() > forwarding as D2C message to output1...")
            #await newIoTHubModuleClient.send_message_to_output(message, "output1")
            await newIoTHubModuleClient.send_message(message)

    try:
        # Set handler on the newIoTHubModuleClient
        newIoTHubModuleClient.on_message_received = receive_message_handler
    except Exception as ex:
        print(f"IN create_IoTHubModuleClient() > ERROR assigning receive_message_handler() to on_message_received > ex = {ex}... ")
        print(f"IN create_IoTHubModuleClient() > CLEANING UP AFTER ERROR...") 
        # Cleanup if failure occurs
        newIoTHubModuleClient.shutdown()
        raise
    print(f"IN create_IoTHubModuleClient() RETURNING a 'newIoTHubModuleClient' object...")
    print(f"EXITING create_IoTHubModuleClient()... ")
    return newIoTHubModuleClient


async def run_CustomCode(someIoTHubModuleClient, someMESSAGE_COUNT):
    # INSTRUCTIONS: CUSTOMIZE this co-routine ("co-routine"??) to do whatever tasks this custom modules is SUPPOSED to be doing...
    # EXAMPLE: the code below generates 100 messages (1 per 10 seconds) and sends them to IotHubModule (proxy)...
    # IMPORTANT TIP! when developing SAMPLE code it's WISE to NOT create loops that INFINITELY send messages - costs will HUGE!
    print(f"ENTERING run_CustomCode()...")
    # INITIALIZE a limit for number of mock messages to send back to IoT Hub... 
    MESSAGE_COUNT = someMESSAGE_COUNT
    # LAUNCH a LOOP that generates 100 test messages and sends them back to the IoT Hub... 
    for i in range(1, MESSAGE_COUNT+1):
        # CREATE a TIMESTAMP to add to the both the D2C message (which also ends up in the MODULE LOG as well!)...
        strTimeStamp = datetime.now()
        # READ-IN a current system metric from psutil...
        intCPUCount = psutil.cpu_count()
        oBatteryStats = psutil.sensors_battery()
        # COMPOSE the message (actually just a test string)...
        message = f"{strTimeStamp} > IN run_CustomCode() > i = {str(i)} > strCPUCount = {intCPUCount} > dictBatteryStats = {oBatteryStats}..."
        # SEND the message to IoT Hub...
        await someIoTHubModuleClient.send_message_to_output(message, "output1")
        print(f"{message}")
        # PAUSE (1000 ms = 1 s)...
        await asyncio.sleep(1)
    print(f"EXITING run_CustomCode().")


def main():
    print(f"ENTERING main()...")
    # EXTRACT an ENVIRONMENT VARIABLES configured in AzureIoTHub when the module was added to the Deployment Plan for an AzureIoTEdge device...
    try:
        _IOTHUB_DEVICE_CONNECTION_STRING = os.environ["IOTHUB_DEVICE_CONNECTION_STRING"]
        print (f"IN main() > retrieving module Environment Variable _IOTHUB_DEVICE_CONNECTION_STRING = {_IOTHUB_DEVICE_CONNECTION_STRING}...")
        _MESSAGE_COUNT = os.environ["MESSAGE_COUNT"]
        print (f"IN main() > retrieving module Environment Variable _MESSAGE_COUNT = {str(_MESSAGE_COUNT)}...")

    except Exception as ex: 
        print (f"IN main() > ERROR retrieving Environment Variable DeviceID > ex = {ex}...")
        print (f"IN main() > DEFAULTING to _IOTHUB_DEVICE_CONNECTION_STRING = {DEFAULT_IOTHUB_DEVICE_CONNECTION_STRING}...")
        _IOTHUB_DEVICE_CONNECTION_STRING = DEFAULT_IOTHUB_DEVICE_CONNECTION_STRING
        print (f"IN main() > DEFAULTING to MESSAGE_COUNT = 20...")
        _MESSAGE_COUNT = 20
        
    # INSTANTIATE an IoTHubModuleClient which enables THIS custom module to listen-from and talk-to the  on-device HubModule...
    # NOTE: the on-device HubModule then acts as a "proxy" back up to the actual IoTHub !
    print (f"IN main() > CALLING create_IoTHubModuleClient()...")
    try:
        _IoTHubModuleClient = create_IoTHubModuleClient(_IOTHUB_DEVICE_CONNECTION_STRING)

    except Exception as ex: 
        print (f"IN main() > ERROR calling create_IoTHubModuleClient() > EXCEPTION = {ex}")

    # Define a handler to cleanup when module is is terminated by Edge
    def module_termination_handler(signal, frame):
        print (f"IN main() > module_termination_handler() > IoTHubClient sample stopped by Edge")
        stop_event.set()

    # Set the Edge termination handler...
    signal.signal(signal.SIGTERM, module_termination_handler)

    # INSTANTIATE an asyncrhonous ENDLESS LOP that will then begin executing the CustomCode in this custom module...
    print(f"IN main() > STARTING loop to run run_CustomCode()...")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_CustomCode(_IoTHubModuleClient, _MESSAGE_COUNT))
    except Exception as ex:
        print(f"IN main() > ERROR with creating loop > EXCEPTION = {ex}")
        raise
    finally:
        print(f"IN main() > Shutting down IoT Hub _IoTHubModuleClient...")
        loop.run_until_complete(_IoTHubModuleClient.shutdown())
        loop.close()
        
    print(f"EXITING main().")

if __name__ == "__main__":
    main()
