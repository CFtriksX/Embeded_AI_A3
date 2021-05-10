import paho.mqtt.client as mqtt
import datetime
import time
import numpy as np

print("creating new instance")
client = mqtt.Client("P1")

broker_address = "test.mosquitto.org"

try:
    client.connect(broker_address)
    client.loop_start()

    client.subscribe("teds20/group09/pressure", qos=2)

    for x in range(10):
        mu, sigma = 1200.00, 1.0
        reading = f'{round(np.random.normal(mu, sigma), 2):.2f}'        
        dt = datetime.datetime.now()
        dt = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        message = f'{reading}|{dt}'

        client.publish("teds20/group09/pressure", message , 2)
        print("Send message", x + 1)
        time.sleep(1)

    client.unsubscribe("teds20/group09/pressure") # unsubscribe

    time.sleep(4)
    client.loop_stop()

    print("\ndisconnecting from broker")
    client.disconnect()
except Exception as e:
    print(f"connection error: {e}")