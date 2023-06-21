import paho.mqtt.client as mqtt  # import client library
import time
import json
import random


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Se ha conectado")
        client.connected_flag = True
    else:
        print("Bad connection Returned code=", rc)
        client.bad_connection_flag = True


def on_subscribe(client, userdata, mid, granted_qos):
    print("Suscripciones satisfactorias")


def on_message(client, userdata, message):
    global message_received
    time.sleep(1)
    message_received = str(message.payload.decode("utf-8"))
    msg = json.loads(message_received)
    print(type(msg["params"]))
    print("received message =", message_received)
    if msg['method'] == "setValue":
        datos = {
            "Humedad": msg["params"]
        }
        serial = json.dumps(datos)
        client.publish("v1/devices/me/telemetry", serial, qos=1)
    if msg['method'] == "setValue2":
        datos = {
            "value": msg["params"]
        }
        serial = json.dumps(datos)
        client.publish("v1/devices/me/telemetry", serial, qos=1)


mqtt.Client.connected_flag = False
mqtt.Client.bad_connection_flag = False

# Parametros A Utilizar

host = "127.0.0.1"
usuario = "fede"
pw = "123"
id = "admin"
topic_rpc = "v1/devices/me/rpc/request/+"
topic_telemetry = "v1/devices/me/telemetry"


client = mqtt.Client(id)  # Creamos el Objeto
client.username_pw_set(username=usuario, password=pw)
client.on_connect = on_connect  # Le Asignamos el callback de conexion
client.on_subscribe = on_subscribe
client.on_message = on_message
client.loop_start()  # Empieza el Loop
print("Conectandose al Host:", host)
client.connect(host)  # Conexion al Broker
while not client.connected_flag and not client.bad_connection_flag:  # wait in loop
    print("Esperando la Conexion...")
    time.sleep(1)
print("Estamos listos para operar")
print("Suscribiendome a los topics asignados...")
client.subscribe(topic_rpc, 1)
time.sleep(60)
client.loop_stop()
