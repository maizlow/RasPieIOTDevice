import variables

import threading
import time
import json
import sys
from awscrt import mqtt
from awsiot import mqtt_connection_builder

mqtt_connection = None
received_count = 0
received_all_event = threading.Event()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
# def on_message_received(topic, payload, dup, qos, retain, **kwargs):
#     print("Received message from topic '{}': {}".format(topic, json.loads(payload)))
#     # global received_count
#     # received_count += 1
#     # if received_count == 0:
#     #     received_all_event.set()
#     return json.loads(payload)


# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))


# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailuredata)
    print("Connection failed with error code: {}".format(callback_data.error))


# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

async def connect_mqtt():
    # Create a MQTT connection
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=variables.ENDPOINT,
        port=8883,
        cert_filepath=variables.CERT_FILEPATH,
        pri_key_filepath=variables.PRI_KEY_FILEPATH,
        ca_filepath=variables.CA_FILEPATH,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=variables.CLIENT_ID,
        clean_session=False,
        keep_alive_secs=60,
        http_proxy_options=None,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    test = 1
    print (test)
    
    print("Connecting to endpoint with client ID")
    connect_future = connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")
    return connection

async def disconnect_mqtt(connection):
    # Disconnect
    print("Disconnecting...")
    disconnect_future = connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

async def subscribe_to_topic(connection, topic, callback_on_message_recieved):
    # Subscribe
    print("Subscribing to topic '{}'...".format(topic))
    subscribe_future, packet_id = connection.subscribe(
        topic=topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=callback_on_message_recieved)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

async def publish_to_topic(connection, topic, payload):
    payload_json = json.dumps(payload)
    print("Publish to topic: " + topic + " with payload: " + payload_json)
    connection.publish(
        topic=topic,
        payload=payload_json,
        qos=mqtt.QoS.AT_LEAST_ONCE)



"""
if __name__ == '__main__':
    

    # Get subscription topics from local DB


    

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    if message_string:
        if message_count == 0:
            print("Sending messages until program killed")
        else:
            print("Sending {} message(s)".format(message_count))

        publish_count = 1
        while (publish_count <= message_count) or (message_count == 0):
            message = "{} [{}]".format(message_string, publish_count)
            print("Publishing message to topic '{}': {}".format(message_topic, message))
            message_json = json.dumps(message)
            mqtt_connection.publish(
                topic=message_topic,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)
            time.sleep(1)
            publish_count += 1

    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    if message_count != 0 and not received_all_event.is_set():
        print("Waiting for all messages to be received...")

    received_all_event.wait()
    print("{} message(s) received.".format(received_count))

    
    """