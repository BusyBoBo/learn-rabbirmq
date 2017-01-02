###############################################
# RabbitMQ in Action
# Chapter 1 - Hello World Consumer
# 
# Requires: pika >= 0.9.5
# 
# Author: Jason J. W. Williams
# (C)2011
###############################################

import pika

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials=credentials)

# /(hwc.1) Establish connection to broker
conn_broker = pika.BlockingConnection(conn_params)

# /(hwc.2) Obtain channel
channel = conn_broker.channel()

# /(hwc.3) Declare the exchange
channel.exchange_declare(exchange="hello-exchange",
                         type="direct",
                         passive=False,
                         durable=True,
                         auto_delete=False)

# /(hwc.4) Declare the queue
channel.queue_declare(queue="hello-queue")

# /(hwc.5) Bind the queue and exchange together on the key "hola"
channel.queue_bind(queue="hello-queue",
                   exchange="hello-exchange",
                   routing_key="hola")


# /(hwc.6) Make function to process incoming messages
def msg_consumer(channel, method, header, body):
    # /(hwc.7) Message acknowledgement
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if body == "quit":
        # /(hwc.8) Stop consuming more messages and quit
        channel.basic_cancel(consumer_tag="hello-consumer")
        channel.stop_consuming()
    else:
        print body

    return

# /(hwc.9) Subscribe our consumer
channel.basic_consume(msg_consumer,
                      queue="hello-queue",
                      consumer_tag="hello-consumer")
# /(hwc.10) Start consuming
channel.start_consuming()
