# -*- coding: utf-8 -*-

# Hello World Producer
# Requires: pika >= 0.10.0
# Author: knitmesh

import pika
import sys

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials=credentials)
# 1. 建立连接到代理
conn_broker = pika.BlockingConnection(conn_params)

# 2. 获取信道
channel = conn_broker.channel()

# /(hwp.3) Declare the exchange
channel.exchange_declare(exchange="hello-exchange",
                         type="direct",
                         passive=False,
                         durable=True,
                         auto_delete=False)

msg = sys.argv[1]
msg_props = pika.BasicProperties()

# /(hwp.4) Create a plaintext message
msg_props.content_type = "text/plain"

# /(hwp.5) Publish the message
channel.basic_publish(body=msg,
                      exchange="hello-exchange",
                      properties=msg_props,
                      routing_key="hola")
