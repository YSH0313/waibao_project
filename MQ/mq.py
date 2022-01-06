# -*- coding: utf-8 -*-
import sys
import pika
import json
import time
import requests
import asyncio_config
from items import BiddingItem
from asyncio_config.my_Requests import MyResponse, MyFormRequests, MyRequests
from config.settings import Rabbitmq, PREFETCH_COUNT, IS_connection, X_MAX_PRIORITY

class Mq(object):
    def __init__(self, queue_name, custom_settings=None):
        if custom_settings:
            for varName, value in custom_settings.items():
                s = globals().get(varName)
                if s:
                    globals()[varName] = value
        if IS_connection == False:
            raise ('请先打开rabbitmq连接权限')
        else:
            self.operating_system = sys.platform
            self.pages = sys.argv[1] if len(sys.argv) > 1 else None
            self.queue_name = (Rabbitmq['Sgin']+'_'+queue_name+'_online' if self.operating_system == 'linux' else Rabbitmq['Sgin']+'_'+queue_name) if not self.pages else (Rabbitmq['Sgin']+'_'+queue_name+'_online_add' if self.operating_system == 'linux' else Rabbitmq['Sgin']+'_'+queue_name + '_add')
            self.vhost_check = '%2F'
            self.s = requests.session()
            self.rabbit_user = Rabbitmq['user']
            self.rabbit_password = Rabbitmq['password']
            self.rabbit_host = Rabbitmq['host']
            self.credentials = pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.credentials, heartbeat=0, socket_timeout=30))
            self.channel = self.connection.channel()

            self.send_credentials = pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)
            self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.send_credentials, heartbeat=0, socket_timeout=30))
            self.send_channel = self.send_connection.channel()
            self.send_channel_count = self.send_channel.queue_declare(queue=self.queue_name, arguments={'x-max-priority': (X_MAX_PRIORITY or 0)}, durable=True)

            self.thread_credentials = pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)
            self.thread_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.thread_credentials, heartbeat=0, socket_timeout=30))
            self.thread_channel = self.thread_connection.channel()
            self.thread_channel_count = self.thread_channel.queue_declare(queue=self.queue_name, arguments={'x-max-priority': (X_MAX_PRIORITY or 0)}, durable=True)

    def is_json(self, myjson):
        if isinstance(myjson, dict):
            return True
        if '{' not in myjson:
            return False
        try:
            json.loads(myjson)
        except ValueError as e:
            return False
        return True

    def obj_json(self, obj):

        return obj.__dict__

    def handle(self, obj):
        bid = BiddingItem()
        for k, v in obj.items():
            if hasattr(bid, k):
                setattr(bid, k, v)
            else:
                return obj
        return bid

    def re_connect_thread(self):
        self.thread_credentials = pika.PlainCredentials(username=self.rabbit_user,password=self.rabbit_password)
        self.thread_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.thread_credentials, heartbeat=0, socket_timeout=30))
        self.thread_channel = self.thread_connection.channel()
        self.thread_channel_count = self.thread_channel.queue_declare(queue=self.queue_name, arguments={'x-max-priority': (X_MAX_PRIORITY or 0)}, durable=True)

    def re_connect_send(self):
        self.send_credentials = pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)
        self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.send_credentials, heartbeat=0, socket_timeout=30))
        self.send_channel = self.send_connection.channel()
        self.send_channel_count = self.send_channel.queue_declare(queue=self.queue_name, arguments={'x-max-priority': (X_MAX_PRIORITY or 0)}, durable=True)

    def send_mqdata(self, mess, level=0, queue_name=None, is_thread=False):  # 生产者
        # 将声明队列持久话,rabbitmq不允许同名队列
        if queue_name != None:
            self.queue_name = queue_name
        if (isinstance(mess, MyFormRequests)) or (isinstance(mess, MyRequests)):
            mess_demo = {}
            for k, v in mess.__dict__.items():
                if (k == 'callback') and (v != None):
                    if (isinstance(v, str)):
                        mess_demo[k] = v
                    else:
                        fun_name = v.__name__
                        mess_demo['callback'] = fun_name
                elif (k == 'meta') and len(v) != 0:
                    for key, value in v.items():
                        if isinstance(value, BiddingItem):
                            mess_demo[k] = dict(v, **{key: json.dumps(value, default=self.obj_json)})
                            break
                        elif not isinstance(value, BiddingItem) and len(v) > 1:
                            mess_demo[k] = v
                        else:
                            mess_demo[k] = {}
                            mess_demo[k][key] = value
                else:
                    mess_demo[k] = v
            mess_last = json.dumps(mess_demo)
            if is_thread:
                while 1:
                    try:
                        self.thread_channel.basic_publish(exchange='',
                                                        routing_key=self.queue_name,
                                                        body=mess_last,
                                                        properties=pika.BasicProperties(priority=mess.level, delivery_mode=2))  # 使消息持久化
                        break
                    except Exception as e:
                        self.re_connect_thread()
            else:
                while 1:
                    try:
                        self.send_channel.basic_publish(exchange='',
                                                   routing_key=self.queue_name,
                                                   body=mess_last,
                                                   properties=pika.BasicProperties(priority=mess.level, delivery_mode=2))  # 使消息持久化
                        break
                    except Exception as e:
                        self.re_connect_send()
            # print('[x] {tt} send %r to %s'.format(tt=str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))%(mess_last, self.queue_name))
        else:
            if is_thread:
                while 1:
                    try:
                        self.thread_channel.basic_publish(exchange='',
                                                   routing_key=self.queue_name,
                                                   body=mess,
                                                   properties=pika.BasicProperties(priority=level, delivery_mode=2))  # 使消息持久化
                        break
                    except Exception as e:
                        self.re_connect_thread()
            else:
                while 1:
                    try:
                        self.send_channel.basic_publish(exchange='',
                                                   routing_key=self.queue_name,
                                                   body=mess,
                                                   properties=pika.BasicProperties(priority=level, delivery_mode=2))  # 使消息持久化
                        break
                    except Exception as e:
                        self.re_connect_send()
            # print('[x] {tt} send %r to %s'.format(tt=str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))%(mess, self.queue_name))
        # self.connection.close()

    def get_mqdata(self, queue_name, callback):  # 消费者
        self.channel.queue_declare(queue=queue_name, durable=True)
        # print('[*] Watting for messages, to exit press CTRL+C.')
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)  # 让rabbitmq不要一次将超过1条消息发送给work
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
        self.channel.start_consuming()

    def getMessageCount(self, queue_name):
        # 返回3种消息数量：ready, unacked, total
        url = 'http://%s:15672/api/queues/%s/%s' % (self.rabbit_host, self.vhost_check, queue_name)
        r = self.s.get(url, auth=(self.rabbit_user, self.rabbit_password))
        if r.status_code != 200:
            # print('队列名为：', queue_name, '的状态码为：', r.status_code, '请视情况决定是否检查!')
            return 0, 0, 0
        dic = json.loads(r.text)
        try:
            return dic['messages']
            # return dic['messages_ready'], dic['messages_unacknowledged'], dic['messages']
        except KeyError:
            return 0

    def delete_queue(self, queue_name):
        # url = f'http://{self.rabbit_host}:15672/api/queues/%2F/{queue_name}'
        url = f'http://{self.rabbit_host}/api/queues/%2F/{queue_name}'
        data = {"vhost": "/", "name": f"{queue_name}", "mode": "delete"}
        self.s.delete(url=url, json=data, auth=(self.rabbit_user, self.rabbit_password))

if __name__ == '__main__':
    mm = Mq('first_spider')
    # mm.channel.queue_delete(queue='ysh_beizhixing_update_status')
    # a = mm.is_json([123123, "{'asd': 123, 'asda': 'asdasd'}"])
    # print(a)
