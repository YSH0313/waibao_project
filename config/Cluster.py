import sys
import oss2
import redis
import json
import time
import hashlib
import logging
import pymysql
import threading
import dateparser
from items import *
from threading import Lock
from kafka import KafkaProducer
from datetime import datetime, date
from confluent_kafka import Producer
from rediscluster import RedisCluster
from config.spider_log import SpiderLog
from config.Stream import File_Type
from asyncio_config.my_Requests import MyRequests, MyFormRequests
from config.settings import REDIS_HOST_LISTS, Mysql, redis_connection, IS_INSERT, OTHER_DB, OTHER_Mysql, kafka_servers, kafka_connection, access_key_id, access_key_secret, bucket_name, endpoint


lock = threading.Lock() #创建锁

logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("root").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("oss2").setLevel(logging.WARNING)

class ExpandJsonEncoder(json.JSONEncoder):
    '''
    采用json方式序列化传入的任务参数，而原生的json.dumps()方法不支持datetime、date，这里做了扩展
    '''
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

class Cluster(SpiderLog):
    name = None
    def __init__(self, key='', custom_settings=None):
        self.db_success_count = 0
        self.ka_success_count = 0
        self.catch_count = 0
        self.right_count = 0
        self.error_count = 0
        self.success_code_count = 0
        self.request_count = 0
        self.wrong_count = 0
        self.fangqi_count = 0
        self.exc_count = 0
        self.other_count = 0
        self.startup_nodes = REDIS_HOST_LISTS
        self.key = 'ysh_' + key
        SpiderLog.__init__(self, custom_settings=custom_settings)
        if custom_settings:
            for varName, value in custom_settings.items():
                s = globals().get(varName)
                if s:
                    globals()[varName] = value
        self.logger.name = logging.getLogger(__name__).name
        if redis_connection:
            if len(REDIS_HOST_LISTS) == 1:
                for k, v in REDIS_HOST_LISTS[0].items():
                    self.pool = redis.ConnectionPool(host=k, port=v, db=1, decode_responses=True)
                    self.r = redis.Redis(connection_pool=self.pool)
            elif len(REDIS_HOST_LISTS) > 1:
                self.r = RedisCluster(startup_nodes=self.startup_nodes, decode_responses=True)
        if IS_INSERT:
            self.db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
            self.cursor = self.db.cursor()
            self.start_db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
            self.start_cursor = self.start_db.cursor()
            self.update_db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
            self.update_cursor = self.update_db.cursor()
        if OTHER_DB:
            self.other_db = pymysql.connect(host=OTHER_Mysql['MYSQL_HOST'], user=OTHER_Mysql['MYSQL_USER'], password=OTHER_Mysql['MYSQL_PASSWORD'], port=OTHER_Mysql['PORT'], db=OTHER_Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
            self.other_cursor = self.other_db.cursor()
            self.consumer_db = pymysql.connect(host=OTHER_Mysql['MYSQL_HOST'], user=OTHER_Mysql['MYSQL_USER'], password=OTHER_Mysql['MYSQL_PASSWORD'], port=OTHER_Mysql['PORT'], db=OTHER_Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
            self.consumer_cursor = self.consumer_db.cursor()
        if kafka_connection:
            # self.producer = Producer({
            #     'bootstrap.servers': kafka_servers['server'],
            #     'security.protocol': 'SASL_PLAINTEXT',
            #     'sasl.mechanism': "SCRAM-SHA-512",
            #     'sasl.username': kafka_servers['username'],
            #     'sasl.password': kafka_servers['password'],
            #     'message.max.bytes': 20000000
            # })
            self.producer = KafkaProducer(bootstrap_servers=kafka_servers['server'])

    def is_json(self, myjson):
        if isinstance(myjson, dict):
            return True
        if '{' not in str(myjson):
            return False
        try:
            json.loads(myjson)
        except (ValueError, TypeError) as e:
            return False
        return True

    def get_len(self, key):
        keys = self.get_keys(key)
        # 每个键的任务数量
        key_len = [(k, self.r.llen(k)) for k in keys]
        # 所有键的任务数量
        task_len = sum(dict(key_len).values())
        return task_len, key_len

    def get_keys(self, key):
        # Redis的键支持模式匹配
        keys = self.r.keys(key + '-[0-9]*')
        # 按优先级将键降序排序
        keys = sorted(keys, key=lambda x: int(x.split('-')[-1]), reverse=True)
        return keys

    def push_task(self, tasks, key=None, level=0):
        '''
        双端队列，左边推进任务
        :param level: 优先级(int类型)，数值越大优先级越高，默认1
        :return: 任务队列任务数量
        '''
        # 重新定义优先队列的key
        if key == None:
            key = self.key
        new_key = key + '-' + str(level)
        # 序列化任务参数
        if (isinstance(tasks, MyFormRequests)) or (isinstance(tasks, MyRequests)):
            mess_demo = {}
            for k, v in tasks.__dict__.items():
                if (k == 'callback') and (v != None):
                    if (isinstance(v, str)):
                        mess_demo[k] = v
                    else:
                        fun_name = v.__name__
                        mess_demo['callback'] = fun_name
                else:
                    mess_demo[k] = v
            tasks = json.dumps(mess_demo, cls=ExpandJsonEncoder)
            self.r.lpush(new_key, tasks)
        elif isinstance(tasks, dict):
            tasks = json.dumps(tasks, cls=ExpandJsonEncoder)
            self.r.lpush(new_key, tasks)
        else:
            self.r.lpush(new_key, tasks)

    def get_bucket(self):
        return oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    def oss_push_img(self, url, data, suffix=''):
        """
        :param url: 详情页链接
        :param data: 文件二进制数据流
        return: 对外能访问的图片URL
        """
        oss_bucket = self.get_bucket()
        stream = File_Type.stream_type(data)
        if not stream:
            suffix_list = ['.doc', '.docx', '.xlr', '.xls', '.xlsx', '.pdf', '.txt', '.jpg', '.png', '.rar', '.zip']
            for i in suffix_list:
                if url.endswith(i):
                    suffix = i
            if not suffix.startswith('.'):
                suffix = '.' + suffix
        else:
            suffix = '.' + stream
        url_md5 = hashlib.sha1(url.encode()).hexdigest()+suffix
        oss_bucket.put_object(url_md5, data)
        oss_url = f'https://bid.snapshot.qudaobao.com.cn/{url_md5}'
        return oss_url

    def is_valid_date(self, strdate):
        '''判断是否是一个有效的日期字符串'''
        try:
            if ":" in strdate:
                time.strptime(strdate, "%Y-%m-%d %H:%M:%S")
            else:
                time.strptime(strdate, "%Y-%m-%d")
            return True
        except:
            return False

    def date_refix(self, str_date):
        if str_date == '空':
            return str_date
        elif str_date:
            try:
                new_date = dateparser.parse(str_date).strftime('%Y-%m-%d %H:%M:%S')
                return new_date
            except AttributeError:
                from dateutil.parser import parse
                new_date = parse(str_date)
                return str(new_date)
            except:
                return None
        else:
            return None

    def key_judge(self, item):
        key_list = ['title', 'url', 'pub_time', 'source', 'html']
        for k in key_list:
            sgin = item.__contains__(k)
            while not sgin:
                return False
        return True

    def value_judge(self, item):
        key_list = ['title', 'url', 'pub_time', 'source', 'html']
        for k in key_list:
            sgin = item.get(k, 0)
            while not sgin:
                return k
        return True

    def data_test(self, item):
        if isinstance(item, BiddingItem):
            item = item.dict()
        item['pub_time'] = self.date_refix(item.get('pub_time'))
        key_judge = self.key_judge(item)
        value_judge = self.value_judge(item)
        if key_judge == True and value_judge == True:
            self.prints(item, is_replace=False)
            self.right_count += 1
        else:
            self.logger.debug(f'\033[5;31;1m{value_judge} \033[5;33;1mfield does not exist, Data validation failed, please check！\033[0m {item}')
            self.error_count += 1
        self.catch_count += 1

    def kafka_producer(self, item):
        if isinstance(item, BiddingItem):
            item = item.dict()
        item['pub_time'] = self.date_refix(item.get('pub_time'))
        key_judge = self.key_judge(item)
        value_judge = self.value_judge(item)
        if key_judge == True and value_judge == True:
            self.producer.send(kafka_servers['topic'], json.dumps(item).encode('utf-8'))
            self.prints(item, is_replace=False, db='kafka')
            self.add_url_sha1(item['url']) if not item.get('show_url') else (self.add_url_sha1(item['url']), self.add_url_sha1(item.get('show_url')))
            self.right_count += 1
        else:
            self.logger.debug(f'\033[5;31;1m{value_judge} \033[5;33;1mfield does not exist, Data validation failed, please check！\033[0m {item}')
            self.error_count += 1
        self.catch_count += 1

    # def kafka_producer(self, topic, item, table, key=None):
    #     try:
    #         item['table'] = table
    #         item['producer_name'] = kafka_servers['producer_name']
    #         item['producer_ip'] = kafka_servers['producer_ip']
    #         self.producer.produce(topic, json.dumps(item, ensure_ascii=False).encode(), key=key)
    #         # self.producer.flush()
    #         self.producer.poll(timeout=10)
    #         # return json.dumps(dict(item), ensure_ascii=False)
    #         self.prints(item)
    #     except Exception as e:
    #         raise e

    def re_connet(self, if_other_db=False):
        try:
            if if_other_db:
                self.other_db = pymysql.connect(host=OTHER_Mysql['MYSQL_HOST'], user=OTHER_Mysql['MYSQL_USER'], password=OTHER_Mysql['MYSQL_PASSWORD'], port=OTHER_Mysql['PORT'], db=OTHER_Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
                self.other_cursor = self.other_db.cursor()
                self.consumer_db = pymysql.connect(host=OTHER_Mysql['MYSQL_HOST'], user=OTHER_Mysql['MYSQL_USER'], password=OTHER_Mysql['MYSQL_PASSWORD'], port=OTHER_Mysql['PORT'], db=OTHER_Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
                self.consumer_cursor = self.consumer_db.cursor()
            else:
                self.db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
                self.cursor = self.db.cursor()
                self.start_db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
                self.start_cursor = self.start_db.cursor()
                self.update_db = pymysql.connect(host=Mysql['MYSQL_HOST'], user=Mysql['MYSQL_USER'], password=Mysql['MYSQL_PASSWORD'], port=Mysql['PORT'], db=Mysql['MYSQL_DBNAME'], charset='utf8', use_unicode=True)
                self.update_cursor = self.update_db.cursor()
        except Exception as e:
            self.logger.error('MySQL major error', exc_info=e)

    def data_deal(self, data):  # 一般数据处理
        if (data == None) or (data == ''):
            data_last = ''
            return data_last
        elif isinstance(data, dict):
            return json.loads(data)
        else:
            sin_list = ['\r', '\n', '\xa0', '\u3000', '\\u3000', '\t', ' ', '&nbsp;', '\\r', '\\n', ',,', '\\', '\\\\', '\ufeff']
            for i in sin_list:
                data = str(data).replace(i, '')
            start_with = ['、']
            for s in start_with:
                if data.startswith(s):
                    data = str(data).replace(s, '')
            return data

    def delete_sql(self, table, where, db_name=None, OTHER_INSERT=False):
        sql = f"""DELETE FROM `{db_name}`.`{table}` WHERE {where};"""
        while 1:
            if OTHER_INSERT:
                try:
                    self.other_cursor.execute(sql)
                    self.other_db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    self.cursor.execute(sql)
                    self.db.commit()
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)
                break
        self.logger.info('删除数据：'+str(sql))

    def trucate_sql(self, table, db_name=None, OTHER_INSERT=False):
        sql = """TRUNCATE `{db_name}`.`{table_name}`""".format(db_name=db_name, table_name=table)
        while 1:
            if OTHER_INSERT:
                try:
                    self.other_cursor.execute(sql)
                    self.other_db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    self.cursor.execute(sql)
                    self.db.commit()
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)
                break

    def get_columns(self, db_name, table_name, OTHER_INSERT=False):
        sql = f"""SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_schema = '{db_name}' AND table_name = '{table_name}'""".format(db_name=db_name, table_name=table_name)
        self.logger.info('查询字段名称：'+sql)
        self.logger.info('===========================================================')
        while 1:
            if OTHER_INSERT:
                try:
                    self.other_cursor.execute(sql)
                    self.other_db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    funcName = sys._getframe().f_back.f_code.co_name
                    if funcName == 'start_requests':
                        self.start_cursor.execute(sql)
                        self.start_db.commit()
                        all_colums = self.start_cursor.fetchall()
                    else:
                        self.cursor.execute(sql)
                        self.db.commit()
                        all_colums = self.cursor.fetchall()
                    field_lists = []
                    for i in all_colums:
                        field_lists.append(i[0])
                    return field_lists
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)
                break

    def prints(self, item, is_replace=True, db=None, is_info=True):
        item_last = {}
        for k, v in item.items():
            if (v == None) or (v == 'None') or (v == ''):
                continue
            elif (('time' not in k) and ('Time' not in k) and ('updated' not in k) and ('date' not in k)) and (isinstance(v, dict) == False):
                if is_replace:
                    item_last[k] = self.data_deal(v)
                else:
                    item_last[k] = v
            elif isinstance(v, dict):
                item_last[k] = str(v)
            elif k == 'body':
                item_last[k] = """{body}""".format(body=v)
            elif ('time' in k) or ('Time' in k) or ('updated' in k) or ('date' in k) or (k.endswith('T')==True):
                if self.is_valid_date(v):
                    item_last[k] = self.date_refix(v)
                else:
                    item_last[k] = v
        if is_info:
            # print(json.dumps(item_last, indent=2, ensure_ascii=False))
            self.logger.info('\r\n{item}'.format(item=json.dumps(item_last, indent=2, ensure_ascii=False)))
            self.logger.info('===========================================================')
        if db == 'kafka':
            self.ka_success_count += 1
        elif db == 'mysql':
            self.db_success_count += 1
        return item_last

    def add_url_sha1(self, url):
        url_sha1 = self.url2sha1(url)
        t_num = str(int(url_sha1[-2:], 16) % 16)
        # sql = f'insert ignore into t_bidding_filter_{t_num} (`url_sha1`) value (%s)'
        self.insert(item={'url_sha1': url_sha1}, table='t_bidding_filter_{t_num}'.format(t_num=t_num), db_name='spider_frame', is_replace=False)

    def url2sha1(self, url):
        import hashlib
        url_sha1 = hashlib.sha1(url.encode()).hexdigest()
        return url_sha1

    def insert(self, item, table, db_name=None, OTHER_INSERT=False, is_update=False, is_replace=True, is_info=True):
        item = self.prints(item, is_replace=is_replace, db='mysql', is_info=is_info)
        field_lists = []
        value_lists = []
        field_num = []
        for k, v in dict(item).items():
            if (v == None) or (v == 'None') or (v == ''):
                continue
            elif ('{' and '}') in str(v):
                field_lists.append("`" + str(k) + "`")
                value_lists.append(str(v))
            else:
                field_lists.append("`" + str(k) + "`")
                value_lists.append(pymysql.converters.escape_string(str(v)))
        [field_num.append('%s') for i in range(1, len(field_lists) + 1)]
        if db_name and is_update:
            new_values = []
            for i in value_lists:
                if i.isdigit():
                    new_values.append(i)
                else:
                    new_values.append("'"+i+"'")
            sql = """INSERT INTO `{db_name}`.`{table_name}`({fields}) VALUES({value_lists}) ON DUPLICATE KEY UPDATE {update_lists}""".format(db_name=db_name, table_name=table, fields=','.join(field_lists), value_lists=','.join(new_values), update_lists=','.join(self.update_item(item, is_insert=True, is_replace=is_replace)))
        elif db_name and is_update==False:
            sql = """INSERT IGNORE INTO `{db_name}`.`{table}` ({fields}) VALUES ({fields_num});""".format(db_name=db_name, table=table, fields=','.join(field_lists), fields_num=','.join(field_num))
        else:
            sql = """INSERT IGNORE INTO `{db_name}`.`{table}` ({fields}) VALUES ({fields_num});""".format(db_name=Mysql['MYSQL_DBNAME'], table=table, fields=','.join(field_lists), fields_num=','.join(field_num))
        while 1:
            if OTHER_INSERT:
                try:
                    if is_update:
                        self.other_cursor.execute(sql)
                        self.other_db.commit()
                    else:

                        self.other_cursor.execute(sql, tuple(value_lists))
                        self.other_db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    if is_update:
                        self.cursor.execute(sql)
                        self.db.commit()
                    else:
                        self.db.ping(reconnect=True)
                        self.cursor.execute(sql, tuple(value_lists))
                        self.db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)

    def select_data(self, field_lists=None, db_name=None, table=None, condition=None, where=0, num_id=0, min_id=0, max_id=0, cond=None, OTHER_INSERT=False, if_dic=False, other_sql=None, is_info=True):
        field_lists_last = []
        if field_lists:
            for i in field_lists:
                field_lists_last.append("`" + str(i) + "`")
        sq1_all = ''
        if (num_id == 0) and (where==0):
            sq1_all = """SELECT {field_lists} FROM `{db}`.`{table}`;""".format(db=db_name, field_lists=','.join(field_lists_last), table=table)
        if cond != None:
          sq1_all = """SELECT {field_lists} FROM `{db}`.`{table}` {cond};""".format(db=db_name, field_lists=','.join(field_lists_last), table=table, cond=cond)
        if (num_id == 0) and (where!=0):
          sq1_all = """SELECT {field_lists} FROM `{db}`.`{table}` WHERE {where};""".format(db=db_name, field_lists=','.join(field_lists_last), table=table, where=where)
        if num_id != 0:
          sq1_all = """SELECT {field_lists} FROM `{db}`.`{table}` WHERE `wid` = {id};""".format(db=db_name, field_lists=','.join(field_lists_last), table=table, id=num_id)
        if (min_id == 0) and (max_id ==0):
            pass
        if (max_id !=0):
          sq1_all = """SELECT {field_lists} FROM `{db}`.`{table}` WHERE (`id` >= {min}) AND (`id` <= {max});""".format(db=db_name, field_lists=','.join(field_lists_last), table=table, min=min_id, max=max_id)
        if condition:
          sq1_all = """SELECT {condition} {field_lists} FROM `{db}`.`{table}`;""".format(condition=condition, field_lists=','.join(field_lists_last), db=db_name, table=table)
        if condition and cond:
          sq1_all = """SELECT {condition} {field_lists} FROM `{db}`.`{table}` {cond};""".format(condition=condition, field_lists=','.join(field_lists_last), db=db_name, table=table, cond=cond)
        if (field_lists==None) and (condition != None) and not where:
            sq1_all = """SELECT {condition} FROM `{db}`.`{table}`;""".format(condition=condition, db=db_name, table=table)
        if (field_lists==None) and (condition != None) and where:
            sq1_all = """SELECT {condition} FROM `{db}`.`{table}` WHERE {where};""".format(condition=condition, db=db_name, table=table, where=where)
        if other_sql:
            sq1_all = other_sql
        # print('\033[1;31;0m{tt}查询字段：\033[0m'.format(tt=str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))), sq1_all)
        if is_info:
            self.logger.info('查询字段：'+sq1_all)
            self.logger.info('===========================================================')
        # print('===========================================================')
        while 1:
            if OTHER_INSERT:
                try:
                    funcName = sys._getframe().f_back.f_code.co_name
                    if funcName == 'start_requests':
                        self.other_cursor.execute(sq1_all)
                        self.other_db.commit()
                        data_all = self.other_cursor.fetchall()
                    else:
                        self.consumer_cursor.execute(sq1_all)
                        self.consumer_db.commit()
                        data_all = self.consumer_cursor.fetchall()
                    dic_data_all = []
                    if if_dic:
                        for i in data_all:
                            dic_data = json.loads(json.dumps(dict(zip(field_lists, i)), default=str, ensure_ascii=False))
                            dic_data_all.append(dic_data)
                        return dic_data_all
                    return data_all
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    funcName = sys._getframe().f_back.f_code.co_name
                    if funcName == 'start_requests':
                        self.start_cursor.execute(sq1_all)
                        self.start_db.commit()
                        data_all = self.start_cursor.fetchall()
                    else:
                        self.cursor.execute(sq1_all)
                        self.db.commit()
                        data_all = self.cursor.fetchall()
                    dic_data_all = []
                    if if_dic:
                        for i in data_all:
                            dic_data = json.loads(json.dumps(dict(zip(field_lists, i)), default=str, ensure_ascii=False))
                            dic_data_all.append(dic_data)
                        return dic_data_all
                    return data_all
                except (pymysql.err.OperationalError) as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)
                # except:
                #     print(sq1_all)
                #     import traceback
                #     traceback.print_exc()


    def update_item(self, item, is_insert=False, is_replace=True):
        field_lists_last = []
        for k, v in item.items():
            if ('time' in k) or ('Time' in k) or (k.endswith('T') == True):
                data = "`" + str(k) + "`" + "='{values}'".format(values=pymysql.converters.escape_string(str(v)))
            elif str(v).isdigit() and is_insert:
                data = "`" + str(k) + "`" + "={values}".format(values=v)
            elif (v=='') or (v==None):
                data = "`" + str(k) + "`" + "=NULL".format(values=v)
            else:
                if is_replace:
                    data = "`" + str(k) + "`" + "='{values}'".format(values=pymysql.converters.escape_string(str(self.data_deal(v))))
                else:
                    data = "`" + str(k) + "`" + "='{values}'".format(values=pymysql.converters.escape_string(str(v)))
            field_lists_last.append(data)
        # if is_insert == False:
            # print(json.dumps(field_lists_last, indent=2, ensure_ascii=False))
            # self.logger.info('\r\n'+json.dumps(field_lists_last, indent=2, ensure_ascii=False))
        return field_lists_last

    def update_data(self, item, db_name, table, where=0, OTHER_INSERT=False, is_replace=True):
        # print('\033[1;31;0m{tt}更新字段：\033[0m'.format(tt=str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))))
        # self.logger.info('更新字段：')
        item = self.update_item(item, is_replace=is_replace)
        update_sql = """UPDATE `{db}`.`{table_name}` SET {field_lists} WHERE {where}""".format(db=db_name, table_name=table, field_lists=','.join(item), where=where)
        while 1:
            if OTHER_INSERT:
                try:
                    funcName = sys._getframe().f_back.f_code.co_name
                    if funcName == 'start_requests':
                        self.other_cursor.execute(update_sql)
                        self.other_db.commit()
                    else:
                        self.consumer_cursor.execute(update_sql)
                        self.consumer_db.commit()
                    break
                except pymysql.err.OperationalError as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet(if_other_db=True)
                    time.sleep(3)
            else:
                try:
                    funcName = sys._getframe().f_back.f_code.co_name
                    if funcName == 'start_requests':
                        self.start_cursor.execute(update_sql)
                        self.start_db.commit()
                    else:
                        self.update_cursor.execute(update_sql)
                        self.update_db.commit()
                    break
                except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
                    self.logger.debug('Mysql database is reconnecting because：'+str(e))
                    self.re_connet()
                    time.sleep(3)
            # print(update_sql)
            self.logger.info('\r\n' + update_sql)

if __name__ == '__main__':
    cl = Cluster()
    # cl.prints({'a': 1})
    # cl.logger.info('成功了')
    # a = cl.select_data(['wid'], 'court_cpws_ent', 1)
    # print(a)
    # cl.delete_sql(table='ceshi_fasong', db_name='spider_frame', where=f"""url = 'https://bulletin.cebpubservice.com/xxfbcmses/search/bulletin.html?searchDate=1996-05-25&dates=300&categoryId=88&industryName=&area=&status=&publishMedia=&sourceInfo=&showStatus=&word=&startcheckDate=2018-02-08&endcheckDate=2018-02-08&page=34'""")

    data = cl.select_data(condition='*', db_name=Mysql['MYSQL_DBNAME'], table='spiderdetails_info', where="""spider_name='zhaoxun_beidajijian' and start_time like '%2021-07-29%'""")[0]
    print(data)
    # anyou_lists = cl.r.lrange('anyou', 0, -1)
    # lawyer_lists = cl.r.lrange('lawyer', 0, -1)
    # print(anyou_lists)
    # print(lawyer_lists)
