# -*- coding: utf-8 -*-
import re
import sys

import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import json
import pika
import socket
import aiohttp
import asyncio
import logging
import chardet  # 字符集检测
import pdfminer
import actuator
import async_timeout
import concurrent.futures
from items import *
from yarl import URL
from collections import Iterator
from config.data_deal import Deal
from asyncio_config.th_read import *
# from config.proxys import self.asy_rand_choi_pool
from asyncio_config.my_Requests import MyResponse
from config.settings import PREFETCH_COUNT, TIME_OUT, X_MAX_PRIORITY, Mysql, IS_PROXY, IS_SAMEIP, Asynch, Waiting_time, Delay_time, max_request, Agent_whitelist, retry_http_codes, UA_PROXY, Auto_clear, log_path
from asyncio_config.web_source import Web_source
from asyncio_config.web_peteer import Web_peteer
import nest_asyncio
nest_asyncio.apply()

socket.timeout = TIME_OUT

logging.getLogger("root").setLevel(logging.WARNING)
class Manager(Deal):
    name = None
    custom_settings = {}

    def __init__(self):
        if self.custom_settings:
            Deal.__init__(self, queue_name=self.name, custom_settings=self.custom_settings, class_name='Manager')
            for varName, value in self.custom_settings.items():
                s = globals().get(varName)
                if s:
                    globals()[varName] = value
        else:
            Deal.__init__(self, queue_name=self.name, class_name='Manager')
        self.pages = int(sys.argv[1]) if len(sys.argv) > 1 else None
        self.logger.name = logging.getLogger(__name__).name
        self.num = PREFETCH_COUNT
        self.timeout = TIME_OUT
        self.x_max_priority = X_MAX_PRIORITY
        self.mysql = Mysql
        self.is_proxy = IS_PROXY
        self.is_sameip = IS_SAMEIP
        self.asynch = Asynch
        self.waiting_time = Waiting_time
        self.delay_time = Delay_time
        self.max_request = max_request
        self.new_loop = asyncio.new_event_loop()
        # 定义一个线程，运行一个事件循环对象，用于实时接收新任务
        self.loop_thread = threading.Thread(target=self.start_loop, args=(self.new_loop,))
        self.loop_thread.setDaemon(True)
        self.loop_thread.start()

        self.web_loop = asyncio.new_event_loop()
        # 定义一个线程，运行一个事件循环对象，用于实时接收新任务
        self.loop_thread = threading.Thread(target=self.start_loop, args=(self.web_loop,))
        self.loop_thread.setDaemon(True)
        self.loop_thread.start()

        self.other_loop = asyncio.new_event_loop()
        # 定义一个线程，运行一个事件循环对象，用于实时接收新任务
        self.loop_other = threading.Thread(target=self.start_loop, args=(self.other_loop,))
        self.loop_other.setDaemon(True)
        self.loop_other.start()
        self.charset_code = re.compile(r'charset=(.*?)"|charset=(.*?)>|charset="(.*?)"', re.S)
        self.last_time = time.time()
        self.starttime = None
        # self.sem = asyncio.Semaphore(PREFETCH_COUNT, loop=self.new_loop)

    def start_loop(self, loop):
        self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.credentials, heartbeat=0, socket_timeout=30))
        self.send_channel = self.send_connection.channel()
        # 一个在后台永远运行的事件循环
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def open_spider(self, spider_name):  # 开启spider第一步检查状态
        data = self.select_data(field_lists=['spider_name'], db_name=self.mysql['MYSQL_DBNAME'], table='spiderlist_monitor', where="""spider_name = '{spider_name}'""".format(spider_name=spider_name))
        self.update_data(item={'log_path': log_path+f'/{self.path_name}.log'}, db_name=self.mysql['MYSQL_DBNAME'], table='spiderdetails_info', where=f"""spider_name = '{spider_name}'""")
        if data and self.operating_system == 'linux' and self.pages:
            self.update_data({'is_run': 'yes', 'start_time': self.now_time()}, db_name=self.mysql['MYSQL_DBNAME'], table='spiderlist_monitor', where="""`spider_name` = '{spider_name}'""".format(spider_name=spider_name))
        elif not data:
            self.logger.info('\033[5;36;1mIf you need to turn on increment, please register and try to run again. If not, please ignore it\033[0m')
            self.logger.info('\033[5;36;1mCrawler service startup for {spider_name}\033[0m'.format(spider_name=spider_name))
            # self.logger.info(self.colored_font('If you need to turn on increment, please register and try to run again. If not, please ignore it'))
            # self.logger.info(self.colored_font('Crawler service startup for {spider_name}'.format(spider_name=spider_name)))
        self.logger.info('Crawler program starts')
        return True

    def run(self, close_clear=False):  # 启动spider的入口
        # package = __import__(self.path+ self.queue_name.replace('ysh_', ''), fromlist=['None'])
        # temp_class = getattr(package, self.queue_name.replace('ysh_', ''))
        # self.duixiang = temp_class()
        # self.duixiang = actuator.LoadSpiders()._spiders[spider_name]()
        if Auto_clear and not close_clear:
            self.delete_queue(queue_name=self.queue_name)
            self.logger.info('The queue has been cleared automatically')
        self.starttime = self.now_time()
        self.start_time = time.time()
        # status = self.open_spider(spider_name=self.name)
        status = True
        if status == True:
            # mq_total = self.send_channel_count.method.message_count
            # if mq_total == 0:
            if 'Breakpoint' in self.custom_settings.keys():
                if Asynch:
                    main_thead(fun_lists=[self.start_requests], queue_name=self.name, signal=self.custom_settings['Breakpoint'])
                else:
                    start_th(fun_lists=[self.start_requests], queue_name=self.name, signal=self.custom_settings['Breakpoint'])
            else:
                if Asynch:
                    # main_thead(fun_lists=[self.start_requests], queue_name=self.name, sender=self.send_mqdata, channel=self.send_channel_count, delete_channel=self.thread_channel)
                    main_thead(fun_lists=[self.start_requests], queue_name=self.name)
                else:
                    start_th(fun_lists=[self.start_requests], queue_name=self.name)
            asyncio.run_coroutine_threadsafe(self.shutdown_spider(spider_name=self.name), self.other_loop)
            self.consumer_status = main_thead([self.consumer], queue_name=self.queue_name)
            self.logger.info('Consumer thread open ' + str(self.consumer_status))
        elif status == False:
            return

    async def shutdown_spider(self, spider_name):  # 监控队列及运行状态
        while 1:
            # try:
                now_time = time.time()
                # self.update_data({'num': self.num, 'sgin': '现在', 'updated_time': self.now_time()}, table='ceshi_num', db_name='spider_frame', where="""id = 1""")
                self.logger.debug("It's been " + str(now_time-self.last_time) + ' seconds since the last time I took data from the queue. The remaining number of queues is ' + str(self.getMessageCount(queue_name=self.queue_name)))
                if (now_time-self.last_time >= self.waiting_time) and (self.getMessageCount(queue_name=self.queue_name) == 0):
                    try:
                        self.update_data({'is_run': 'no', 'end_time': self.now_time()}, db_name=self.mysql['MYSQL_DBNAME'], table='spiderlist_monitor', where="""`spider_name` = '{spider_name_demo}'""".format(spider_name_demo=spider_name))
                    except Exception as e:
                        self.logger.error("Crawler closed, abnormal update of running status，Forced closing！", exc_info=True)
                        stop_thread(self.consumer_status)
                        self.connection.close()
                        break
                    self.finished_info(self.starttime, self.start_time)  # 完成时的日志打印
                    # os._exit(0)  # 已淘汰使用
                    stop_thread(self.consumer_status)
                    self.connection.close()
                    break
                time.sleep(self.delay_time)
            # except:
            #     import traceback
            #     traceback.print_exc()

    def consumer(self, queue_name):  # 消费者
        while 1:
            try:
                self.channel.queue_declare(queue=queue_name, arguments={'x-max-priority': (self.x_max_priority or 0)}, durable=True)
                self.channel.basic_qos(prefetch_count=1)  # 让rabbitmq不要一次将超过1条消息发送给work
                self.channel.basic_consume(queue=queue_name, on_message_callback=self.Requests)
                self.channel.start_consuming()
            except Exception as e:
                self.logger.error(repr(e)+'Channel is missing, reconnecting', exc_info=True)
                # self.logger.debug(repr(e)+'Channel is missing, reconnecting')
                self.credentials = pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_host, credentials=self.credentials, heartbeat=0))
                self.channel = self.connection.channel()

    def Requests(self, ch, method, properties, body):  # 消息处理函数
        self.last_time = time.time()
        self.num -= 1
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动发送ack,如果没有发送,队列里的消息将会发给下一个worker
        while self.num < 0:
            self.logger.debug('The request queue is full')
            time.sleep(1)
        flag = self.is_json(body.decode('utf-8'))
        if flag == False:
            fun_lists = self.parse_only(body=body.decode('utf-8'))
            if isinstance(fun_lists, Iterator):
                for p in fun_lists:
                    self.send_mqdata(p)
            self.num += 1
        if (flag == True):
            contents = json.loads(body.decode('utf-8'))
            callback_demo = contents.get('callback')
            is_encode = contents.get('is_encode')
            url = contents.get('url')
            headers = contents.get('headers')
            params = contents.get('params')
            data = contents.get('data')
            json_params = contents.get('json_params')
            timeout = contents.get('timeout')
            is_websource = contents.get('is_websource')
            page_click = contents.get('page_click')
            before_do = contents.get('before_do')
            input_box = contents.get('input_box')
            input_text = contents.get('input_text')
            input_click = contents.get('input_click')
            meta = contents.get('meta')
            for k, v in meta.items():
                if self.is_json(v) and not isinstance(v, dict):
                    meta[k] = json.loads(v, object_hook=self.handle)
            level = contents.get('level')
            proxy = contents.get('proxy')
            meta['proxy'] = proxy if proxy and self.is_sameip else meta.get('proxy')
            verify_ssl = contents.get('verify_ssl')
            allow_redirects = contents.get('allow_redirects')
            is_file = contents.get('is_file')
            retry_count = 0 if contents.get('retry_count') == None else contents.get('retry_count')
            is_change = contents.get('is_change')
            param = meta, meta.get('proxy') if (meta if meta else {}) else proxy
            meta = param[0]
            proxy = param[1] if meta.get('proxy') else proxy
            ignore_ip = contents.get('ignore_ip')
            methods = 'POST' if contents.get('method') == 'POST' else 'GET'
            timeout = timeout if timeout else self.timeout
            if is_websource:
                asyncio.run_coroutine_threadsafe(
                    self.web_html(proxy=proxy, is_change=is_change, meta=meta, url=url, is_websource=is_websource,
                                  retry_count=retry_count, page_click=page_click, before_do=before_do,
                                  input_box=input_box, input_text=input_text, input_click=input_click, timeout=timeout,
                                  callback=callback_demo, body=body), self.web_loop)
            else:
                asyncio.run_coroutine_threadsafe(self.start_Requests(method=methods, is_encode=is_encode, url=url, body=body, headers=headers,
                                                                     params=params, data=data, json_params=json_params,
                                                                     timeout=timeout, callback=callback_demo, meta=meta,
                                                                     level=level, proxy=proxy, verify_ssl=verify_ssl,
                                                                     is_file=is_file, retry_count=retry_count, is_change=is_change,
                                                                     allow_redirects=allow_redirects, ignore_ip=ignore_ip,
                                                                     request_info=body.decode('utf-8')), self.new_loop)

    async def web_html(self, proxy, is_change, meta, url, is_websource, retry_count, page_click, before_do, input_box, input_text, input_click, timeout, callback, body):  # 浏览器渲染
        self.is_proxy = True if (len([False for i in Agent_whitelist if i in url]) == 0) and IS_PROXY == True else False
        new_body = json.loads(body.decode('utf-8'))
        if self.is_proxy == False:
            proxy = None
        elif self.is_proxy and ((proxy == None) or (is_change)):
            proxy = await self.asy_rand_choi_pool()
            if self.is_sameip:
                meta['proxy'] = proxy
        while (retry_count < self.max_request):
            new_body['proxy'] = proxy = proxy
            try:
                self.logger.info(f'Webdriver starts, and the target website is: {url}  <{page_click if page_click else input_text}>')
                # web_source = Web_source(proxy=proxy)
                # html, cookies = await web_source.run(url, timeout, is_websource, page_click, before_do, input_box, input_text, input_click)
                web_peteer = Web_peteer()
                html, cookies, status_code = await web_peteer.web_peteer(url=url, proxy=proxy, timeout=timeout)
                cookie = ''
                for i in cookies:
                    cookie += i.get('name') + '=' + i.get('value') + '; '
                response_last = MyResponse(url=url, cookies=cookie, meta=meta, status_code=status_code,
                                           text=html, content=html.encode('utf-8'), proxy=proxy)
                # response_last = MyResponse(url=url, cookies=cookie, meta=meta, status_code=200,
                #                            text=html, content=html.encode('utf-8'), proxy=proxy)
                self.num += 1
                self.logger.info(f'Web page rendering complete----{url}')
                await self.__deal_fun(callback=callback, response_last=response_last)
                break
            except Exception as e:
                if (not self.is_proxy) and (self.max_request):
                    retry_count += 1
                    await self.retry('GET', url, retry_count, repr(e), new_body)
                else:
                    retry_count += 1
                    mess = json.loads(body.decode('utf-8'))
                    mess['is_change'] = True
                    mess['retry_count'] = retry_count
                    self.send_mqdata(mess=json.dumps(mess))
                    # self.delete_sql(table='ceshi_fasong', db_name='spider_frame', where=f"""url = '{mess['url']}'""")
                    await self.retry('GET', url, retry_count, repr(e), new_body)
                    break
        else:
            response_last = MyResponse(url=url, cookies='', meta=meta, status_code=None,
                                       text='', content=b'', proxy=proxy)
            await self.__deal_fun(callback=callback, response_last=response_last)

    # 请求处理函数
    async def start_Requests(self, method='GET', url=None, body=None, headers=None, params=None, data=None, json_params=None, cookies=None, timeout=None,
                             callback=None, meta=None, level=0, request_info=None, proxy=None, verify_ssl=None, allow_redirects=True, is_file=False, retry_count=0, is_change=False, is_encode=None, ignore_ip=False):
        # async with self.sem:
        try:
            # 监测环境下，进行标书请求去重
            if self.pages:
                if await self.contarst_data(url):
                    self.num += 1
                    return

            new_body = json.loads(body.decode('utf-8'))
            self.is_proxy = True if (len([False for i in Agent_whitelist if i in url]) == 0) and IS_PROXY == True else False
            if self.is_proxy == False:
                proxy = None
            elif self.is_proxy and ((proxy == None) or (is_change)):
                proxy = await self.asy_rand_choi_pool()
                if self.is_sameip:
                    meta['proxy'] = proxy
                    new_body['meta']['proxy'] = proxy
            if isinstance(headers, dict):
                headers['User-Agent'] = await self.get_ua() if UA_PROXY else headers['User-Agent']
            # self.insert({'fun_name': callback + '发送的请求', 'url': url, 'md5': self.production_md5(callback + url)},
            #             table='ceshi_fasong', db_name='spider_frame', is_replace=False)
            while (retry_count < self.max_request):
                new_body['proxy'] = proxy = proxy if not ignore_ip else None
                try:
                    with async_timeout.timeout(timeout=timeout):
                        async with aiohttp.ClientSession(headers=headers, conn_timeout=timeout, cookies=cookies) as session:
                            if (method == 'GET'):
                                async with session.get(url=URL(url, encoded=True) if is_encode else url, params=params, data=data, json=json_params, headers=headers, proxy=proxy, verify_ssl=verify_ssl, timeout=timeout, allow_redirects=allow_redirects) as response:
                                    res = await response.read()
                                    await self.infos(response.status, method, url)  # 打印日志
                                    text = await self.deal_code(res=res, body=body, is_file=is_file)
                                    response_last = MyResponse(url=url, headers=response.headers, content_type=response.headers.get('Content-Type'), data=data, cookies=response.cookies, meta=meta, retry_count=retry_count,
                                                               text=text, content=res, status_code=response.status, request_info=request_info, proxy=proxy, level=level)
                                    await self.Iterative_processing(method=method, callback=callback, response_last=response_last, body=body, level=level, retry_count=retry_count)

                            elif (method == "POST"):
                                async with session.post(url=URL(url, encoded=True) if is_encode else url, params=params, data=data, json=json_params, headers=headers, proxy=proxy, verify_ssl=verify_ssl, timeout=timeout, allow_redirects=allow_redirects) as response:
                                    res = await response.read()
                                    await self.infos(response.status, method, url)  # 打印日志
                                    text = await self.deal_code(res=res, body=body, is_file=is_file)
                                    response_last = MyResponse(url=url, headers=response.headers, content_type=response.headers.get('Content-Type'), data=data, cookies=response.cookies, meta=meta, retry_count=retry_count,
                                                               text=text, content=res, status_code=response.status, request_info=request_info, proxy=proxy, level=level)
                                    await self.Iterative_processing(method=method, callback=callback, response_last=response_last, body=body, level=level, retry_count=retry_count)
                    break
                except (aiohttp.ClientProxyConnectionError, aiohttp.ServerTimeoutError, TimeoutError, concurrent.futures._base.TimeoutError, aiohttp.ClientHttpProxyError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ClientPayloadError) as e:
                    retry_count += 1
                    await self.retry(method, url, retry_count, repr(e), new_body)
                    if self.is_proxy:
                        proxy = await self.asy_rand_choi_pool()
                        if self.is_sameip:
                            meta['proxy'] = proxy
                            new_body['meta']['proxy'] = proxy

                except pdfminer.pdfparser.PDFSyntaxError as e:
                    response_last = MyResponse(url=url, headers={}, data=data, cookies=cookies, meta=meta, text='PDF无法打开或失效', content=b'', status_code=200, proxy=proxy)
                    await self.Iterative_processing(method=method, callback=callback, response_last=response_last, body=body, level=level, retry_count=retry_count)

                except Exception as e:
                    if (not self.is_proxy) and (self.max_request):
                        retry_count += 1
                        await self.retry(method, url, retry_count, repr(e), new_body)
                        self.logger.error(repr(e) + ' Returning to the queue ' + str(new_body), exc_info=True)
                    else:
                        retry_count += 1
                        mess = json.loads(body.decode('utf-8'))
                        mess['is_change'] = True
                        mess['retry_count'] = retry_count
                        self.send_mqdata(mess=json.dumps(mess), level=level)
                        # self.delete_sql(table='ceshi_fasong', db_name='spider_frame', where=f"""url = '{mess['url']}'""")
                        self.logger.error(repr(e) + ' Returning to the queue ' + str(new_body), exc_info=True)
                        break
            else:
                response_last = MyResponse(url=url, headers={}, data={}, cookies=None, meta=meta, retry_count=retry_count,
                                           text='', content=b'', status_code=None, request_info=request_info, proxy=proxy, level=level)
                await self.Iterative_processing(method=method, callback=callback, response_last=response_last,
                                                body=body, level=level, retry_count=retry_count)
        except Exception as e:
            import traceback
            traceback.print_exc()
        self.num += 1

    async def contarst_data(self, url):
        url_sha1 = self.url2sha1(url)
        t_num = str(int(url_sha1[-2:], 16) % 16)
        url_sha1 = self.select_data(['url_sha1'], table='t_bidding_filter_{t_num}'.format(t_num=t_num),
                                    db_name='spider_frame', where=f"""url_sha1='{url_sha1}'""")
        if url_sha1:
            self.logger.info(f'Data already exists, the request has been skipped：{url}')
            return True

    async def deal_code(self, res, body, is_file):  # 编码处理函数
        if is_file:
            text = None
            return text
        charset_code = chardet.detect(res[0:1])['encoding']
        # charset_code = self.deal_re(self.charset_code.search(str(res)))
        if charset_code:
            try:
                text = res.decode(charset_code)
                if not self.is_contain_chinese(text):
                    text = await self.cycle_charset(res, body)  # 此处存疑
                return text
            except (UnicodeDecodeError, TypeError, LookupError):
                text = await self.cycle_charset(res, body)
                if not text:
                    text = str(res, charset_code, errors='replace')
                return text
            except Exception as e:
                self.logger.error(repr(e) + ' Decoding error ' + body.decode('utf-8'), exc_info=True)
        else:
            text = await self.cycle_charset(res, body)
            return text

    async def cycle_charset(self, res, body):  # 异常编码处理函数
        charset_code_list = ['utf-8', 'gbk', 'gb2312', 'utf-16']
        for code in charset_code_list:
            try:
                text = res.decode(code)
                return text
            except UnicodeDecodeError:
                continue
            except Exception as e:
                self.logger.error(repr(e) + ' Decoding error ' + body.decode('utf-8'), exc_info=True)

    async def Iterative_processing(self, method, callback, response_last, body, level, retry_count):  # 迭代器及异常状态码处理函数
        if (response_last.status_code != 200) and (response_last.status_code in retry_http_codes) and (retry_count < self.max_request):
            mess = json.loads(body.decode('utf-8'))
            mess['retry_count'] = retry_count = int(retry_count) + 1
            if self.is_proxy:
                mess['proxy'] = await self.asy_rand_choi_pool()
                if self.is_sameip:
                    mess['meta']['proxy'] = mess['proxy']
            if (retry_count < self.max_request):
                self.send_mqdata(mess=json.dumps(mess), level=level)
                # self.delete_sql(table='ceshi_fasong', db_name='spider_frame', where=f"""url = '{mess['url']}'""")
                await self.retry(method, response_last.url, str(retry_count), 'Wrong status code {status}'.format(status=response_last.status_code), str(mess))
                self.exc_count += 1
            elif (retry_count == self.max_request):
                self.logger.debug('Give up <{message}>'.format(message=body.decode('utf-8')))
                self.fangqi_count += 1
                response_last.retry_count = retry_count
                await self.__deal_fun(callback=callback, response_last=response_last)
            return

        if (response_last.status_code != 200) and (response_last.status_code != None) and (response_last.status_code not in retry_http_codes):
            mess = json.loads(body.decode('utf-8'))
            if int(retry_count) < 3:
                mess['retry_count'] = retry_count = int(retry_count) + 1
                if self.is_proxy:
                    mess['proxy'] = await self.asy_rand_choi_pool()
                    if self.is_sameip:
                        mess['meta']['proxy'] = mess['proxy']
                self.send_mqdata(mess=json.dumps(mess), level=level)
                await self.retry(method, response_last.url, str(retry_count), 'Other wrong status code {status}'.format(status=response_last.status_code), str(mess))
                self.other_count += 1
            else:
                self.logger.debug('Give up <{message}>'.format(message=body.decode('utf-8')))
                self.fangqi_count += 1
                response_last.retry_count = retry_count
                await self.__deal_fun(callback=callback, response_last=response_last)
            return

        if (retry_count == self.max_request):
            self.logger.debug('Give up <{message}>'.format(message=body.decode('utf-8')))
            self.fangqi_count += 1
            response_last.retry_count = retry_count
        await self.__deal_fun(callback=callback, response_last=response_last)

    async def __deal_fun(self, callback, response_last):
        if self.__getattribute__(callback)(response=response_last):
            for c in self.__getattribute__(callback)(response=response_last):
                if c.meta == {}:
                    c.meta['proxy'] = response_last.meta.get('proxy')
                else:
                    c.meta = dict(response_last.meta, **c.meta)
                # if int(response_last.level) + 1 > self.x_max_priority:
                #     c.level = self.x_max_priority
                # else:
                #     c.level = int(response_last.level) + 1  # 优先级自动递增，适用yield
                self.send_mqdata(c)

    async def infos(self, status, method, url):  # 日志函数
        self.request_count += 1
        self.logger.info('Mining ({status}) <{method} {url}>'.format(status=status, method=method, url=url))
        if str(status) == '200':
            self.success_code_count += 1
            self.logger.debug('Catched from <{status} {url}>'.format(status=status, url=url))

    async def retry(self, method, url, retry_count, abnormal, message):  # 重试日志函数
        self.logger.debug('Retrying <{method} {url}> (failed {retry_count} times): {abnormal}'.format(method=method, url=url, retry_count=retry_count, abnormal=abnormal) + str(message))
        self.wrong_count += 1
