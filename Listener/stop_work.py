import os
import time
from MQ.mq import Mq
from config.settings import Waiting_time


class QueueMonitoring(Mq):
    def __init__(self, queur_name, process_name):
        Mq.__init__(self, queur_name)
        self.process_name = process_name
        self.self_pid = os.getpid()

    def delete_queue(self):
        while 1:
            time.sleep(60)
            reidual_num = self.send_channel_count.method.message_count
            print('这次查询队列数为：', reidual_num)
            if reidual_num == 0:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '队列', self.queue_name, '目前为空，开始监控……')
                time.sleep(Waiting_time)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '队列', self.queue_name, '持续{Waiting_time}秒消息数量为0已删除队列'.format(Waiting_time=Waiting_time))
                reidual_num = self.send_channel_count.method.message_count
                if reidual_num == 0:
                    self.process_name.kill()
                    break
        print(reidual_num)
        print('*******************队列{queue_name}监控完毕*******************'.format(queue_name=self.queue_name))
        print('==========================================================')
        return


if __name__ == '__main__':
    queue = QueueMonitoring('ysh_testtt', 'send_channel')
    # queue.delete_queue()
    queue.channel.queue_delete(queue='ysh_tests_2')
    a = queue.send_channel.method.message_count
    print(a)