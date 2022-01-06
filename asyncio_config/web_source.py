import os
import time
import subprocess

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait  # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC  # available since 2.26.0
from selenium.webdriver.common.by import By

from scrapy.selector import Selector
from urllib.parse import urljoin


class Web_source(object):
    def __init__(self, proxy):
        self.proxy = proxy
        self.options = webdriver.FirefoxOptions()
        # self.options.add_argument("--headless")  # 为Chrome配置无头模式
        self.options.add_argument(f"--proxy-server={proxy}")
        self.driver = webdriver.Firefox(options=self.options)  # 在启动浏览器时加入配置
        # self.driver = webdriver.Firefox()  # 在启动浏览器时加入配置
        # self.driver = webdriver.Chrome()  # 在启动浏览器时加入配置
        self.driver.implicitly_wait(10)
        self.info_pid = None

    async def run(self, url, timeout, xpath, page_click, before_do, input_box, input_text, input_click):
        info = subprocess.Popen(f'ps -ef|grep {self.proxy}', shell=True)
        self.info_pid = info.pid
        self.driver.set_page_load_timeout(timeout)
        self.driver.get(url)
        self.driver.maximize_window()
        WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))
        if page_click:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, page_click if page_click and not before_do else before_do)))
            if before_do:
                self.driver.find_element(By.XPATH, before_do).click()
                WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((By.XPATH, page_click)))
            self.driver.find_element(By.XPATH, page_click).click()
        elif input_box and input_text and input_click:
            self.driver.find_element(By.XPATH, input_box).clear()
            self.driver.find_element(By.XPATH, input_box).send_keys(input_text)
            self.driver.find_element(By.XPATH, input_click).click()
            # WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((By.XPATH, input_click)))
            time.sleep(1)
        time.sleep(1)
        # self.driver.page_source.encode('utf-8')
        html = self.driver.page_source
        cookies = self.driver.get_cookies()
        self.driver.quit()
        return html, cookies


if __name__ == '__main__':
    import asyncio
    web_source = Web_source(None)
    # url = 'http://www.pudong.gov.cn/shpd/CateogryPaging/019004006/Default_3.htm'
    url = 'http://www.pudong.gov.cn/shpd/department/019004/019004006/'
    xpath = '//ul/li[@class="wb-data-list"]'
    html, cookies = asyncio.get_event_loop().run_until_complete(web_source.run(url, 15, xpath, None, None, None, None, None))
    print(html)
    s = Selector(text=html)
    data_list = s.xpath(xpath)
    print(len(data_list))
    for i in data_list:
        title = i.xpath('./div/a/text()').extract_first('')
        pub_time = i.xpath('./span/text()').extract_first('')
        url = i.xpath('./div/a/@href').extract_first('')
        print(title, url, pub_time)
