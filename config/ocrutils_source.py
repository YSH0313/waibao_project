import os
import requests
import json
import shutil
import time

URL = "https://api.bltools.bailian-ai.com/getOcrOriginData?ocrType=2"
SRC_DATA_PATH = "/tmp/ocr_source"

if not os.path.exists(SRC_DATA_PATH):
    os.mkdir(SRC_DATA_PATH)

class RecoBase():

    def __init__(self, filename, is_merge=True):
        self.filename = filename
        self.is_merge = is_merge # 多页合并

        self.__files__ = {}

    def __get_file__(self):
        f = open(self.filename, "rb")
        self.__files__["file"] = f
        print(self.__files__, type(self.__files__))
        return self.__files__

    def __close_file__(self):
        for k, v in self.__files__.items():
            v.close()

    def __req__(self):
        try:
            resp = requests.post(URL, files=self.__get_file__())
            self.__close_file__()
            jdata = json.loads(resp.text)
            result = jdata.get("data").get("ocrInfo")
            return result
        except Exception as e:
            print(e)
            return None

    def __fill_black__(self, pre_end_pos, curr_pos):
        if pre_end_pos == 0:
            return ""
        width = curr_pos - pre_end_pos
        if width < 20:
            return ""
        return " " * int(width/20)

    def reco(self):
        if not self.filename:
            print("文件不存在 或 下载文件失败")
            return []

        ocr_info_dict = self.__req__()
        if not ocr_info_dict:
            return []

        result_list = []
        for k, v in sorted(ocr_info_dict.items(), key=lambda k: k[0]):
            page = []
            for lines in v:
                row = ""
                pre_end_pos = 0
                for c in lines:
                    curr_pos = c.get("left")
                    fill = self.__fill_black__(pre_end_pos, curr_pos)
                    row += fill + c.get("words")
                    pre_end_pos = curr_pos + c.get("width")
                page.append(row)
            result_list.append(page)

        if self.is_merge:
            merge_list = []
            for page in result_list:
                merge_list += page
            return merge_list
        return result_list


class RecoUrl(RecoBase):

    def __init__(self, url, is_merge=True):
        super().__init__("", is_merge)
        self.filename = self.__download__(url)

    def __get_postfix__(self, url):
        if "." not in url:
            return None

        postfix = url.split(".")[-1]
        if len(postfix) > 5 or len(postfix) < 2:
            return None
        return '.' + postfix

    def __mk_filename__(self, url):
        temp_name = str(int(time.time() * 10000))
        postfix = self.__get_postfix__(url)
        filename = f"{SRC_DATA_PATH}/{temp_name}"
        if postfix:
            filename += postfix
        return filename

    def __download__(self, url):
        try:
            resp = requests.get(url, stream=True)
            if resp.status_code != 200:
                print(f"Download Failed ! Http Code: {resp.status_code}")
                return None

            filename = self.__mk_filename__(url)
            with open(filename, "wb") as fw:
                shutil.copyfileobj(resp.raw, fw)
        except Exception as e:
            print("附件下载异常")
            print(e)
            return None
        return filename

    def __remove__(self):
        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__remove__()

class RecoFactory():

    @staticmethod
    def reco_file(filename, is_merge=True):
        reco = RecoBase(filename, is_merge)
        return reco.reco()

    @staticmethod
    def reco_url(url, is_merge=True):
        reco = RecoUrl(url, is_merge)
        return reco.reco()

    @staticmethod
    def show(result_list):
        if not result_list:
            print("--- No Data ---")
            return
        if isinstance(result_list[0], list):
            for i, page in enumerate(result_list):
                print(f"{'-'*15} Page {i+1} {'-'*15}")
                for row in page:
                    print(row)
        else:
            for row in result_list:
                print(row)

if __name__ == '__main__':
    # filename = "/Users/jiapeng/Desktop/北京朝阳医院标讯.pdf"
    # filename = "/Users/jiapeng/Desktop/db41cf0d5f0d49369bb5fade73085409_3.jpeg"
    # result = RecoFactory.reco_file(filename, True)

    # urlfile = "https://bid.snapshot.qudaobao.com.cn/f9e8f1a3229a874c15310c495052db75cc7f1d7d.pdf"
    urlfile = 'http://www.mysfybjy.com/upload/file/202109/02/202109021040237101.xlsx'
    result = RecoFactory.reco_url(urlfile, True)

    RecoFactory.show(result)
