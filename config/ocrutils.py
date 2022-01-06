import io
import re

import pdfminer.psparser
import pdfplumber
import requests
import json
# from config.settings import ocr_url as URL

URL = "https://api.bltools.bailian-ai.com/getOcrOriginData?ocrType=2"

class RecoBase():

    def __init__(self, filename, is_merge=True):
        self.filename = filename
        self.is_merge = is_merge # 多页合并

        self.__files__ = {}

    def __get_file__(self, File_bytes, file_type):
        self.__files__["file"] = (f'filename.{file_type}', File_bytes)
        return self.__files__

    def __close_file__(self):
        for k, v in self.__files__.items():
            v.close()

    def __req__(self, File_bytes, file_type):
        try:
            resp = requests.post(URL, files=self.__get_file__(File_bytes, file_type))
            # self.__close_file__()
            jdata = json.loads(resp.text)
            result = jdata.get("data").get("ocrInfo")
            return result
        except Exception as e:
            # print(e)
            return None

    def __fill_black__(self, pre_end_pos, curr_pos):
        if pre_end_pos == 0:
            return ""
        width = curr_pos - pre_end_pos
        if width < 20:
            return ""
        return " " * int(width/20)

    def reco(self, File_bytes, file_type):
        ocr_info_dict = self.__req__(File_bytes, file_type)
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

    def __init__(self, is_merge=True):
        super().__init__("", is_merge)
        # self.filename = self.__download__(url)

    def __get_postfix__(self, url):
        if "." not in url:
            return None

        postfix = url.split(".")[-1]
        if len(postfix) > 5 or len(postfix) < 2:
            return None
        return '.' + postfix

class RecoFactory():

    @staticmethod
    def reco_url(file_bytes, file_type, is_merge=True):
        reco = RecoUrl(is_merge)

        File_bytes = io.BytesIO(file_bytes)
        if file_type == 'pdf':
            try:
                pdf = pdfplumber.open(File_bytes)
                if len(pdf.pages) > 10:
                    return []
                pdf.close()
            except:
                pass
        File_bytes = io.BytesIO(file_bytes)
        f1 = io.BufferedReader(File_bytes)
        result = reco.reco(f1, file_type)
        # print(result)
        return result

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
    url = 'https://www.cse-bidding.com/zgbqdzzb/cgUploadController.do?openFileById&id=2c913b207d47161a017d47d242e60123'
    # url = 'http://gkml.xiaogan.gov.cn/u/cms/ymx/202112/141037538dbq.jpg'

    resp = requests.get(url, stream=True)
    Content_Type = resp.headers['Content-Type']
    file_type = re.search('/(.*)', Content_Type, re.S).groups()[0]

    result = RecoFactory.reco_url(resp.content, 'pdf', True)
    print(result)
    # RecoFactory.show(result)
