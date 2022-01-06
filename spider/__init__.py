# # 读取docx中的文本代码示例
# import docx
# import io
# # 获取文档对象
#
#
# import tempfile
#
#
#
#
# import requests
#
# url = 'http://www.gf.com.cn/file/download?file_id=61aeed1c62b96a4d570cd5e7'
# header = {
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#     "Accept-Encoding": "gzip, deflate",
#     "Accept-Language": "zh-CN,zh;q=0.9",
#     "Cache-Control": "max-age=0",
#     "Connection": "keep-alive",
#     "Cookie": "__jsluid_h=60ff5391638eb28830c4308ce406a692; Hm_lvt_0d69488dca97571f560a08a67eb398a6=1639571045; Hm_lpvt_0d69488dca97571f560a08a67eb398a6=1639571068; gfwsid=s%3Aeda0ad00-5da5-11ec-9e03-87aa60725cfe_64325_157747_186.I2BRWIpHOLwjTIMIEN%2BGkHF2ssR%2FDClcrfIFC3nHUq8",
#     "Host": "www.gf.com.cn",
#     "If-None-Match": "W/\"6bfe-KlRv6SB7z5sVn9bitukYzg\"",
#     "Referer": "http://www.gf.com.cn/about/news?tab=3",
#     "Upgrade-Insecure-Requests": "1",
#     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
# }
# response = requests.get(url, header)
# # print(response.content)
# from tempfile import TemporaryFile  # , NamedTemporaryFile
#
# # 1. 读取
# f = TemporaryFile(mode="w+")
# #   参数：
# #       1). mode="w+"   允许 打开的模式， 默认 为 w+b 模式
# #           w     写模式
# #           w+    读写模式
# #           w+b   读写 Bytes 模式
# #       2). buffering=-1     缓冲区大小， -1 是不限制
# #       3). encoding=None    读取的文件的字符编码
# rr = open(r'/Users/yuanshaohang/Downloads/广发证券CDH License采购供应商征集公告.docx', 'rb')
# f.write(rr.read().decode('gbk',"ignore"))  # 写入
# f.seek(0)  # 将 光标 切换到开始
#
# # line = f.readlines()  # 按照 每一行进行读取
# line = f.read()  # 读取全部
# # print(line)
#
# file = docx.Document(f)
# print(file, type(file))
# print("段落数:" + str(len(file.paragraphs)))  # 段落数为13，每个回车隔离一段
#
# # 输出每一段的内容
# for para in file.paragraphs:
#     print(para.text)
#
# # 输出段落编号及段落内容
# for i in range(len(file.paragraphs)):
#     print("第" + str(i) + "段的内容是：" + file.paragraphs[i].text)
