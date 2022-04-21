import struct
import base64

class File_Type:

    @staticmethod
    def typeList():
        type_dict = {'jpg': ['FFD8FFE000104A464946'],
                     'png': ['89504E470D0A1A0A0000'],
                     'gif': ['47494638396126026F01'],
                     'tif': ['49492A','4D4D'],
                     'bmp': ['424D8E1B030000000000'],

                     'html': ['3C21444F435459504520'],
                     'htm': ['3C21646F637479706520'],
                     'css': ['48544D4C207B0D0A0942'],
                     'js': ['696B2E71623D696B2E71'],
                     'rtf': ['7B5C727466315C616E73'],

                     'eml': ['46726F6D3A203D3F6762'],
                     'mdb': ['5374616E64617264204A'],
                     'ps': ['252150532D41646F6265'],

                     'pdf': ['255044462D312E', '255044462D312E340A25', '255044462D312E350D0A'],
                     'rmvb': ['2E524D46000000120001'],
                     'flv': ['464C5601050000000900'],
                     'mp4': ['00000020667479706D70'],
                     'mpg': ['000001BA210001000180'],

                     'wmv': ['3026B2758E66CF11A6D9'],
                     'wav': ['52494646E27807005741'],
                     'avi': ['52494646D07D60074156'],
                     'mid': ['4D546864000000060001'],

                     'zip': ['504B0304140000000800', '504B0304140000080800', '504B03040A0000080000'],
                     'rar': ['526172211A0700CF9073'],
                     'ini': ['235468697320636F6E66'],
                     'jar': ['504B03040A0000080000'],
                     'exe': ['4D5A9000030000000400'],
                     'jsp': ['3C25402070616765206C'],
                     'mf': ['4D616E69666573742D56'],

                     'xml': ['3C3F786D6C2076657273'],
                     'sql': ['494E5345525420494E54'],
                     'java': ['7061636B616765207765'],
                     'bat': ['406563686F206F66660D'],
                     'gz': ['1F8B0800000000000000'],

                     'properties': ['6C6F67346A2E726F6F74'],
                     'class': ['CAFEBABE0000002E0041'],
                     'docx': ['504B0304140006000800', '504B03040A0000000000'],
                     'doc': ['D0CF11E0A1B11AE10000'],
                     # 'xls': ['D0CF11E0A1B11AE10000'],
                     # 'xlsx': ['504B03040A0000000000'],
                     'torrent': ['6431303A637265617465'],

                     'mov': ['6D6F6F76'],
                     'wpd': ['FF575043'],
                     'dbx': ['CFAD12FEC5FD746F'],
                     'pst': ['2142444E'],
                     'qdf': ['AC9EBD8F'],
                     'pwl': ['E3828596'],
                     'ram': ['2E7261FD']
                     }
        ret = {}
        for k_hex, v_prefix in type_dict.items():
            ret[k_hex] = v_prefix
        return ret

    @staticmethod
    def bytes2hex(bytes):
        # num = len(bytes)
        hexstr = u""
        for i in range(10):
            try:
                t = u"%x" % bytes[i]
                if len(t) % 2:
                    hexstr += u"0"
                hexstr += t
            except IndexError:
                return ''
        return hexstr.upper()

    @staticmethod
    def bytes2hex_up(bytes):
        num = len(bytes)
        hexstr = u""
        for i in range(num):
            t = u"%x" % bytes[i]
            if len(t) % 2:
                hexstr += u""
            hexstr += t
        return hexstr.upper()

    @staticmethod
    def stream_type(stream):
        tl = File_Type.typeList()
        ftype = None
        for type_name, hcode_list in tl.items():
            flag = False
            for hcode in hcode_list:
                s_hcode = File_Type.bytes2hex(stream)
                # print("转成十六进制的编码", s_hcode, '=', "头文件", hcode, type_name)
                if s_hcode == hcode:
                    flag = True
                    break
                elif s_hcode != hcode:
                    numOfBytes = int(len(hcode) / 2)
                    hbytes = struct.unpack_from("B" * numOfBytes, stream[0:numOfBytes])
                    s_hcode = File_Type.bytes2hex_up(hbytes)
                    # print("转成十六进制的编码", s_hcode, '=', "头文件", hcode, type_name)
                    if s_hcode == hcode:
                        flag = True
                        break
            if flag:
                ftype = type_name
                break
        return ftype


if __name__=="__main__":
    import requests
    header = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
    }
    # url = 'https://bid.snapshot.qudaobao.com.cn/f9e8f1a3229a874c15310c495052db75cc7f1d7d.pdf'
    # url = 'http://www.mysfybjy.com/upload/file/202109/02/202109021040237101.xlsx'
    # url = 'http://www.mysfybjy.com/upload/file/202108/31/202108311138192677.xls'
    # url = 'http://221.180.255.192/TPFrame/webSiteDownloadAction_LY.action?cmd=download&AttachGuid=25223748-83e0-4451-88d3-b2b1a06a7be6&ClientType=Z190'
    # url = 'http://www.gf.com.cn/file/download?file_id=61aeed1c62b96a4d570cd5e7'
    # url = 'https://image.baidu.com/search/down?tn=download&word=download&ie=utf8&fr=detail&url=https%3A%2F%2Fgimg2.baidu.com%2Fimage_search%2Fsrc%3Dhttp%253A%252F%252Fimg.jj20.com%252Fup%252Fallimg%252Ftp09%252F210611094Q512b-0-lp.jpg%26refer%3Dhttp%253A%252F%252Fimg.jj20.com%26app%3D2002%26size%3Df9999%2C10000%26q%3Da80%26n%3D0%26g%3D0n%26fmt%3Djpeg%3Fsec%3D1643871924%26t%3D2832b589fa0940ac17e14d1d72e66772&thumburl=https%3A%2F%2Fimg0.baidu.com%2Fit%2Fu%3D1305456094%2C3865830840%26fm%3D26%26fmt%3Dauto'
    # url = 'http://crfsdi.crcc.cn/picture/0/d65fecc84601431b85e689a4f39dc4e2.png'
    # url = 'http://cr16g.crcc.cn/module/download/downfile.jsp?classid=0&filename=10875d39baae4ba3bbd7d39e21565cf0.doc'
    url = 'https://details.cebpubservice.com:7443/bulletin/getBulletin/8a9494827dbc84bd017e0fc2914333b2'

    resp = requests.get(url, headers=header, stream=True)
    # file_type = File_Type()
    ftype = File_Type.stream_type(resp.content)
    print("[ftype]", ftype)
