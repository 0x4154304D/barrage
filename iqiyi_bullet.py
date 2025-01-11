#!/usr/bin/env python
import json
import zlib

import pandas as pd
import requests
from lxml import etree

HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.66"
}

PROXIES = {
    "http:": "http://x.x.x.x",
    "https:": "https://x.x.x.x",
}


# 1.获取视频tv_id列表
def get_epsode_list(album_id):
    epsode_list = []
    base_url = "https://pcw-api.iqiyi.com/albums/album/avlistinfo?aid="

    for page in range(1, 3):
        url = f"{base_url}{album_id}&page={page}&size=30"
        res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10)
        data = json.loads(res.text)
        page_epsode_list = list(
            map(lambda x: str(x["tvId"]), data["data"]["epsodelist"])
        )
        epsode_list.extend(page_epsode_list)

    return epsode_list


# 2.获取视频弹幕存储XML
def get_barrage(epsode_id: str):
    # 弹幕文件每 300s(5min) 一个根据剧集时长进行循环
    for page in range(1, 17):
        results = []
        # https://cmts.iqiyi.com/bullet/tv_id[-4:-2]/tv_id[-2:]/tv_id_300_x.z
        base_url = "https://cmts.iqiyi.com/bullet/"
        url = f"{base_url}{epsode_id[-4:-2]}/{epsode_id[-2:]}/{epsode_id}_300_{page}.z"

        # 请求弹幕压缩文件
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10)
        if response.status_code != 200:
            continue
        res_byte = bytearray(response.content)
        try:
            xml = zlib.decompress(res_byte)
            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(xml, parser)
            for bullet_info in root.xpath("//bulletInfo"):
                user_name = bullet_info.xpath("./userInfo/name/text()")
                content = bullet_info.xpath("./content/text()")
                like_count = bullet_info.xpath("./likeCount/text()")
                diss_count = bullet_info.xpath("./dissCount/text()")
                if content and user_name and like_count and diss_count:
                    results.append(
                        {
                            "user_name": user_name[0],
                            "content": content[0],
                            "like_count": int(like_count[0]),
                            "diss_count": int(diss_count[0]),
                        }
                    )
            # 转为 DataFrame
            df = pd.DataFrame(results)
            filename = f"./bullet/{epsode_id}.csv"
            print(f"save {filename}")
            df.to_csv(
                filename, index=False, encoding="utf-8", mode="a"
            )  # Windows Excel 打开则使用 utf_8_sig
            results.clear()
        except zlib.error as zlib_e:
            print(f"zlib decompress file:{url} err: {zlib_e}")
        except Exception as e:
            print(f"XML parse error: {e}")
            return
    return


if __name__ == "__main__":
    # 剧集页 F12 观看
    albumId = "203164301"
    # url = 'https://pcw-api.iqiyi.com/album/album/othtrailer/203164301?contenttype=4&size=150'
    epsode_list = get_epsode_list(albumId)
    for epsode in epsode_list:
        get_barrage(epsode)
