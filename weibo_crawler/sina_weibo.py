#coding=utf-8

import random
import time
import json
import traceback
import MySQLdb

from publicopinion import *
from token_manager import TokenManager, TokenError

import setting

###########################################################
#                     util函数
###########################################################
def transfer_time(time_str):
    """trans "Tue May 24 18:04:53 +0800 2011" into localtime,
       assume all timezone is +0800
    """
    t = time_str.split()
    timezone = t.pop(4)
    assert timezone == "+0800"  # assume all are +0800
    src_str = " ".join(t)
    src_t = time.strptime(src_str, "%a %b %d %H:%M:%S %Y")
    dst_time = time.strftime("%Y-%m-%d %H:%M:%S", src_t)
    return dst_time

def get_mysql_conn():
    conn = MySQLdb.connect(host=setting.DB_HOST, db=setting.DB_DB,
                           user=setting.DB_USER, passwd=setting.DB_PASSWD,
                           charset="utf8")
    conn.autocommit(False)
    cursor = conn.cursor()
    cursor.execute("SET NAMES 'utf8mb4'")
    cursor.close()
    return conn

###########################################################
#                     爬虫相关定义
###########################################################
class SinaWeiboCrawler(BaseCrawler):

    BASE_URL = "https://api.weibo.com/2/statuses/user_timeline.json?access_token=%s&uid=%d"

    def __init__(self, conn):
        super(SinaWeiboCrawler, self).__init__(30, 0.2)
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.token_manager = TokenManager(setting.TOKEN_AUTO)
        with open("user_id.txt") as f:
            self.users = [int(p.strip()) for p in f]        
        self.site_id = setting.SITE_ID

    def __del__(self):
        #del self.recorder
        self.cursor.close()
        super(SinaWeiboCrawler, self).__del__()
       
    def dispatch(self):
        for user_id in self.users:
            try:
                ret = self.token_manager.get_token()
            except TokenError, e:
                LOG_CRITICAL("Can't get token any more:%s" % str(e))
                return
            if ret is None:
                LOG_CRITICAL("Get token return None")
                return
            token, proxy = ret
            url = SinaWeiboCrawler.BASE_URL % (token, user_id)
            if proxy is None:
                req = Request(url, 20, proxy, True, False)
            else:
                req = Request(url, 20, proxy, True, True)
            req.uid = user_id
            req.retry = 3
            self.send(req)
            
    def handle_ok(self, req):
        content = req.get_content()
        #print content
        objs = json.loads(content)
        #print objs
        if "error_code" in objs:
            LOG_ERROR("weibo error,uid is %d:%s-%s" % \
                      (req.uid, objs["error_code"], objs["error"]))
            return
        for obj in objs["statuses"]:
            sql = u"INSERT IGNORE INTO source_" + str(self.site_id) + \
                  u" VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                params = (obj["id"], self.site_id, time.strftime("%Y-%m-%d %H:%M:%S"),
                          transfer_time(obj["created_at"]), obj["text"], "",
                          obj["user"]["screen_name"], obj["user"]["id"],
                          obj["user"]["url"], obj["reposts_count"],
                          obj["comments_count"])
            except KeyError:
                LOG_WARNING("get status error,key not found:uid is %d,%s" % \
                            (req.uid, traceback.format_exc()))
                continue
            self.cursor.execute(sql, params)
        self.conn.commit()

    def handle_error(self, req, errno, errmsg):
        LOG_ERROR("Download error,%d,%s:uid is " % (errno, errmsg, req.uid))

###########################################################
#                       主函数
###########################################################
def main():
    LOG_INFO("Sina Weibo crawler started")
    try:
        conn = get_mysql_conn()
    except Exception, e:
        LOG_CRITICAL("Client get_mysql_conn failed:%s" % str(e))
        return
    try:
        crawler = SinaWeiboCrawler(conn)
    except Exception, e:
        LOG_CRITICAL("Crawler init failed due to %s" % traceback.format_exc())
        return
    try:
        crawler.run()
    except Exception, e:
        LOG_CRITICAL("Crawler exit unexpectly:%s" % traceback.format_exc())
    conn.close()
    LOG_INFO("Sina Weibo crawler exit")

if __name__ == "__main__":
    main()

