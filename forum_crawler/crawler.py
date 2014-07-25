#coding=utf-8

"""
一个基于异步pycurl的简陋的爬虫框架
版本：0.1
作者：
修订历史：
"""


import setting        # 用户自定义的setting模块，其中定义有LOG相关的设置

################################################################################
#
#                            LOG相关函数定义
#
# LOG级别从低到高为DEBUG，INFO，WARNING，ERROR，CRITICAL
# 包括两种LOG，一般的信息输出到特定的LOG文件中，包括DEBUG，INFO，WARNING，ERROR
# 实时性要求比较高的CRITICAL信息在输出到文件中的同时会发送到邮件中
#
################################################################################

import logging
from logging import Formatter
from logging import FileHandler
from logging.handlers import SMTPHandler

formatter = Formatter("[%(asctime)s][%(levelname)s]:%(message)s",
                      "%Y-%m-%d %H:%M:%S")

file_handler = FileHandler(setting.LOG_PATH)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

email_handler = SMTPHandler(setting.EMAIL_HOST, setting.EMAIL_SENDER,
                            setting.EMAIL_RECEIVER, setting.EMAIL_SUBJECT,
                            setting.EMAIL_CREDENTIALS)
email_handler.setFormatter(formatter)
email_handler.setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(email_handler)

def LOG_DEBUG(msg):
    return logging.debug(msg)

def LOG_INFO(msg):
    return logging.info(msg)

def LOG_WARNING(msg):
    return logging.warning(msg)

def LOG_ERROR(msg):
    return logging.error(msg)

def LOG_CRITICAL(msg):
    return logging.critical(msg)

################################################################################
#
#                 爬虫相关类定义，包括请求Request和BaseCrawler
#
################################################################################

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass

import time
import pycurl
pycurl.global_init(pycurl.GLOBAL_ALL)

class BaseRequest(object):
    """对pycurl的curl的封装，最基本的请求类型，有对curl的特殊的操作需求可以继承该类并对self.curl进行操作。
    """
    def __init__(self):
        self.curl = pycurl.Curl()
        self.prior = False    # 所有的请求会先放入一个发送缓冲区中，该值为True时请求插入缓冲区前部
        self.retry = 0
        self.type = type       # 设置retry的次数，当设置有retry时，请求执行失败后将会再次放入缓冲区

    def __del__(self):
        self.curl.close()
        self.curl = None

    def reset(self):
        """请求失败并重试时会直接在multicurl中perform，所以当请求中定义有一次性的资源时多次perform会出现问题，
           例如StringIO，多次往其中写入数据会造成数据的叠加，得到的并非希望的结果。因此在每次perform之前需要
           重新申请这类资源。
           在该函数中释放上次申请的资源并为下次perform申请新的资源。
        """
        pass

class Request(BaseRequest):
    """一种常用的请求类型，根据URL获取内容，可以设置HTTP代理，并可以利用HTTP PROXY TUNNEL模拟HTTPS代理。
       不考虑重定向和添加/获取HTTP请求头，类似需求可以继承该类或BaseRequest。
    """
    def __init__(self, url, timeout=25, proxy=None, https=False, httpsproxytunnel=False):
        super(Request, self).__init__()
        self.curl.setopt(pycurl.NOSIGNAL, 1)
        self.curl.setopt(pycurl.URL, url)
        self.curl.setopt(pycurl.TIMEOUT, timeout)
        if proxy: c.setopt(pycurl.PROXY, proxy)
        if https:
            self.curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            self.curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        if httpsproxytunnel: self.curl.setopt(pycurl.HTTPPROXYTUNNEL, 1)
        self.fp = None
        self.url = url

    def __del__(self):
        if self.fp is not None: self.fp.close()
        super(Request, self).__del__()

    def reset(self):
        if self.fp is not None: self.fp.close()
        self.fp = StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, self.fp.write)

    def set_proxy(self, proxy):
        """重新设置请求的代理。
           注意请求重试时不会自动更换代理。
        """
        c.setopt(pycurl.PROXY, proxy)

    def get_content(self):
        """获取下载后的文本内容
        """
        return self.fp.getvalue()

class BaseCrawler(object):
    """最基础的爬虫框架，基于Pycurl的异步机制和协程实现。
    """
    def __init__(self, max_concurrent, loop_interval):
        """max_concurrent:最大异步并发数
           loop_interval:当没有得到下载结果时主循环的延时
        """
        self.__max_curls = max_concurrent
        self.__loop_interval = loop_interval
        self.__free_curl_cnt = self.__max_curls
        self.__curls = [None for i in range(self.__max_curls)]
        self.__multi_curl = pycurl.CurlMulti()
        self.__send_buffer = []       # 发送缓冲区
        self.__looper = None

        self.__suspending = False          # 结合suspend和resume控制dispatch是否正在挂起
        self.__dispatching = False         # 当前执行位置是否位于dispatch自定义函数中，控制send函数
        self.__dispatch_closed = False     # dispatch是否已经退出

    def __del__(self):
        self.__multi_curl.close()
        self.__multi_curl = None
        self.__send_buffer = None

    def send(self, request):
        """将一个请求发送到发送缓冲区。
        """
        request.reset()
        if request.prior is True: self.__send_buffer.insert(0, request)
        else: self.__send_buffer.append(request)
        if self.__dispatching: self.__looper.send(None)

    def suspend(self):
        """挂起dispatch进程，调用resume将恢复dispatch的执行。
           该函数只能在dispatch中调用。
        """
        self.__suspending = True
        self.__looper.send(None)

    def resume(self):
        """恢复dispatch的执行。该函数不能在dispatch中调用。
        """
        self.__suspending = False

    def dispatch(self):
        """自定义函数，控制爬虫的爬取流程。
        """
        pass

    def handle_ok(self, req):
        """自定义函数，处理成功的request。
           当爬虫需要处理多种类型的请求时，可以为请求添加type成员，并在该函数中
           对req.type进行判断以执行不同的处理流程。
        """
        pass

    def handle_error(self, req, errno, errmsg):
        """自定义函数，处理失败的request。
        """
        pass

    def __loop(self):
        """爬虫执行主循环，基本流程为：从请求缓冲区中获取下载请求，
           送入pycurl.multicurl中进行下载，等待下载结果并调用自定义
           回调函数分别处理下载结果。
        """
        while 1:
            # dispatch自定义函数已经退出，且发送缓冲区已经没有request，且没有request正在下载
            if self.__dispatch_closed and not self.__send_buffer and self.__free_curl_cnt == self.__max_curls: break

            # multicurl从发送缓冲区接受尽可能多的request直至达到最大并发
            while self.__free_curl_cnt > 0:
                req = None
                try:
                    req = self.__send_buffer.pop(0)
                # 当缓冲区中没有请求时先调用dispatch填充发送缓冲区，直到达到最大并发数或dispatch挂起
                except IndexError:
                    if self.__suspending: break
                    else:
                        try:
                            self.__dispatching = True
                            cpu = (yield)
                            self.__dispatching = False
                        except GeneratorExit:
                            self.__dispatch_closed = True
                            self.__suspending = True    # dispatch已经退出，使之挂起
                            self.__dispatching = False
                            break

                if req is None: continue
                # 寻找空闲的curl并将请求添加进multicurl
                for i,j in enumerate(self.__curls):
                    if j is None:
                        req.curl.id = i       # 给req.curl加上req所在的位置id，以便回调时找到req
                        self.__curls[i] = req
                        self.__multi_curl.add_handle(req.curl)
                        self.__free_curl_cnt -= 1
                        break

            # 因为之前一次性尽可能多的往multicurl添加了request，所以没必要立即
            # 进入下一次循环以获取新的request，而是等待直到得到一个下载结果
            while 1:
                ret, active_num = self.__multi_curl.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM: break
            while 1:
                queued_num, ok_list, err_list = self.__multi_curl.info_read()
                if not ok_list and not err_list: self.__multi_curl.select(self.__loop_interval)
                for c in ok_list:
                    self.__multi_curl.remove_handle(c)
                    self.__free_curl_cnt += 1
                    self.handle_ok(self.__curls[c.id])
                    self.__curls[c.id] = None
                for c, errno, errmsg in err_list:
                    self.__multi_curl.remove_handle(c)
                    self.__free_curl_cnt += 1
                    req = self.__curls[c.id]
                    self.__curls[c.id] = None
                    # 如果request设置了retry则直接放入发送缓冲区，等到所有重试
                    # 都失败再调用handle_error
                    if req.retry > 0:
                        req.retry -= 1
                        self.send(req)
                        continue
                    self.handle_error(req, errno, errmsg)
                if queued_num == 0: break

    def run(self):
        """启动爬虫。
        """
        self.__looper = self.__loop()
        self.__looper.next()
        self.dispatch()
        try:
            self.__looper.close()
        except RuntimeError:
            pass

class ForumCrawler(BaseCrawler):
    """这是一个针对最常见的论坛新闻类站点的爬虫样例，其特点包括：
       1、页面结构分为topic和thread两类，thread页面是具体的一个帖子主题或是
       一个具体的新闻，topic页面是thread页面URL的汇总，并按照发表或最新更新时间对
       thread进行了排序；
       2、使用Request类，不考虑代理，不考虑HTTP头等特殊情况。
    """
    def __init__(self, board_ids, max_concurrent=30, loop_interval=0.2):
        """board_ids：标识所要爬取的board的ID列表，ID用于从get_next_topic_url
           函数中获得Topic页面的URL。
        """
        super(ForumCrawler, self).__init__(max_concurrent, loop_interval)
        self.board_ids = board_ids
        self.stop = False

    def __del__(self):
        super(ForumCrawler, self).__del__()

    def dispatch(self):
        for board_id in self.board_ids:
            #$$$$$$$$$$$$$$$$$$$$$$$$$
            self.stop = False
            page = 0
            while 1:
                page += 1
                topic_url = self.get_next_topic_url(board_id, page)
                #print topic_url
                if topic_url is None: break
                #@@@@@@@@@@@@@@@@@@@@
                req = Request(topic_url)
                req.type = "topic"
                self.send(req)
                #@@@@@@@@@@@@@@@@@@@@
                self.suspend()    # 等待该topic页面下载解析完成再翻页，因为需要
                                  # 根据该topic页的解析结果决定是否需要继续翻页

    def handle_ok(self, req):
        content1 = req.get_content()
        if req.type == "topic":
            for url, args in self.parse_topic(content1):
                req = Request(url)
                #@@@@@@@@@@@@@@@@@
                req.type = "thread"
                req.args = args
                self.send(req)
            self.resume()
        elif req.type == "thread":
            return self.parse_thread(content1, req.args)
        else:
            return self.handle_other_ok(req)

    def handle_error(self, req, errno, errmsg):
        if req.type == "topic":
            LOG_ERROR("Get topic-url:%s failed,error is %d,%s" % (req.url, errno, errmsg))
            #LOG_CRITICAL("Get topic page failed,crawler exit unnormally")
            LOG_ERROR("Get topic page failed,crawler exit unnormally")
        elif req.type == "thread":
            LOG_ERROR("Get thread-url:%s failed,error is %d,%s" % (req.url, errno, errmsg))
        else:
            #$$$$$$$$$$$$$$$$$$$$$$$$$$$
            return self.handle_other_error(req, errno, errmsg)

    def handle_other_ok(self, req):
        """处理除topic和thread页面之外的其他类型请求，如图片下载请求等。
        """
        pass

    def handle_other_error(self, req, errno, errmsg):
        """处理除topic和thread页面之外的其他类型请求，如图片下载请求等。
        """
        pass

    def get_next_topic_url(self, board_id, page):
        """自定义函数，返回指定board_id和page的topic页面。当没有下一页时返回None,
           并在函数内部检查self.stop，为True则返回None，表示不再继续翻页。
        """
        pass

    def parse_topic(self, content1):
        """自定义生成器函数，解析topic页面并yield每一条thread的信息，yield数据的格式
           为(url,[附带数据1，附带数据2，...])，附带数据列表会作为args参数传递给相应的
           parse_thread函数。
           注意，当在解析过程中发现帖子的最新更新时间早于爬虫上次启动时间时，停止yield
           数据，并设置self.stop为True。
        """
        pass

    def parse_thread(self, content1, args):
        """自定义函数，解析thread页面并把解析结果持久化。args为parse_topic中的附带数据列表，
           用于从parse_topic传递数据给parse_thread。
        """
        pass

################################################################################
#
#                  爬取的数据类型定义，包括新闻、微博和论坛帖子
#
################################################################################

import struct

class News(object):
    def __init__(self, id, site, url, board, post_time, scratch_time, title,
                 content, image, poster, srcmedia, allow_comment=True,
                 comment_num=0, shared_num=0):
        self.id = id
        assert self.id > 0
        self.site = site
        assert self.site > 0
        self.url = url
        self.board = board
        self.post_time = post_time
        self.scratch_time = scratch_time
        self.title = title
        self.content = content
        self.image = ",".join(image[:8])
        self.poster = poster
        self.srcmedia = srcmedia
        self.allow_comment = allow_comment
        self.comment_num = comment_num
        self.shared_num = shared_num
        assert self.comment_num >= 0 and self.shared_num >= 0

class Post(object):
    def __init__(self, id, site, url, board, post_time, scratch_time,
                 newreply_time, title, content, image, poster, poster_id,
                 poster_url, read_num=0, reply_num=0):
        self.id = id
        assert self.id > 0
        self.site = site
        assert self.site > 0
        self.url = url
        self.board = board
        self.post_time = post_time
        self.scratch_time = scratch_time
        self.newreply_time = newreply_time
        self.title = title
        self.content = content
        self.image = ",".join(image[:8])
        self.poster = poster
        self.poster_id = poster_id
        self.poster_url = self.poster_url
        self.read_num = read_num
        self.reply_num = reply_num
        assert self.read_num >= 0 and self.reply_num >= 0

class Status(object):
    def __init__(self, id, site, post_time, scratch_time, content, image,
                 poster, poster_id, poster_url, repost_num=0, comment_num=0):
        self.id = id
        assert self.id > 0
        self.site = site
        assert self.site > 0
        self.post_time = post_time
        self.scratch_time = scratch_time
        self.content = content
        self.image = ",".join(image[:8])
        self.poster = poster
        self.poster_id = poster_id
        self.poster_url = self.poster_url
        self.repost_num = repost_num
        self.comment_num = comment_num
        assert self.repost_num >= 0 and self.comment_num >= 0

class Image(object):
    def __init__(self, data_id, site, num, filetype, content):
        """data_id：图片所属的文本ID
           filetype：文件的后缀名，如jpg
           num：图片在该文本的所有图片中的序号
        """
        self.content = content
        self.file_name = "%d_%d_%d.%s" % (site, data_id, num, filetype)

################################################################################
#
#                      数据的持久化方法Recorder定义
#
################################################################################

#import MySQLdb

class Recorder(object):
    """数据持久化的方法基类。
    """
    def __init__(self):
        pass

    def __del__(self):
        pass

    def record(self, obj, *args, **kwargs):
        """存放图片时需要指定dir参数，即文件的存放目录。
        """
        if isinstance(obj, News):
            return self.record_news(obj, *args, **kwargs)
        if isinstance(obj, Post):
            return self.record_post(obj, *args, **kwargs)
        if isinstance(obj, Status):
            return self.record_status(obj, *args, **kwargs)
        if isinstance(obj, Image):
            return self.record_image(obj, *args, **kwargs)

    def record_news(obj, *args, **kwargs):
        pass

    def record_post(obj, *args, **kwargs):
        pass

    def record_status(obj, *args, **kwargs):
        pass

    def record_image(obj, *args, **kwargs):
        image_dir = kwargs["dir"]
        f = open("%s/%s" %(image_dir, obj.file_name), "w")
        f.write(obj.content)
        f.close()

class MySQLRecorder(Recorder):

    def __init__(self, host, db, user, passwd):
        super(MySQLRecorder, self).__init__()
        self.conn = MySQLdb.connect(host, db, user, passwd, charset="utf-8")
        conn.autocommit(False)
        cursor = conn.cursor()
        cursor.execute("SET NAMES 'utf8mb4'")
        cursor.close()

    def __del__(self):
        self.conn.close()

    def record_news(obj):
        cur = self.conn.cursor()
        query = "INSERT INTO news_%d VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                 %s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE \
                 scratch_time=%s,comment_num=%s,shared_num=%s"
        params = (self.id, self.srcid, self.srcidstr, self.url, self.site,
                  self.board, self.post_time, self.scratch_time, self.title,
                  self.content, self.image, self.poster, self.srcmedia,
                  self.allow_comment, self.comment_num, self.shared_num,
                  self.scratch_time, self.comment_num, self.shared_num)
        cur.execute(query, params)
        self.conn.commit()
        cur.close()

    def record_post(obj):
        cur = self.conn.cursor()
        query = "INSERT INTO post_%d VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                 %s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE \
                 scratch_time=%s,newreply_time=%s,read_num,reply_num=%s"
        params = (self.id, self.srcid, self.srcidstr, self.url, self.site,
                  self.board, self.post_time, self.scratch_time,
                  self.newreply_time, self.title, self.content, self.image,
                  self.poster, self.poster_id, self.poster_idstr,
                  self.poster_url, self.read_num, self.reply_num,
                  self.scratch_time, self.newreply_time, self.read_num,
                  self.reply_num)
        cur.execute(query, params)
        self.conn.commit()
        cur.close()

    def record_status(obj):
        cur = self.conn.cursor()
        query = "INSERT INTO status_%d VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                 %s,%s,%s,%s) ON DUPLICATE KEY UPDATE \
                 scratch_time=%s,reply_num,comment_num=%s"
        params = (self.id, self.srcid, self.srcidstr, self.site,
                  self.post_time, self.scratch_time, self.content,
                  self.image, self.poster, self.poster_id, self.poster_url,
                  self.repost_num, self.comment_num)
        cur.execute(query, params)
        self.conn.commit()
        cur.close()


