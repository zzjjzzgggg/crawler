#coding=utf-8

import json
import urllib
import random
import time
import re

from publicopinion import *
import account
import token

class TokenError(Exception):
    """错误代码
           1: 获取token信息失败
           2: 登录失败
           3: 无法继续获取可用token（手动模式下所有token过期或自动模式下重登录失败）
    """
    def __init__(self, errno, errmsg):
        Exception.__init__(self, errno, errmsg)
        self.errno, self.errmsg = errno, errmsg

    def __str__(self):
        return "%d,%s" % (self.errno, self.errmsg)    

AUTH_URL = "https://api.weibo.com/oauth2/authorize"
AUTH_REDIRECT_URL = "http://www.xjtu.edu.cn"
def authorize(appid, user_email, password, proxy):
    """使用帐号登录，获取token，返回None表示登录失败。
    """
    if proxy is None:
        req = Request(AUTH_URL, 20, None, True, False)
    else:
        req = Request(AUTH_URL, 20, proxy, True, True)
    req.curl.setopt(pycurl.REFERER, "https://api.weibo.com")
    post = {'response_type': 'token',
            'passwd': password,
            'action': 'submit',
            'userId': user_email, 
            'redirect_uri': AUTH_REDIRECT_URL,
            'client_id': app_id}
    req.curl.setopt(pycurl.POSTFIELDS, urllib.urlencode(post))
    try:
        req.curl.perform()
        url = self.curl.getinfo(pycurl.REDIRECT_URL)
    except pycurl.error:
        return None

    try:
        token = re.findall(r'token=(.*?)(?=&)', url)[0]
    except IndexError:
        token = None
    return token
            
LIMIT_URL = "https://api.weibo.com/2/account/rate_limit_status.json?access_token=%s"
EXPIRE_URL = "https://api.weibo.com/oauth2/get_token_info"
def get_token_info(token, proxy):
    """获取token的访问限制和重置时间以及过期时间。
       正常返回(remain_hit, reset_in_secs, expire_in_secs),错误返回None
    """
    # 获取remain_hit和reset_in_secs
    limit_url = LIMIT_URL % token
    if proxy is None:
        req = Request(limit_url, 20, None, True, False)
    else:
        req = Request(limit_url, 20, proxy, True, True)
    req.reset()
    try:
        req.curl.perform()
    except pycurl.error, e:
        return None
    obj = json.loads(req.get_content())
    if "remaining_user_hits" not in obj:
        return None
    remain_hits = obj["remaining_user_hits"]
    reset_in_secs = time.time() + obj["reset_time_in_seconds"]

    # 获取expire_in_secs
    if proxy is None:
        req = Request(EXPIRE_URL, 20, None, True, False)
    else:
        req = Request(EXPIRE_URL, 20, proxy, True, True)
    req.reset()
    post = {'access_token': token}
    req.curl.setopt(pycurl.POSTFIELDS, urllib.urlencode(post))
    try:
        req.curl.perform()
    except pycurl.error, e:
        return None
    obj = json.loads(req.get_content())
    if "expire_in" not in obj:
        return None
    expire_in_secs = time.time() + obj["expire_in"]
    return (remain_hits, reset_in_secs, expire_in_secs)


AFTER_TIME_OFFSET = 20    # 向后时间裕量，因为时间计算可能有误，留出一定裕量，
                          # 以保证加上裕量后限制一定重置了和token一定过期了
BEFORE_TIME_OFFSET = 20   # 向前时间裕量，因为时间计算可能有误，留出一定裕量，
                          # 在过期时间之前20秒时即锁定该token，不再使用该
                          # token获取数据，以免token已经过期了
HIT_RESERVED = 10         # 为了防止新浪服务器统计延时和不准确，remain_hit
                          # 设置一个裕量，小于该值就不使用该token了
class TokenManager(object):

    def __init__(self, auto_mode):
        self.auto_mode = auto_mode
        self.zombies = account.zombies
        self.apps = account.apps

        # load proxy
        if self.auto_mode:
            with open("proxy.txt") as f:
                self.proxies = [p.strip() for p in f]
        else:
            self.proxies = [None]

        self.tokens = {}   # token:[appid,email,password,proxy,remain_hit,reset_time,expire_time]
        # get tokens
        if self.auto_mode:
            self.__get_tokens()
        else:
            with open("token.txt") as f:
                tokens = [p.strip() for p in f]
            for t in tokens:
                self.tokens[t] = [None, None, None, None, None, None, None]
        if not self.tokens:
            raise TokenError, (2, "All Login failed")

        # get token attribute
        for token in self.tokens:
            proxy = self.tokens[token][3]
            ret = get_token_info(token, proxy)
            if ret is None:
                LOG_CRITICAL("Get token info failed:token is %s" % token)
            else:
                self.tokens[token][-3:] = ret
        if not [t for t in self.tokens if self.tokens[t][-1] is not None]:
            raise TokenError, (1, "None token info fetched")

    def __get_tokens(self):
        pairs = []
        for app in self.apps:
            t = [(app, i) for i in self.apps[app] if i in self.zombies]
            pairs.extend(t)
        tokens = []
        for i, (app_id, zombie_id) in enumerate(pairs):
            email, password = self.zombies[zombie_id]
            proxy = self.proxies[i / 6]
            token = authorize(app_id, email, password, proxy)
            if token is None:
                LOG_CRITICAL("Sina Weibo authorize faield:appid-%d,user-%s" % \
                             (app_id, email))
            else:
                self.tokens[token] = [app_id, email, password, proxy, None,
                                      None, None]

    def get_token(self):
        """获得一个可用的token，无可用token返回None，当无法再
           继续获取可用token时抛出异常3。
        """
        # re-authorize all expired token
        expired = [i for i in self.tokens if self.tokens[i][-1] <= \
                                             (time.time() - AFTER_TIME_OFFSET)]
        if expired:
            if not self.auto_mode:    # 手动模式下无法自动注册，直接去掉过期token
                for i in expired:
                    del self.tokens[i]
            else:
                for t in expired:
                    app_id, email, password, proxy = self.tokens[t][:4]
                    del self.tokens[t]
                    token = authorize(app_id, email, password, proxy)
                    if token is None:
                        LOG_CRITICAL("Sina Weibo re-authorize faield:appid-%d,user-%s" % \
                                     (app_id, email))
                    else:
                        self.tokens[token] = [app_id, email, password, proxy, None,
                                              None, None]

        # 当没有合法token时，抛出异常3
        if not self.tokens:
            raise TokenError, (3, "no valid token")

        # 可能已经过期的token标记出来
        marked = [i for i in self.tokens if self.tokens[i][-1] <= \
                                            (time.time() + BEFORE_TIME_OFFSET)]

        # 判断是否需要重置访问限制
        reset = [i for i in self.tokens if self.tokens[i][-2] <= \
                                           (time.time() - AFTER_TIME_OFFSET)]
        if reset:
            for token in self.tokens:
                proxy = self.tokens[token][3]
                ret = get_token_info(token, proxy)
                if ret is None:
                    LOG_CRITICAL("Get token info failed:token is %s" % token)
                    del self.tokens[token]    # 获取token属性失败则直接去掉
                else:
                    self.tokens[token][-3:] = ret
        
        # 获取可用的token，规则是不在标记的集合中且remain_hit大于裕量
        avails = [i for i in self.tokens if self.tokens[i][-3] > HIT_RESERVED and i not in marked]
        if avails:
            token = random.choice(avails)
            self.tokens[token][-3] -= 1
            return token, self.tokens[token][3]
        else:
            return None

def test():
    print get_token_info("2.00qxtrrB0giTcV24b37fc179K6vesD", None)

if __name__ == "__main__":
    test()
