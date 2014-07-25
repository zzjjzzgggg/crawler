#coding=utf-8

#**********************************************************
#                 crawler system definition
#**********************************************************
# LOG related
EMAIL_HOST = "stu.xjtu.edu.cn"
EMAIL_SENDER = "liaochen@stu.xjtu.edu.cn"
EMAIL_RECEIVER = ["liaochen.2@stu.xjtu.edu.cn"]
EMAIL_SUBJECT = "Error from SinaWeiboCrawler"
EMAIL_CREDENTIALS = ("liaochen", "liaochen198732")
LOG_PATH = "sina_weibo_crawler.log"


# Crawler parameters
SITE_ID = 19
TOKEN_AUTO = False

# Proxy
#PROXY_SERVICE_URL = "http://localhost:8000/get-proxy/"

#**********************************************************
#                    MySQL
#**********************************************************
DB_HOST = "localhost"
DB_DB = "public_opinion"
DB_USER = "root"
DB_PASSWD = "QAZ))#@%@2012"

