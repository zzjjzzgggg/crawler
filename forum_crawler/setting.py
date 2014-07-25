 #coding=utf-8

#**********************************************************
#                 system defined
#**********************************************************
# LOG related
EMAIL_HOST = "stu.xjtu.edu.cn"
EMAIL_SENDER = "liaochen@stu.xjtu.edu.cn"
EMAIL_RECEIVER = ["15191815642@163.com"]
EMAIL_SUBJECT = u"Error from 花果山下论坛".encode('gbk')
EMAIL_CREDENTIALS = ("liaochen", "liaochen198732")
LOG_PATH = "D:/lianyungang/hgsh/hgsh.log"

# Crawler parameters
SITE_ID = 1
MAX_CONCURRENT = 30

# Proxy
PROXY_SERVICE_URL = "http://localhost:8000/get-proxy/"

#**********************************************************
#                 self defined
#**********************************************************

TOKEN_LIMIT = 150
