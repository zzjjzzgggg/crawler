#coding=utf-8

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
file_handler.setLevel(logging.DEBUG)

email_handler = SMTPHandler(setting.EMAIL_HOST, setting.EMAIL_SENDER, 
                            setting.EMAIL_RECEIVER, setting.EMAIL_SUBJECT, 
                            setting.EMAIL_CREDENTIALS)
email_handler.setFormatter(formatter)
email_handler.setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
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

