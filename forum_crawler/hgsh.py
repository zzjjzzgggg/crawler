#coding=utf8

import calendar, time, sys, re, os
import traceback
sys.path.append("D:/lianyungang")
import crawler
from crawler import *
import MySQLdb

IMGTYPES = '.jpg.gif.png.jpeg.bmp'

class LuntanHgshTask(crawler.ForumCrawler):
    def __init__(self, conn, board_ids, last_time):      #################你的构造函数的形参没加，这样子是没法初始化基类的
        super(LuntanHgshTask, self).__init__(board_ids, 30, 0.2)
        self.stop = False
        self.conn = conn
        self.last_time = last_time
        self.blocks_pattern = re.compile(r'<tbody id="normalthread.*?</a></em>',re.S)
        #self.sub_pattern = re.compile(ur'<[^<>]*?>|<[^<>]*?<|[\r\n\t]+')  #？？？
        self.url_pattern = re.compile(r'<a href="(.*?)"')  #url中有&amp；看能否正常运行
        self.title_pattern = re.compile(r'onclick="atarget\(this\)" class="xst" >(.*?)</a>')  
        self.last_posttime_pattern = re.compile(r'goto=lastpost#lastpost">.*?(\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2})')  
        self.poster_pattern = re.compile(r'<a href="home\.php\?mod=space&amp;uid=.*?c="1">(.*?)</a>')
        self.poster_url_pattern = re.compile(r'<a href="(home\.php\?mod=space&amp;uid=\d+)"')
        self.num_info_pattern = re.compile(r'class="xi2">(\d+)</a><em>(\d+)</em></td>')
        
        self.block_pattern = re.compile(r'<em id="authorposton.*?</td></tr></table>', re.S)
        self.content_pattern = re.compile(r'<table cellspacing="0" cellpadding="0"><tr>(.*?)</td></tr></table>', re.S)
        self.post_time_pattern = re.compile(r'发表于.*?(\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2})')
        self.attachments_pattern = re.compile(r'<img.*?zoomfile="(.*?)"')
        self.id_pattern = re.compile(r'tid=(\d+)&')
        self.poster_id_pattern = re.compile(r'uid=(\d+)')

        self.sub_p_1 = re.compile('<script type="text/javascript" reload="1">(.*?)</script>',re.S)
        self.sub_p_2 = re.compile('\r\n')
        self.sub_p_3 = re.compile('<[^<>]*?>')
        self.sub_p_4 = re.compile('\n+')

    def __del__(self):
        super(LuntanHgshTask, self).__del__()

    def get_next_topic_url(self, board_id, page):
        if self.stop:
            return None   
        else:
            if page==1:
                next_url = "http://bbs.lyg01.net/forum.php?mod=forumdisplay&fid=2" 
            else:
                next_url = "http://bbs.lyg01.net/forum.php?mod=forumdisplay&fid=2&page=%d" %\
                           (page)
            print next_url,'111111'
            return next_url

    def handle_other_ok(self, req):
        x = req.get_content()
        #print '^^^^^^^^^^^^^^^^', req.name
        f = open("D:/education/imgs/%s" %(req.name), "wb")
        f.write(x)
        f.close()
        

    def handle_other_error(self, req, errno, errmsg):
        LOG_ERROR("Get img-url:%s failed,error is %d,%s" % (req.url, errno, errmsg))
        #LOG_CRITICAL("Get image page failed,crawler exit unnormally")
        
    def parse_html_content(self, str):
        '''return a unicode string'''
        str = re.sub(self.sub_p_1, '', str)
        str = re.sub(self.sub_p_2, '', str)
        str = re.sub(self.sub_p_3, '', str)
        str = re.sub(self.sub_p_4, '', str)
        #str = re.sub(self.sub_p_5, '', str)
        
        str = str.replace('&nbsp;', ' ')
        str = str.replace('&#8203','')
        str = str.replace('&quot;', '"')
        #str = str.replace('&amp;', '&')
        #str = str.replace('&#8226;', '.')
        #str = str.replace('&#148;', '”')
        #str = str.replace('&#160;', ' ')
        #str = str.replace('&lt;', '<')
        #str = str.replace('&gt;', '>')
        #str = str.replace('&amp;', '&')
        
        ustr = str#.decode('utf8','ignore')
        return ustr
    
    def parse_topic(self, content1):
        #print content1
        blocks = re.findall(self.blocks_pattern, content1)
        for b in blocks:
            url = re.findall(self.url_pattern, b)[0]
            url = "http://bbs.lyg01.net/" + url
            url = url.replace('amp;','')
            #print url
            last_posttime = re.findall(self.last_posttime_pattern,b)[0]
            #print last_posttime
            title = re.findall(self.title_pattern, b)[0]
            poster = re.findall(self.poster_pattern, b)[0]
            poster_url = re.findall(self.poster_url_pattern, b)[0]
            num_info = re.findall(self.num_info_pattern, b)[0]
            args = []
            args.append([url, last_posttime, title, poster, poster_url, num_info])  
            last_posttime = time.strptime(last_posttime, '%Y-%m-%d %H:%M:%S')
            last_time = time.strptime(self.last_time, '%Y-%m-%d %H:%M:%S')
            if calendar.timegm(last_posttime) < calendar.timegm(last_time):
                self.stop = True
            yield (url, args)
            
    def parse_thread(self, content1, args):
        #try:
            if content1.count(u'<title>提示信息'.encode('utf8')) != 0:
                return
            #print content1
            url,newreply_time,title,poster,poster_url,num_info= args[0]
            #print url,'###########'
            post_id = re.findall(self.id_pattern, url)[0]
            id = post_id   #*********注意id是关键字，最好别用这个变量名
            #print id
            scratch_time = time.strftime('%Y-%m-%d %H:%M:%S')
            #print scratch_time
            block = re.findall(self.block_pattern, content1)[0]
            #print block.decode('utf8', 'ignore')
            title = title.decode('utf8', 'ignore')    
            #print title
            poster = poster.decode('utf8', 'ignore')
            #print poster
            read_num = num_info[1]
            read_num = int(read_num)
            #print read_num
            reply_num = num_info[0]
            reply_num = int(reply_num)
            #print reply_num
            board = u'港城民生'
            poster_url = "http://bbs.lyg01.net/" + poster_url
            poster_url = poster_url.replace('amp;','')
            #print poster_url
            poster_id = re.findall(self.poster_id_pattern, poster_url)[0]
            poster_id = int(poster_id)
            #print poster_id
            post_time = re.findall(self.post_time_pattern, block)[0]
            #print post_time
            content = re.findall(self.content_pattern, block)
            #content = map(lambda x: self.parse_html_content(x), content)
            #print content
            contents = content[0]
            content = map(lambda x: self.parse_html_content(x), content)[0]
            #print content
            image1 = re.findall(self.attachments_pattern, contents)
            image1 = map(lambda y: "http://bbs.lyg01.net/" + y, image1)
            #print image1
            c=[]
            for i,img in enumerate(image1):
                ext = os.path.splitext(img.lower())[1]
                if i==8:
                    break
                if IMGTYPES.find(ext)!=-1:
                    imgname = '48_%s_%d%s' % (id, i, ext)
                req = Request(img)
                req.retry = 3
                req.name = imgname
                req.type="imgs"
                self.send(req)
                c.append(imgname)
            image=','.join(c)
            #print image
            cursor = self.conn.cursor()
            query = u"INSERT INTO source_48 VALUES (%s,48,%s,%s,%s," + \
            u"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY" + \
            u" UPDATE newreply_time=%s,read_num=%s,reply_num=%s"
            params = (id, url, board, scratch_time,
                      post_time, newreply_time, title, content, image, poster, poster_id,
                      poster_url, read_num, reply_num, newreply_time, read_num, reply_num)
            #print query % params
            cursor.execute(query, params)
            self.conn.commit()            
        #except:
            #print 'error',traceback.print_exc()
        



if __name__ == "__main__":
    last_time = open('D:/lianyungang/hgsh/hgsh.txt').read()
    print last_time
    board_ids = ['']  
    conn = MySQLdb.connect(host="localhost", db="public_opinion", user="root",
                           passwd="QAZ))#@%@2012", charset="utf8")
    c = LuntanHgshTask(conn, board_ids, last_time)
    time1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    try:
        c.run()
    except Exception, e:
        traceback.print_exc()
        LOG_ERROR("preprocess and run error")
        #LOG_CRITICAL("Run failed,crawler exit unnormally")
        sys.exit()
    conn.close()
    file = open("D:/lianyungang/hgsh/hgsh.txt", "w")
    file.write(time1)
    file.close()











