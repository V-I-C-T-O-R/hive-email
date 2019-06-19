import time
from datetime import date, timedelta
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
import smtplib
from openpyxl import Workbook
from pyhive import hive

def sendLogic(func):
    def wrapper(*args, **kw):
        for i in range(5):
            conn = func(*args, **kw)
            if conn is not None:
                return conn
            else:
                time.sleep(1)
        return None

    return wrapper

class Email():
    def __init__(self, smtpserver, smtpport, password, from_mail, to_mail, cc_mail=None):
        self.smtpserver = smtpserver
        self.smtpport = smtpport
        self.password = password
        self.from_mail = from_mail
        self.to_mail = to_mail
        self.cc_mail = cc_mail

    def attachAttributes(self, msg, subject, from_name, from_mail, to_mail, cc_mail=None):
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = Header(from_name + " <" + from_mail + ">", "utf-8")
        msg["To"] = Header(",".join(to_mail), "utf-8")
        if cc_mail:
            msg["Cc"] = Header(",".join(cc_mail), "utf-8")

    def attachBody(self, msg, body, type):
        msgtext = MIMEText(body, type, "utf-8")
        msg.attach(msgtext)

    def attachAttachment(self, msg, attfile):
        att = MIMEBase("application", "octet-stream")

        try:
            file = open(attfile, "rb")
            att.set_payload(file.read())
            encoders.encode_base64(att)
        except(Exception) as err:
            print(str(err))
        finally:
            if file in locals():
                file.close()

        if "\\" in attfile:
            list = attfile.split("\\")
            filename = list[len(list) - 1]
        else:
            filename = attfile
        att.add_header("Content-Disposition", "attachment; filename='%s'" % filename)

        msg.attach(att)

    def __send_running(self,msg):
        try:
            smtp = smtplib.SMTP_SSL(self.smtpserver, self.smtpport)
            smtp.login(self.from_mail, self.password)
            if self.cc_mail == None:
                smtp.sendmail(self.from_mail, self.to_mail, msg.as_string())
            else:
                smtp.sendmail(self.from_mail, self.to_mail + self.cc_mail, msg.as_string())
            print('发送邮件到%s成功' % ",".join(self.to_mail))
        except(smtplib.SMTPRecipientsRefused):
            print("Recipient refused")
        except(smtplib.SMTPAuthenticationError):
            print("Auth error")
        except(smtplib.SMTPSenderRefused):
            print("Sender refused")
        except(smtplib.SMTPException) as e:
            print(e.message)
        finally:
            smtp.quit()

    def send_email(self,subject,body,file=None):
        msg = MIMEMultipart()
        self.attachAttributes(msg,subject,'浪里小白龙',self.from_mail,self.to_mail,self.cc_mail)
        if file:
            self.attachAttachment(msg, file)
        self.attachBody(msg,body,"plain")
        self.__send_running(msg)


class HiveHandler():
    def __init__(self,config):
        self.hive = self._get_conn(config)
        self.cursor = self.hive.cursor()

    @sendLogic
    def _get_conn(self,config):
        try:
            conn = hive.Connection(host=config.get('host'), port=config.get('port'), username='hdfs', database='default')
        except BaseException as e:
            print(e)
            conn = None
        return conn

    def query_datas(self,sql):
        self.cursor.execute(sql)
        datas = self.cursor.fetchall()
        results = []
        for data in datas:
            result = []
            for d in data:
                result.append(str(d).replace('\x159','').replace('\x03',''))

            results.append(result)
        return results

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.hive:
            self.hive.close()


def generate_excel(file_name,file_path,contents):
    wb = Workbook()
    ws = wb.create_sheet(file_name, 0)
    ws.append({'A': '会话1', 'B': '会话2', 'C': '会话3', 'D': '会话4', 'E': '会话5', 'F': '会话6', 'G': '会话7',
               'H': '会话8','I': '会话9', 'J': '会话10'})
    for content in contents:
        ws.append(
            {'A': content[0], 'B': content[1], 'C': content[2], 'D': content[3], 'E': content[4], 'F': content[5], 'G': content[6],
             'H': content[7],'I': content[8], 'J': content[9]})
    wb.save(file_path)
    wb.close()

if __name__ == '__main__':
    config = {
        'host':'192.172.1.1',
        'port':10000,
        'email_host':'smtp.qq.com',
        'email_port':465,
        'email_user':'1234567@qq.com',
        'email_pass':'dtsdfjbbgtjlwwdcja',#qq邮箱授权码
        'email_to_list':['1234567@qq.com','12345678@qq.com','123456789@qq.com'],
        'file_address':'/data/files/'
    }
    template_sql = '''select * from test a
        where get_json_object(a.content,'$.creatTime') >= '%s'
        and get_json_object(a.content,'$.creatTime') < '%s'
    '''
    file_name = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
    yesterday = file_name + ' 00:00:00'
    today_date = date.today().strftime("%Y-%m-%d")
    today = today_date + ' 00:00:00'
    query_sql = template_sql % (yesterday,today)
    print('开始执行今日(%s)任务'%today_date)
    hive = HiveHandler(config)
    print('开始查询Hive')
    contents = hive.query_datas(query_sql)
    print('查询Hive完成')
    file_path = config.get('file_address')+"{0}.xlsx".format(file_name)
    generate_excel(file_name,file_path,contents)
    hive.close()

    email = Email(config.get('email_host'),config.get('email_port'),config.get('email_pass'),config.get('email_user'),config.get('email_to_list'))
    email.send_email(file_name+' 数据','内容详情见附件',file_path)
    print('执行任务成功')