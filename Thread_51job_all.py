import json
import re
import time
import threading

import requests
from lxml import etree
from queue import Queue
from dbModels.Handle_peewee import peewee_job_data
from dbModels.Handle_mangodb import mango
# from Handle_elasticsearch import


# 定义一个请求方法用于反复调用
def handle_request(url):
    # 默认请求头
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3610.2 Safari/537.36",
    }
    # 设置代理,当前使用的是阿布云的动态代理
    proxy = {
        "http": "http://HJ35208VA497416D:4F204A339F62A7EB@http-dyn.abuyun.com:9020",
        "https": "http://HJ35208VA497416D:4F204A339F62A7EB@http-dyn.abuyun.com:9020",
    }
    # 此处我们加上了代理,可能会请求失败，加入while True循环和try、except
    while True:
        try:
            response = requests.get(url = url, headers = header)#, proxies=proxy)
            if response.status_code == 200:
                break
        except:
            continue
    # 设置了网页编码
    response.encoding = 'gbk'
    # 返回response.text
    return response.text


# 日志文件输出方法
def write_log(log_text):
    # 定义文件名，打开方式，编码
    with open("log_51job.txt","a+",encoding="utf-8") as f:
        # 写入信息
        f.write(log_text)
        # 换行
        f.write("\n")


# 列表页处理类
class Crawl_list_url(threading.Thread):
    # 重写父类，定义公共变量，定义传入参数
    def __init__(self, thread_name, info_queue):
        super(Crawl_list_url,self).__init__()
        # 线程的名称
        self.thread_name = thread_name
        # 信息队列
        self.info_queue = info_queue
        # 当前线程类成功从数据库取出job_dict的总数，初始为0
        self.get_job_dict_count = 0
        # 当前线程类拼接并成功请求的列表页总量，初始为0
        self.page_count = 0
        # 当前线程类成功解析列表页的数据总量，初始为0
        self.job_count = 0

    # 线程类的run方法，编写主逻辑
    def run(self):
        # 输出当前线程信息
        print('当前启动的页码处理线程为：{thread_name}'.format(thread_name=self.thread_name))
        # 定义一个循环，不断重复
        while True:
            # 防止代码运行失败，造成程序异常
            try:
                # 从数据库中取出之前获取好的列表信息
                job_dict = mango.mongo_get_data("job_list")
                # 如果取出成功，则取出数+1
                self.get_job_dict_count += 1
            # try运行失败后，运行下面代码
            except:
                # 控制台输出信息
                print("job_list没有数据")
                # 结束循环
                break
            # 调用方法，传入参数
            self.make_page_url(job_dict)
        # 记录信息
        put_url_count_log = "{thread_name}取出job_code总共{get_job_dict_count}条，拼接page_url总共{page_count}条，放入队列job信息总共{job_count}条。".format(
            thread_name=self.thread_name, get_job_dict_count=self.get_job_dict_count, page_count=self.page_count, job_count=self.job_count)
        # 往日志文件写入信息
        write_log(put_url_count_log)

    # 列表页翻页逻辑，拼接url，再进行请求获取response.text
    def make_page_url(self,job_dict):
        # 从传参中取出job_code
        job_code = job_dict['job_code']
        # 根据job_code拼接url
        page_url = 'https://search.51job.com/list/000000,000000,' + job_code + ',00,9,99,+,2,1.html?ord_field=1'
        # 正则匹配字符串
        pattern = r"window\.__SEARCH_RESULT__ = (.*?)\</script\>"
        # 请求拼接的url获得response.text
        response = handle_request(page_url)
        # 根据re.findall取第一个下标，获得结果
        result = re.findall(pattern, response)[0]
        # 格式化为json数据
        temp = json.loads(result)
        # 根据格式化后的json数据获取total_page，当前job_code的总页码数
        total_page = temp['total_page']
        # 根据格式化后的json数据获取jobid_count，总岗位量
        jobid_count = temp['jobid_count']
        # 记录信息
        total_page_log = "当前{thread_name}的job_code为：{job_code},页码总量为：{total_page},数据总量为：{jobid_count}。".format(thread_name=self.thread_name, job_code=job_code,total_page=total_page, jobid_count=jobid_count)
        # 往日志文件写入信息
        write_log(total_page_log)
        # 控制台输出信息
        print(total_page_log)
        # for...in...range获取数字
        for page in range(1, int(total_page) + 1):
            # 根据job_code和数字拼接url
            page_url = 'https://search.51job.com/list/000000,000000,' + job_code + ',00,9,99,+,2,' + str(page) + '.html?ord_field=1'
            # 请求url获得response.text
            response = handle_request(page_url)
            # 防止操作失败，程序异常
            try:
                # 调用方法，传入参数
                self.parse_list_html(response,job_dict)
                # 如果解析成功，则记录解析列表页+1
                self.page_count += 1
            # 操作失败后的逻辑
            except:
                # 记录信息
                put_url_error_log = "{thread_name}线程解析page_url失败：{page_url}"
                # 往日志文件写入信息
                write_log(put_url_error_log)

    # 解析列表页方法，获取列表页数据，往信息队列放入数据
    def parse_list_html(self,response,job_dict):
        # 正则匹配字符串
        pattern = r"window\.__SEARCH_RESULT__ = (.*?)\</script\>"
        # 根据re.findall取第一个下标，获得结果
        result = re.findall(pattern, response)[0]
        # 格式化为json数据
        temp = json.loads(result)
        # 定义空列表用以装载字典数据
        info_list = []
        # 遍历格式化的json中的指定字段
        for item in temp['engine_search_result']:
            # 定义装载数据的字典
            info_dict = {}
            # 岗位ID
            info_dict['job_id'] = int(item['jobid'])
            # 岗位名称
            info_dict['job_name'] = item['job_name']
            # 月薪
            info_dict['salary'] = item['providesalary_text']
            # 工作地点
            info_dict['workarea'] = item['workarea_text']
            # 条件要求
            info_dict['attribute_text'] = str(item['attribute_text']).replace("[", "").replace("]", "")
            # 福利待遇
            info_dict['jobwelf'] = item['jobwelf']
            # 岗位编号
            info_dict['job_code'] = int(job_dict['job_code'])
            # 岗位方向
            info_dict['job'] = job_dict['job']
            # 岗位性质
            info_dict['job_type'] = job_dict['worktype']
            # 岗位职能
            info_dict['job_area'] = job_dict['area_name']
            # 公司名称
            info_dict['company_name'] = item['company_name']
            # 公司性质
            info_dict['companytype'] = item['companytype_text']
            # 公司规模
            info_dict['companysize'] = item['companysize_text']
            # 公司领域
            info_dict['companyind'] = item['companyind_text']
            # 岗位链接
            info_dict['job_href'] = item['job_href']
            # 公司链接
            info_dict['company_href'] = item['company_href']
            # 发布时间
            info_dict['upload'] = item['updatedate']
            # 采集时间
            info_dict['crawl_time'] = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(time.time()))
            # 将字典添加到列表中
            info_list.append(info_dict)
        # 将列表put到信息队列中
        self.info_queue.put(info_list)
        # 记录成功放入队列的数据量
        self.job_count += len(temp['engine_search_result'])


# 获取岗位详情页HTML线程类
class Crawl_info_html(threading.Thread):
    # 重写父类，定义公共变量，定义传入参数
    def __init__(self,thread_name,info_queue,data_queue):
        super(Crawl_info_html,self).__init__()
        # 线程名称
        self.thread_name = thread_name
        # 信息队列
        self.info_queue = info_queue
        # 数据队列
        self.data_queue = data_queue
        # 从信息队列中成功获取数据总数，初始为0
        self.get_info_list_count = 0
        # 往数据队列成功放入数据总数，初始为0
        self.put_data_count = 0

    def run(self):
        print("当前启动的网页处理线程为：{thread_name}".format(thread_name=self.thread_name))
        # 标志位为Flase时，队列不为空，运行循环，从队列中取出数据
        while not info_flag:
            # 防止取出队列中的数据时失败，造成程序异常
            try:
                # 把文本数据get出来
                info_list = self.info_queue.get(block = False)
                # 记录当前线程类从队列中取出的数据量
                self.get_info_list_count += 1
                # 遍历从队列中取出的列表数据
                for info in info_list:
                    # 取出岗位详情页url
                    job_url = info['job_href']
                    # 请求岗位详情页url，获得response.text
                    res = handle_request(job_url)
                    # 使用etree.HTML进行格式化
                    job_html = etree.HTML(res)
                    # 往data_queue队列中放入job_html,info两个信息，格式为列表
                    self.data_queue.put([job_html,info],block = False)
                # 记录当前线程类往data_queue队列放入了多少个数据
                self.put_data_count += len(info_list)
                # 控制台输出信息，便于监视程序运行状况
                print("{thread_name}成功放入队列{list_size}条数据".format(thread_name=self.thread_name,list_size = len(info_list)))
            # 如果取出失败则跳过当前，继续进行循环
            except:
                continue
        # 防止写入失败，程序异常
        try:
            # 线程类结束前记录工作量
            Crawl_html_count_log = "{thread_name}取出info_list总计{get_info_list_count}条，放入队列job_html总计{put_data_count}条。".format(thread_name=self.thread_name,get_info_list_count=self.get_info_list_count,put_data_count=self.put_data_count)
            # 写入日志文件
            write_log(Crawl_html_count_log)
        # 记录异常信息
        except Exception as e:
            # 往日志文件中写入异常信息
            write_log(e)


# 数据入库线程类
class Crawl_data_to_db(threading.Thread):
    # 重写父类，定义公共变量，定义传入参数
    def __init__(self,thread_name,data_queue,lock):
        super(Crawl_data_to_db,self).__init__()
        # 线程名称
        self.thread_name = thread_name
        # 数据队列
        self.data_queue = data_queue
        # 线程锁
        self.lock = lock
        # 当前线程类成功从队列中获取的数据总数，初始为0
        self.get_info_count = 0
        # 当前线程类成功解析数据并入库总数，初始为0
        self.parse_data_count = 0

    # 线程类默认运行方法，编写主逻辑
    def run(self):
        # 输出线程信息
        print("当前启动的数据处理线程为：{thread_name}".format(thread_name=self.thread_name))
        # 标志位为Flase，队列不为空，运行循环，从队列中取出数据
        while not data_flag:
            # 防止取出队列中的数据时失败，造成程序异常
            try:
                # 取出队列中的数据
                html_list = self.data_queue.get(block = False)
                # 记录当前线程类从队列中取出了多少数据
                self.get_info_count += 1
                # 防止解析页面失败造成程序异常
                try:
                    # 队列中取出的数据为一个列表，列表中第一个数据为etree.HTML格式化后的HTML页面
                    html = html_list[0]
                    # 列表中第二个数据为解析列表页时得到的数据，是一个字典
                    info_dict = html_list[1]
                    # 往info_dict这个字典中添加key：job_info并根据xpath解析页面获取value，岗位详情信息
                    info_dict['job_info'] = str(html.xpath("//div[@class='bmsg job_msg inbox']//text()")).replace(" ", "")
                    # 往info_dict这个字典中添加key：keyword并根据xpath解析页面获取value，岗位关键词信息
                    # 由于岗位详情页中可能没有关键字内容，所以使用if、else进行获取，也可以使用try、except
                    if html.xpath("//div[@class='mt10']/p[2]/a"):
                        info_dict['keyword'] = ''
                        for keyword in html.xpath("//div[@class='mt10']/p[2]/a/text()"):
                            info_dict['keyword'] = str(info_dict['keyword']) + str(keyword) + ','
                    else:
                        info_dict['keyword'] = ''
                    # 往info_dict这个字典中添加key：contact_info并根据xpath解析页面获取value，联系方式
                    if html.xpath("//div[@class='bmsg inbox']/a/@onclick"):
                        search_text = re.compile(", '(.*?)'")
                        info_dict['contact_info'] = search_text.search(
                            str(html.xpath("//div[@class='bmsg inbox']/a/@onclick"))).group(1)
                    else:
                        info_dict['contact_info'] = ''
                    # 往info_dict这个字典中添加key：company_info并根据xpath解析页面获取value，公司介绍信息
                    if html.xpath("//div[@class='tmsg inbox']/text()"):
                        info_dict['company_info'] = html.xpath("//div[@class='tmsg inbox']/text()")[0]
                    else:
                        info_dict['company_info'] = ''
                    # 调用线程锁
                    with self.lock:
                        # 使用SQLAlchemy存储到MySQL中
                        # data_51job.sqlalchemy_insert_data(info_dict)
                        # 使用peewee存储到MySQL中
                        peewee_job_data(info_dict)
                        # 存储到MongoDB中
                        # mango.mango_insert_data("collection_51job", info_dict)
                    # 记录存储的数据数
                    self.parse_data_count += 1
                    # 数据信息用于监控程序运行
                    print("{thread_name}总计入库{parse_data_count}条数据".format(thread_name=self.thread_name,
                                                                                parse_data_count=self.parse_data_count))
                # 如果队列中取出的信息解析失败，则记录错误信息并写入日志文件中
                except:
                    log_text = "{thread_name}解析取出info_list失败：{info_list}".format(thread_name=self.thread_name, info_list=html_list)
                    write_log(log_text)
            # 如果取出失败则跳过当前，继续进行循环
            except:
                continue
        # 防止写入失败，程序异常
        try:
            # 线程类结束前记录工作量
            parse_data_count_log = "{thread_name}取出info_list总计{get_info_count}条，入库数据总计{parse_data_count}条。".format(
                thread_name=self.thread_name, get_info_count=self.get_info_count, parse_data_count=self.parse_data_count)
            # 写入日志文件
            write_log(parse_data_count_log)
        # 记录异常信息
        except Exception as e:
            # 往日志文件中写入异常信息
            write_log(e)


# 标志位，用以控制线程类中循环从队列中取出数据
info_flag = False
data_flag = False


def main():
    # 往日志文件输入当前时间，记录此次程序运行
    write_log("本次爬虫启动时间：{time}".format(time=time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(time.time()))))
    # 信息队列，传递列表页信息
    info_queue = Queue()
    # 数据队列，为最后存储数据传递数据
    data_queue = Queue()
    # 定义一个锁
    lock = threading.Lock()
    # 列表页处理线程池，根据线程名称列表定义线程池，列表长度就是线程池大小，根据电脑情况定制数量
    crawl_list_info_list = ["页码处理线程1号"]#,"页码处理线程2号","页码处理线程3号"]
    # 空列表用以装载start后的线程，方便后续进行join操作
    list_info_thread_list = []
    # 依次启动页码处理线程,传入线程名称和info_queue队列
    for thread_name_list in crawl_list_info_list:
        thread_lsit_info = Crawl_list_url(thread_name_list,info_queue)
        # 启动线程
        thread_lsit_info.start()
        list_info_thread_list.append(thread_lsit_info)
    # 页码处理线程池，根据线程名称列表定义线程池，列表长度就是线程池大小，根据电脑情况定制数量
    Crawl_html_list = ["网页处理线程1号", "网页处理线程2号", "网页处理线程3号"]
    html_thread_list = []
    # 依次启动网页处理线程，传入线程名称和html_queue、data_queue两个队列
    for thread_name_html in Crawl_html_list:
        thread_html = Crawl_info_html(thread_name_html,info_queue,data_queue)
        # 启动线程
        thread_html.start()
        html_thread_list.append(thread_html)
    # 页码处理线程池，根据线程名称列表定义线程池，列表长度就是线程池大小，根据电脑情况定制数量
    Crawl_data_list = ["数据处理线程1号", "数据处理线程2号"]#, "数据处理线程3号"]
    data_thread_list = []
    # 依次启动网页处理线程，传入线程名称和html_queue、data_queue两个队列
    for thread_name_data in Crawl_data_list:
        thread_data = Crawl_data_to_db(thread_name_data, data_queue, lock)
        # 启动线程
        thread_data.start()
        data_thread_list.append(thread_data)
    # 依次结束页码处理线程，页码线程没有flag判断，直接挂起等待结束即可
    for thread_list_info_join in list_info_thread_list:
        thread_list_info_join.join()
        print(thread_list_info_join.thread_name, '处理结束')
    # 引入全局变量html_flag,当html_queue中还有数据时pass，html_queue为空时置html_flag为True
    global info_flag
    while not info_queue.empty():
        pass
    info_flag = True
    # 依次结束网页处理线程，当html_flag为True时，线程类中的循环结束，将线程挂起，等待线程运行完成后结束
    for thread_html_join in html_thread_list:
        thread_html_join.join()
        print(thread_html_join.thread_name,"处理结束")
    # 引入全局变量html_flag,当html_queue中还有数据时pass，html_queue为空时置html_flag为True
    global data_flag
    while not data_queue.empty():
        pass
    data_flag = True
    # 依次结束网页处理线程，当html_flag为True时，线程类中的循环结束，将线程挂起，等待线程运行完成后结束
    for thread_data_join in data_thread_list:
        thread_data_join.join()
        print(thread_data_join.thread_name,"处理结束")
    # 往日志文件输入当前时间，记录此次程序运行
    write_log("本次爬虫结束时间：{time}".format(time=time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(time.time()))))


if __name__ == '__main__':
    # 入口函数
    main()
