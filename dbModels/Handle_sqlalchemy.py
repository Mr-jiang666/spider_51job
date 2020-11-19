# coding:utf-8
from sqlalchemy import create_engine, Integer,String,Text,DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column


# 创建数据库的连接
engine = create_engine("mysql://root:abc123456@127.0.0.1:3306/db_51job?charset=utf8mb4")
# 操作数据库，需要我们创建一个session
Session = sessionmaker(bind=engine)

# 声明一个基类
Base = declarative_base()


# 数据表类，声明表名称和字段名称、类型、长度等
class table_51job(Base):
    # 表名称
    __tablename__ = 'table_51job'
    # 岗位Id
    job_id = Column(Integer,primary_key=True)
    # 岗位名称
    job_name = Column(String(length=100), default="")
    # 薪资
    salary = Column(String(length=30), default="")
    # 工作地点
    work_area = Column(String(length=30), default="")
    # 招聘要求
    attribute_text = Column(String(length=60), default="")
    # 福利待遇
    jobwelf = Column(Text, default="")
    # 职位编号
    job_code = Column(Integer, default=0)
    # 职位方向
    job = Column(String(length=20), default="")
    # 职位性质
    job_type = Column(String(length=20), default="")
    # 职位性质
    job_area = Column(String(length=20), default="")
    # 职位信息
    job_info = Column(Text, default="")
    # 关键字
    keyword = Column(String(length=100), default="")
    # 联系方式
    contact_info = Column(String(length=100), default="")
    # 公司名称
    company_name = Column(String(length=100), default="")
    # 公司类型
    companytype = Column(String(length=30), default="")
    # 公司领域
    companyind = Column(String(length=30), default="")
    # 公司规模
    companysize = Column(String(length=30), default="")
    # 公司信息
    company_info = Column(Text, default="")
    # 职位链接
    job_url = Column(String(length=200), default="")
    # 公司链接
    company_url = Column(String(length=200), default="")
    # 发布时间
    upload = Column(String(length=5), default="")
    # 抓取日期
    crawl_time = Column(DateTime)


class job_list_51job(Base):
    # 表名称
    __tablename__ = 'job_list_51job'
    # 职位编号
    job_code = Column(Integer, primary_key=True)
    # 职位方向
    job = Column(String(length=10), default="")
    # 职位性质
    job_type = Column(String(length=20), default="")
    # 职位性质
    job_area = Column(String(length=20), default="")
    # 抓取日期
    crawl_time = Column(DateTime)


class data_51job(object):
    def __init__(self):
        # 实例化session信息
        self.mysql_session = Session()

    # 数据的存储方法
    def sqlalchemy_insert_data(self,item):
        # 存储的数据结构
        global dict
        data = table_51job(
            # 岗位ID
            job_id = item['job_id'],
            # 岗位名称
            job_name = item['job_name'],
            # 月薪
            salary = item['salary'],
            # 工作地点
            work_area = item['workarea'],
            # 条件要求
            attribute_text = str(item['attribute_text']).replace("[","").replace("]",""),
            # 福利待遇,
            jobwelf = item['jobwelf'],
            # 职位编号
            job_code = item['job_code'],
            # 职位方向
            job = item['job'],
            # 职位性质
            job_type = item['job_type'],
            # 职位性质
            job_area = item['job_area'],
            # 职位信息
            job_info = item['job_info'],
            # 关键字
            keyword = item['keyword'],
            # 联系方式
            contact_info = item['contact_info'],
            # 公司名称
            company_name = item['company_name'],
            # 公司性质
            companytype = item['companytype'],
            # 公司规模
            companysize = item['companysize'],
            # 公司领域
            companyind = item['companyind'],
            # 公司信息
            company_info = item['company_info'],
            # 岗位链接
            job_url = item['job_href'],
            # 公司链接
            company_url = item['company_href'],
            # 发布时间
            upload = item['upload'],
            # 抓取时间
            crawl_time = item['crawl_time'],
        )
        query_result = self.mysql_session.query(table_51job).filter(table_51job.job_id==item['job_id']).first()
        if query_result:
            dict = dict(data)
            try:
                self.mysql_session.query(table_51job).filter(table_51job.job_id == item['job_id']).update(dict)
                # 提交数据到数据库
                self.mysql_session.commit()
                print('update数据库table_51job信息：%s：%s：%s' % (item['job_id'], item['job_name'], item['company_name']))
            except:
                print("该数据入库存在问题：%s" % item)
        else:
            try:
                # 插入数据
                self.mysql_session.add(data)
                # 提交数据到数据库
                self.mysql_session.commit()
                print('add数据库table_51job信息：%s：%s：%s' % (item['job_id'], item['job_name'], item['company_name']))
            except:
                print("该数据入库存在问题：%s" % item)

    # 数据的存储方法
    def sqlalchemy_jobList_data(self, item):
        # 存储的数据结构
        global dict
        data = job_list_51job(
            # 职位编号
            job_code = item['job_code'],
            # 职位方向
            job = item['job'],
            # 职位性质
            job_type = item['job_type'],
            # 职位性质
            job_area = item['job_area'],
            # 抓取时间
            crawl_time = item['crawl_time'],
        )
        query_result = self.mysql_session.query(table_51job).filter(table_51job.job_id == item['job_id']).first()
        if query_result:
            dict = dict(data)
            try:
                self.mysql_session.query(table_51job).filter(table_51job.job_id == item['job_id']).update(dict)
                # 提交数据到数据库
                self.mysql_session.commit()
                print('update数据库job_list_51job信息：%s：%s：%s：%s' % (item['job_code'], item['job'], item['job_type'], item['job_area']))
            except:
                print("该数据入库存在问题：%s" % item)
        else:
            try:
                # 插入数据
                self.mysql_session.add(data)
                # 提交数据到数据库
                self.mysql_session.commit()
                print('add数据库job_list_51job信息：%s：%s：%s：%s' % (item['job_code'], item['job'], item['job_type'], item['job_area']))
            except:
                print("该数据入库存在问题：%s" % item)


data_51job = data_51job()


if __name__ == '__main__':
    # 创建数据表
    table_51job.metadata.create_all(engine)
    # data = {'job_id': 120834385, 'job_name': '学徒（前端/PHP/网络推广/SEO/抖音运营/自媒体运营）', 'salary': '3-5千/月', 'workarea': '深圳-龙华区', 'attribute_text': "'深圳-龙华区', '在校生/应届生', '中专', '招若干人'", 'jobwelf': '五险一金 员工旅游 专业培训 绩效奖金 公司宿舍', 'job_code': 120, 'job': 'PHP开发工程师', 'job_type': '后端开发', 'job_area': '计算机/互联网/通信/电子', 'job_info': "['零基础也不怕！只要用心学！', '想学习互联网技术，别去培训机构交钱了！在这里可以免费学，做好工作还可以领钱，', '这里没有套路，只有以诚相待！', '1、身体健康、吃苦耐劳、服从管理；', '2、对互联网充满好奇，有志从事互联网行业工作；', '3、公司提供免费的技能培训，包学会；', '4、公司提供的免费培训岗位有：', '1）需求分析工程师', '2）产品助理', '3）UI设计师', '4）前端开发工程师', '5）PHP开发工程师', '6）测试工程师', '7）网络推广/SEO', '8）抖音运营', '9）自媒体运营', '5、公司提供免费的宿舍，主动申请完成公司的工作任务即可分钱；']", 'keyword': '前端开发,PHP开发,', 'contact_info': '坂田街道顺兴工业园', 'department': '', 'job_href': 'https://jobs.51job.com/shenzhen-lhq/120834385.html?s=01&t=0', 'company_name': '深圳掌邦科技有限公司', 'company_href': 'https://jobs.51job.com/all/co4693685.html', 'companytype': '民营公司', 'companysize': '少于50人', 'companyind': '计算机软件', 'company_info': '深圳掌邦科技有限公司（简称“掌邦科技”），是中国领先的企业电子商务服务运营商，一直致力于为中小企业提供互联网电子商务服务，包括网站建设、全网营销、域名注册等。', 'upload': '10-22', 'crawl_time': '2020--10--22 21:02:15'}
    # data_51job.sqlalchemy_insert_data(data)