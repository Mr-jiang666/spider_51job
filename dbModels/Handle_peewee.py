# coding:utf-8
from peewee import *


db = MySQLDatabase("handle-51job",host ="127.0.0.1", port=3306, user="root", password="abc123456")


class BaseModel(Model):
    class Meta:
        database = db


class data_51job(BaseModel):
    # 岗位Id
    job_id = CharField(primary_key= True, max_length=10,verbose_name="岗位Id")
    # 岗位名称
    job_name  = CharField(max_length=50, default="", verbose_name="岗位名称")
    # 薪资
    salary = CharField(max_length=15, default="", verbose_name="薪资")
    # 工作地点
    work_area = CharField(max_length=15, default="", verbose_name="工作地点")
    # 招聘要求
    attribute_text = CharField(max_length=50, default="", verbose_name="招聘要求")
    # 福利待遇
    jobwelf = TextField(default="", verbose_name="福利待遇")
    # 职位编号
    job_code = IntegerField(default=0,verbose_name="职位编号")
    # 职位方向
    job = CharField(max_length=20, default="", verbose_name="职位方向")
    # 职位性质
    job_type = CharField(max_length=20, default="", verbose_name="职位性质")
    # 职位领域
    job_area = CharField(max_length=20, default="", verbose_name="职位领域")
    # 职位信息
    job_info = TextField(default="", verbose_name="职位信息")
    # 关键字
    keyword = CharField(max_length=100, default="", verbose_name="关键字")
    # 联系方式
    contact_info = CharField(max_length=100, default="", verbose_name="联系方式")
    # 公司名称
    company_name = CharField(max_length=25, default="", verbose_name="公司名称")
    # 公司类型
    companytype = CharField(max_length=25, default="", verbose_name="公司类型")
    # 公司领域
    companyind = CharField(max_length=25, default="", verbose_name="公司领域")
    # 公司规模
    companysize = CharField(max_length=25, default="", verbose_name="公司规模")
    # 公司信息
    company_info = TextField(default="", verbose_name="公司信息")
    # 职位链接
    job_url = CharField(default="", verbose_name="职位链接")
    # 公司链接
    company_url = CharField(default="", verbose_name="公司链接")
    # 发布时间
    upload = CharField(default="", max_length=30, verbose_name="发布时间")
    # 采集时间
    crawl_time = DateTimeField(verbose_name="采集时间")


class data_job_list(BaseModel):
    # 职位编号
    job_code = IntegerField(primary_key=True,verbose_name="职位编号")
    # 职位方向
    job = CharField(max_length=10, default="",verbose_name="职位方向")
    # 职位性质
    job_type = CharField(max_length=20, default="",verbose_name="职位性质")
    # 职位领域
    job_area = CharField(max_length=20, default="",verbose_name="职位领域")
    # 采集时间
    crawl_time = DateTimeField(verbose_name="采集时间")


def peewee_job_data(item):
    data = data_51job(
    # 岗位Id
    job_id = item['job_id'],
    # 岗位名称
    job_name = item['job_name'],
    # 薪资
    salary = item['salary'],
    # 工作地点
    work_area = item['workarea'],
    # 招聘要求
    attribute_text=item['attribute_text'],
    # 福利待遇
    jobwelf=item['jobwelf'],
    # 职位编号
    job_code=item['job_code'],
    # 职位方向
    job=item['job'],
    # 职位性质
    job_type=item['job_type'],
    # 职位性质
    job_area=item['job_area'],
    # 职位信息
    job_info=item['job_info'],
    # 关键字
    keyword=item['keyword'],
    # 联系方式
    contact_info=item['contact_info'],
    # 公司名称
    company_name = item['company_name'],
    # 公司类型
    companytype = item['companytype'],
    # 公司领域
    companyind = item['companyind'],
    # 公司规模
    companysize = item['companysize'],
    # 公司信息
    company_info=item['company_info'],
    # 职位链接
    job_url = item['job_href'],
    # 公司链接,
    company_url = item['company_href'],
    # 发布时间
    upload = item['upload'],
    # 采集时间
    crawl_time = item['crawl_time'],
    )
    try:
        existed_job = data_51job.select().where(data_51job.job_id == item['job_id']).get()
    except:
        existed_job = None
    if existed_job != None:
        try:
            data.update()
            print("update岗位信息：%s：%s：%s"%(item['job_id'],item['job_name'],item['company_name']))
        except:
            print("该数据入库存在问题：%s" % item)
    else:
        try:
            data.save(force_insert=True)
            print('add岗位信息：%s：%s：%s' %(item['job_id'],item['job_name'],item['company_name']))
        except:
            print("该数据入库存在问题：%s" % item)


def peewee_job_list_data(item):
    data = data_job_list(
        # 职位编号
        job_code=item['job_code'],
        # 职位方向
        job=item['job'],
        # 职位性质
        job_type=item['job_type'],
        # 职位性质
        job_area=item['job_area'],
        # 抓取时间
        crawl_time=item['crawl_time'],
    )
    try:
        existed_job = data_job_list.select().where(data_job_list.job_code == item['job_code']).get()
    except:
        existed_job = None
    if existed_job != None:
        try:
            data.update()
            print("update列表信息：%s：%s：%s"%(item['job_code'],item['job'],item['job_area']))
        except:
            print("该数据入库存在问题：%s" % item)
    else:
        try:
            data.save(force_insert=True)
            print('add列表信息：%s：%s：%s' %(item['job_code'],item['job'],item['job_area']))
        except:
            print("该数据入库存在问题：%s" % item)


if __name__ == '__main__':
    db.create_tables([data_51job,data_job_list])
    # data = {'job_id': '125982943', 'job_name': '运维工程师', 'job_href': 'https://jobs.51job.com/chongqing/125982943.html?s=01&t=0', 'salary': '5-9千/月', 'workarea': '重庆', 'attribute_text': "'重庆', '2年经验', '大专', '招1人'", 'jobwelf': '五险一金 专业培训 绩效奖金 弹性工作', 'company_name': '重庆替比网络科技有限公司', 'company_href': 'https://jobs.51job.com/all/co3352103.html', 'companytype': '民营公司', 'companysize': '少于50人', 'companyind': '计算机软件', 'upload': '10-09', 'crawl_time': '2020--10--09 17:24:12'}
    # peewee_insert_data(data)
