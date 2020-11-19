import json
import re
import time

from lxml import etree
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dbModels.Handle_mangodb import mango


class Handle_jobList():
    def __init__(self):
        chrome_option = Options()
        chrome_option.add_argument('--headless')
        self.driver = webdriver.Chrome(options=chrome_option)
        self.url = "https://search.51job.com/list/000000,000000,0106,00,9,99,+,2,1.html?ord_field=1"
        self.flag = True
        self.num = {
            "area_li":1,
            "worktype_tbody":1,
            "worktype_td": 1,
            "jobname_tr": 1,
            "jobname_td": 1,
        }

    # 获取职位领域
    def parse_area(self):
        try:
            self.driver.find_element_by_xpath("//div[@class='j_search_in']/div[@class='e_e e_com'][1]/p[@class='at']").click()
        except:
            pass
        html_one = etree.HTML(self.driver.page_source)
        all_area = html_one.xpath("//div[@id='popop']//ul/li")
        if self.num["area_li"] <= int(len(all_area)):
            self.driver.find_element_by_xpath("//div[@id='popop']//ul/li[{0}]".format(self.num["area_li"])).click()
            area_name = html_one.xpath("//div[@id='popop']//ul/li[{0}]/text()".format(self.num["area_li"]))[0].replace(" ", "").replace("\n", "")
            self.parse_worktype(html_one, area_name)
        else:
            self.flag = False

    # 获取工作类型
    def parse_worktype(self,html_one,area_name):
        all_tbody = html_one.xpath("//div[@class='de d3']/div/table/tbody")
        if self.num["worktype_tbody"] <= int(len(all_tbody)):
            all_td = html_one.xpath("//div[@class='de d3']/div/table/tbody[{0}]/tr[1]/td[@class='js_more']".format(self.num["worktype_tbody"]))
            if self.num["worktype_td"] <= int(len(all_td)):
                self.driver.find_element_by_xpath("//div[@class='de d3']/div/table/tbody[{0}]/tr[1]/td[@class='js_more'][{1}]/em".format(self.num["worktype_tbody"],self.num["worktype_td"])).click()
                worktype= html_one.xpath("//div[@class='de d3']/div/table/tbody[{0}]/tr[1]/td[@class='js_more'][{1}]/em/text()".format(self.num["worktype_tbody"],self.num["worktype_td"]))[0].replace(" ","").replace("\n","")
                html_two = etree.HTML(self.driver.page_source)
                self.parse_jobname(html_two,worktype,area_name)
            else:
                self.num["worktype_td"] = 1
                self.num["worktype_tbody"] += 1
        else:
            self.num["worktype_tbody"] = 1
            self.num["area_li"] += 1

    # 获取岗位
    def parse_jobname(self,html_two,worktype,area_name):
        all_jobname_tr = html_two.xpath("//div[@class='de d3']/div/table/tbody/tr[2]/td/div/table/tbody/tr")
        if self.num["jobname_tr"] <= int(len(all_jobname_tr)):
            all_tr_td = html_two.xpath("//div[@class='de d3']/div/table/tbody/tr[2]/td/div/table/tbody/tr[{0}]/td[@class='js_more']".format(self.num["jobname_tr"]))
            if self.num["jobname_td"] <= int(len(all_tr_td)):
                job_name = html_two.xpath("//div[@class='de d3']/div/table/tbody/tr[2]/td/div/table/tbody/tr[{0}]/td[@class='js_more'][{1}]/em/text()".format(self.num["jobname_tr"],self.num["jobname_td"]))[0].replace(" ", "").replace("\n", "")
                if job_name != "所有":
                    try:
                        self.driver.find_element_by_xpath("//div[@class='tin']/span[@class='ttag']").click()
                    except:
                        pass
                    self.driver.find_element_by_xpath("//div[@class='de d3']/div/table/tbody/tr[2]/td/div/table/tbody/tr[{0}]/td[@class='js_more'][{1}]/em".format(self.num["jobname_tr"],self.num["jobname_td"])).click()
                    self.driver.find_element_by_xpath("//div[@class='panel_lnp panel_py panel_ct2']/div[@class='but_box']/span").click()
                    self.driver.find_element_by_xpath("//div[@class='e_e e_but']/button").click()
                    time.sleep(1)
                    self.driver.find_element_by_xpath("//span[@event-type='16']").click()
                    time.sleep(1)
                    self.parse_data(worktype,area_name,job_name)
                    self.num["jobname_td"] += 1
                else:
                    self.num["jobname_td"] += 1
            else:
                self.num["jobname_td"] = 1
                self.num["jobname_tr"] += 1
        else:
            self.num["jobname_tr"] = 1
            self.num["worktype_td"] += 1

    # 获取岗位对应的编码
    def parse_data(self,worktype,area_name,job_name):
        now_url = self.driver.current_url
        self.driver.refresh()
        page_source = self.driver.page_source
        pattern_code = re.compile('https://search.51job.com/list/000000,000000,(.*?),00,9,99,')
        pattern_text = r"window\.__SEARCH_RESULT__ = (.*?)\</script\>"
        result = re.findall(pattern_text,page_source)[0]
        json_data = json.loads(result)
        info = {}
        # 工作编号
        info['job_code'] = int(pattern_code.search(now_url).group(1))
        # 工作名称
        info['job'] = job_name
        # 工作类型
        info['worktype'] = worktype
        # 工作领域
        info['area_name'] = area_name
        # 总页数
        info['total_page'] = json_data['total_page']
        # 总数据量
        info['jobid_count'] = json_data['jobid_count']
        # 采集时间
        info['crawl_time'] = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(time.time()))
        # print(info)
        mango.mango_insert_data("job_list",info)

    # 主方法
    def main(self):
        self.driver.maximize_window()
        self.driver.get(self.url)
        while self.flag:
            self.parse_area()


if __name__ == '__main__':
    jobList = Handle_jobList()
    jobList.main()
