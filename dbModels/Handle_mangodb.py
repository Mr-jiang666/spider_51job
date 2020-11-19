import pymongo


class Mongo_client(object):
    def __init__(self):
        # myclient = pymongo.MongoClient("mongodb://192.168.110.129:27017")
        myclient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        # myclient.admin.authenticate("linxiaman", "abc123456")
        self.mydb = myclient['db_51job']

    def mango_insert_data(self,collection,item):
        mycollection = self.mydb[collection]
        item = dict(item)
        if collection == "collection_51job":
            try:
                result = mycollection.find_one({"job_id": item['job_id'],"job_code":item['job_code']})
            except:
                result = None
            if result:
                mycollection.find_one_and_delete({"job_id": item['job_id'],"job_code":item['job_code']})
                mycollection.insert_one(item)
                print("update岗位信息：%s" % item)
            else:
                mycollection.insert_one(item)
                print("add岗位信息：%s" % item)
        elif collection == "job_list":
            try:
                find = mycollection.find_one({"job_code": item['job_code']})
            except:
                find = None
            if find:
                mycollection.find_one_and_delete({"job_code": item['job_code']})
                mycollection.insert_one(item)
                print("update列表信息：%s" % item)
            else:
                mycollection.insert_one(item)
                print("add列表信息：%s" % item)
        elif collection == "keyword_51job":
            try:
                find = mycollection.find_one({"job_id": item['job_id']})
            except:
                find = None
            if find:
                mycollection.find_one_and_delete({"job_id": item['job_id']})
                mycollection.insert_one(item)
                print("update列表信息：%s" % item)
            else:
                mycollection.insert_one(item)
                print("add列表信息：%s" % item)

    def mongo_get_data(self,collection):
        mycollection = self.mydb[collection]
        result = mycollection.find_one_and_delete({})
        return result


mango = Mongo_client()


if __name__ == '__main__':
    data = mango.mongo_get_data("job_list")
    for item in data:
        print(item)