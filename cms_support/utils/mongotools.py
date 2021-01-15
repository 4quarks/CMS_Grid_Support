# coding=utf-8
import pymongo
import os
from pymongo.collation import Collation
import json
from cms_support.utils.query_utils import Time
from cms_support.sites.vofeed import VOFeed


class MongoDB:
    def __init__(self):
        client = pymongo.MongoClient('mongodb://' + os.environ["MONGODB_HOST"])
        # DATABASE
        self.db = client.sitesupport
        # COLLECTIONS
        self.vofeed = self.db.vofeed

    @staticmethod
    def insert_one_document(collection, dict_to_insert):
        collection.insert_one(dict_to_insert)

    @staticmethod
    def insert_list_documents(collection, list_to_insert, delete_collection=False):
        if delete_collection:
            collection.drop()
        collection.insert(list_to_insert)

    @staticmethod
    def find_document(collection, query, project=None):
        # Collations --> sensitive find cases
        if not project:
            project = {"_id": 0}
        cursor = collection.find(query, project).collation(Collation(locale="en", strength=2))
        return [row for row in cursor]

    @staticmethod
    def find_unique_fields(collection, unique_field):
        """
        list of unique fields []
        """
        return collection.distinct(unique_field)

    @staticmethod
    def update_document(collection, find_by, dict_edit):
        """
        {'id':'147862'},{"$set":{"date_creation": "2020-07-15 00:00:00"}}
        """
        # Collations --> sensitive find cases
        query = {"$set": dict_edit}
        cursor = collection.update(find_by, query, True)


class SiteInfo:
    def __init__(self):
        self.mongo = MongoDB()
        time = Time(hours=24)
        self.vofeed = VOFeed(time)

    def write_json_all_resources(self):
        algo = self.mongo.find_document(self.mongo.vofeed, {})
        with open("sites_resources.json", "w") as outfile:
            json.dump(algo, outfile)

    def update_mongo_vofeed(self, site_name=""):
        all_resources = self.vofeed.get_site_resources("/.*{}.*/".format(site_name))
        self.mongo.insert_list_documents(self.mongo.vofeed, all_resources, delete_collection=True)

    def get_resource_filtered(self, flavour="", hostname="", site=""):
        query = {}
        if flavour:
            query.update({"flavour": {'$regex': flavour}})
        if hostname:
            query.update({"hostname": {'$regex': hostname}})
        if site:
            query.update({"site": {'$regex': site}})
        return self.mongo.find_document(self.mongo.vofeed, query)

    def get_list(self, field=""):
        """

        :param field: site|hostname|flavour
        :return:
        """
        unique_list = []
        if field:
            unique_list = self.mongo.find_unique_fields(self.mongo.vofeed, field)
        return unique_list


if __name__ == "__main__":
    # mongo = MongoDB()
    # query_ex = {"qty": 5}
    # algo = mongo.find_document(mongo.vofeed, query_ex)
    site_info = SiteInfo()
    # site_info.update_mongo_vofeed()
    print(site_info.write_json_all_resources())
