import pymongo
import os
from pymongo.collation import Collation


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
    def update_document(collection, find_by, dict_edit):
        """
        {'id':'147862'},{"$set":{"date_creation": "2020-07-15 00:00:00"}}
        """
        # Collations --> sensitive find cases
        query = {"$set": dict_edit}
        cursor = collection.update(find_by, query, True)


if __name__ == "__main__":
    mongo = MongoDB()
    query_ex = {"qty": 5}
    algo = mongo.find_document(mongo.vofeed, query_ex)
