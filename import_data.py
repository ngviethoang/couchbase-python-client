from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
import datetime
from random import *
from faker import Faker

USERNAME = 'Administrator'
PASSWORD = '123456'

STORE_NUM = 100
CUSTOMERS_NUM = 1000

fake = Faker()

def main():
	cluster = Cluster('couchbase://localhost')
    cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))

    insert_bucket(cluster, 'stores', STORE_NUM, 1, 0)
    insert_bucket(cluster, 'customers', CUSTOMERS_NUM, 1, 0)
    insert_bucket(cluster, 'orders', 10000, 10000, 0)


def insert_bucket(cluster, bucket_name, bulk_num, times, start):
    bucket = cluster.open_bucket(bucket_name)
    docs = {}

    for t in range(start, times):
        # update to docs array
        for id in range(0, bulk_num):
            id += bulk_num * t

            if bucket_name.startswith('orders'):
                doc = {
                    str(id): {
                        #'id': id,
                        'sId': randint(0, STORE_NUM - 1),
                        'cId': randint(0, CUSTOMERS_NUM - 1),
                        'price': round(uniform(0.5, 100), 2)
                    }
                }
            else if bucket_name == 'customers':
                doc = {
                    str(id): {
                        #'id': id,
                        'name': fake.name()
                    }
                }
            else: #stores
            	doc = {
                    str(id): {
                        #'id': id,
                        'name': fake.text()[:randint(10, 20)],
                        'address': fake.address()
                    }
                }
            docs.update(doc)
            doc.clear()
        # insert into db
        try:
            bucket.insert_multi(docs)
            print(str(t + 1) + '/' + str(times))
        except CouchbaseError as exc:
            for k, res in exc.all_results.items():
                if res.success:
                    print("Success")
                else:
                    print("Key {0} failed with error code {1}".format(k, res.rc))
                    print("Exception {0} would have been thrown".format(CouchbaseError.rc_to_exctype(res.rc)))
        docs.clear()


start = datetime.datetime.now()
main()
print("start: " + str(start))
print("end: " + str(datetime.datetime.now()))
