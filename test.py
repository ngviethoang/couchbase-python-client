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

    insert_bucket(cluster, 'stores', STORE_NUM, 1)
    insert_bucket(cluster, 'customers', CUSTOMERS_NUM, 1)
    # insert_bucket(cluster, 'orders', 10000, 10000)


def insert_bucket(cluster, bucket_name, bulk_num, times):
    bucket = cluster.open_bucket(bucket_name)
    docs = {}

    for t in range(0, times):
        # update to docs array
        for id in range(0, bulk_num):
            id += bulk_num * t

            if bucket_name == 'orders':
                doc = {
                    str(id): {
                        'id': id,
                        'store_id': randint(0, STORE_NUM),
                        'customer_id': randint(0, CUSTOMERS_NUM)
                    }
                }
            else:
                doc = {
                    str(id): {
                        'id': id,
                        'name': fake.name() if bucket_name == 'customers' else fake.address()
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