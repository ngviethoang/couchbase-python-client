from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
import datetime
from random import *

USERNAME = 'Administrator'
PASSWORD = '123456'

STORE_NUM = 100
CUSTOMERS_NUM = 1000

def main():
    cluster = Cluster('couchbase://localhost')
    cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))

    insertBucket(cluster, 'stores', STORE_NUM, 1)
    # insertBucket(cluster, 'customers', CUSTOMERS_NUM, 1)
    # insertBucket(cluster, 'orders', 10000, 10000)

def insertBucket(cluster, bucket, bulk_num, times):
    cluster.open_bucket(bucket)
    docs = {}
    doc = {}

    for t in range(0, times):
        for id in range(0, bulk_num):
            id += bulk_num * t

            if bucket == 'orders':
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
                        'id': id
                    }
                }

            docs.update(doc)
            doc.clear()
        try:
            bucket.insert_multi(docs)
            print(str(t + 1) + '/' + str(times))
        except CouchbaseError as exc:
            for k, res in exc.all_results.items():
                if res.success:
                    print("Success")
                else:
                    print("Key {0} failed with error code {1}".format(k, res.rc))
                    print("Exception {0} would have been thrown".format(
                        CouchbaseError.rc_to_exctype(res.rc)))
        docs.clear()

start = datetime.datetime.now()
main()
print("start: " + str(start))
print("end: " + str(datetime.datetime.now()))