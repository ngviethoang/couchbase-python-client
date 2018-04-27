from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
import datetime
from random import *
from faker import Faker

USERNAME = 'Administrator'
PASSWORD = '123456'

STORE_NUM = 1000
CUSTOMERS_NUM = 10000

fake = Faker()

def main():
    cluster = Cluster('couchbase://localhost')
    cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))

    # insert_bucket(cluster, 'stores', STORE_NUM, 1, 0)
    insert_bucket(cluster, 'customers', CUSTOMERS_NUM, 1, 0)
    #insert_bucket(cluster, 'orders', 10000, 10000, 0)


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
                        'sid': str(randint(0, STORE_NUM - 1)),
                        'cid': str(randint(0, CUSTOMERS_NUM - 1)),
                        'payment': round(uniform(0.5, 500), 2)
                    }
                }
            elif bucket_name == 'customers':
                doc = {
                    str(id): {
                        #'id': id,
                        'name': fake.name(),
                        'address': fake.address()
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
            print('{}/{}'.format(t + 1, times))
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
print('start: {}'.format(start))
print('end: {}'.format(datetime.datetime.now()))
