from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
import datetime

USERNAME = 'admin'
PASSWORD = '123456'
BUCKET = 'test'

def main():
    cluster = Cluster('couchbase://localhost')
    cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))
    bucket = cluster.open_bucket(BUCKET)

    docs = {}

    total = 100000000
    bulk_num = 10000

    for t in range(0, int(total/bulk_num)):
        for id in range(0, bulk_num):
            id += bulk_num * t
            docs.update({str(id): {'id': id}})
        try:
            bucket.insert_multi(docs)
            print(t)
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