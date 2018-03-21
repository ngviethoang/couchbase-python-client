from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
import datetime
import string
from random import *

USERNAME = 'Administrator'
PASSWORD = '123456'
BUCKET = 'test'

def main():
    cluster = Cluster('couchbase://localhost')
    cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))
    bucket = cluster.open_bucket(BUCKET)

    docs = {}
    doc = {}

    bulk_num = 10000
    tt = 10000

    for t in range(0, tt):
        for id in range(0, bulk_num):
            id += bulk_num * t
            doc = {str(id): {'id': id}}
            docs.update(doc)
            doc.clear()
        try:
            bucket.insert_multi(docs)
            print(str(t + 1) + '/' + str(tt))
        except CouchbaseError as exc:
            for k, res in exc.all_results.items():
                if res.success:
                    print("Success")
                else:
                    print("Key {0} failed with error code {1}".format(k, res.rc))
                    print("Exception {0} would have been thrown".format(
                        CouchbaseError.rc_to_exctype(res.rc)))
        docs.clear()

def rand_str(min_char, max_char):
    allchar = string.ascii_letters
# + string.punctuation + string.digits
    return "".join(choice(allchar) for x in range(randint(min_char, max_char)))

start = datetime.datetime.now()
main()
print("start: " + str(start))
print("end: " + str(datetime.datetime.now()))
