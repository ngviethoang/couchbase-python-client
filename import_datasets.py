import os
import csv
from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError
from faker import Faker
import time
from random import *

fake = Faker()

USERNAME = 'Administrator'
PASSWORD = '123456'

DIR_NAME = 'datasets-netflix'

cluster = Cluster('couchbase://localhost')
cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))


def main():
    # read_movie_file()

    insert_ratings()

    # filenum = 1
    # id = 0
    # while filenum <= 4:
    #     print('file {}'.format(filenum))
    #     id = read_rating_file(filenum, id)
    #     if id is None:
    #         break
    #     id += 1
    #     filenum += 1


def insert_ratings():
    total = 100000000
    bulk = 100000
    run_time = 0

    for i in range(0, int(total / bulk)):
        docs = {}
        print(str(i))

        cnt = i * bulk
        for id in range(0, bulk):
            doc = {
                str(cnt): {
                    'mid': str(randint(1, 17769)),
                    'cid': str(randint(1, 10000)),
                    'r': randint(1, 5)
                }
            }
            cnt += 1
            docs.update(doc)
            doc.clear()

        print('docs {}'.format(len(docs)))

        r_time = upsert_docs('ratings', docs, run_time)
        run_time += r_time

        docs.clear()

    print('%s seconds' % run_time)


def read_rating_file(filenum, start_id):
    total = 100000
    bulk = 5000

    docs = {}
    doc = {}
    cnt = start_id
    movie_id = 0
    filename = 'combined_data_' + str(filenum) + '.txt'

    print('{}'.format(cnt))

    with open(os.path.join(DIR_NAME, filename), 'r') as fp:
        for line in fp:
            if ':' in line:
                movie_id = line.split(':')[0]
            else:
                values = line.split(',')
                if len(values) == 3:
                    customer_id = values[0]
                    rating = int(values[1])
                    # date = values[2].strip()

                    doc = {
                        str(cnt): {
                            'mid': movie_id,
                            'cid': customer_id[:4],
                            'r': rating,
                            # 'd': date
                        }
                    }

            docs.update(doc)
            doc.clear()

            if cnt > 0 and cnt % bulk == 0:
                upsert_docs('ratings', docs)
                docs.clear()

            cnt += 1

            if cnt > total:
                return None

        upsert_docs('ratings', docs)
        docs.clear()

    print('{}'.format(cnt))
    return cnt


def read_movie_file():
    docs = {}
    doc = {}
    filename = 'movie_titles.csv'

    with open(os.path.join(DIR_NAME, filename), 'r') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            movie_id = row[0]
            try:
                release_year = int(row[1])
            except ValueError:
                release_year = None
            title = row[2]

            doc = {
                str(movie_id): {
                    'r_year': release_year,
                    'title': title
                }
            }
            docs.update(doc)
        doc.clear()
    upsert_docs('movies', docs)
    docs.clear()


def upsert_docs(bucket_name, docs, run_time=None):
    bucket = cluster.open_bucket(bucket_name)
    r_time = 0

    try:
        start_time = time.time()

        bucket.insert_multi(docs)

        r_time = time.time() - start_time
        print(r_time)

        print('insert bucket {} docs {}'.format(bucket_name, len(docs)))
    except CouchbaseError as exc:
        print('run time %s' % str(run_time))
        # for k, res in exc.all_results.items():
        #     if res.success:
        #         print("Success")
        #     else:
        #         print("Key {0} failed with error code {1}".format(k, res.rc))
        #         print("Exception {0} would have been thrown".format(CouchbaseError.rc_to_exctype(res.rc)))

    return r_time


main()
