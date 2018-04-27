import sys
import os
import csv
import datetime
from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError

USERNAME = 'Administrator'
PASSWORD = '123456'

DIR_NAME = 'datasets-netflix'

cluster = Cluster('couchbase://localhost')
cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))


def main():
    read_movie_file()

    filenum = 1
    id = 0
    while (filenum <= 4):
        print('file {}'.format(filenum))
        id = read_rating_file(filenum, id)
        id += 1
        filenum += 1


def read_rating_file(filenum, start_id):
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

            if cnt > 0 and cnt % 10000 == 0:
                upsert_docs('ratings', docs)
                docs.clear()

            cnt += 1
        upsert_docs('ratings', docs)
        docs.clear()

    print('{}'.format(cnt))
    return cnt


def read_movie_file():
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


def upsert_docs(bucket_name, docs):
    bucket = cluster.open_bucket(bucket_name)

    try:
        bucket.insert_multi(docs)
        print('insert bucket {} docs {}'.format(bucket_name, len(docs)))
    except CouchbaseError as exc:
        for k, res in exc.all_results.items():
            if res.success:
                print("Success")
            else:
                print("Key {0} failed with error code {1}".format(k, res.rc))
                print("Exception {0} would have been thrown".format(CouchbaseError.rc_to_exctype(res.rc)))


main()
