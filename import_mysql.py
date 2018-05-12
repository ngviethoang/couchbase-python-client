import os
import csv
from random import *
import pymysql
from faker import Faker
import time

fake = Faker()

db = pymysql.connect(host="127.0.0.1", user="root", passwd="123456", db="movies")
cursor = db.cursor()

docs = []

DIR_NAME = 'datasets-netflix'


def main():
    # read_movie_file()
    # insert_customers()

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


def read_rating_file(filenum, start_id):
    total = 1000
    bulk = 1000

    cnt = start_id
    movie_id = 0
    filename = 'combined_data_' + str(filenum) + '.txt'
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

                    doc = [movie_id, customer_id[:4], rating]

                    docs.append(doc)

            if cnt > 0 and cnt % bulk == 0:
                print('docs {} {}'.format(len(docs), len(docs[0])))
                upsert_docs('ratings', docs)
            cnt += 1

            if cnt > total:
                return None

        print('docs {}'.format(len(docs)))
        upsert_docs('ratings', docs)
        return cnt


def read_movie_file():
    filename = 'movie_titles.csv'
    doc = []

    with open(os.path.join(DIR_NAME, filename), 'r') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            movie_id = row[0]
            try:
                release_year = int(row[1])
            except ValueError:
                release_year = None
            title = row[2]

            doc = [movie_id, release_year, title]
            if len(doc) == 3:
                docs.append(doc)
        del doc[:]

    print('docs {}'.format(len(docs)))
    upsert_docs('movies', docs)
    del docs[:]


def insert_customers():
    docs = []
    for id in range(0, 10000):
        doc = [fake.name(), fake.address()]
        docs.append(doc)

    print('docs {}'.format(len(docs)))
    upsert_docs('customers', docs)
    del docs[:]


def upsert_docs(bucket_name, docs):
    if bucket_name == 'movies':
        docs.pop()
        cursor.executemany("insert into movies (id, release_year, title) values (%s, %s, %s)", docs)
    elif bucket_name == 'ratings':
        cursor.executemany("insert into ratings (movie_id, customer_id, rating) values (%s, %s, %s)", docs)
    elif bucket_name == 'customers':
        cursor.executemany("insert into customers (name, address) values (%s, %s)", docs)

    db.commit()


def insert_ratings():
    total = 10000
    bulk = 1000
    run_time = 0

    for i in range(0, int(total / bulk)):
        docs = []
        for id in range(0, bulk):
            doc = [randint(1, 17769), randint(1, 10000), randint(1, 5)]
            docs.append(doc)

        print('docs {}'.format(len(docs)))

        start_time = time.time()
        upsert_docs('ratings', docs)
        r_time = time.time() - start_time
        print(r_time)
        run_time += r_time

        del docs[:]

    print('%s seconds' % run_time)


main()
db.close()
