import sys  
import os
import csv
import datetime
from couchbase.cluster import Cluster, PasswordAuthenticator, CouchbaseError

USERNAME = 'Administrator'
PASSWORD = '123456'

DIR_NAME = 'netflix-prize-data'
docs = {}
doc = {}

cluster = Cluster('couchbase://localhost')
cluster.authenticate(PasswordAuthenticator(USERNAME, PASSWORD))

def main():

	read_movie_file()

	filenum = 1
	while(filenum <= 4):
		read_rating_file(filenum)
		filenum += 1

def read_rating_file(filenum):
	cnt = 0
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
		  			rating = values[1]
		  			date = values[2].strip()

		  			k = str(filenum) + str(cnt)
			  		doc = {
	                    k: {
	                    	'mid': movie_id,
	                        'cid': customer_id,
	                        'r': rating,
	                        'd': date
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

def read_movie_file():
	filename = 'movie_titles.csv'

	with open(os.path.join(DIR_NAME, filename), 'r') as csvfile:
	    readCSV = csv.reader(csvfile, delimiter=',')
	    for row in readCSV:
	        movie_id = row[0]
	        release_year = row[1]
	        title = row[2]
	        
	        doc = {
	        	str(movie_id): {
	        		'ry': release_year,
	        		't': title
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
        print('insert bucket {}'.format(bucket_name))
    except CouchbaseError as exc:
        for k, res in exc.all_results.items():
            if res.success:
                print("Success")
            else:
                print("Key {0} failed with error code {1}".format(k, res.rc))
                print("Exception {0} would have been thrown".format(CouchbaseError.rc_to_exctype(res.rc)))

main()