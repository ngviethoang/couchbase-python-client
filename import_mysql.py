import sys
import os
import csv
import datetime
import pymysql

db = pymysql.connect(host="127.0.0.1", user="root", passwd="ngviethoang", db="netflix") 
cursor=db.cursor()

docs = []

DIR_NAME = 'datasets-netflix'

def main():

	read_movie_file()

	filenum = 1
	while(filenum <= 4):
		print('file {}'.format(filenum))
		#read_rating_file(filenum)
		filenum += 1

def read_rating_file(filenum):
	docs = {}
	doc = {}
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
		  			rating = int(values[1])
		  			# date = values[2].strip()

		  			id = str(filenum) + str(cnt)
			  		doc = [id, movie_id, customer_id, rating]

	                docs.append(doc)
            		del doc[:]

            		if cnt > 0 and cnt % 10000 == 0:
            			upsert_docs('ratings', docs)
            			del docs[:]
			cnt += 1
		upsert_docs('ratings', docs)
		del docs[:]

def read_movie_file():
	filename = 'movie_titles.csv'

	with open(os.path.join(DIR_NAME, filename), 'r') as csvfile:
	    readCSV = csv.reader(csvfile, delimiter=',')
	    for row in readCSV:
	        movie_id = row[0]
	        release_year = row[1]
	        title = row[2]
	        
	        doc = [movie_id, release_year, title]
	        if len(doc) == 3:
	        	docs.append(doc)
            del doc[:]
    	upsert_docs('movies', docs)
    	del docs[:]


def upsert_docs(bucket_name, docs):
	if bucket_name == 'movies':
		print(docs)
		docs.pop()
		cursor.executemany("insert into movies (id, release_year, title) values (%s, %s, %s)", docs)
	elif bucket_name == 'ratings':
		cursor.executemany("insert into ratings (id, movie_id, customer_id, rating) values (%s, %s, %s, %s)", docs)

main()
db.commit()
db.close()