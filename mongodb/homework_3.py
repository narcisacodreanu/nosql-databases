import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient()

# command used to import the BSON data: mongorestore -d moviesdb movies.bson 

database = client.moviesdb

movies = database.movies

'''
A. Update all movies in your genre that have a "NOT RATED" 
value at the "rated" key to have a value of "Pending rating" instead. 
The operation must be in-place and atomic at the document-level.
'''

movies.update_many(
	{ 'genres':'Documentary', 'rated' : 'UNRATED' },
	{ '$set': {'rated' : 'Pending rating'}}
	)

# for movie in movies.find({'genres':'Documentary', 'rated' : 'Pending rating'}):
# 	pprint.pprint(movie)

'''
B. Find a movie with your genre in imdb (Links to an external site.)Links to an 
external site. and insert it into your database with the fields listed below. 
Each field type must match that of the other documents in the collection. For example, 
the genres field is an array, so your new document must also represent the genres as an array.
'''

new_movie = {
	'title' : 'Pandas',
	'year' : 2018,
	'countries' : ['USA'],
	'genres' : ['Documentary', 'Short'],
	'directors' : ['David Douglas', 'Drew Fellman'],
	'imdb' : {
		'id' : 4444444,
		'rating' : 7.5,
		'votes' : 40
	}

}
movies.insert_one(new_movie)

# for movie in movies.find({'title':'Pandas'}):
# 	pprint.pprint(movie)


'''
C. Use the aggregation framework to find the total number of movies in your genre. 
The result of the aggregation should be one single document in this format: 
[{"_id"=>"Comedy", "count"=>14046}]
'''

result_documentaries = movies.aggregate([
	{'$match' : {'genres':'Documentary'}},
	{'$group' : {'_id' : 'Documentary', 'count' : {'$sum': 1}}}
	])

for elem in result_documentaries:
	pprint.pprint(elem)

'''
D. Use the aggregation framework to find the number of movies made in the country 
you were born in with a rating of "Pending rating". The document result from the 
aggregation must be in the form:

# When country is Hungary:
# => [{"_id"=>{"country"=>"Hungary", "rating"=>"Pending rating"}, "count"=>9}]
'''

# Note that for Romania it unfortunately does not find anything. :(

# result_country = movies.aggregate([
# 	{'$match' : {'countries':'Romania', 'rated' : 'Pending rating'}},
# 	{'$group' : {'_id' : {'country' : 'Romania', 'rating' : 'Pending rating'}, 'count' : {'$sum': 1}}}
# 	])

# for elem in result_country:
# 	pprint.pprint(elem)

# I left it as USA to show that it works.
result_country_USA = movies.aggregate([
	{'$match' : {'countries':'USA', 'rated' : 'Pending rating'}},
	{'$group' : {'_id' : {'country' : 'USA', 'rating' : 'Pending rating'}, 'count' : {'$sum': 1}}}
	])

for elem in result_country_USA:
	pprint.pprint(elem)

'''
E. Create an example using the $lookup pipeline operator. You'll have to add a few documents to 
one collection, add some documents to another collection, and then do an aggregation with the $lookup 
on a field that you define. The examples fields can be anything, you just have to set up the data 
and return at least two results from the lookup.
'''
# create a collection in our database to hold information about some tech companies
database.techcompanies.insert([
	{ 'name' : 'Microsoft', 'founded' : 1975},
	{ 'name' : 'Spotify', 'founded' : 2006},
	{ 'name' : 'WeWork', 'founded' : 2010},
	{ 'name' : 'Google', 'founded' : 1998},
	])

# create another collection that stores some cities in the US and what companies have headquarters there
# note that this is not completely accurate
database.cities.insert([
	{ 'city' : 'New York', 'companies' : ['Spotify', 'Google', 'WeWork']},
	{ 'city' : 'Seattle', 'companies' : ['Microsoft', 'Google']},
	{ 'city' : 'Houston', 'companies' : ['Microsoft']},
	{ 'city' : 'Los Angeles', 'companies' : ['Google', 'WeWork']}
	])

# perform a lookup to see all the headquarters of each company in our place
result_lookup = database.techcompanies.aggregate([
   {
     '$lookup':
       {
         'from' : 'cities',
         'localField' : 'name',
         'foreignField' : 'companies',
         'as' : 'company_headquarters'
       }
  }
])

for elem in result_lookup:
	pprint.pprint(elem)
