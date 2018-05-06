# Put the use case you chose here. Then justify your database choice:
# Photo app - using neo4j
# using python3
#
# Explain what will happen if coffee is spilled on one of the servers in your cluster, causing it to go down.
# fine to just write text
#
# What data is it not ok to lose in your app? What can you do in your commands to mitigate the risk of lost data?
# search with reobusness-related things in the documentation
#

from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

# Helper methods

# NODE METHODS
# method used to delete/clear everything in the database
def delete_graph():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run("MATCH(n) "
                   "DETACH DELETE n ")

# method used to create a user; takes in the username, which is a unique identifier
def create_user(tx, username, name, age):
    for record in tx.run("CREATE (a:User {username: $username, name: $name, age: $age})"
    	   				"RETURN a.username, a.name, a.age", 
    	   				username=username, name=name, age=age):
    	print('User: ', record["a.username"], record["a.name"], record["a.age"])

# method used to create a photo; takes in the unique id, location, caption, date
def create_photo(tx, id, location, caption, date):
    for record in tx.run("CREATE (a:Photo {id: $id, location: $location, caption: $caption, date : $date})"
    	   				"RETURN a.id, a.location, a.caption, a.date", 
    	   				id=id, location=location, caption=caption, date=date):
    	print('Photo: ', record["a.id"], record["a.location"], record["a.caption"], record["a.date"])

# method used to create a video; takes in the unique id, location, caption, date
def create_video(tx, id, location, caption, date):
    for record in tx.run("CREATE (a:Video {id: $id, location: $location, caption: $caption, date : $date})"
    	   				"RETURN a.id, a.location, a.caption, a.date", 
    	   				id=id, location=location, caption=caption, date=date):
    	print('Video: ', record["a.id"], record["a.location"], record["a.caption"], record["a.date"])

# method used to create a comment; takes in the unique id, content, date
def create_comment(tx, id, content, date):
    for record in tx.run("CREATE (a:Comment {id: $id, content: $content, date : $date})"
    	   				"RETURN a.id, a.content, a.date", 
    	   				id=id, content=content, date=date):
    	print('Comment: ', record["a.id"], record["a.content"], record["a.date"])

# method used to create a message; takes in the unique id, content, date
def create_message(tx, id, content, date):
    for record in tx.run("CREATE (a:Message {id: $id, content: $content, date : $date})"
    	   				"RETURN a.id, a.content, a.date", 
    	   				id=id, content=content, date=date):
    	print('Message: ', record["a.id"], record["a.content"], record["a.date"])

# method used to create a collection; takes in the unique id, name of collection
def create_collection(tx, id, name):
    for record in tx.run("CREATE (a:Collection {id: $id, name: $name})"
    	   				"RETURN a.id, a.name", 
    	   				id=id, name=name):
    	print('Collection: ', record["a.id"], record["a.name"])

# method used to create a filter; takes in the name of filter
def create_filter(tx, name):
    for record in tx.run("CREATE (a:Filter {name: $name})"
    	   				"RETURN a.name", 
    	   				name=name):
    	print('Filer: ', record["a.name"])


# RELATIONSHIP METHODS

# method used to create a follow relationship
def follow(tx, username_a, username_b):
    for record in tx.run("MATCH (a:User),(b:User) "
                         "WHERE a.username = $username_a AND b.username = $username_b "
                         "CREATE (a)-[r:FOLLOWS]->(b) "
                         "RETURN a.username, type(r), b.username", username_a = username_a, username_b = username_b):
        print(record["a.username"], record["type(r)"], record["b.username"])

# method used to create posting a photo relationship; passing the photo id and user who is posting
def post_photo(tx, photo_id, username):
    for record in tx.run("MATCH (a:Photo),(b:User) "
                         "WHERE a.id = $photo_id AND b.username = $username "
                         "CREATE (a)-[r:POSTED_BY]->(b) "
                         "RETURN a.id, type(r), b.username", photo_id = photo_id, username = username):
        print(record["a.id"], record["type(r)"], record["b.username"])

# method used to create posting a video relationship; passing the video id and user who is posting
def post_video(tx, video_id, username):
    for record in tx.run("MATCH (a:Video),(b:User) "
                         "WHERE a.id = $video_id AND b.username = $username "
                         "CREATE (a)-[r:POSTED_BY]->(b) "
                         "RETURN a.id, type(r), b.username", video_id = video_id, username = username):
        print(record["a.id"], record["type(r)"], record["b.username"])

# method used to create posting a video comment relationship
def write_video_comment(tx, comment_id, username, video_id):
	# commented by user
    for record in tx.run("MATCH (a:Comment),(b:User) "
                         "WHERE a.id = $comment_id AND b.username = $username "
                         "CREATE (a)-[r:COMMENT_BY]->(b) "
                         "RETURN a.id, type(r), b.username", comment_id = comment_id, username = username):
        print(record["a.id"], record["type(r)"], record["b.username"])
    # commented about video
    for record in tx.run("MATCH (a:Comment),(b:Video) "
                     "WHERE a.id = $comment_id AND b.id = $video_id "
                     "CREATE (a)-[r:COMMENT_ABOUT]->(b) "
                     "RETURN a.id, type(r), b.id", comment_id = comment_id, video_id = video_id):
    	print(record["a.id"], record["type(r)"], record["b.id"])

# method used to create posting a photo comment relationship
def write_photo_comment(tx, comment_id, username, photo_id):
	# commented by user
    for record in tx.run("MATCH (a:Comment),(b:User) "
                         "WHERE a.id = $comment_id AND b.username = $username "
                         "CREATE (a)-[r:COMMENT_BY]->(b) "
                         "RETURN a.id, type(r), b.username", comment_id = comment_id, username = username):
        print(record["a.id"], record["type(r)"], record["b.username"])
    # commented about photo
    for record in tx.run("MATCH (a:Comment),(b:Photo) "
                     "WHERE a.id = $comment_id AND b.id = $photo_id "
                     "CREATE (a)-[r:COMMENT_ABOUT]->(b) "
                     "RETURN a.id, type(r), b.id", comment_id = comment_id, photo_id = photo_id):
    	print(record["a.id"], record["type(r)"], record["b.id"])

# method used to create a photo like relationship 
def like_photo(tx, username, photo_id):
	# user likes photo
    for record in tx.run("MATCH (a:User),(b:Photo) "
                         "WHERE a.username = $username AND b.id = $photo_id "
                         "CREATE (a)-[r:LIKES]->(b) "
                         "RETURN a.username, type(r), b.id", username = username, photo_id=photo_id):
        print(record["a.username"], record["type(r)"], record["b.id"])
    # photo liked by user 
    for record in tx.run("MATCH (a:Photo),(b:User) "
                     "WHERE a.id = $photo_id AND b.username = $username "
                     "CREATE (a)-[r:LIKED_BY]->(b) "
                     "RETURN a.id, type(r), b.username", photo_id = photo_id, username = username):
    	print(record["a.id"], record["type(r)"], record["b.username"])

# method used to create a video like relationship 
def like_video(tx, username, video_id):
	# user likes video
    for record in tx.run("MATCH (a:User),(b:Video) "
                         "WHERE a.username = $username AND b.id = $video_id "
                         "CREATE (a)-[r:LIKES]->(b) "
                         "RETURN a.username, type(r), b.id", username = username, video_id=video_id):
        print(record["a.username"], record["type(r)"], record["b.id"])
    # video liked by user 
    for record in tx.run("MATCH (a:Video),(b:User) "
                     "WHERE a.id = $video_id AND b.username = $username "
                     "CREATE (a)-[r:LIKED_BY]->(b) "
                     "RETURN a.id, type(r), b.username", video_id = video_id, username = username):
    	print(record["a.id"], record["type(r)"], record["b.username"])

# method used to send a message 
def send_message(tx, message_id, username_a, username_b):
	# sent from
    for record in tx.run("MATCH (a:Message),(b:User) "
                         "WHERE a.id = $message_id AND b.username = $username_a "
                         "CREATE (a)-[r:SENT_FROM]->(b) "
                         "RETURN a.id, type(r), b.username", message_id = message_id, username_a = username_a):
        print(record["a.id"], record["type(r)"], record["b.username"])
	# sent to
    for record in tx.run("MATCH (a:Message),(b:User) "
                         "WHERE a.id = $message_id AND b.username = $username_b "
                         "CREATE (a)-[r:SENT_TO]->(b) "
                         "RETURN a.id, type(r), b.username", message_id = message_id, username_b = username_b):
        print(record["a.id"], record["type(r)"], record["b.username"])

def appropriate_collection(tx, collection_id, username):
    for record in tx.run("MATCH (a:Collection),(b:User) "
                         "WHERE a.id = $collection_id AND b.username = $username "
                         "CREATE (a)-[r:CREATED_BY]->(b) "
                         "RETURN a.id, type(r), b.username", collection_id = collection_id, username = username):
        print(record["a.id"], record["type(r)"], record["b.username"])

# add a photo to a collection
def add_photo_to_collection(tx, photo_id, collection_id):
    for record in tx.run("MATCH (a:Photo),(b:Collection) "
                         "WHERE a.id = $photo_id AND b.id = $collection_id "
                         "CREATE (a)-[r:BELONGS_TO]->(b) "
                         "RETURN a.id, type(r), b.name", photo_id = photo_id, collection_id = collection_id):
        print(record["a.id"], record["type(r)"], record["b.name"])

# add a video to a collection
def add_video_to_collection(tx, video_id, collection_id):
    for record in tx.run("MATCH (a:Video),(b:Collection) "
                         "WHERE a.id = $video_id AND b.id = $collection_id "
                         "CREATE (a)-[r:BELONGS_TO]->(b) "
                         "RETURN a.id, type(r), b.name", video_id = video_id, collection_id = collection_id):
        print(record["a.id"], record["type(r)"], record["b.name"])

# use filter on photo 
def use_filter_on_photo(tx, filter_name, photo_id):
    for record in tx.run("MATCH (a:Filter),(b:Photo) "
                         "WHERE a.name = $filter_name AND b.id = $photo_id "
                         "CREATE (a)-[r:USED_ON]->(b) "
                         "RETURN a.name, type(r), b.id", filter_name = filter_name, photo_id = photo_id):
        print(record["a.name"], record["type(r)"], record["b.id"])	

# Set up the database

print('===========================\n'+
	'Setting up the database\n'+
	'===========================')

delete_graph()

# session to create the users
with driver.session() as session_a:
	session_a.write_transaction(create_user, 'narcisa.c', 'Narcisa', 23)
	session_a.write_transaction(create_user, 'theodorablecorgi', 'Theo', 1)
	session_a.write_transaction(create_user, 'annarocklady', 'Anna', 22)

# session to create photos and videos
with driver.session() as session_b:
	session_b.write_transaction(create_photo, '1', 'New York', 'debacch', '04/07/2018')
	session_b.write_transaction(create_photo, '2', 'New York', 'Lazy pup <3', '05/05/2018')	
	session_b.write_transaction(create_photo, '3', 'New York', 'We didn\'t coordinate outfits', '04/07/2018')	
	session_b.write_transaction(create_video, '4', 'New York', 'Happy National Puppy Day!', '03/23/2018')

# session to create comments
with driver.session() as session_c:
	session_c.write_transaction(create_comment, '5', 'love corgis!!!', '04/01/2018')
	session_c.write_transaction(create_comment, '6', 'this photo of us is great!', '04/07/2018')

# session to create messages and collections
with driver.session() as session_d:
	session_d.write_transaction(create_message, '12', 'hey, you still coming to the show?', '05/04/2018')
	session_d.write_transaction(create_collection, '13', 'Puppies')

# session to create filters

with driver.session() as session_e:
	session_e.write_transaction(create_filter, 'Clarendon')
	session_e.write_transaction(create_filter, 'Mayfair')

# session to create relationships 

with driver.session() as session_f:
	# follow relationships
	session_f.write_transaction(follow, 'narcisa.c', 'annarocklady')
	session_f.write_transaction(follow, 'narcisa.c', 'theodorablecorgi')
	session_f.write_transaction(follow, 'annarocklady', 'theodorablecorgi')
	session_f.write_transaction(follow, 'annarocklady', 'narcisa.c')
	# posting relationships
	session_f.write_transaction(post_photo, '1', 'narcisa.c')
	session_f.write_transaction(post_photo, '2', 'theodorablecorgi') 
	session_f.write_transaction(post_photo, '3', 'annarocklady')
	session_f.write_transaction(post_video, '4', 'theodorablecorgi')
	# commenting and liking
	session_f.write_transaction(write_video_comment, '5', 'narcisa.c', '4')
	session_f.write_transaction(write_photo_comment, '6', 'annarocklady', '1')
	session_f.write_transaction(like_photo, 'narcisa.c', '3')	
	session_f.write_transaction(like_video, 'annarocklady', '4')
	session_f.write_transaction(like_video, 'narcisa.c', '4')
	session_f.write_transaction(like_photo, 'annarocklady', '1')
	session_f.write_transaction(like_video, 'annarocklady', '2')
	# sending messages
	session_f.write_transaction(send_message, '12', 'annarocklady', 'narcisa.c')
	# create collection and add a photo a video
	session_f.write_transaction(appropriate_collection, '13', 'narcisa.c')	
	session_f.write_transaction(add_photo_to_collection, '2', '13')
	session_f.write_transaction(add_video_to_collection, '4', '13')
	# use filters 
	session_f.write_transaction(use_filter_on_photo, 'Clarendon', '2')
	session_f.write_transaction(use_filter_on_photo, 'Mayfair', '1')
	session_f.write_transaction(use_filter_on_photo, 'Mayfair', '3')	

print('===========================\n'+
	'Done setting up\n'+
	'===========================\n')

print('===========================\n'+
	'Performing actions\n'+
	'===========================')


# Action 1: <liking a photo>
with driver.session() as action_session_a:

	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
		'Action 1: narcisa.c likes a photo:\n'+
		'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

	action_session_a.write_transaction(like_photo, 'narcisa.c', '2')

# Action 2: <commenting on a video>

	print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
		'Action 2: narcisa.c comments on a video:\n'+
		'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

	action_session_a.write_transaction(create_comment, '20', 'we want more videos of corgis', '05/06/2018')
	action_session_a.write_transaction(write_video_comment, '20', 'narcisa.c', '4')

# Action 3: <looking at all the existing comments on a video>

print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
	'Action 3: narcisa.c looks at all the existing comments on the corgi video:\n'+
	'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')	

with driver.session() as comment_session:
	with comment_session.begin_transaction() as tx:
	    for record in tx.run("MATCH (a:Comment)-[r]-(b:Video) "
	                         "WHERE b.id ='4' AND type(r) = 'COMMENT_ABOUT'"
	                         "RETURN a.content, b.id"):
	        print('Comment on video ', record["b.id"], ': ', record["a.content"])

# Action 4: <looking at all the existing received messages from a user>

print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
	'Action 4: narcisa.c looks at all of her received messages from annarocklady:\n'+
	'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

with driver.session() as message_session:
	with message_session.begin_transaction() as tx:
	    for record in tx.run("MATCH (a:Message)-[r]-(b:User), (c:User) "
	                         "WHERE b.username = 'narcisa.c' AND type(r) = 'SENT_TO' AND c.username = 'annarocklady'"
	                         "RETURN a.content, b.username, type(r), c.username"):
	        print('Message ', record["type(r)"], record["b.username"], ' from ', record["c.username"],': ', record["a.content"])

# Action 5: <sending a message>

with driver.session() as action_session_b:

	print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
		'Action 5: narcisa.c sends a message to annarocklady:\n'+
		'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

	action_session_b.write_transaction(create_message, '33', 'hey! sorry for not responding earlier', '05/06/2018')
	action_session_b.write_transaction(send_message, '33', 'narcisa.c', 'annarocklady')

# Action 6: <looking at all of the photos and videos liked by a user>

print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
	'Action 6: narcisa.c looks at all of the photos and videos liked by annarocklady:\n'+
	'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

with driver.session() as photo_session:
	with photo_session.begin_transaction() as tx:
	    for record in tx.run("MATCH (a:User)-[r]-(f) "
	                         "WHERE a.username ='annarocklady' AND type(r) = 'LIKES'"
	                         "RETURN f.id, f.caption, type(r), a.username, labels(f)"):
	        print(record["a.username"], record["type(r)"], record["labels(f)"],record["f.id"], 'with caption: ', record["f.caption"])

# Action 7: <looking at all the photos that use a certain filter>

print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
	'Action 7: narcisa.c looks at all of the photos that use the Mayfair filter:\n'+
	'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

with driver.session() as filter_session:
	with filter_session.begin_transaction() as tx:
	    for record in tx.run("MATCH (a:Filter)-[r]-(b:Photo) "
	                         "WHERE a.name ='Mayfair' AND type(r) = 'USED_ON'"
	                         "RETURN a.name, type(r), labels(b), b.id, b.caption"):
	        print(record["a.name"], record["type(r)"], record["labels(b)"],record["b.id"], 'with caption: ', record["b.caption"])

# Action 8: <looking at all the media posted in a certain location>

print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'+
	'Action 8: narcisa.c looks at all of the photos that were posted in NY:\n'+
	'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

with driver.session() as date_loc_session:
	with date_loc_session.begin_transaction() as tx:
	    for record in tx.run("MATCH (a:Photo) "
	                         "WHERE a.location='New York' "
	                         "RETURN labels(a), a.id, a.caption, a.date, a.location"):
	        print(record["labels(a)"], record["a.id"], 'with caption: ', record["a.caption"],
	         'posted in ', record["a.location"], ' on ', record["a.date"])


