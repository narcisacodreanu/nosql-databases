# Set up the url and send a GET request to it. The base url is:
# "https://api.nasa.gov/planetary/apod?api_key=bsupjf88eHXC43FbHGh0iMffi7hjJMZ1VN9iQLll"

# Make the request and print out the "url" key in the response, which is the image url 

# name: narcisacodreanu, ncc2130
# date: 04/03/2018
# using python 3 for compilation

import requests
from pprint import pprint

# NASA url using my api key
url = 'https://api.nasa.gov/planetary/apod?api_key=bsupjf88eHXC43FbHGh0iMffi7hjJMZ1VN9iQLll'

# get the NASA photo from last year on my birthday, Feb 20th, 2017
payload = {'date' : '2017-02-20'}
result = requests.get(url, params = payload)

# print the json result - for debugging purposes 
# pprint(result.json())

# print the url of the beautiful photo
print(result.json()['url'])
