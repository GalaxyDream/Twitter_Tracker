#----------------------------------------------
#A function to get column 
#----------------------------------------------
from reader_csv_column import CsvFile,EXCLUDE
import json

csvfilename = 'HPV_relevant_anoucement.csv'
csvfile = CsvFile(csvfilename)
user_ids = []
user = []
topic_column = csvfile.get_column('id')
for i in topic_column:
    user_ids.append(i)
    user.append(int(i))
print(user_ids)
print(user)


import sys

print(sys.version)

#from twython import Twython
filename = '123'
import twython
APP_KEY = "Zt9Zfm2dmvx3PWc5o9VS3AEkz"
APP_SECRET = "Ljp8bDrpaOJJobxwjx8i25IjzJEoBIDKg92VCup3F4Dx8FR76L"
OAUTH_TOKEN = "4718250675-JWFut0BpNF4QiYTOlIVRFxicsqtwh8PfnCY8anN"
OAUTH_TOKEN_SECRET = "Dteb4Ud7qazLWk0saVntVzHPSB3c8J3B9b4YKqt4GhxVy"

twitter = twython.Twython(APP_KEY, APP_SECRET, oauth_version=2)
ACCESS_TOKEN = twitter.obtain_access_token()

twitter = twython.Twython(APP_KEY, access_token=ACCESS_TOKEN)
tweet = twitter.get_retweets(id = '693063605735034880', count = 100)
print (tweet)



