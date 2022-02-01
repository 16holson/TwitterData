from datetime import datetime

import pyodbc
import requests
import base64
from dataclasses import dataclass
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient

api_key = '11c8477a9bfc46d690c45e2f14d03710'
api_endpoint = 'https://westus2.api.cognitive.microsoft.com/'
api_call = TextAnalyticsClient(endpoint=api_endpoint, credential=AzureKeyCredential(api_key))

# Connecting to the database
server = 'wsu-cs3550.westus2.cloudapp.azure.com'
database = 'hunterolson'
username = 'hunterolson'
password = 'keno.mines.finals'
connection = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=' +
                            server + ';DATABASE=' + database +
                            ';UID=' + username + ';PWD=' + password)
cursor = connection.cursor()
cursor.execute("SELECT @@version;")

# Connecting to Twitter
bearerToken = 'AAAAAAAAAAAAAAAAAAAAAFRoVgEAAAAAwavgzrgIz%2FMGci%2F56lqliXCSG68%3DLHxMkBcaPOneVEuexaD1CvQnrSj0pqJiRZpg6zMEoxTBcIN2aZ'

searchUrl = 'https://api.twitter.com/2/tweets/search/recent'

queryParameters = {
    'query': 'Vaccine -is:retweet lang:en',
    'expansions': 'author_id,geo.place_id',
    'tweet.fields': 'id,text,author_id,geo,created_at,entities',
    'user.fields': 'id,name,username,created_at,location',
    'max_results': 10
}

headers = {
    'Authorization': 'Bearer {}'.format(bearerToken),
    'User-Agent': 'TopicPull-RRead'
}

tweetCounter = 0
totalTweets = 1000

oldHashtag = 'SELECT H.HashtagId FROM Hashtags AS H WHERE H.Hashtag = ?'
tweetAdd = "EXEC AddTweet @TweetId = ?, @PersonId = ?, @TweetText = ?, @DayTimeSent = ?, @TweetJson = ?"
personAdd = "EXEC AddPerson @PersonId = ?, @Username = ?, @ActualName = ?, @Location = ?"
hashtagAdd = "EXEC AddHashtag @Hashtag = ?"
tweetHashtagAdd = "EXEC AddTweetHashtag @TweetId = ?, @HashtagId = ?"
tweetSentimentAdd = "EXEC AddTweetSentiment @TweetId = ?, @SentimentId = ?, @SentimentValue = ?"
keyPhraseAdd = "EXEC AddKeyPhrase @KeyPhrase = ?"
tweetKeyPhraseAdd = "EXEC AddTweetKeyPhrase @TweetId = ?, @KeyPhraseId = ?"
oldKeyPhrase = 'SELECT K.KeyPhraseId FROM KeyPhrases AS K WHERE K.KeyPhrase = ?'
positive = 1
neutral = 2
negative = 3
mixed = 4

#1st query
while (tweetCounter < totalTweets):
    apiDocument = []
    response = requests.request('GET', searchUrl, headers=headers, params=queryParameters)
    parseJson = response.json()
    nextToken = parseJson['meta']['next_token']
    queryParameters['next_token'] = nextToken

    # Fill in People table
    for person in parseJson['includes']['users']:
        try:
            peopleData = (person['id'], person['username'], person['name'], person['location'])
        except:
            peopleData = (person['id'], person['username'], person['name'], None)
        try:
            cursor.execute(personAdd, peopleData)
        except:
            print()

    # Fill in Tweet table
    for tweet in parseJson['data']:
        apiDocument.append({
            'id': str(tweet['id']),
            'language': 'en',
            'text': tweet['text']
        })
        hashtags = []
        try:
            tweetData = (int(tweet['id']), int(tweet['author_id']), str(tweet['text']), datetime.strptime(tweet['created_at'], '%Y-%m-%dT%H:%M:%S.000Z'), str(tweet))
            cursor.execute(tweetAdd, tweetData)
        except:
            print()
        # Fill in Hashtag table
        try:
            for hashtag in tweet['entities']['hashtags']:
                currentHashtags = cursor.execute('SELECT H.Hashtag FROM Hashtags AS H').fetchall()
                betterHashtags = []
                for h in currentHashtags:
                    betterHashtags.append(h[0])
                #Is hashtag already in database
                if(hashtag['tag'] in betterHashtags):
                    oldId = cursor.execute(oldHashtag, hashtag['tag'])
                    oldTweetHashtag = (tweet['id'], oldId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
                else:
                    cursor.execute(hashtagAdd, hashtag['tag'])
                    #tweetHashtag
                    hashtagId = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
                    tweetHashtagData = (tweet['id'], hashtagId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
        except:
            print()
    # Azure
    results = api_call.analyze_sentiment(apiDocument, show_opinion_mining=True)

    # Sentiments
    for sentiment in results:
        posData = (sentiment['id'], positive, sentiment['confidence_scores']['positive'])
        neuData = (sentiment['id'], neutral, sentiment['confidence_scores']['neutral'])
        negData = (sentiment['id'], negative, sentiment['confidence_scores']['negative'])
        cursor.execute(tweetSentimentAdd, posData)
        cursor.execute(tweetSentimentAdd, neuData)
        cursor.execute(tweetSentimentAdd, negData)
    keyResults = api_call.extract_key_phrases(apiDocument)

    # KeyPhrases
    for key_result in keyResults:
        for phrases in key_result['key_phrases']:
            temp = cursor.execute("SELECT K.KeyPhrase FROM KeyPhrases AS K").fetchall()
            temp2 = []
            for t in temp:
                temp2.append(t[0])
            if phrases in temp2:
                #tweetkeyphrases
                oldKeyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                oldTweetKeyPhraseData = (key_result['id'], oldKeyId)
                cursor.execute(tweetKeyPhraseAdd, oldTweetKeyPhraseData)
            else:
                #keyphrases
                cursor.execute(keyPhraseAdd, phrases)
                #tweetkeyphrases
                keyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                if keyId is None:
                    print('keyId: ', keyId)
                else:
                    tweetKeyPhraseData = (key_result['id'], keyId)
                    cursor.execute(tweetKeyPhraseAdd, tweetKeyPhraseData)
    tweetCounter += 10

cursor.commit()

#2nd query
queryParameters2 = {
    'query': 'Covid -is:retweet lang:en',
    'expansions': 'author_id,geo.place_id',
    'tweet.fields': 'id,text,author_id,geo,created_at,entities',
    'user.fields': 'id,name,username,created_at,location',
    'max_results': 10
}
tweetCounter2 = 0
totalTweets2 = 1000

while (tweetCounter2 < totalTweets2):
    apiDocument = []
    response = requests.request('GET', searchUrl, headers=headers, params=queryParameters2)
    parseJson = response.json()
    nextToken = parseJson['meta']['next_token']
    queryParameters['next_token'] = nextToken

    # Fill in People table
    for person in parseJson['includes']['users']:
        try:
            peopleData = (person['id'], person['username'], person['name'], person['location'])
        except:
            peopleData = (person['id'], person['username'], person['name'], None)
        try:
            cursor.execute(personAdd, peopleData)
        except:
            print()

    # Fill in Tweet table
    for tweet in parseJson['data']:
        apiDocument.append({
            'id': str(tweet['id']),
            'language': 'en',
            'text': tweet['text']
        })
        hashtags = []
        try:
            tweetData = (int(tweet['id']), int(tweet['author_id']), str(tweet['text']), datetime.strptime(tweet['created_at'], '%Y-%m-%dT%H:%M:%S.000Z'), str(tweet))
            cursor.execute(tweetAdd, tweetData)
        except:
            print()
        # Fill in Hashtag table
        try:
            for hashtag in tweet['entities']['hashtags']:
                currentHashtags = cursor.execute('SELECT H.Hashtag FROM Hashtags AS H').fetchall()
                betterHashtags = []
                for h in currentHashtags:
                    betterHashtags.append(h[0])
                #Is hashtag already in database
                if(hashtag['tag'] in betterHashtags):
                    oldId = cursor.execute(oldHashtag, hashtag['tag'])
                    oldTweetHashtag = (tweet['id'], oldId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
                else:
                    cursor.execute(hashtagAdd, hashtag['tag'])
                    #tweetHashtag
                    hashtagId = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
                    tweetHashtagData = (tweet['id'], hashtagId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
        except:
            print()
    # Azure
    results = api_call.analyze_sentiment(apiDocument, show_opinion_mining=True)

    # Sentiments
    for sentiment in results:
        #print(sentiment['id'], ' - ', sentiment['sentiment'], ' - ', sentiment['confidence_scores']['positive'], '\n\n')
        posData = (sentiment['id'], positive, sentiment['confidence_scores']['positive'])
        neuData = (sentiment['id'], neutral, sentiment['confidence_scores']['neutral'])
        negData = (sentiment['id'], negative, sentiment['confidence_scores']['negative'])
        cursor.execute(tweetSentimentAdd, posData)
        cursor.execute(tweetSentimentAdd, neuData)
        cursor.execute(tweetSentimentAdd, negData)
    keyResults = api_call.extract_key_phrases(apiDocument)

    # KeyPhrases
    for key_result in keyResults:
        for phrases in key_result['key_phrases']:
            temp = cursor.execute("SELECT K.KeyPhrase FROM KeyPhrases AS K").fetchall()
            temp2 = []
            for t in temp:
                temp2.append(t[0])
            if phrases in temp2:
                #tweetkeyphrases
                oldKeyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                oldTweetKeyPhraseData = (key_result['id'], oldKeyId)
                cursor.execute(tweetKeyPhraseAdd, oldTweetKeyPhraseData)
            else:
                #keyphrases
                cursor.execute(keyPhraseAdd, phrases)
                #tweetkeyphrases
                keyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                if keyId is None:
                    print('keyId: ', keyId)
                else:
                    tweetKeyPhraseData = (key_result['id'], keyId)
                    cursor.execute(tweetKeyPhraseAdd, tweetKeyPhraseData)
    tweetCounter2 += 10

cursor.commit()

#
# #3rd query
queryParameters3 = {
    'query': 'Omicron -is:retweet lang:en',
    'expansions': 'author_id,geo.place_id',
    'tweet.fields': 'id,text,author_id,geo,created_at,entities',
    'user.fields': 'id,name,username,created_at,location',
    'max_results': 10
}

tweetCounter3 = 0
totalTweets3 = 1000

while (tweetCounter3 < totalTweets3):
    apiDocument = []
    response = requests.request('GET', searchUrl, headers=headers, params=queryParameters3)
    parseJson = response.json()
    nextToken = parseJson['meta']['next_token']
    queryParameters['next_token'] = nextToken

    # Fill in People table
    for person in parseJson['includes']['users']:
        try:
            peopleData = (person['id'], person['username'], person['name'], person['location'])
        except:
            peopleData = (person['id'], person['username'], person['name'], None)
        try:
            cursor.execute(personAdd, peopleData)
        except:
            print()

    # Fill in Tweet table
    for tweet in parseJson['data']:
        apiDocument.append({
            'id': str(tweet['id']),
            'language': 'en',
            'text': tweet['text']
        })
        hashtags = []
        try:
            tweetData = (int(tweet['id']), int(tweet['author_id']), str(tweet['text']), datetime.strptime(tweet['created_at'], '%Y-%m-%dT%H:%M:%S.000Z'), str(tweet))
            cursor.execute(tweetAdd, tweetData)
        except:
            print()
        # Fill in Hashtag table
        try:
            for hashtag in tweet['entities']['hashtags']:
                currentHashtags = cursor.execute('SELECT H.Hashtag FROM Hashtags AS H').fetchall()
                betterHashtags = []
                for h in currentHashtags:
                    betterHashtags.append(h[0])
                #Is hashtag already in database
                if(hashtag['tag'] in betterHashtags):
                    oldId = cursor.execute(oldHashtag, hashtag['tag'])
                    oldTweetHashtag = (tweet['id'], oldId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
                else:
                    cursor.execute(hashtagAdd, hashtag['tag'])
                    #tweetHashtag
                    hashtagId = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
                    tweetHashtagData = (tweet['id'], hashtagId)
                    cursor.execute(tweetHashtagAdd, tweetHashtagData)
        except:
            print()
    # Azure
    results = api_call.analyze_sentiment(apiDocument, show_opinion_mining=True)

    # Sentiments
    for sentiment in results:
        #print(sentiment['id'], ' - ', sentiment['sentiment'], ' - ', sentiment['confidence_scores']['positive'], '\n\n')
        posData = (sentiment['id'], positive, sentiment['confidence_scores']['positive'])
        neuData = (sentiment['id'], neutral, sentiment['confidence_scores']['neutral'])
        negData = (sentiment['id'], negative, sentiment['confidence_scores']['negative'])
        cursor.execute(tweetSentimentAdd, posData)
        cursor.execute(tweetSentimentAdd, neuData)
        cursor.execute(tweetSentimentAdd, negData)
    keyResults = api_call.extract_key_phrases(apiDocument)

    # KeyPhrases
    for key_result in keyResults:
        for phrases in key_result['key_phrases']:
            temp = cursor.execute("SELECT K.KeyPhrase FROM KeyPhrases AS K").fetchall()
            temp2 = []
            for t in temp:
                temp2.append(t[0])
            if phrases in temp2:
                #tweetkeyphrases
                oldKeyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                oldTweetKeyPhraseData = (key_result['id'], oldKeyId)
                cursor.execute(tweetKeyPhraseAdd, oldTweetKeyPhraseData)
            else:
                #keyphrases
                cursor.execute(keyPhraseAdd, phrases)
                #tweetkeyphrases
                keyId = cursor.execute(oldKeyPhrase, phrases).fetchval()
                if keyId is None:
                    print('keyId: ', keyId)
                else:
                    tweetKeyPhraseData = (key_result['id'], keyId)
                    cursor.execute(tweetKeyPhraseAdd, tweetKeyPhraseData)
    tweetCounter3 += 10

cursor.commit()
print('Finished')
