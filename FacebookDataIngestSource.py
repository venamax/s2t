import sys
from itertools import ifilter
import requests
import json
from pprint import pprint

class FacebookDataIngestSource:
  """Ingest data from Facebook"""
  
  def __init__(self, config):
    self.config = config
    self.pages = []
    self.post = []
    
  def __iter__(self):
    if 'track' in self.config:
        self.track = self.config['track']
    else: 
        self.track = ['ski','surf','board']
    
#### Retrieve the consumer key and secret
    consumer_key = self.config['consumer_key']
    consumer_secret = self.config['consumer_secret']

#### Define url for http request for access token
    auth_url = 'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=%s&client_secret=%s'%(consumer_key,consumer_secret)
#### Get authorization token from Facebook and store it for future use
    token_req = requests.get(auth_url)
    self.access_token = token_req.text.split('=')[1]

#### Request id for pages associated to search term    
    page_fields='page&fields=id,name'
    
#### Retrieve term to search    
    for term in self.track:

#### Define url for http request to get pages id associated to search term    
        page_request_url = 'https://graph.facebook.com/search?q=%s&type=%s&access_token=%s'%(term, page_fields, self.access_token)
        page_request = requests.get(page_request_url).json()
        
#### Get a list of pages id and names associated to search term    
        for i in range(len(page_request['data'])):
            self.pages.append((page_request['data'][i]['id'],page_request['data'][i]['name']))
    
    for page in self.pages:
        video_url = 'https://graph.facebook.com/v2.5/%s/videos?&fields=permalink_url,sharedposts,likes,comments&access_token=%s'%(page[0],self.access_token)
        request = requests.get(video_url).json()
        pprint(request)
        for i in range(len(request)):
            self.post.append(request)
    self.source_iterator = ifilter(lambda x: x, self.pages.iter_lines() + (self.post.iter_lines(),) )
    
    return self

  def next(self):
    return {'post' : json.loads(self.source_iterator.next())} 
    





