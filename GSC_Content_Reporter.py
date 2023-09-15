
import argparse, httplib2
from urllib.parse import urlparse
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from oauth2client import tools, client, file



def authorize_creds(creds,authorizedcreds='authorizedcreds.dat'):
   '''
   Authorize credentials using OAuth2.
   '''
   # Variable parameter that controls the set of resources that the access token permits.
   SCOPES = ['https://www.googleapis.com/auth/webmasters']


   # Path to secrets.json file
   CLIENT_SECRETS_PATH = creds


   # Create a parser to be able to open browser for Authorization
   parser = argparse.ArgumentParser(
       formatter_class=argparse.RawDescriptionHelpFormatter,
       parents=[tools.argparser])
   flags = parser.parse_args([])


   # Creates an authorization flow from a clientsecrets file.
   # Will raise InvalidClientSecretsError for unknown types of Flows.
   flow = client.flow_from_clientsecrets(
       CLIENT_SECRETS_PATH, scope = SCOPES,
       message = tools.message_if_missing(CLIENT_SECRETS_PATH))


   # Prepare credentials and authorize HTTP
   # If they exist, get them from the storage object
   # credentials will get written back to the 'authorizedcreds.dat' file.
   storage = file.Storage(authorizedcreds)
   credentials = storage.get()


   # If authenticated credentials don't exist, open Browser to authenticate
   if credentials is None or credentials.invalid:
       credentials = tools.run_flow(flow, storage, flags)      # Add the valid creds to a variable


   # Take the credentials and authorize them using httplib2
   http = httplib2.Http()                                      # Creates an HTTP client object to make the http request
   http = credentials.authorize(http=http)                     # Sign each request from the HTTP client with the OAuth 2.0 access token
   webmasters_service = build('searchconsole', 'v1', http=http)   # Construct a Resource to interact with the API using the Authorized HTTP Client.



   return webmasters_service


def getSiteData(site, webmasters_service):
   start_date = datetime.now() - timedelta(days=33)
   end_date = datetime.now() - timedelta(days=3)
   request = {
       "startDate": str(start_date.date()),
       "endDate": str(end_date.date()),
       "dimensions": ["query","page"]
   }
   response = webmasters_service.searchanalytics().query(siteUrl=site, body=request).execute()
   return response


def getSiteUrls(site, webmasters_service):
    start_date = datetime.now() - timedelta(days=368)
    end_date = datetime.now() - timedelta(days=3)
    request = {
        "startDate": str(start_date.date()),
        "endDate": str(end_date.date()),
        "dimensions": ["page"],
        "rowLimit": 25000
    }
    response = webmasters_service.searchanalytics().query(siteUrl=site, body=request).execute()
    pages = []
    for row in response['rows']:
        page = row['keys'][0]
        pages.append(page)
    return set(pages)


def getPageData(page, webmasters_service):
   parsed_url = urlparse(page)
   start_date = datetime.now() - timedelta(days=33)
   end_date = datetime.now() - timedelta(days=3)
   request = {
       "startDate": str(start_date.date()),
       "endDate": str(end_date.date()),
       "dimensions": ["query"]
   }
   return request


class DataContainer:
    def __init__(self):
        self.data = []

    def callback(self, request_id, response, exception):
        if exception is not None:
            pass
        else:
            self.data.append(response)


if __name__ == '__main__':
  creds = 'secrets.json'
  webmasters_service = authorize_creds(creds)

  site = "https://www.myawesomesite.com"
  response = getSiteData(site, webmasters_service)

  for row in response['rows']:
      query = row['keys'][0]
      page = row['keys'][1]
      impressions = row['impressions']
      clicks = row['clicks']
      ctr = row['ctr']
      position = row['position']

      print(
          f"Query: {query}, Page: {page}, Impressions: {impressions}, Clicks: {clicks}, CTR: {ctr}, Position: {position}")

  urls = getSiteUrls(site, webmasters_service)
  dc = DataContainer()
  batch = webmasters_service.new_batch_http_request(callback=dc.callback)

  batch_count = 0
  for url in urls:
      batch_count += 1
      try:
        data = getPageData(url, webmasters_service)
        siteUrl = urlparse(url)
        batch.add(webmasters_service.searchanalytics().query(siteUrl=f'{siteUrl.scheme}://{siteUrl.netloc}', body=data))
        if batch_count == 40:
          batch.execute()
          batch_count = 0
          batch = webmasters_service.new_batch_http_request(callback=dc.callback)
      except Exception as e:
          print(e)
  try:
      batch.execute()
  except Exception as e:
      print(e)
  print(dc.data)
