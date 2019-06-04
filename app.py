
from urllib.request import urlopen
from bs4 import BeautifulSoup as BS
import requests
import zipfile
import io
import pandas as pd
import os
import cherrypy
from jinja2 import Environment, FileSystemLoader
import redis


URL = 'https://www.bseindia.com/markets/equity/EQReports/Equitydebcopy.aspx'


def getBhavcopy():
    #Scrap the given URL , download and extract the zip file
    url = urlopen(URL)
    html = url.read()
    soup = BS(html, "html.parser")
    file = soup.find(id='btnhylZip')
    link = file.get('href', None)
    r = requests.get(link)
    zipfile_csv = zipfile.ZipFile(io.BytesIO(r.content))
    zipfile_csv.extractall()
    return zipfile_csv.namelist()[0]


def loadRedis(csv_file):
    #Loading the data in the redis server
    
    csv_data = pd.read_csv(csv_file)
    # The required fileds are stored
    csv_data = csv_data[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()
    for index, row in csv_data.iterrows():
        r.hmset(row['SC_CODE'], row.to_dict())
        r.set("equity:"+row['SC_NAME'], row['SC_CODE'])

class homePage:
    @cherrypy.expose
    def index(self):
        html_file = env.get_template('showcase.html')
        self.result = []
        for key in r.scan_iter("equity:*"):
            code = r.get(key)
            self.result.append(r.hgetall(code).copy())
        self.result = self.result[0:10]
        return html_file.render(result=self.result)
    
    @cherrypy.expose   
    def search(self, query = None): 
        html_file = env.get_template('search.html')
        self.searchItems = []
        query = query.upper()   
        for key in r.scan_iter("equity:"+query+"*"):
            code = r.get(key)
            self.searchItems.append(r.hgetall(code).copy())
        return html_file.render(search = self.searchItems)
    

if __name__ == '__main__':
    csv_file = getBhavcopy()
    r = redis.from_url(os.environ.get("REDIS_URL")
    """StrictRedis(host="localhost",
        port=6379,
        charset="utf-8",
        decode_responses=True,
        db=1)"""
    loadRedis(csv_file)
    env = Environment(loader=FileSystemLoader('media'))
    config = {
                'global': {     'server.socket_host':  '0.0.0.0',
                                'server.socket_port':  int(os.environ.get('PORT', '5000'))
                },
                '/assets': {    'tools.staticdir.root': os.path.dirname(os.path.abspath(__file__)),
                                'tools.staticdir.on': True,
                                'tools.staticdir.dir': 'assets',
                }
    }

cherrypy.quickstart(homePage(), '/', config=config)
