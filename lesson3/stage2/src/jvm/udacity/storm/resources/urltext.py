import storm
import urllib2
from bs4 import BeautifulSoup

class URLBolt(storm.BasicBolt):
    def process(self, tup):
        url = tup.values[0]
        req = urllib2.Request(url)
        try:
            html_doc = urllib2.urlopen(req)

            soup = BeautifulSoup(html_doc.read())
            words = soup.findAll({'title' : True, 'p' : True})
            if words:
                for word in words:
                    storm.emit([word.string])
        except:
            pass
URLBolt().run()
