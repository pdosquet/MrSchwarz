from bs4             import BeautifulSoup
from pymongo         import MongoClient
from multiprocessing import Pool
import progressbar
import datetime
import requests
import time
import sys

#----------------------------------------- 
#Utils functions
#----------------------------------------- 
def saveToMongo(collName, data):  
    clt  = MongoClient('localhost', 27017)
    db   = clt['DataBase']
    coll = db[collName]
    coll.insert_many(data)
    
def getBs4ElementOrEmptyString(soup, tag, values):
    try:
        return soup.findAll(tag, values)[0].get_text()
    except IndexError:
        return ""

def requestLinkWithRetry(link):
    res = None
    tries = 0
    while(tries < 3):
        try:
            res = requests.get(link)
            return res
        except TimeoutError: 
            tries += 1
    return None
    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(n):
    print("Getting the full page...")
    URL = f'https://finmasters.com/credit/page/{n}/'
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):
    links = []
    div = soup.findAll("div", {"class": "span-8"})#b-sitemap__item?
    
    try:
        for h3 in div[0].findAll("h3"):
            for a in h3.findAll("a"):
                links.append(str(a["href"]))
    except IndexError:
        print("No <div> elements found.")
        return links
    
    return links
#corriger chaque article 2 fois + date 
def getArticlesFromLinks(links):
    print("Extracting articles from links...")
    
    #Request all articles in parralel
    p = Pool(10)
    results = p.map(requestLinkWithRetry, links)
    p.close()
    
    #make soup from articles
    soups = []
    for result in progressbar.progressbar(results):
        if(not result is None):
            soups.append(BeautifulSoup(result.content, 'html.parser'))
    return soups
    
def getFormattedArticles(articles):
    print("Formatting articles...")
    formArt = []
    for a in progressbar.progressbar(articles):
    
        #check for empty article
        content     = a.findAll("article")
        if(not content):
            continue
           
        #extract content and clean
        content      = getBs4ElementOrEmptyString(a,"article", {})
        code         = content.rfind("});")
        if(code > 0):            
            content = content[code:]
        editorec = content.rfind("Editors' Recommendations")
        if(editorec > 0):            
            content = content[0:editorec]
            
        #extract various meta data
        author      = getBs4ElementOrEmptyString(a,"span", {"class": "label posted-by-name"})
        title       = getBs4ElementOrEmptyString(a,"h1", {"itemprop": "headline"})
        type        = getBs4ElementOrEmptyString(a,"div", {"class": "posted-in"})
                
        #extract and format time data
        time        = a.findAll("meta", {"property": "article:published_time"})
        if(len(time)>0):
            time = time[0]
        else:
            continue

        time        = datetime.datetime.strptime(time["content"][0:-6], '%Y-%m-%dT%H:%M:%S')
        date        = f'{time.year}-{time.month}-{time.day}'
        
        #format data for storage
        metaData    = {"author":author.strip(),
                        "title":title.strip(),
                        "type":type.strip()}
        formArt.append({"date":date,"metaData":metaData,"txt":content.strip()})
    return formArt

#----------------------------------------- 
#Main
#----------------------------------------- 
if __name__ == '__main__':        

    #set crawl time period
    pages   = [str(n) for n in range(1,10)]

    
    #Crawl main loop
    links   = []
    for  n in pages:
        print('------------------------')
        print(f'CURRENT PAGE : {n}')
        print('------------------------')

        page     = getPage(n)
        links    = getArticlesLinks(page) 
        if(len(links) == 0):
            print("Nothing here")
            continue
        articles = getArticlesFromLinks(links)
        formArt  = getFormattedArticles(articles)
        if(len(formArt) > 0):
            saveToMongo("FinMastersCredit",formArt)     
                
                