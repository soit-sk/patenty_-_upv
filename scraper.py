#!/usr/bin/env python
# -*- coding: utf-8 -*-
 

import scraperwiki
import lxml.html
#import urllib2
import re
from datetime import datetime
from time import sleep
import time

#this could use enum, not sure if scraperwiki supports it
statusDict = \
    {
    "zastavená": 0, #"stopped",
    "v konaní": 1, #"in_progress",
    "udelený": 2, #"accepted",
    "zamietnutá": 3, #"rejected",
    }

overviewUrl="http://registre.indprop.gov.sk/registre/search.do?value%28cislo_zapisu%29=&value%28cislo_prihlasky%29=&value%28majitel%29=&value%28povodca%29=&value%28register%29=p"
detailUrl = "http://registre.indprop.gov.sk/registre/detail/popup.do?register=p&puv_id=%d"

#morph.io has trouble stopping scraper after 24 hours
time_limit = 16 * 60 * 60
start_time = time.time()

def toDate(s):
    try:
        date = datetime.strptime(s, "%Y-%m-%d").date()
    except:
        date = None
    return date
    
def toText(s):
    return s.strip()

def toStatus(s):
    return statusDict.get(s.encode("utf-8"))

def fetchHtml(url):
    #urldata = urllib2.urlopen(url)
    #html = urldata.read()

    html = scraperwiki.scrape(url)
    root = lxml.html.fromstring(html)

    return root

def getMaxId():
    """Get max id from the overview page"""
    root = fetchHtml(overviewUrl)
    rows = root.cssselect("div[class='listItemTitle'] span a")
    max_id = 0
    
    for row in rows:
        m = re.search("puv_id=(\d+)", str(row.attrib['href']))
        id = int(m.group(1))
        max_id = max(max_id, id)
    return max_id

#maps human caption to tuple (db_field, conversion_function)
caption2field = \
    {
    "Názov": ("name", toText),
    "Číslo prihlášky": ("application_no", toText),
    "Dátum podania prihlášky": ("date_submitted", toDate),
    "Číslo dokumentu": ("document_no", toText),
    "Stav": ("status", toStatus),
    #following two deliberately map to the same field, one is used before
    #patent is accepted, the other after accepting
    "Meno (názov) majiteľa (-ov)": ("owner_name", toText),
    "Meno (názov) prihlasovateľa (-ov)": ("owner_name", toText),
    "Medzinárodné patentové triedenie": ("international_classification", toText),
    }

# recover after interruptions (e.g., CPUTimeExceededError)
min_id = scraperwiki.sqlite.get_var("min_id")
if not min_id:
    min_id = 1
print "Start or continue from id: ", min_id

max_id = getMaxId()
#max_id = 10
print "max id is: %d\n" % max_id

for id in xrange(min_id, max_id+1):
    current_time = time.time()
    if (current_time - start_time) >= time_limit:
        print 'Time limit reached (%d s)...' % time_limit
        break

    scraperwiki.sqlite.save_var("min_id", id)
    try:
        root = fetchHtml(detailUrl % id)
    except:
        print "Failed to fetch id %d" % id
        sleep(30)
        continue
    rows = root.cssselect("table[class='tdetail'] tr")
    
    if len(rows) < 1:
        if id % 5000 == 0:
            print "No data for id %d" % id
        continue
    
    dbData = {'id': id}
    for row in rows:
        tds = row.cssselect("td")
        caption = tds[1].text_content().encode("utf-8")
        value = tds[2].text_content()
        
        field_conversion = caption2field.get(caption)
        if field_conversion is None:
            continue #ignored field
        (field, conversion) = field_conversion
        if field:
            dbData[field] = conversion(value)
    
    if len(dbData) > 1: #skip page in case no data for id is returned
        scraperwiki.sqlite.save(unique_keys=['id'], data = dbData)

# time limit was not reached, so every id was read
# want to start from minimum id next time
if (current_time - start_time) <= time_limit:
    scraperwiki.sqlite.save_var("min_id", 1)
