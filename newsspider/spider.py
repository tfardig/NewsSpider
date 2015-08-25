import time

import datetime
import newspaper
import requests
import pika
import pickle
from pymongo import MongoClient

import re


def get_mongo():
    return MongoClient('localhost')['spider']


def add_huburl(hub_url):
    db = get_mongo()
    db['hubs'].ensure_index('hub_url', unique=True)
    db['hubs'].insert({'hub_url': hub_url, 'links': []})


def get_huburls():
    db = get_mongo()
    for h in db['hubs'].find():
        yield h['hub_url']


def update_links(hub_url, links):
    db = get_mongo()
    db['hubs'].update({'hub_url': hub_url}, {'$set': {'links': links}})


def get_links(hub_url):
    db = get_mongo()
    hub_links = db['hubs'].find_one({'hub_url': hub_url})
    if not hub_links:
        db['hubs'].insert({'hub_url': hub_url, 'links': []})
        return []
    return hub_links.get('links', [])


def retrieve_links(url):
    r = requests.get(url)
    html = r.text
    links = set()
    for m in re.finditer(r'<a[^>]*href="([^"]*)', html):
        links.add(m.group(1))
    return links


def find_new_links(hub_url):
    links = retrieve_links(hub_url)
    print 'Got %d links' % len(links)
    links = set([valid_link(l, hub_url) for l in links])
    print '%d valid links' % len(links)
    old_links = get_links(hub_url)
    update_links(hub_url, list(links))
    if old_links:
        print 'Had %d old links' % len(old_links)
        links.difference_update(old_links)
        if links:
            return list(links)
    return []


def parse_article(article_url):
    article = newspaper.Article(article_url)
    article.download()
    article.parse()

    return {
        'title': article.title,
        'text': article.text,
        'published': article.published_date,
        'url': article_url
    }


def save_article(json_article):
    db = get_mongo()
    db['articles'].insert(json_article)


def article_exists(hub_url, article_url):
    db = get_mongo()
    a = db['articles'].find_one({'source': hub_url, 'url': article_url})
    return bool(a)


def store_failed_parse(hub_url, article_url):
    db = get_mongo()
    db['failed'].update({'hub_url': hub_url},
                        {'$push': {'urls': article_url}}, upsert=True)


def make_complete_link(link, hub_url):
    if link.startswith('/'):
        return hub_url + link
    else:
        return link


def valid_link(link, hub_url):
    if link:
        if link.startswith('/'):
            return hub_url + link
        elif link.startswith(hub_url) and len(link) > len(hub_url):
            return link
    else:
        return False


def check_all(channel):
    for hub_url in get_huburls():
        print 'Checking %s...' % hub_url
        links = find_new_links(hub_url)
        print 'Got %d new links:' % len(links)
        if links:
            for link in links:
                try:
                    print 'Adding %s to queue' % link
                    channel.basic_publish(
                        '', 'spider.new_article_urls',
                        pickle.dumps({'link': link, 'hub': hub_url}))
                    # print 'Parsing %s' % link
                    # if not article_exists(links, hub_url):
                    #     article = parse_article(link)
                    #     article['source'] = hub_url
                    #     article['inserted'] = datetime.datetime.now(),
                    #     save_article(article)
                    # else:
                    #     print 'Article already in database...'
                except Exception as e:
                    print e
                    store_failed_parse(hub_url, link)


def check_loop():
    connection = pika.BlockingConnection()
    channel = connection.channel()
    while True:
        check_all(channel)
        time.sleep(60)
