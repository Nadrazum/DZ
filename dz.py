from typing import Dict, List, Any
import hashlib
import requests
from bs4 import BeautifulSoup
import re
from elasticsearch import Elasticsearch
import datasketch
import kshingle as ks
import pymorphy2
import string




URL = 'https://warhammergames.ru/news/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}

def get_html(url, parametrs=None):
    req = requests.get(url, headers=HEADERS, params=parametrs)
    return req

def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.findAll(class_="swchItem", onclick=re.compile("spages"))
    return int(pagination[-2].get_text())

def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.findAll('div', class_='news-view')

    wh = []
    for item in items:
        wh.append({
            'title': item.find('div', class_='news-view-title').get_text(strip=True),
            'link': item.find('a').get('href'),
            'text': item.find('div', class_='news-view-text').get_text(strip=True),
            'time': item.find('div', class_='news-view-info').get_text(strip=True)[1:18],
            'text+':  canonize(item.find('div', class_='news-view-text').get_text(strip=True))
        })

    return wh
    
def canonize(source):
    no_meaning_words = ['это', 'этот',
                        'эти', 'этим', 'этом',
                        'как', 'так',
                        'и', 'в', 'над',
                        'к', 'до', 'не',
                        'на', 'но', 'за',
                        'то', 'с', 'ли',
                        'а', 'во', 'от',
                        'со', 'для', 'о',
                        'же', 'ну', 'вы',
                        'бы', 'что', 'кто',
                        'он', 'она', 'еще',
                        'мы', "по"]

    source = source.lower()

    for simvol in string.punctuation:
        if simvol in source:
            source = source.replace(simvol, '')

    new_words = []
    for word in source.split(' '):
        if word not in no_meaning_words:
            new_words.append(word)
    source = " ".join(new_words)

    pymorph = pymorphy2.MorphAnalyzer()
    text_norm = ""
    for word in source.split(' '):
        word = pymorph.parse(word)[0].normal_form
        text_norm += word
        text_norm += ' '

    return text_norm


def parse():
    html = get_html(URL)
    if html.status_code == 200:
        wh = []
        pages_count = get_pages_count(html.text)
        print("Сколько страничек парсить? \nВведите число до:", pages_count)
        pages_count = int(input())
        for page in range(1, pages_count + 1):
            print(f" Парсинг страницы {page}  из {pages_count}")
            html = get_html(URL, parametrs={'page': page})
            html.url = html.url.replace("=", "")
            html = get_html(html.url)
            wh.extend(get_content(html.text))
        for WH in wh:
            print('Title: ' + WH['title'] + ":\n" +
                  WH['text'] + '\n' +
                  'Link: ' + WH['link'] + '\n' +
                  'Time: ' + WH['time'] + '\n'+
                   WH['text+'] + '\n' )
        print(f'Получено {len(wh)} новостей')
    elif html.status_code == 404:
        print("Error 404")
    else:
        print('Error' + str(html.status_code))

    return wh


#222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222

def hash_text(string):
    hash_object = hashlib.md5(string.encode())
    return str(hash_object.hexdigest())

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('This program was connected to elastic')
    else:
        print('Awww it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "dow": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text"
                    },
                    "link": {
                        "type": "text"
                    },
                    "time": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, id, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name,doc_type='dow', id=id, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored
        
def search_word(es_objekt, index_name, search):
    res = es_objekt.search(index=index_name, body=search)
    return res
    

#------------------------------------------------------------------------------------------------

wh = parse()

text0 = ' игра быть отключить обновление разработчики'
text0 = canonize(text0)
s1 = ks.shingleset_k(text0, k=3)
m1 = datasketch.MinHash(num_perm=128)
WH1 = wh
c = 0
WH0 = []
for WH in WH1:
    s2 = ks.shingleset_k(WH['text+'], k=3)
    for s in s1:
        m1.update(s.encode('utf8'))
    m2 = datasketch.MinHash(num_perm=128)
    for s in s2:
        m2.update(s.encode('utf8'))
    if m2.jaccard(m1) > 0.23:
        c += 1
        WH0.append(WH['text'])
if c == 0:
    print("таких статей нет")
else:
    print("Статьи подходящиаие под запрос: " + '\n' + text0)
    for WH in range(len(WH0)):
        print(WH0[WH])


    print('Количество таких статей: ')
    print(c)

es = connect_elasticsearch()
if create_index(es, 'dow'):
	for WH in wh:
    		out = store_record(es, 'dow', hash_text(WH['text']), WH)

print('\nSearch:')
search_object = {'_source': ['title'], 'query':{'match': {'text': 'Warhammer'}}}
search_content = search_word(es,'dow', search_object)
print(search_content)




