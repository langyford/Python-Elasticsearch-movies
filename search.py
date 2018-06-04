#!/usr/bin/env python3

import sys
sys.path.append('/Users/lukelangford/Library/Python/2.7/lib/python/site-packages')
from elasticsearch import Elasticsearch
import requests
import tkinter

es = Elasticsearch(['https://search-moviesearch-2gwtif2k6ovqc7mzn6nqixltgi.ap-southeast-2.es.amazonaws.com:443'])

def search():
    searchStr = searchBar.get()
    searchResult = es.search(index="movies", body={"query": {"match": {'name':searchStr}}})
    print(searchResult)

def setupUI():
    root = tkinter.Tk()
    label = tkinter.Label(root, text="Movie search")
    label.pack()
    searchBar = tkinter.Entry(root)
    searchBar.pack()
    searchButton = tkinter.Button(root, text = "Search", command = search)
    searchButton.pack()
    
    
    searchStr = searchBar.get()
    searchResult = es.search(index="actors", body={"query": {"match": {'name':'Angelina Jolie'}}})
    actorID = searchResult['hits']['hits'][0]['_id']
    
    searchResult = es.search(index="parts", body={"query": {"match": {'actor_id':actorID}}})
    movies = searchResult['hits']['hits']
    movieIDs = []
    for movie in movies:
        movieIDs.append(movie['_source']['movie_id'])
    
    moviesData = []
    for movieID in movieIDs:
        searchResult = es.search(index="movies", body={"query": {"match": {'_id':movieID}}})
        moviesData.append(searchResult)
    
    for movieData in moviesData:
        print(movieData['hits']['hits'][0]['_source']['title'])
        
    root.mainloop()
    
setupUI()