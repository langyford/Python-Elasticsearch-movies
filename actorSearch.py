#!/usr/bin/env python3

#import sys
#sys.path.append('/Users/lukelangford/Library/Python/2.7/lib/python/site-packages')
from elasticsearch import Elasticsearch
import requests
import tkinter

es = Elasticsearch(['https://search-moviesearch-2gwtif2k6ovqc7mzn6nqixltgi.ap-southeast-2.es.amazonaws.com:443'])

def search():
    resultsList.delete(1.0, tkinter.END)
    
    searchStr = searchBar.get()

    resultsList.insert(tkinter.END, "Movies with: "+searchStr+" starring in them \n \n")
    
    searchResult = es.search(index="actors", body={"query": {"match": {'name':searchStr}}})
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
        resultsList.insert(tkinter.END, movieData['hits']['hits'][0]['_source']['title']+"\n")


root = tkinter.Tk()
label = tkinter.Label(root, text="Search an actor")
label.pack()
searchBar = tkinter.Entry(root)
searchBar.pack()
searchButton = tkinter.Button(root, text = "Search", command = search)
searchButton.pack()
resultsList = tkinter.Text(root)
resultsList.pack()

searchResult = es.search(index="actors", body={"query": {"match": {'name':""}}})
root.mainloop()