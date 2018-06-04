#!/usr/bin/env python3

import sys
sys.path.append('/Users/lukelangford/Library/Python/2.7/lib/python/site-packages')

import sqlite3
from elasticsearch import Elasticsearch
import requests

es = Elasticsearch(['https://search-moviesearch-2gwtif2k6ovqc7mzn6nqixltgi.ap-southeast-2.es.amazonaws.com:443'])

# SQLITE database convert to usable format in python
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def connectToDatabase():
    connection = sqlite3.connect("movies.sqlite3")
    connection.row_factory = dict_factory
    cursor = connection.cursor()
    #connection.close()
    return cursor

def queryDatabase(query, cursor):
    cursor.execute(query)
    results = cursor.fetchall()
    return results
            
def insertMovies():
    cursor = connectToDatabase()
    query = "SELECT * FROM movies"
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "title": result["title"],
            "description": result["description"],
            "external_id": result["external_id"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"],
            "budget": result["budget"],
            "picture": result["picture"],
            "release_date": result["release_date"]
        }
        if es.index(index="movies", doc_type="movie", id=result["id"], body=document):
            print("Movie: " + document['title'] + " inserted")
    
def insertActors():
    cursor = connectToDatabase()
    query = "SELECT * FROM actors"
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "name": result["name"],
            "external_id": result["external_id"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"],
            "picture": result["picture"]
        }
        if es.index(index="actors", doc_type="actor", id=result["id"], body=document):
            print("Actor: " + document['name'] + " inserted")
    
def insertGenres():
    cursor = connectToDatabase()
    query = "SELECT * FROM genres"
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "name": result["name"],
            "external_id": result["external_id"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"]
        }
        if es.index(index="genres", doc_type="genre", id=result["id"], body=document):
            print("Genre: " + document['name'] + " inserted")
            
    
def insertParts(startPos, limit):
    cursor = connectToDatabase()
    query = "SELECT * FROM parts LIMIT {}, {}".format(startPos, limit)
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "movie_id": result["movie_id"],
            "actor_id": result["actor_id"],
            "character": result["character"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"]
        }
        try:
            es.index(index="parts", doc_type="part", id=result["id"], body=document)
            print("Parts: " + document['character'] + " inserted")
        except:
            print("Error inserting part data")
        
    
def insertSequences():
    cursor = connectToDatabase()
    query = "SELECT * FROM sqlite_sequence"
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "name": result["name"],
            "seq": result["seq"]
        }
        if es.index(index="sqlite_sequence", doc_type="sequence", id=result["name"], body=document):
            print("Sequence: " + document['name'] + " inserted")
            
    
def insertTaggings():
    cursor = connectToDatabase()
    query = "SELECT * FROM taggings"
    results = queryDatabase(query, cursor)
    
    for result in results:      
        document = {
            "movie_id": result["movie_id"],
            "genre_id": result["genre_id"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"]
        }
        if es.index(index="taggings", doc_type="tagging", id=result["id"], body=document):
            print("Tagging inserted")
            
    
def viewDBTables():
    cursor = connectToDatabase()
    #query = "SELECT name FROM sqlite_master WHERE type = 'table'"
    query = "SELECT * FROM taggings LIMIT 1"
    #query = "SELECT COUNT(*) AS count FROM sqlite_sequence";
    results = queryDatabase(query, cursor)
    print(results)
        
insertMovies()
insertActors()
insertGenres()
insertSequences()
insertTaggings()
insertParts(0, 40000)

#viewDBTables()