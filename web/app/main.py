from elasticsearch import Elasticsearch
from flask import Flask,jsonify,url_for,redirect,render_template,request, abort
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from math import ceil
import secrets

app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.config['SECRET_KEY'] = secrets.token_hex(16)

# create connection with elasticsearch version
client = Elasticsearch(
    "http://localhost:9200",
    )

index_name = 'movies'

def getUser(user_id):
    query = {
        "query": {
            "term": {'user_id': str(user_id)}
        }
    }

    result = client.search(index='users', body=query)
    user = result['hits']['hits']
    if user:
        return user[0]  # Assuming you want to return the first matching user
    else:
        return None  # User not found


# Get all movies
def get_all_movies():
    query = {
        "query": {
             "match_all": {}
        }
    }
    result = client.search(index=index_name, body=query)
    total_movies = result['hits']['total']['value']
    return total_movies

def get_movies(page=1, size=12):
    page = 1 if page <= 0 else page
    query = {
        "query": {
            "match_all": {}
        },
        "from": (page - 1) * size,
        "size": size
    }
    result = client.search(index=index_name, body=query)
    return result['hits']['hits']

def get_movie_by_title(title):
    query = {
        "query": {
            "match": {'title': title}
        }
    }

    result = client.search(index=index_name, body=query)
    movies = result['hits']['hits']
    if movies:
        return movies[0] 
    else:
        return None

@app.route('/user/<string:user_id>', methods=['GET', 'POST'])
def index(user_id):
    page = int(request.args.get('page', 1))
    if page <= 0:
        abort(404)
    size = int(request.args.get('size', 12))
    user = getUser(user_id)
    if user:
        # User exists, proceed to get movies and render template
        movies = get_movies(page, size)
        total_movies = get_all_movies()
        total_pages = ceil(total_movies / size)
        return render_template("index.html", movies=movies, page=page, size=size, total_pages=total_pages, user=user)
    else:
        # User not found, render a custom error template
        return render_template("user_not_find.html", user_id=user_id), 404

  

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=9090,
        debug=True
    )