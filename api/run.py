import json
from flask import Flask, jsonify, abort

app = Flask(__name__)

# Read JSON data from file :
with open('movies_rating.json') as f:
    json_data = json.load(f)

@app.route('/')
def display_endpoints():
    return "Endpoints : api/movies, tapi//user/<int:user_id>, api/movies/<int:movie_id>"

@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(json_data)

@app.route('/api/user/<int:user_id>', methods=['GET'])
def get_user_by_id(user_id):
    user_movies = [item for item in json_data if item["userId"] == str(user_id)]
    if user_movies:
        return jsonify(user_movies)
    else :
        abort(404, f'User with ID {user_id} not found')

@app.route('/api/movies/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    movies = [item for item in json_data if item["movie"]["movieId"] == str(movie_id)]
    if movies:
        return jsonify(movies)
    else:
        abort(404, f'Movie with ID {movie_id} not found')

if __name__ == '__main__':
    app.run(debug=True)