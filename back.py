from flask import Flask, request, jsonify
from fetchdata import *
from flask_cors import CORS
import logging
logging.basicConfig(level=logging.INFO)
import os

app = Flask(__name__, static_folder=os.path.join(os.getcwd(), 'static'))
CORS(app)
connection = None


def get_connection():
    global connection
    if connection is None:
        connection = connect()
    return connection

@app.route('/ranking', methods=['POST'])
def rankings():
    data = request.get_json()
    if not data:
        logging.info("ranking fetch")
        return jsonify({'error': 'No data provided'}), 400
    
                       
    seasonid = data.get('seasonid', None)
    logging.info(seasonid)
    if seasonid:
        connection = get_connection()
        ranking = select_rank(seasonid, connection)
        ranking_data = [{"first": r[1].split()[0] if len(r[1].split()) > 0 else r[1],  # First name or entire name if split doesn't work
    "last": r[1].split()[1:] if len(r[1].split()) > 1 else [],  
    "time": r[4],             
    "pts": r[5],
    "picture": r[6], "rank": r[0], "country": r[2], "team": r[7], "tname": r[8], "laps": r[3]}  for r in ranking]
        logging.info(ranking_data[0])
        return jsonify({'ranking': ranking_data})


@app.route('/year', methods=['GET'])
def years():
    try:
        connection=get_connection()
        print(connection)
        print(f"YEARSSSSSSSSSSSSSSS {select_year(connection)}")
        year=[years[0] for years in select_year(connection)]
        print(year)
        return jsonify({'year':year})
    except Exception as e:
        print(f"Error {e}")

@app.route('/grandprix', methods=['POST'])
def grandprix():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        year=data.get('year', None)
        if year:
            connection=get_connection()
            grandprix=dict(select_grandprix(year,connection))
            i=0
            for key in grandprix:
                grandprix[key]=[grandprix[key], i+1]
                i+=1
            logging.info(grandprix)
            return jsonify({'grandprix': grandprix})
        return jsonify({'error': "no record found"}), 100
    except:
        return jsonify({'error': "error"}), 100
        
    
if __name__ == '__main__':
    app.run(debug=True)
    
    