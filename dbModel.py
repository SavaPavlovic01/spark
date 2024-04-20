from flask import Flask
from flask import request
from flask_sqlalchemy import SQLAlchemy


CONNECTION_STRING = "sqlite:///Spark.db"

application = Flask(__name__)
application.config['SQLALCHEMY_DATABASE_URI'] = CONNECTION_STRING
database = SQLAlchemy(application)


class Day(database.Model):
    __tablename__ = "Day"
    date = database.Column("datum", database.String(50), primary_key = True)
    cnt = database.Column("Cnt", database.Integer)
    min_pop = database.Column("min_popularity", database.Integer)
    max_pop = database.Column("max_popularity", database.Integer)
    avg_pop = database.Column("average_popularity", database.Integer)

    genres = database.Column("genres", database.String(100))
    album_types = database.Column("album_types", database.String(50))

@application.route("/insertDay", methods = ["POST"])
def insertDay():
    dataDay = Day()
    dataDay.date = request.json['date']
    dataDay.cnt = request.json['cnt']
    dataDay.min_pop = request.json['min_pop']
    dataDay.max_pop = request.json['max_pop']
    dataDay.avg_pop = request.json['avg_pop']
    dataDay.genres = request.json['genres']
    dataDay.album_types = request.json['album_types']
    database.session.add(dataDay)
    database.session.commit()
    return "ok"

if __name__ == "__main__":
    application.run(debug=True)

