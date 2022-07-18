from audioop import avg
import statistics
from datetime import datetime, timedelta

from flask import Flask, jsonify
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
api = Api(app)
# docker run --name postgresql-dmole -e POSTGRES_USER=postgresUser -e POSTGRES_PASSWORD=postgresPasswordSeCrEt -e POSTGRES_DB=gh_api_db -p 5432:5432 -v /data:/data -d postgres
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgresUser:postgresPasswordSeCrEt@localhost:5432/gh_api_db"
db = SQLAlchemy(app)

class RepositoryModel(db.Model):
	id = db.Column(db.BigInteger, primary_key=True)
	owner = db.Column(db.String(100), nullable=False) 
	name = db.Column(db.String(100), nullable=False)

	def __repr__(self):
		return f"Repository (owner = {self.owner}, name = {self.name}, id = {self.id})"

class RepositoryEventModel(db.Model):
	id = db.Column(db.BigInteger, primary_key=True)
	repository_id = db.Column(db.BigInteger, nullable=False)
	event_type = db.Column(db.String(100), nullable=False) 
	event_time = db.Column(db.DateTime(100), nullable=False)

	def __repr__(self):
		return f"Repository event (id = {self.id}, repository_id = {self.repository_id} event_type = {self.event_type}, event_time = {self.event_time})"

db.create_all()

repository_model_resource_fields = {
	'id': fields.Integer,
	'owner': fields.String,
	'name': fields.String
}

class Repository(Resource):
	@marshal_with(repository_model_resource_fields)
	def get(self, owner, name):
		result = RepositoryModel.query.filter_by(owner=owner, name=name).first()
		if not result:
			abort(404, message=f"Could not find repository with owner={owner} and name={name}")
		return result

	@marshal_with(repository_model_resource_fields)
	def put(self, owner, name):
		# args = video_put_args.parse_args()
		result = RepositoryModel.query.filter_by(owner=owner, name=name).first()
		if result:
			abort(409, message=f"Repository with owner={owner} and name={name} already exists")

		video = RepositoryModel(owner=owner, name=name)
		db.session.add(video)
		db.session.commit()

		result = RepositoryModel.query.filter_by(owner=owner, name=name).first()
		return result, 201

	def delete(self,  owner, name):
		result = RepositoryModel.query.filter_by(owner=owner, name=name).first()
		if not result:
			abort(404, message=f"Repository with owner={owner} and name={name} does not exist")

		RepositoryModel.query.filter_by(owner=owner, name=name).delete()
		db.session.commit()
		return '', 204

class Repositories(Resource):
	@marshal_with(repository_model_resource_fields)
	def get (self):
		result = RepositoryModel.query.all()
		return result

class RepositoryMetrics(Resource):
	def get (self, owner, name, offset_minutes):
		repository_result = RepositoryModel.query.filter_by(owner=owner, name=name).first()
		if not repository_result:
			abort(404, message=f"Repository with owner={owner} and name={name} does not exist")

		time_before_offset_minutes = datetime.now() - timedelta(minutes=int(offset_minutes))

		repository_event_result = RepositoryEventModel.query.filter_by(repository_id = repository_result.id).filter(RepositoryEventModel.event_time >= time_before_offset_minutes).order_by(RepositoryEventModel.event_time.desc()).all() #.filter(RepositoryEventModel.event_time >= time_before_offset_minutes)

		avg_between_pulls = [abs((j.event_time-i.event_time).total_seconds()) for i,j in zip(repository_event_result, repository_event_result[1:])] 
		avg_between_pulls = "not enough pull events" if avg_between_pulls == [] else statistics.mean(avg_between_pulls)
		sum_watch_events = len(list(filter(lambda repo_event: repo_event.event_type == "WatchEvent", repository_event_result)))
		sum_pull_request_events = len(list(filter(lambda repo_event: repo_event.event_type == "PullRequestEvent", repository_event_result)))
		sum_issues_events = len(list(filter(lambda repo_event: repo_event.event_type == "IssuesEvent", repository_event_result)))

		return jsonify(
			{ "repository": f"{owner}/{name}", 
			"offset[min]": str(offset_minutes), 
			"average_time_between_pulls[s]": str(avg_between_pulls),
			"watch_events_count": str(sum_watch_events),
			"pull_request_events_count": str(sum_pull_request_events),
			"issues_events_count": str(sum_issues_events),
			})

api.add_resource(Repository, "/repository/<string:owner>/<string:name>")
api.add_resource(Repositories, "/repository")
api.add_resource(RepositoryMetrics, "/repositoryMetrics/<string:owner>/<string:name>/<string:offset_minutes>")

if __name__ == "__main__":
	app.run(debug=True)