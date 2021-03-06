import threading
import requests
import json
from datetime import datetime
import math

from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sqlalchemy.orm import Session

# TOKEN that needs to be filled in for api_miner to work
github_personal_access_token = ""

# declare database connection
base = declarative_base()
engine = sa.create_engine("postgresql://postgresUser:postgresPasswordSeCrEt@localhost:5432/gh_api_db")
base.metadata.bind = engine
session = orm.scoped_session(orm.sessionmaker())(bind=engine)

# declare models
class RepositoryModel(base):
    __tablename__ = 'repository_model' 
    id = sa.Column(sa.BigInteger, primary_key=True)
    owner = sa.Column(sa.String(100), nullable=False) 
    name = sa.Column(sa.String(100), nullable=False)

    def __repr__(self):
        return f"Repository (owner = {self.owner}, name = {self.name}, id = {self.id})"

class RepositoryEventModel(base):
    __tablename__ = 'repository_event_model' 
    id = sa.Column(sa.BigInteger, primary_key=True)
    repository_id = sa.Column(sa.BigInteger, nullable=False)
    event_type = sa.Column(sa.String(100), nullable=False) 
    event_time = sa.Column(sa.DateTime(100), nullable=False)

    def __repr__(self):
        return f"Repository event (id = {self.id}, repository_id = {self.repository_id} event_type = {self.event_type}, event_time = {self.event_time})"

def mine_api_invoke():
    try:
        session = Session(engine)
        repositories = session.query(RepositoryModel).all()
        repositories_count = len(repositories)

        for repository in repositories:
            # send request
            url = f"https://api.github.com/repos/{repository.owner}/{repository.name}/events"
            headers = {"Accept": "application/vnd.github.inertia-preview+json", "Authorization": f"token {github_personal_access_token}"} #{"Accept": "application/vnd.github.inertia-preview+json"}
            r = requests.get(url, headers=headers)
            events = json.loads(r.text)
            # convert response to list of dicts of events
            processed_events = list(map(lambda e: {"id":e["id"], "event_type":e["type"], "event_time":datetime.fromisoformat(e["created_at"].replace('T',' ').replace('Z','')), "repository_id": repository.id}, events))
            # filter events based on event type
            processed_events = list(filter(lambda e: e["event_type"] == "WatchEvent" or e["event_type"] == "PullRequestEvent" or e["event_type"] == "IssuesEvent", processed_events))
            # convert list of dicts of events to list of models of events
            processed_events = list(map(lambda e: RepositoryEventModel(id=e["id"], event_type=e["event_type"], event_time=e["event_time"], repository_id=e["repository_id"]), processed_events))
            # add events to db # TODO nicer as list
            if processed_events:
                for event in processed_events:
                    session.merge(event)
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        session.commit()
        session.close()
        # compute next invocation based on number of request and per request time computed from rate limit mentioned in Github docs (5000r/h)
        max_request_rate_evenly_distributed_sec =  (5000 / (60*60))
        how_long_to_wait_sec = math.ceil(max_request_rate_evenly_distributed_sec * repositories_count)
        # ensure next invocation
        threading.Timer(how_long_to_wait_sec, mine_api_invoke).start() 

mine_api_invoke()
pass
