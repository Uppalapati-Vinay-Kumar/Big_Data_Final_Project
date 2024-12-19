"""
Connects to a Cassandra database using DataStax Astra's Secure Connect Bundle,
authenticates using credentials from a JSON file, and sets the keyspace to 'spotify'.
"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Secure Connect Bundle Path
cloud_config= {
  'secure_connect_bundle': 'secure-connect-vinay-db.zip'
}

# Token for the database
with open("vinay_db-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]


auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
if session:
  print('Connected!')
else:
  print("An error occurred.")


# Setting the keyspace
session.set_keyspace('spotify')
print(f"Connected to keyspace: {session.keyspace}")

def get_session():
    """
    Returns the current Cassandra session.
    
    Returns:
        session (Cassandra session): Active session for 'spotify' keyspace.
    """
    return session