# config.py (4 PARTISI, 3 NODE)

CLUSTER_TOPOLOGY = {
    "nodes": {
        0: {"host": "localhost", "port": 8000},
        1: {"host": "localhost", "port": 8001},
        2: {"host": "localhost", "port": 8002},
    },

    "partitions": {
        # Partisi 0: Leader di Node 0, Follower di Node 1
        0: {"leader": 0, "followers": [1]},
        
        # Partisi 1: Leader di Node 1, Follower di Node 2
        1: {"leader": 1, "followers": [2]},
        
        # Partisi 2: Leader di Node 2, Follower di Node 0
        2: {"leader": 2, "followers": [0]},
        
        # Partisi 3: Leader di Node 0, Follower di Node 2
        3: {"leader": 0, "followers": [2]},
    }
}