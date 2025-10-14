import time

def execute_query(collection, pipeline, query_name):
    """Executes an aggregation pipeline on a MongoDB collection and measures execution time."""
    print(f"Executing query: {query_name}")

    start_time = time.time()
    query_result = list(collection.aggregate(pipeline))
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Query '{query_name}' execution time: {execution_time:.4f} seconds")

    return query_result, execution_time