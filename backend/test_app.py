import pytest
import json
import threading
from app import app, streams

@pytest.fixture
def client():
    # Clear the in-memory storage before each test to ensure isolation
    streams.clear()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

def test_xadd(client):
    data = {'key1': 'value1', 'key2': 'value2'}
    response = client.post('/xadd/test_stream', json=data)
    assert response.status_code == 200
    assert 'id' in json.loads(response.data)

def test_xrange(client):
    # First, add some data to the stream
    data1 = {'key1': 'value1'}
    data2 = {'key2': 'value2'}
    client.post('/xadd/test_stream', json=data1)
    client.post('/xadd/test_stream', json=data2)

    # Retrieve the data
    response = client.get('/xrange/test_stream')
    assert response.status_code == 200
    entries = json.loads(response.data)
    assert len(entries) > 0
    assert 'fields' in entries[0]

def test_xlen(client):
    # First, add some data to the stream
    data = {'key1': 'value1'}
    client.post('/xadd/test_stream', json=data)

    # Get the length of the stream
    response = client.get('/xlen/test_stream')
    assert response.status_code == 200
    length = json.loads(response.data)['length']
    assert length > 0

def test_xread(client):
    # First, add some data to the stream
    data1 = {'key1': 'value1'}
    data2 = {'key2': 'value2'}
    client.post('/xadd/test_stream', json=data1)
    client.post('/xadd/test_stream', json=data2)

    # Read the stream
    streams_param = 'test_stream 0-0'
    response = client.get(f'/xread?streams={streams_param}')
    assert response.status_code == 200
    results = json.loads(response.data)
    assert len(results) > 0
    assert results[0][0] == 'test_stream'

    #test $ parameter
    streams_param = 'test_stream $'
    response = client.get(f'/xread?streams={streams_param}')
    assert response.status_code == 200
    results = response.get_json()
    # should be an empty list since no new items were added after the last entry.
    assert results == []

def test_xadd_concurrently(client):
    """
    Tests that concurrently adding entries to the same stream is thread-safe.
    """
    stream_name = "concurrent_stream"
    num_threads = 10
    requests_per_thread = 20
    total_requests = num_threads * requests_per_thread

    def worker(thread_id):
        for i in range(requests_per_thread):
            # Each worker adds a unique entry
            data = {'thread': thread_id, 'request_num': i}
            response = client.post(f'/xadd/{stream_name}', json=data)
            assert response.status_code == 200

    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # After all threads are done, check the final length of the stream
    response = client.get(f'/xlen/{stream_name}')
    assert response.status_code == 200
    data = response.get_json()
    assert data['length'] == total_requests