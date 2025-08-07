from flask import Flask, request, jsonify
import time
from threading import Lock

app = Flask(__name__)

@app.route('/')
def index():
    return 'Redis Stream Service Backend is running.'

class Stream:
    """Represents a single stream, containing its entries and its own lock."""
    def __init__(self):
        self.entries = []
        self.lock = Lock()

# In-memory storage for streams
# The main dictionary is protected by a lock for safe addition/removal of streams.
# Each stream's entry list is protected by its own lock for concurrent operations.
streams = {}
streams_dict_lock = Lock()

# XADD: Add entry to a stream
@app.route('/xadd/<stream>', methods=['POST'])
def xadd(stream):
    data = request.json or {}
    # Get or create the stream object atomically under the main dict lock
    with streams_dict_lock:
        stream_obj = streams.setdefault(stream, Stream())

    # Lock only this specific stream to add an entry
    with stream_obj.lock:
        entry_id = f"{int(request.args.get('ms', default=None) or int(time.time() * 1000))}-{len(stream_obj.entries)}"
        entry = {'id': entry_id, 'fields': data}
        stream_obj.entries.append(entry)
    return jsonify({'id': entry_id})

# XRANGE: Get entries in a stream
@app.route('/xrange/<stream>', methods=['GET'])
def xrange(stream):
    count = int(request.args.get('count', 100))
    # Check for the stream object under the main dict lock
    with streams_dict_lock:
        stream_obj = streams.get(stream)

    if not stream_obj:
        return jsonify([])

    # Lock the specific stream, copy its data, and release
    with stream_obj.lock:
        entries = stream_obj.entries[:]
    # Simple implementation: ignore '-' and '+' for now, just return up to count entries
    return jsonify(entries[:count])

# XLEN: Get length of a stream
@app.route('/xlen/<stream>', methods=['GET'])
def xlen(stream):
    # Check for the stream object under the main dict lock
    with streams_dict_lock:
        stream_obj = streams.get(stream)

    if not stream_obj:
        return jsonify({'length': 0})

    # Lock the specific stream to get its length
    with stream_obj.lock:
        length = len(stream_obj.entries)
    return jsonify({'length': length})

# XREAD: Read entries from one or more streams
@app.route('/xread', methods=['GET'])
def xread():
    streams_param = request.args.get('streams')
    if not streams_param:
        return jsonify({'error': 'Streams parameter is required'}), 400

    streams_with_ids = streams_param.split()
    if len(streams_with_ids) % 2 != 0:
        return jsonify({'error': 'Streams and IDs must be paired'}), 400

    stream_names = []
    ids = []
    for i in range(0, len(streams_with_ids), 2):
        stream_names.append(streams_with_ids[i])
        ids.append(streams_with_ids[i+1])

    count = int(request.args.get('count', 100))

    # 1. Get the stream objects we need to read from
    stream_objs_map = {}
    with streams_dict_lock:
        for name in stream_names:
            if name in streams:
                stream_objs_map[name] = streams[name]

    # 2. To prevent deadlocks, always acquire locks in a consistent order (sorted by stream name)
    sorted_names = sorted(stream_objs_map.keys())
    acquired_locks = []
    copied_data = {}
    try:
        for name in sorted_names:
            lock = stream_objs_map[name].lock
            lock.acquire()
            acquired_locks.append(lock)

        # 3. With all necessary locks acquired, copy the data
        for name in sorted_names:
            copied_data[name] = stream_objs_map[name].entries[:]
    finally:
        # 4. Release all locks, even if an error occurred
        for lock in reversed(acquired_locks):
            lock.release()

    # 5. Process the copied data without holding any locks
    results = []
    for stream_name, last_id in zip(stream_names, ids):        
        stream_entries = copied_data.get(stream_name, [])
        
        effective_id = last_id
        if last_id == '$':
            if stream_entries:
                effective_id = stream_entries[-1]['id']
            else:
                # If stream is empty, we want only new items. In a non-blocking call,
                # there are none. Use a very high ID to ensure no existing items are matched.
                effective_id = f'{int(time.time() * 1000) + 3600000}-0'

        stream_results = []
        for entry in stream_entries:
            if entry['id'] > effective_id:
                stream_results.append(entry)
        
        if stream_results:
            results.append([stream_name, stream_results[:count]])

    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
