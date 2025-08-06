from flask import Flask, request, jsonify
import time

app = Flask(__name__)

@app.route('/')
def index():
    return 'Redis Stream Service Backend is running.'


# In-memory storage for streams
streams = {}

# XADD: Add entry to a stream
@app.route('/xadd/<stream>', methods=['POST'])
def xadd(stream):
    data = request.json or {}
    entry_id = f"{int(request.args.get('ms', default=None) or int(time.time() * 1000))}-{len(streams.get(stream, []))}"
    entry = {'id': entry_id, 'fields': data}
    streams.setdefault(stream, []).append(entry)
    return jsonify({'id': entry_id})

# XRANGE: Get entries in a stream
@app.route('/xrange/<stream>', methods=['GET'])
def xrange(stream):
    start = request.args.get('start', '-')
    end = request.args.get('end', '+')
    count = int(request.args.get('count', 100))
    entries = streams.get(stream, [])
    # Simple implementation: ignore '-' and '+' for now, just return up to count entries
    return jsonify(entries[:count])

# XLEN: Get length of a stream
@app.route('/xlen/<stream>', methods=['GET'])
def xlen(stream):
    return jsonify({'length': len(streams.get(stream, []))})

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
    
    results = []
    for stream_name, last_id in zip(stream_names, ids):
        stream_entries = streams.get(stream_name, [])
        
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
