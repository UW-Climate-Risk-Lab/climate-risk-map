from flask import Flask, send_file, request, Response
import os

app = Flask(__name__)
DIRECTORY = os.getenv('DIRECTORY', '/usr/src/app/data')

@app.route('/<path:filename>', methods=['GET'])
def download_file(filename):
    path = os.path.join(DIRECTORY, filename)
    print(f"Path: {path}")
    if not os.path.isfile(path):
        return "File not found", 404
    
    range_header = request.headers.get('Range', None)
    if not range_header:
        return send_file(path)
    
    size = os.path.getsize(path)
    byte_range = range_header.strip().split('=')[1]
    byte_range = byte_range.split('-')
    byte_range = [int(b) if b else None for b in byte_range]
    
    start = byte_range[0] if byte_range[0] else 0
    end = byte_range[1] if byte_range[1] else size - 1
    
    length = end - start + 1

    with open(path, 'rb') as f:
        f.seek(start)
        data = f.read(length)
    
    response = Response(data, 206, mimetype='application/octet-stream')
    response.headers.add('Content-Range', f'bytes {start}-{end}/{size}')
    return response

if __name__ == '__main__':
    app.run(port=8080, host='0.0.0.0')

