# import socketio
# # from aiohttp_wsgi import serve

# # import eventlet
# # create a Socket.IO server instance
# sio = socketio.Server()

# # create a Flask web application instance
# # app = socketio.WSGIApp(sio)

# # define an event handler for the 'message' event
# @sio.on('message')
# def handle_message(sid, message):
#     print('Received:', message)
    
#     # send a response back to the client
#     sio.emit('response', 'Hello, client!', room=sid)

# @sio.event
# def connect(sid, environ, auth):
#     print('connect ', sid)

# if __name__ == '__main__':
# #     # run the application on localhost:8000
# #     app.run(host='localhost', port=8000)
# #     # eventlet.wsgi.server(eventlet.listen(('127.0.0.1', 8000)), app)
#     sio.bind('http://localhost:5000')
#     sio.wait()
# server.py

from flask import Flask
from flask_socketio import SocketIO, emit
# from flask_cors import CORS

app = Flask(__name__)
# CORS(app, resources={r'/*': {'origins': '*'}})
socketio = SocketIO(app)

# @app.route('/')
# def index():
#     return render_template('index.html')

@socketio.on('client_message')
def handle_message(message):
    print('received message: ' + message)
    emit('message', "server_response")

@socketio.on('connect')
def connection():
    print('conn established!')
    emit("server_message","from server")

@socketio.on('event_name')
def handle_my_custom_event(json):
    emit('event_name', json)

if __name__ == '__main__':
    # app.run(port=8000)
    socketio.run(app)