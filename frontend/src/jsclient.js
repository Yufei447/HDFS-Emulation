// const io = require('socket.io-client');

// // set the host and port number of the server
// const host = '127.0.0.1';
// const port = 5000;

// // connect to the server using socket.io
// const socket = io(`http://127.0.0.1:${port}`);

// socket.emit('message', 'Hello, server!');

// socket.on('connect', function() {
//     console.log('connected to server');
//   });
  
//   socket.on('message', function(message) {
//     console.log('received message: ' + message);
//   });
import { reactive } from "vue";
import { io } from "socket.io-client";

export const state = reactive({
  connected: false,
  messageEvents: []
});

export const client = io('http://localhost:8000');
// const client = io('http://localhost:5000');

client.on('connect', () => {
  // console.log('connected to server');
  // client.emit('message', directoryName);
  state.connected = true;
});

client.on("disconnect", () => {
  state.connected = false;
});

client.on('message', (...args) => {
  state.messageEvents.push(args);
});
