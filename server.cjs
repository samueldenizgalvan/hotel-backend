const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

let matches = [];

io.on('connection', (socket) => {
  console.log('🔗 Client connected:', socket.id);

  socket.emit('existingMatches', matches);

  socket.on('createMatch', (match) => {
    matches.push(match);
    io.emit('matchCreated', match);
    console.log(`🎾 Match created by ${match.creatorName} (${match.sport} - ${match.date} ${match.time})`);
  });

  socket.on('requestToJoin', ({ matchId, request }) => {
    const match = matches.find(m => m.id === matchId);
    if (!match) return;

    const isLimited = ['Padel', 'Tennis', 'Table Tennis'].includes(match.sport);
    if (isLimited && match.joinRequests.length >= 3) {
      return;
    }

    match.joinRequests.push(request);
    io.emit('matchUpdated', match);
  });

  socket.on('updateNote', ({ matchId, note }) => {
    const match = matches.find(m => m.id === matchId);
    if (!match) return;

    match.note = note;
    io.emit('matchUpdated', match);
  });

  socket.on('disconnect', () => {
    console.log('❌ Client disconnected:', socket.id);
  });
});

// Limpieza automática de partidos caducados
setInterval(() => {
  const now = new Date();
  matches = matches.filter(match => {
    const matchDate = new Date(match.date);
    matchDate.setHours(0, 0, 0, 0);
    const expiration = new Date(matchDate);
    expiration.setDate(expiration.getDate() + 1);
    return now < expiration;
  });
}, 1000 * 60 * 10); // cada 10 minutos

server.listen(3000, () => {
  console.log('🚀 Server running on http://localhost:3000');
});
