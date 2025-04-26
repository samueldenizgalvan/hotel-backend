// src/server.mjs
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import express from 'express';
import cors from 'cors';
import http from 'http';
import { Server } from 'socket.io';
import { Pool } from 'pg';
import cron from 'node-cron';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// 1) CORS
const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000'; // Ajusta según tu frontend
app.use(cors({ origin: FRONTEND }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: FRONTEND, methods: ['GET', 'POST'] },
});

// 2) PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// 3) Inicializar tabla desde src/init.sql
(async () => {
  try {
    const sqlPath = path.join(__dirname, 'init.sql');
    const sql = fs.readFileSync(sqlPath, 'utf8');
    await pool.query(sql);
    console.log('✅ Tabla `matches` lista en PostgreSQL');
  } catch (e) {
    console.error('❌ Error al inicializar la tabla:', e);
  }
})();

// --- Helpers load/save/update ---

async function loadMatches(hotel) {
  const query = 'SELECT * FROM matches WHERE hotel = $1';
  const { rows } = await pool.query(query, [hotel]);
  return rows.map(r => ({
    id: r.id,
    creatorName: r.creator_name,
    sport: r.sport,
    date: r.date.toISOString().slice(0, 10),
    time: r.time,
    note: r.note,
    joinRequests: r.join_requests,
    hotel: r.hotel,
  }));
}

async function saveMatch(match) {
  const { creatorName, sport, date, time, note, id, joinRequests, hotel } = match;
  const query = `
    INSERT INTO matches (id, creator_name, sport, date, time, note, join_requests, hotel)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id
  `;
  const values = [id, creatorName, sport, date, time, note, joinRequests, hotel];
  const result = await pool.query(query, values);
  return result.rows[0].id;
}

async function updateJoinRequests(id, joinRequests) {
  const query = 'UPDATE matches SET join_requests = $1 WHERE id = $2';
  await pool.query(query, [joinRequests, id]);
}

// 4) Cron diario
cron.schedule('0 23 * * *', async () => {
  const today = new Date().toISOString().slice(0, 10);
  await pool.query('DELETE FROM matches WHERE date = $1', [today]);
  console.log(`🧹 Cron-clean: eliminados partidos de ${today}`);
});

// 5) Handlers de Socket.IO
io.on('connection', socket => {
  console.log('🔌 Nuevo cliente conectado:', socket.id);

  let userHotel = null;

  // Guardar el hotel del usuario cuando se identifica
  socket.on('identify', (userName) => {
    console.log(`👤 Cliente identificado: ${userName}`);
  });

  // Manejar solicitud de partidos filtrados por hotel
  socket.on('getMatches', async (hotel) => {
    userHotel = hotel;
    try {
      const data = await loadMatches(hotel);
      console.log(`📤 Enviando existingMatches para hotel ${hotel}: ${data.length} partidos`);
      socket.emit('existingMatches', data);
    } catch (err) {
      console.error('❌ Error al cargar partidos:', err);
    }
  });

  // Enviar la lista inicial de partidos (si ya conocemos el hotel)
  if (userHotel) {
    loadMatches(userHotel)
      .then(data => {
        console.log(`📤 Enviando existingMatches inicial para hotel ${userHotel}: ${data.length} partidos`);
        socket.emit('existingMatches', data);
      })
      .catch(console.error);
  }

  // Cuando el cliente crea un partido
  socket.on('createMatch', async match => {
    console.log('📥 createMatch recibido:', match);
    try {
      const matchId = await saveMatch(match);
      console.log('✅ Partido guardado en BD:', matchId);

      // Cargar y emitir la lista actualizada de partidos para el hotel
      const updatedMatches = await loadMatches(match.hotel);
      console.log(`📤 Enviando existingMatches después de crear partido para hotel ${match.hotel}: ${updatedMatches.length} partidos`);
      io.emit('existingMatches', updatedMatches);
    } catch (err) {
      console.error('❌ Error al guardar partido:', err);
    }
  });

  // Cuando el cliente pide unirse
  socket.on('requestToJoin', async ({ matchId, request }) => {
    console.log('📥 requestToJoin recibido para match', matchId, request);
    try {
      const res = await pool.query('SELECT join_requests, hotel FROM matches WHERE id=$1', [matchId]);
      if (res.rowCount === 0) {
        console.error(`❌ No se encontró la partida con ID: ${matchId}`);
        return;
      }
      const { join_requests, hotel } = res.rows[0];
      join_requests.push(request);
      await updateJoinRequests(matchId, join_requests);
      console.log('✅ JoinRequests actualizados en BD para', matchId);

      // Cargar y emitir la lista actualizada de partidos para el hotel
      const updatedMatches = await loadMatches(hotel);
      io.emit('existingMatches', updatedMatches);
    } catch (err) {
      console.error('❌ Error al actualizar joinRequests:', err);
    }
  });

  // Cuando el cliente elimina su participación
  socket.on('removePlayer', async ({ matchId, playerName }) => {
    console.log('📥 removePlayer recibido:', matchId, playerName);
    try {
      const res = await pool.query('SELECT join_requests, hotel FROM matches WHERE id=$1', [matchId]);
      if (res.rowCount === 0) {
        console.error(`❌ No se encontró la partida con ID: ${matchId}`);
        return;
      }
      const { join_requests, hotel } = res.rows[0];
      const updatedJoinRequests = join_requests.filter(r => r.guestName !== playerName);
      await updateJoinRequests(matchId, updatedJoinRequests);
      console.log('✅ Participación eliminada para', playerName);

      // Cargar y emitir la lista actualizada de partidos para el hotel
      const updatedMatches = await loadMatches(hotel);
      io.emit('existingMatches', updatedMatches);
    } catch (err) {
      console.error('❌ Error al eliminar participación:', err);
    }
  });
});

// 6) Levantamos el servidor
const PORT = process.env.PORT || 10000; // Render usa el puerto 10000 según los logs
server.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));