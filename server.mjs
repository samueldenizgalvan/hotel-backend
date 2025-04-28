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
    joinRequests: (() => {
      try {
        const parsed = typeof r.join_requests === 'string'
          ? JSON.parse(r.join_requests)
          : r.join_requests;
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    })(),
    
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
  console.log('✅ Partido guardado:', result.rows[0]);
  return result.rows[0].id;
}

async function updateJoinRequests(id, joinRequests) {
  const query = 'UPDATE matches SET join_requests = $1 WHERE id = $2';
  await pool.query(query, [JSON.stringify(joinRequests), id]);
}


// 4) Cron diario
cron.schedule('0 23 * * *', async () => {
  const today = new Date().toISOString().slice(0, 10);
  try {
    const res = await pool.query('SELECT id, date FROM matches WHERE date = $1', [today]);

    if (res.rowCount === 0) {
      console.log(`🧹 Cron-clean: ningún partido para eliminar el ${today}`);
      return;
    }

    const deletedIds = res.rows.map(r => r.id);
    await pool.query('DELETE FROM matches WHERE date = $1', [today]);

    console.log(`🧹 Cron-clean: eliminados ${deletedIds.length} partidos para el ${today}`);
    deletedIds.forEach(id => {
      console.log(`🗑️ Partido eliminado por cron: ID ${id}`);
    });

    // Emitir los partidos actualizados a todos los clientes conectados
    const result = await pool.query('SELECT * FROM matches');
    io.emit('matchesUpdate', result.rows);
    console.log(`📢 Enviado matchesUpdate con ${result.rowCount} partidos restantes`);

  } catch (err) {
    console.error('❌ Error en el cron de limpieza:', err);
  }
});



// 5) Handlers de Socket.IO
io.on('connection', socket => {
  console.log('🔌 Nuevo cliente conectado:', socket.id);

  // Identificación del usuario
  socket.on('identify', (userName) => {
    console.log(`👤 Cliente identificado: ${userName}`);
  });

  socket.on('editNote', async ({ matchId, newNote }) => {
    console.log("📥 editNote recibido:", matchId, newNote);
    try {
      await pool.query('UPDATE matches SET note = $1 WHERE id = $2', [newNote, matchId]);
      console.log('📝 Nota actualizada para', matchId);
  
      // Recargar partidos
      const res = await pool.query('SELECT hotel FROM matches WHERE id = $1', [matchId]);
      const hotel = res.rows[0]?.hotel;
  
      if (hotel) {
        const updatedMatches = await loadMatches(hotel);
        io.emit('existingMatches', updatedMatches);
      }
    } catch (err) {
      console.error('❌ Error al actualizar nota:', err);
    }
  });
  

  // Recibir y guardar el hotel del usuario
  socket.on('getMatches', async (hotel) => {
    socket.data.hotel = hotel;
    try {
      const data = await loadMatches(hotel);
      console.log(`📤 Enviando existingMatches para hotel ${hotel}: ${data.length} partidos`);
      socket.emit('existingMatches', data);
    } catch (err) {
      console.error('❌ Error al cargar partidos:', err);
    }
  });

  // Crear una nueva partida
  socket.on('createMatch', async match => {
    console.log('📥 createMatch recibido:', match);
    
    if (!match.date || match.date === '' || !match.time || match.time === '') {
      console.error('❌ Error: Fecha y hora son obligatorios');
      socket.emit('error', { message: 'Fecha y hora son obligatorios' });
      return;
    }
  
    try {
      const matchId = await saveMatch(match);
      console.log('✅ Partido guardado en BD:', matchId);
  
      // Verificar que el partido se guardó
      const verify = await pool.query('SELECT * FROM matches WHERE id = $1', [matchId]);
      console.log('🔍 Verificación del partido:', verify.rows);
  
      const updatedMatches = await loadMatches(match.hotel);
      console.log(`📤 Enviando existingMatches después de crear partido para hotel ${match.hotel}: ${updatedMatches.length} partidos`);
      io.emit('existingMatches', updatedMatches);
    } catch (err) {
      console.error('❌ Error al guardar partido:', err);
      socket.emit('error', { message: 'No se pudo guardar el partido' });
    }
  });

  // Petición para unirse a una partida
  socket.on('requestToJoin', async ({ matchId, request }) => {
    console.log('📥 requestToJoin recibido para match', matchId, request);
    try {
      const res = await pool.query('SELECT join_requests, hotel FROM matches WHERE id=$1', [matchId]);
      if (res.rowCount === 0) {
        console.error(`❌ No se encontró la partida con ID: ${matchId}`);
        return;
      }

      let { join_requests, hotel } = res.rows[0];
      if (!Array.isArray(join_requests)) {
        try {
          join_requests = JSON.parse(join_requests);
        } catch {
          join_requests = [];
        }
      }

      join_requests.push(request);
      await updateJoinRequests(matchId, join_requests);
      console.log('✅ JoinRequests actualizados en BD para', matchId);

      const updatedMatches = await loadMatches(hotel);
      io.emit('existingMatches', updatedMatches);
    } catch (err) {
      console.error('❌ Error al actualizar joinRequests:', err);
    }
  });

  // Eliminar un jugador de una partida
  socket.on('removePlayer', async ({ matchId, playerName }) => {
    console.log('📥 removePlayer recibido:', matchId, playerName);
    try {
      const res = await pool.query('SELECT join_requests, hotel FROM matches WHERE id=$1', [matchId]);
      if (res.rowCount === 0) {
        console.error(`❌ No se encontró la partida con ID: ${matchId}`);
        return;
      }

      let { join_requests, hotel } = res.rows[0];
      if (!Array.isArray(join_requests)) {
        try {
          join_requests = JSON.parse(join_requests);
        } catch {
          join_requests = [];
        }
      }

      const updatedJoinRequests = join_requests.filter(r => r.guestName !== playerName);
      await updateJoinRequests(matchId, updatedJoinRequests);
      console.log('✅ Participación eliminada para', playerName);

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
