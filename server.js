const express = require("express");
const admin = require("firebase-admin");
const multer = require("multer");
const cors = require("cors");
const { parseBuffer } = require("music-metadata");

// Initialize Firebase Admin using Render environment variable
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_KEY_JSON);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  storageBucket: "afterburn-radio.firebasestorage.app",
});

const bucket = admin.storage().bucket();
const app = express();
app.use(cors());
app.use(express.json());

// Multer configuration to handle temporary file uploads for songs
const upload = multer({ storage: multer.memoryStorage() });
const MAX_CHUNK_SIZE_BYTES = 512 * 1024;

// --- IN-MEMORY RADIO STATE ---
let songLibrary = []; // Array of { id, title, author, filePath, createdAt }
let playlistQueue = []; // Array of song IDs representing the upcoming rotation
let activeListeners = []; // Array of Express response streams connected to /stream
let currentPlayback = null; // { songId, currentChunkIndex, nextChunkIndex, positionSeconds }

let currentStreamMode = "AUTODJ"; // 'AUTODJ' or 'LIVE'
let liveBroadcastBuffer = []; // Chunks for live broadcast interruption

// --- ID TRANSLATION & HELPER UTILITIES ---

/**
 * Translates a single Song ID into its full song object.
 * Returns null if the ID does not exist in the library.
 */
function getSongById(id) {
  return songLibrary.find((song) => song.id === id) || null;
}

/**
 * Translates an array of Song IDs into an array of full song objects (filtering out stale/deleted IDs).
 */
function resolveQueueIds(ids) {
  return ids.map((id) => getSongById(id)).filter(Boolean);
}

function serializeSong(song) {
  return {
    ...song,
    chunks: song.chunks.map(
      ({ index, sizeBytes, startSeconds, durationSeconds }) => ({
        index,
        sizeBytes,
        startSeconds,
        durationSeconds,
        url: `/api/songs/${song.id}/chunks/${index}`,
      }),
    ),
  };
}

function createTrackChunks(fileSize, durationSeconds) {
  const chunks = [];

  for (let startByte = 0, index = 0; startByte < fileSize; index += 1) {
    const endByte = Math.min(startByte + MAX_CHUNK_SIZE_BYTES, fileSize) - 1;
    // Clip timing is proportional to bytes, so variable-bitrate tracks are approximate.
    const startSeconds = (startByte / fileSize) * durationSeconds;
    const endSeconds = ((endByte + 1) / fileSize) * durationSeconds;

    chunks.push({
      index,
      startByte,
      endByte,
      sizeBytes: endByte - startByte + 1,
      startSeconds,
      durationSeconds: endSeconds - startSeconds,
    });
    startByte = endByte + 1;
  }

  return chunks;
}

async function readTrackChunk(song, chunk) {
  const buffers = [];
  let bytesRead = 0;
  const fileStream = bucket
    .file(song.filePath)
    .createReadStream({ start: chunk.startByte, end: chunk.endByte });

  for await (const buffer of fileStream) {
    buffers.push(buffer);
    bytesRead += buffer.length;
  }

  if (bytesRead !== chunk.sizeBytes) {
    throw new Error(
      `Incomplete clip ${chunk.index} for track ${song.id}: expected ${chunk.sizeBytes} bytes, received ${bytesRead}`,
    );
  }

  return Buffer.concat(buffers, bytesRead);
}

// --- 1. TRACK MANAGEMENT (ADD, EDIT, REMOVE) ---

// Upload a new song to Firebase and add to library
app.post("/api/songs", upload.single("audio"), async (req, res) => {
  try {
    const { title, author } = req.body;
    if (!req.file)
      return res.status(400).json({ error: "No audio file uploaded" });

    let durationSeconds;
    try {
      const metadata = await parseBuffer(req.file.buffer, {
        mimeType: req.file.mimetype,
        size: req.file.size,
      });
      durationSeconds = metadata.format.duration;
    } catch (error) {
      return res.status(400).json({
        error: "Unable to read the audio duration from the uploaded track",
        details: error.message,
      });
    }

    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
      return res
        .status(400)
        .json({ error: "The uploaded track has no readable duration" });
    }

    const fileId = Date.now().toString();
    const fileName = `tracks/${fileId}_${req.file.originalname}`;
    const fileRef = bucket.file(fileName);

    // Upload file buffer to Firebase Storage
    await fileRef.save(req.file.buffer, {
      metadata: { contentType: req.file.mimetype },
    });

    const newSong = {
      id: fileId,
      title: title || "Unknown Title",
      author: author || "Unknown Artist",
      filePath: fileName,
      mimeType: req.file.mimetype,
      fileSizeBytes: req.file.size,
      durationSeconds,
      chunks: createTrackChunks(req.file.size, durationSeconds),
      createdAt: new Date().toISOString(), // Track when the song was added
    };

    songLibrary.push(newSong);
    playlistQueue.push(fileId); // Automatically add new songs to the end of the queue

    res
      .status(201)
      .json({ message: "Song added successfully", song: serializeSong(newSong) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get all songs in the library
app.get("/api/songs", (req, res) => {
  res.json(songLibrary.map(serializeSong));
});

// Get a single song by its ID (Translation endpoint)
app.get("/api/songs/:id", (req, res) => {
  const song = getSongById(req.params.id);
  if (!song) return res.status(404).json({ error: "Song not found" });
  res.json(serializeSong(song));
});

// Return one byte-range clip from a track
app.get("/api/songs/:id/chunks/:index", (req, res) => {
  const song = getSongById(req.params.id);
  if (!song) return res.status(404).json({ error: "Song not found" });

  const chunkIndex = Number(req.params.index);
  if (!Number.isInteger(chunkIndex) || chunkIndex < 0) {
    return res.status(400).json({ error: "Invalid clip index" });
  }

  const chunk = song.chunks[chunkIndex];
  if (!chunk) return res.status(404).json({ error: "Clip not found" });

  const fileStream = bucket.file(song.filePath).createReadStream({
    start: chunk.startByte,
    end: chunk.endByte,
  });

  res.status(200).set({
    "Content-Type": song.mimeType || "application/octet-stream",
    "Content-Length": chunk.sizeBytes,
    "X-Track-Id": song.id,
    "X-Track-Duration-Seconds": String(song.durationSeconds),
    "X-Track-Position-Seconds": String(chunk.startSeconds),
    "X-Clip-Index": String(chunk.index),
    "X-Clip-Start-Seconds": String(chunk.startSeconds),
    "X-Clip-Duration-Seconds": String(chunk.durationSeconds),
  });

  fileStream.on("error", (error) => {
    console.error(`Error reading clip ${chunk.index} for track ${song.id}:`, error);
    if (res.headersSent) {
      res.destroy(error);
    } else {
      res.status(500).json({ error: "Unable to read track clip" });
    }
  });
  fileStream.pipe(res);
});

// Edit song metadata (title, author)
app.put("/api/songs/:id", (req, res) => {
  const { title, author } = req.body;
  const song = getSongById(req.params.id);

  if (!song) return res.status(404).json({ error: "Song not found" });

  if (title) song.title = title;
  if (author) song.author = author;

  res.json({ message: "Song updated successfully", song: serializeSong(song) });
});

// Remove a song from library and queue
app.delete("/api/songs/:id", async (req, res) => {
  try {
    const songIndex = songLibrary.findIndex((s) => s.id === req.params.id);
    if (songIndex === -1)
      return res.status(404).json({ error: "Song not found" });

    const song = songLibrary[songIndex];

    // Delete from Firebase Storage
    await bucket
      .file(song.filePath)
      .delete()
      .catch(() => {});

    // Remove from library and queue
    songLibrary.splice(songIndex, 1);
    playlistQueue = playlistQueue.filter((id) => id !== req.params.id);

    res.json({ message: "Song deleted successfully" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// --- 3. QUEUE MANAGEMENT API ---

// Get current queue state (translates IDs into detailed song objects for clients)
app.get("/api/queue", (req, res) => {
  const detailedQueue = resolveQueueIds(playlistQueue);
  res.json(detailedQueue.map(serializeSong));
});

// Get raw queue IDs only
app.get("/api/queue/ids", (req, res) => {
  res.json(playlistQueue);
});

// Reorder or overwrite the queue using an array of song IDs
app.put("/api/queue", (req, res) => {
  const { newQueueIds } = req.body; // Expects an array of song IDs
  if (!Array.isArray(newQueueIds))
    return res
      .status(400)
      .json({ error: "Invalid queue format. Expects an array of song IDs." });

  // Optional validation: ensure IDs actually exist in the library before applying
  playlistQueue = newQueueIds.filter((id) => getSongById(id) !== null);

  res.json({ message: "Queue updated successfully", queue: playlistQueue });
});

// --- 4. LIVE BROADCAST INTERRUPTION API ---

// Start a live broadcast (interrupts Auto-DJ)
app.post("/api/live/start", (req, res) => {
  currentStreamMode = "LIVE";
  liveBroadcastBuffer = [];
  res.json({ message: "Live broadcast started. Stream mode is now LIVE." });
});

// Push audio chunks from your microphone/client during live broadcast
app.post(
  "/api/live/chunk",
  express.raw({ type: "audio/mpeg", limit: "5mb" }),
  (req, res) => {
    if (currentStreamMode !== "LIVE") {
      return res
        .status(400)
        .json({ error: "Station is not currently in LIVE mode" });
    }

    const chunk = req.body;
    // Broadcast live chunk directly to all connected listeners instantly
    activeListeners.forEach((listenerRes) => {
      listenerRes.write(chunk);
    });

    res.sendStatus(200);
  },
);

// End live broadcast (returns control back to Auto-DJ loop)
app.post("/api/live/end", (req, res) => {
  currentStreamMode = "AUTODJ";
  res.json({ message: "Live broadcast ended. Returning to Auto-DJ rotation." });
});

// --- 5. THE STREAMING ENDPOINT & AUTO-DJ LOOP ---

// Client listener endpoint
app.get("/stream", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "audio/mpeg",
    "Transfer-Encoding": "chunked",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });

  activeListeners.push(res);

  req.on("close", () => {
    activeListeners = activeListeners.filter((listener) => listener !== res);
  });
});

// Current Auto-DJ track position and clip to request next
app.get("/api/playback", (req, res) => {
  const song = currentPlayback ? getSongById(currentPlayback.songId) : null;
  const nextChunk =
    song && currentPlayback.nextChunkIndex !== null
      ? song.chunks[currentPlayback.nextChunkIndex]
      : null;

  res.json({
    mode: currentStreamMode,
    paused: currentStreamMode === "LIVE" && song !== null,
    song: song
      ? {
          id: song.id,
          title: song.title,
          author: song.author,
          durationSeconds: song.durationSeconds,
        }
      : null,
    currentChunkIndex: currentPlayback?.currentChunkIndex ?? null,
    nextChunkIndex: currentPlayback?.nextChunkIndex ?? null,
    positionSeconds: currentPlayback?.positionSeconds ?? 0,
    chunkDurationSeconds: nextChunk?.durationSeconds ?? null,
    chunkUrl:
      song && nextChunk
        ? `/api/songs/${song.id}/chunks/${nextChunk.index}`
        : null,
  });
});

// Background Auto-DJ Engine Loop
async function startAutoDJLoop() {
  while (true) {
    if (currentStreamMode === "LIVE" || playlistQueue.length === 0) {
      // If live or queue is empty, pause briefly and check again
      await new Promise((resolve) => setTimeout(resolve, 1000));
      continue;
    }

    // Pull the next song ID from the queue using our translation helper
    const nextSongId = playlistQueue.shift();
    const song = getSongById(nextSongId);

    if (!song) continue; // Skip if song was deleted/missing

    // Put it back at the end of the queue for a continuous loop rotation
    playlistQueue.push(nextSongId);

    currentPlayback = {
      songId: song.id,
      currentChunkIndex: null,
      nextChunkIndex: 0,
      positionSeconds: 0,
    };

    try {
      console.log(
        `Auto-DJ Streaming: ${song.title} by ${song.author} (ID: ${song.id}, Added: ${song.createdAt})`,
      );
      for (const chunk of song.chunks) {
        while (currentStreamMode === "LIVE") {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        const clipBuffer = await readTrackChunk(song, chunk);

        // Keep the entire clip pending until the live broadcast has finished.
        while (currentStreamMode === "LIVE") {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        activeListeners.forEach((listenerRes) => {
          listenerRes.write(clipBuffer);
        });
        currentPlayback = {
          songId: song.id,
          currentChunkIndex: chunk.index,
          nextChunkIndex: chunk.index + 1,
          positionSeconds: chunk.startSeconds + chunk.durationSeconds,
        };
      }
      currentPlayback = null;
    } catch (err) {
      console.error(`Error streaming track ${song.title}:`, err);
      if (currentPlayback?.songId === song.id) currentPlayback = null;
    }
  }
}

// Start server and background loop
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Radio backend running on port ${PORT}`);
  startAutoDJLoop();
});
