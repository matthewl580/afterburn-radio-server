const express = require("express");
const admin = require("firebase-admin");
const multer = require("multer");
const cors = require("cors");

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

// --- IN-MEMORY RADIO STATE ---
let songLibrary = []; // Array of { id, title, author, filePath, createdAt }
let playlistQueue = []; // Array of song IDs representing the upcoming rotation
let activeListeners = []; // Array of Express response streams connected to /stream

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

// --- 1. TRACK MANAGEMENT (ADD, EDIT, REMOVE) ---

// Upload a new song to Firebase and add to library
app.post("/api/songs", upload.single("audio"), async (req, res) => {
  try {
    const { title, author } = req.body;
    if (!req.file)
      return res.status(400).json({ error: "No audio file uploaded" });

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
      createdAt: new Date().toISOString(), // Track when the song was added
    };

    songLibrary.push(newSong);
    playlistQueue.push(fileId); // Automatically add new songs to the end of the queue

    res.status(201).json({ message: "Song added successfully", song: newSong });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get all songs in the library
app.get("/api/songs", (req, res) => {
  res.json(songLibrary);
});

// Get a single song by its ID (Translation endpoint)
app.get("/api/songs/:id", (req, res) => {
  const song = getSongById(req.params.id);
  if (!song) return res.status(404).json({ error: "Song not found" });
  res.json(song);
});

// Edit song metadata (title, author)
app.put("/api/songs/:id", (req, res) => {
  const { title, author } = req.body;
  const song = getSongById(req.params.id);

  if (!song) return res.status(404).json({ error: "Song not found" });

  if (title) song.title = title;
  if (author) song.author = author;

  res.json({ message: "Song updated successfully", song });
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
  res.json(detailedQueue);
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

    try {
      console.log(
        `Auto-DJ Streaming: ${song.title} by ${song.author} (ID: ${song.id}, Added: ${song.createdAt})`,
      );
      const fileStream = bucket.file(song.filePath).createReadStream();

      for await (const chunk of fileStream) {
        // If someone triggered a live broadcast mid-song, pause the track loop
        while (currentStreamMode === "LIVE") {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        // Pipe audio chunk to all listeners
        activeListeners.forEach((res) => {
          res.write(chunk);
        });
      }
    } catch (err) {
      console.error(`Error streaming track ${song.title}:`, err.message);
    }
  }
}

// Start server and background loop
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Radio backend running on port ${PORT}`);
  startAutoDJLoop();
});
