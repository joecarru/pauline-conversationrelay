/**
 * Pauline Joseph — CAOS V1 ConversationRelay WebSocket Server
 * Architecture: Twilio ConversationRelay → WebSocket → Make.com → OpenAI → WebSocket → ElevenLabs
 *
 * THIS IS THE TEST SERVER for Pauline-Joseph-ElevenLabs-Test Studio flow.
 * It does NOT modify the working Pauline-Joseph-Voice-AI flow.
 * Make.com remains the AI orchestration layer. This server is voice transport only.
 *
 * Flow:
 *  1. Twilio ConversationRelay connects to /ws (WebSocket)
 *  2. Server sends Pauline's greeting via ElevenLabs (no Make.com needed for greeting)
 *  3. Caller speaks → Twilio STT → ConversationRelay sends 'prompt' event
 *  4. Server POSTs transcript to Make.com webhook (same webhook used by Studio today)
 *  5. Make.com runs CAOS logic → OpenAI → returns response text
 *  6. Server sends response text to ConversationRelay
 *  7. ConversationRelay sends text to ElevenLabs → caller hears natural voice
 */

import Fastify from 'fastify';
import FastifyWebSocket from '@fastify/websocket';
import { config } from 'dotenv';

config();

// ── Validate required environment variables at startup ─────────────────────────

const REQUIRED_ENV = ['MAKECOM_WEBHOOK_URL', 'SERVER_DOMAIN'];
const missing = REQUIRED_ENV.filter((k) => !process.env[k]);
if (missing.length) {
  console.error(`\n❌  Missing required environment variables: ${missing.join(', ')}\n`);
  process.exit(1);
}

// ── Pauline's opening greeting ─────────────────────────────────────────────────
// Sent immediately on call connect. Does NOT go through Make.com.
// This matches the greeting already tested and approved in the working Studio flow.

const GREETING =
  "Hi, this is Pauline Joseph calling with Modern Day Commodities. We're a Georgia commercial general contractor. I'm calling about a commercial construction opportunity and wanted to make sure I'm speaking with the right project contact.";

// ── Make.com webhook configuration ────────────────────────────────────────────

const MAKECOM_WEBHOOK_URL = process.env.MAKECOM_WEBHOOK_URL;
const MAKECOM_TIMEOUT_MS = parseInt(process.env.MAKECOM_TIMEOUT_MS || '12000');

// ── App setup ──────────────────────────────────────────────────────────────────

const app = Fastify({ logger: false });
await app.register(FastifyWebSocket);

// Session store: one entry per active call
const sessions = new Map();

// ── Health check ───────────────────────────────────────────────────────────────

app.get('/health', async () => ({
  status: 'ok',
  service: 'Pauline-ConversationRelay-ElevenLabs-Test',
  activeSessions: sessions.size,
  makecomWebhook: MAKECOM_WEBHOOK_URL.replace(/\/[^/]+$/, '/***'), // Redact webhook ID in logs
  elevenlabsVoice: process.env.ELEVENLABS_VOICE_ID || '(not set)',
  timestamp: new Date().toISOString(),
}));

// ── WebSocket handler ──────────────────────────────────────────────────────────

app.register(async (fastify) => {
  fastify.get('/ws', { websocket: true }, (socket, req) => {
    const sessionId = `sess_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

    const session = {
      id: sessionId,
      socket,
      callSid: null,
      from: null,    // Caller number (the contact being called)
      to: null,      // MDC outbound number
      history: [],   // Conversation history sent to Make.com for context
      processing: false,
    };

    sessions.set(sessionId, session);
    console.log(`\n[${sessionId}] New ConversationRelay session`);

    // ── Handle incoming events from Twilio ConversationRelay ──────────────────

    socket.on('message', async (raw) => {
      let event;
      try {
        event = JSON.parse(raw.toString());
      } catch {
        console.error(`[${sessionId}] Bad message:`, raw.toString().substring(0, 200));
        return;
      }

      switch (event.type) {

        // ── connected: call answered, ConversationRelay ready ──────────────────
        case 'connected':
          session.callSid = event.callSid;
          session.from = event.parameters?.To || null;   // Number that was called
          session.to = event.parameters?.From || null;   // MDC number
          console.log(`[${sessionId}] Connected | callSid: ${event.callSid}`);

          // Send Pauline's greeting immediately — no Make.com needed
          sendText(socket, GREETING, sessionId);
          session.history.push({ role: 'assistant', content: GREETING });
          break;

        // ── prompt: caller spoke, Twilio STT produced transcript ───────────────
        case 'prompt':
          const transcript = (event.voicePrompt || '').trim();
          if (!transcript) return;
          if (session.processing) {
            console.log(`[${sessionId}] Skipping prompt — still processing previous response`);
            return;
          }

          console.log(`[${sessionId}] Caller: "${transcript}"`);
          session.history.push({ role: 'user', content: transcript });
          session.processing = true;

          try {
            const aiResponse = await callMakecomWebhook(session, transcript, sessionId);
            session.history.push({ role: 'assistant', content: aiResponse });
            sendText(socket, aiResponse, sessionId);
          } catch (err) {
            console.error(`[${sessionId}] Make.com error:`, err.message);
            // Graceful fallback — don't let the call go silent
            const fallback = "I'm sorry, I had a brief issue there. Could you repeat that?";
            sendText(socket, fallback, sessionId);
          } finally {
            session.processing = false;
          }
          break;

        // ── interrupt: caller talked over Pauline ──────────────────────────────
        case 'interrupt':
          console.log(`[${sessionId}] Interrupted | heard: "${event.utteranceUntilInterrupt}"`);
          session.processing = false;
          // Update history to reflect only what was actually spoken before interrupt
          if (event.utteranceUntilInterrupt && session.history.length > 0) {
            const lastMsg = session.history[session.history.length - 1];
            if (lastMsg.role === 'assistant') {
              lastMsg.content = event.utteranceUntilInterrupt;
            }
          }
          break;

        // ── dtmf: caller pressed a key ─────────────────────────────────────────
        case 'dtmf':
          console.log(`[${sessionId}] DTMF: ${event.digit}`);
          break;

        // ── callEnded: call is over ────────────────────────────────────────────
        case 'callEnded':
          console.log(`[${sessionId}] Call ended | duration: ${event.callDuration || 'unknown'}s`);
          sessions.delete(sessionId);
          break;

        default:
          console.log(`[${sessionId}] Unknown event: ${event.type}`);
      }
    });

    socket.on('close', () => {
      console.log(`[${sessionId}] WebSocket closed`);
      sessions.delete(sessionId);
    });

    socket.on('error', (err) => {
      console.error(`[${sessionId}] WebSocket error:`, err.message);
      sessions.delete(sessionId);
    });
  });
});

// ── POST transcript to Make.com webhook ───────────────────────────────────────
//
// Make.com receives this payload and must return { "response": "..." }
//
// Payload sent to Make.com:
// {
//   "source": "conversationrelay",       ← identifies this as the ElevenLabs test path
//   "callSid": "CA...",
//   "sessionId": "sess_...",
//   "transcript": "caller's exact words",
//   "history": [                          ← full conversation so far (for context)
//     { "role": "assistant", "content": "Hi, this is Pauline..." },
//     { "role": "user", "content": "..." },
//     ...
//   ]
// }
//
// Make.com must return:
// { "response": "Pauline's reply text here" }
//
// If Make.com returns only a plain text string, the server handles that too.

async function callMakecomWebhook(session, transcript, sessionId) {
  const payload = {
    source: 'conversationrelay',
    callSid: session.callSid,
    sessionId: session.id,
    transcript: transcript,
    history: session.history,
  };

  console.log(`[${sessionId}] → Make.com | transcript: "${transcript}"`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), MAKECOM_TIMEOUT_MS);

  try {
    const res = await fetch(MAKECOM_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    if (!res.ok) {
      throw new Error(`Make.com returned HTTP ${res.status}`);
    }

    // Handle both JSON and plain text responses from Make.com
    const contentType = res.headers.get('content-type') || '';
    let responseText;

    if (contentType.includes('application/json')) {
      const json = await res.json();
      // Accept multiple possible field names for flexibility
      responseText =
        json.response ||
        json.text ||
        json.message ||
        json.reply ||
        JSON.stringify(json);
    } else {
      // Plain text response
      responseText = (await res.text()).trim();
    }

    if (!responseText) {
      throw new Error('Make.com returned empty response');
    }

    console.log(`[${sessionId}] ← Make.com | response: "${responseText.substring(0, 100)}${responseText.length > 100 ? '...' : ''}"`);
    return responseText;

  } catch (err) {
    if (err.name === 'AbortError') {
      throw new Error(`Make.com timed out after ${MAKECOM_TIMEOUT_MS}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

// ── Send text to ConversationRelay → ElevenLabs speaks it ────────────────────

function sendText(socket, text, sessionId) {
  if (socket.readyState !== 1 /* OPEN */) return;
  socket.send(JSON.stringify({
    type: 'text',
    token: text,
    last: true,     // Send as complete utterance (Make.com returns full sentence, not tokens)
  }));
  console.log(`[${sessionId}] → ElevenLabs: "${text.substring(0, 100)}${text.length > 100 ? '...' : ''}"`);
}

// ── Start server ───────────────────────────────────────────────────────────────

const port = parseInt(process.env.PORT || '8080');

try {
  await app.listen({ port, host: '0.0.0.0' });
  const domain = process.env.SERVER_DOMAIN || `localhost:${port}`;

  console.log(`
╔══════════════════════════════════════════════════════════════╗
║   Pauline Joseph — ConversationRelay ElevenLabs Test Server  ║
╠══════════════════════════════════════════════════════════════╣
║  WebSocket : wss://${domain.padEnd(41)}║
║  Health    : https://${domain.padEnd(40)}║
║  Make.com  : ${(MAKECOM_WEBHOOK_URL.length > 47 ? MAKECOM_WEBHOOK_URL.substring(0, 44) + '...' : MAKECOM_WEBHOOK_URL).padEnd(47)}║
║  Voice     : ElevenLabs ${(process.env.ELEVENLABS_VOICE_ID || '(set ELEVENLABS_VOICE_ID)').padEnd(37)}║
╚══════════════════════════════════════════════════════════════╝
`);
} catch (err) {
  console.error('Server failed to start:', err);
  process.exit(1);
}
