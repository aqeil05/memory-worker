// ─── KV helpers ───────────────────────────────────────────────────────────────

// Safe JSON parse — returns fallback instead of throwing on corrupt KV values.
function safeJson(val, fallback) {
  if (val === null || val === undefined) return fallback;
  try {
    return JSON.parse(val);
  } catch {
    console.error(`dedup: corrupt KV value (${String(val).slice(0, 80)}…), using fallback`);
    return fallback;
  }
}
// KV key layout (mem: prefix avoids collision with CRM worker keys):
//   mem:msg:{messageId}          → "1"           (90-day TTL) — email dedup (per message, not thread)
//   mem:sub:{email}              → JSON string    (3-day TTL)  — Graph subscriptions
//   cache:graph_token            → access token   (55-min TTL) — shared with CRM worker
//   group:{chatId}               → JSON string    (permanent)  — group→project mapping
//   setup:{token}                → JSON string    (48h TTL)    — Closed Won setup tokens
//   tg:bot:history:{chatId}      → JSON array     (2h TTL)     — conversation history
//   tg:bot:pending:{chatId}      → JSON string    (10-min TTL) — clarification follow-up

const TTL = {
  CONVERSATION:  60 * 60 * 24 * 90,  // 90 days
  GRAPH_TOKEN:   60 * 55,             // 55 minutes
  SUBSCRIPTION:  60 * 60 * 24 * 3,   // 3 days
  SETUP_TOKEN:   60 * 60 * 48,        // 48 hours
  BOT_HISTORY:   60 * 60 * 2,         // 2 hours
  BOT_PENDING:   60 * 10,             // 10 minutes
};

// ── Email message dedup (by messageId — unique per email, not per thread) ─────
// Using messageId (not conversationId) so every new email in a thread is
// processed individually, preserving full chronological project history.

export async function isKnownMessage(kv, messageId) {
  return (await kv.get(`mem:msg:${messageId}`)) !== null;
}

export async function markMessage(kv, messageId) {
  await kv.put(`mem:msg:${messageId}`, "1", { expirationTtl: TTL.CONVERSATION });
}

// ── Microsoft Graph token cache (shared with CRM — same Azure app) ────────────

export async function getCachedGraphToken(kv) {
  return kv.get("cache:graph_token");
}

export async function setCachedGraphToken(kv, token) {
  await kv.put("cache:graph_token", token, { expirationTtl: TTL.GRAPH_TOKEN });
}

// ── Graph subscription state (memory worker's own subscriptions) ──────────────

export async function getSubscription(kv, email) {
  const val = await kv.get(`mem:sub:${email}`);
  return safeJson(val, null);
}

export async function setSubscription(kv, email, data) {
  await kv.put(`mem:sub:${email}`, JSON.stringify(data), { expirationTtl: TTL.SUBSCRIPTION });
}

// ── Telegram group → project mapping ─────────────────────────────────────────

export async function getGroupProject(kv, chatId) {
  const val = await kv.get(`group:${chatId}`);
  return safeJson(val, null);
}

export async function setGroupProject(kv, chatId, data) {
  // Permanent — no TTL. Group link persists until manually removed.
  await kv.put(`group:${chatId}`, JSON.stringify(data));
}

// ── Closed Won setup tokens ───────────────────────────────────────────────────

export async function getSetupToken(kv, token) {
  const val = await kv.get(`setup:${token}`);
  return safeJson(val, null);
}

export async function setSetupToken(kv, token, data) {
  await kv.put(`setup:${token}`, JSON.stringify(data), { expirationTtl: TTL.SETUP_TOKEN });
}

export async function deleteSetupToken(kv, token) {
  await kv.delete(`setup:${token}`);
}

// ── Bot conversation history ──────────────────────────────────────────────────

export async function getBotHistory(kv, chatId) {
  const val = await kv.get(`tg:bot:history:${chatId}`);
  return safeJson(val, []);
}

export async function setBotHistory(kv, chatId, history) {
  await kv.put(`tg:bot:history:${chatId}`, JSON.stringify(history), { expirationTtl: TTL.BOT_HISTORY });
}

// ── Clarification / report-topic pending state ────────────────────────────────
// payload.type = "clarification" | "report"

export async function getBotPending(kv, chatId) {
  const val = await kv.get(`tg:bot:pending:${chatId}`);
  return safeJson(val, null);
}

export async function setBotPending(kv, chatId, data) {
  await kv.put(`tg:bot:pending:${chatId}`, JSON.stringify(data), { expirationTtl: TTL.BOT_PENDING });
}

export async function deleteBotPending(kv, chatId) {
  await kv.delete(`tg:bot:pending:${chatId}`);
}

// ── Report/summary draft (awaiting export or refine button) ───────────────────
// payload = { type: "summary"|"report", topic, project, json, iteration }

const DRAFT_TTL = 60 * 60 * 2; // 2 hours — matches BOT_HISTORY TTL

export async function getDraft(kv, chatId) {
  const val = await kv.get(`tg:bot:draft:${chatId}`);
  return safeJson(val, null);
}

export async function setDraft(kv, chatId, data) {
  await kv.put(`tg:bot:draft:${chatId}`, JSON.stringify(data), { expirationTtl: DRAFT_TTL });
}

export async function deleteDraft(kv, chatId) {
  await kv.delete(`tg:bot:draft:${chatId}`);
}

// ── Refine-pending: waiting for feedback text after Refine button tapped ──────

export async function getRefinePending(kv, chatId) {
  const val = await kv.get(`tg:bot:refine:${chatId}`);
  return safeJson(val, null);
}

export async function setRefinePending(kv, chatId, data) {
  await kv.put(`tg:bot:refine:${chatId}`, JSON.stringify(data), { expirationTtl: TTL.BOT_PENDING });
}

export async function deleteRefinePending(kv, chatId) {
  await kv.delete(`tg:bot:refine:${chatId}`);
}
