// ─── Microsoft Graph API ──────────────────────────────────────────────────────

import { getCachedGraphToken, setCachedGraphToken, getSubscription, setSubscription } from "./dedup.js";

const GRAPH_BASE = "https://graph.microsoft.com/v1.0";
const GRAPH_SCOPE = "https://graph.microsoft.com/.default";

// ── Retry helper ──────────────────────────────────────────────────────────────
// Retries the given async function up to `retries` times with exponential backoff.
// Only retries on thrown errors (network failures, non-ok responses that throw).

async function withRetry(fn, retries = 3) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < retries - 1) {
        const delay = Math.pow(2, i) * 500; // 500ms, 1s, 2s
        console.warn(`Graph API retry ${i + 1}/${retries} in ${delay}ms: ${err.message}`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

export const INBOXES = [
  "peterkimani@wearedaya.com",
  "procurement@wearedaya.com",
];

// ── Access token (client credentials, cached 55 min) ─────────────────────────

export async function getAccessToken(env) {
  const cached = await getCachedGraphToken(env.DAYA_KV);
  if (cached) return cached;

  const tokenUrl = `https://login.microsoftonline.com/${env.AZURE_TENANT_ID}/oauth2/v2.0/token`;
  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: env.AZURE_CLIENT_ID,
      client_secret: env.AZURE_CLIENT_SECRET,
      scope: GRAPH_SCOPE,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Graph token fetch failed: ${res.status} ${body}`);
  }

  const { access_token } = await res.json();
  await setCachedGraphToken(env.DAYA_KV, access_token);
  return access_token;
}

// ── Fetch full email message (includes hasAttachments flag) ───────────────────

export async function fetchMessage(env, userEmail, messageId) {
  const select = "id,subject,body,from,conversationId,receivedDateTime,hasAttachments";
  const url = `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/messages/${messageId}?$select=${select}`;

  const msg = await withRetry(async () => {
    let token = await getAccessToken(env);
    let res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });

    // If 401, the cached token may be stale — clear it and retry once with a fresh one
    if (res.status === 401) {
      await setCachedGraphToken(env.DAYA_KV, null);
      token = await getAccessToken(env);
      res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`fetchMessage failed: ${res.status} ${body}`);
    }
    return res.json();
  });
  const rawHtml = msg.body?.content || "";
  const bodyText = msg.body?.contentType?.toLowerCase() === "text"
    ? rawHtml
    : stripHtml(rawHtml);

  return {
    id: msg.id,
    subject: msg.subject || "",
    bodyText,
    from: msg.from?.emailAddress?.address || "",
    fromName: msg.from?.emailAddress?.name || "",
    conversationId: msg.conversationId || "",
    receivedAt: msg.receivedDateTime || "",
    hasAttachments: msg.hasAttachments || false,
  };
}

// ── Fetch PDF attachments for a message ───────────────────────────────────────
// Returns [{filename, contentBytes}] — contentBytes is already base64 from Graph.
// Skips PDFs over 4MB (Graph limit for inline contentBytes).

// ── Fetch document attachments (PDF + Word) ───────────────────────────────────
// Returns { pdfs: [{filename, contentBytes}], docxTexts: [{filename, text}] }
// Uses two-step approach to avoid 400 errors from itemAttachment/referenceAttachment:
//   Step 1: List metadata only (no contentBytes in $select)
//   Step 2: Fetch content individually for each matching attachment

import { extractDocxText } from "./docx-reader.js";

const DOCX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
const MAX_ATTACHMENT_SIZE = 4 * 1024 * 1024; // 4MB

export async function fetchDocumentAttachments(env, userEmail, messageId, hasAttachments, maxSize = MAX_ATTACHMENT_SIZE) {
  if (!hasAttachments) return { pdfs: [], docxTexts: [] };

  const token = await getAccessToken(env);

  // Step 1: List attachment metadata only
  // @odata.type is returned automatically — do not include in $select
  const listUrl = `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/messages/${encodeURIComponent(messageId)}/attachments` +
    `?$select=id,name,contentType,size`;

  let listData;
  try {
    listData = await withRetry(async () => {
      const res = await fetch(listUrl, { headers: { Authorization: `Bearer ${token}` } });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`fetchDocumentAttachments list failed: ${res.status} ${body}`);
      }
      return res.json();
    });
  } catch (err) {
    console.warn(`fetchDocumentAttachments list gave up after retries — skipping: ${err.message}`);
    return { pdfs: [], docxTexts: [] };
  }
  const allAtts = listData.value || [];

  const pdfAtts = allAtts.filter(a =>
    a["@odata.type"] === "#microsoft.graph.fileAttachment" &&
    a.contentType === "application/pdf" &&
    a.size < maxSize
  );
  const docxAtts = allAtts.filter(a =>
    a["@odata.type"] === "#microsoft.graph.fileAttachment" &&
    a.contentType === DOCX_CONTENT_TYPE &&
    a.size < maxSize
  );

  if (pdfAtts.length === 0 && docxAtts.length === 0) return { pdfs: [], docxTexts: [] };

  // Step 2: Fetch content for each attachment individually
  const pdfs = [];
  const docxTexts = [];

  for (const att of [...pdfAtts, ...docxAtts]) {
    const contentUrl = `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/messages/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(att.id)}`;

    let content;
    try {
      content = await withRetry(async () => {
        const res = await fetch(contentUrl, { headers: { Authorization: `Bearer ${token}` } });
        if (!res.ok) {
          const body = await res.text();
          throw new Error(`content fetch failed: ${res.status} ${body}`);
        }
        return res.json();
      });
    } catch (err) {
      console.warn(`fetchDocumentAttachments: skipping "${att.name}" after retries: ${err.message}`);
      continue;
    }
    if (!content.contentBytes) continue;

    if (att.contentType === "application/pdf") {
      pdfs.push({ filename: att.name, contentBytes: content.contentBytes });
    } else {
      // Decode base64 → Uint8Array → extract text from docx ZIP
      const binaryStr = atob(content.contentBytes);
      const bytes = new Uint8Array(binaryStr.length);
      for (let i = 0; i < binaryStr.length; i++) bytes[i] = binaryStr.charCodeAt(i);

      const text = await extractDocxText(bytes);
      if (text) {
        console.log(`Extracted ${text.length} chars from Word doc: ${att.name}`);
        docxTexts.push({ filename: att.name, text });
      }
    }
  }

  return { pdfs, docxTexts };
}

// ── Fetch recent messages (lightweight — IDs + metadata only, no body) ────────

export async function fetchRecentMessages(env, userEmail, limit = 100) {
  const select = "id,conversationId,subject,receivedDateTime";
  // Fetch in pages of 100 (Graph API max recommended page size),
  // following @odata.nextLink until we have `limit` emails or the inbox is exhausted.
  let url = `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/mailFolders/Inbox/messages` +
    `?$select=${select}&$top=100&$orderby=receivedDateTime desc`;

  const all = [];
  while (url && all.length < limit) {
    const data = await withRetry(async () => {
      let token = await getAccessToken(env);
      let res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });

      if (res.status === 401) {
        await setCachedGraphToken(env.DAYA_KV, null);
        token = await getAccessToken(env);
        res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`fetchRecentMessages failed for ${userEmail}: ${res.status} ${body}`);
      }
      return res.json();
    });

    for (const msg of data.value || []) {
      all.push({
        id: msg.id,
        conversationId: msg.conversationId || "",
        subject: msg.subject || "",
        receivedAt: msg.receivedDateTime || "",
      });
      if (all.length >= limit) break;
    }

    // Follow next page link unless we have enough emails
    url = all.length < limit ? (data["@odata.nextLink"] || null) : null;
  }

  console.log(`fetchRecentMessages: ${all.length} messages fetched for ${userEmail}`);
  return all;
}

// ── Subscription management ───────────────────────────────────────────────────

export async function registerSubscription(env, userEmail) {
  const existing = await getSubscription(env.DAYA_KV, userEmail);
  if (existing?.subscriptionId) {
    try {
      await patchSubscription(env, existing.subscriptionId);
      console.log(`Renewed existing subscription for ${userEmail}`);
      return existing.subscriptionId;
    } catch (err) {
      console.warn(`Renew failed for ${userEmail}, creating new: ${err.message}`);
    }
  }

  const token = await getAccessToken(env);
  const expiresAt = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000 - 60_000).toISOString();

  const res = await fetch(`${GRAPH_BASE}/subscriptions`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      changeType: "created",
      notificationUrl: `${env.MEMORY_WORKER_URL}/webhook`,
      resource: `users/${userEmail}/mailFolders/Inbox/messages`,
      expirationDateTime: expiresAt,
      clientState: env.MEMORY_CLIENT_STATE,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`registerSubscription failed for ${userEmail}: ${res.status} ${body}`);
  }

  const { id: subscriptionId } = await res.json();
  await setSubscription(env.DAYA_KV, userEmail, { subscriptionId, expiresAt });
  console.log(`Registered new subscription for ${userEmail}: ${subscriptionId}`);
  return subscriptionId;
}

export async function renewSubscriptions(env) {
  for (const email of INBOXES) {
    const sub = await getSubscription(env.DAYA_KV, email);
    if (!sub?.subscriptionId) {
      console.warn(`No subscription in KV for ${email} — run /setup to register`);
      continue;
    }
    try {
      await patchSubscription(env, sub.subscriptionId);
      const expiresAt = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000 - 60_000).toISOString();
      await setSubscription(env.DAYA_KV, email, { subscriptionId: sub.subscriptionId, expiresAt });
      console.log(`Renewed subscription for ${email}`);
    } catch (err) {
      console.error(`Failed to renew subscription for ${email}: ${err.message}`);
    }
  }
}

async function patchSubscription(env, subscriptionId) {
  const token = await getAccessToken(env);
  const expiresAt = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000 - 60_000).toISOString();

  const res = await fetch(`${GRAPH_BASE}/subscriptions/${subscriptionId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ expirationDateTime: expiresAt }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`patchSubscription failed: ${res.status} ${body}`);
  }
}

// ── Strip HTML ────────────────────────────────────────────────────────────────

function stripHtml(html) {
  return html
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/?(p|div|li|tr|h[1-6]|blockquote)[^>]*>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}
