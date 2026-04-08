// ─── Email Webhook Handler ────────────────────────────────────────────────────
// Receives Graph change notifications, fetches each email (+ PDF attachments),
// extracts facts via Claude Haiku, and stores them in OneDrive Excel.

import { fetchMessage, fetchDocumentAttachments, fetchRecentMessages } from "./graph.js";
import { extractEmailFacts } from "./memory.js";
import { appendFacts } from "./onedrive.js";
import { isKnownMessage, markMessage } from "./dedup.js";

// ── Webhook entry point ───────────────────────────────────────────────────────

export async function handleEmailWebhook(request, env, ctx) {
  const url = new URL(request.url);

  // Graph subscription validation handshake
  if (url.searchParams.has("validationToken")) {
    return new Response(url.searchParams.get("validationToken"), {
      headers: { "Content-Type": "text/plain" },
    });
  }

  // Validate clientState to confirm notification is from our subscription
  let body;
  try {
    body = await request.json();
  } catch {
    return new Response("Bad request", { status: 400 });
  }

  const notifications = body.value || [];
  for (const n of notifications) {
    if (n.clientState !== env.MEMORY_CLIENT_STATE) {
      console.warn("Rejected notification with wrong clientState");
      continue;
    }

    const messageId = n.resourceData?.id;
    if (!messageId) continue;

    // Determine which inbox this notification is for.
    // Prefer subscription ID lookup (reliable even when Graph uses GUID user IDs in resource URLs).
    // Fall back to resource-string matching for backwards compatibility.
    const userEmail = await resolveUserEmailFromSub(env, n.subscriptionId, n.resource);
    if (!userEmail) {
      console.warn(`Could not resolve inbox for subscriptionId=${n.subscriptionId} resource=${n.resource}`);
      continue;
    }

    // Enqueue for background processing — queue consumer has no 30s waitUntil limit
    await env.EMAIL_QUEUE.send({ userEmail, messageId });
  }

  return new Response(null, { status: 202 });
}

// ── Process a single email ────────────────────────────────────────────────────
// Exported so the queue consumer in index.js can call it directly.
// Throws on transient errors (Graph API, Claude) so the queue can auto-retry (max 3×).
// Returns silently on "no facts" — those are acked without retry.

export async function processEmail(env, userEmail, messageId) {
  // Dedup by messageId — skip if this exact email was already processed.
  // We use messageId (not conversationId) so every email in a thread is
  // processed individually, preserving full chronological project history.
  if (await isKnownMessage(env.DAYA_KV, messageId)) {
    console.log(`Skipping already-processed message: ${messageId}`);
    return;
  }

  // Fetch full email body (includes hasAttachments flag) — throws on API error → queue retries
  const msg = await fetchMessage(env, userEmail, messageId);

  // Fetch PDF and Word document attachments
  const { pdfs, docxTexts } = await fetchDocumentAttachments(env, userEmail, messageId, msg.hasAttachments);

  if (pdfs.length > 0 || docxTexts.length > 0) {
    console.log(`Processing email with ${pdfs.length} PDF(s) + ${docxTexts.length} Word doc(s): ${msg.subject}`);
  }

  // Extract company name + facts from email body, PDFs, and Word docs — throws on Claude error → queue retries
  const { company, facts } = await extractEmailFacts(env, {
    from: msg.from,
    subject: msg.subject,
    body: msg.bodyText,
    date: msg.receivedAt,
    pdfs,
    docxTexts,
  });

  if (!company || facts.length === 0) {
    // Still mark as processed so we don't retry an email that genuinely has no facts
    await markMessage(env.DAYA_KV, messageId);
    console.log(`No useful facts extracted from: ${msg.subject}`);
    return;
  }

  // Build fact rows
  const sourceTag = [pdfs.length > 0 && "pdf", docxTexts.length > 0 && "docx"].filter(Boolean);
  const source = sourceTag.length > 0 ? `email+${sourceTag.join("+")}` : "email";
  const createdAt = new Date().toISOString();
  const factRows = facts.map(fact => ({
    company,
    threadId: msg.conversationId,
    subject: msg.subject,
    sender: msg.from,
    emailDate: msg.receivedAt,
    fact,
    source,
    createdAt,
  }));

  await appendFacts(env, factRows);
  // Mark as processed only after facts are successfully stored.
  // Trade-off: if the worker crashes between appendFacts and markMessage, the email
  // will be re-processed on the next webhook — producing duplicate facts that
  // appendFacts deduplication will discard. Better than permanently skipping.
  await markMessage(env.DAYA_KV, messageId);
  console.log(`Stored ${factRows.length} facts for "${company}" — ${msg.subject}`);
}

// ── Resolve inbox email from subscription ID (primary) or resource string (fallback) ──
// Graph sometimes sends GUID user IDs in resource URLs instead of email addresses,
// so resource-string matching silently fell back to inbox[0] for procurement emails.
// Subscription ID lookup is authoritative — we know which inbox each sub was created for.

const INBOXES = [
  "peterkimani@wearedaya.com",
  "procurement@wearedaya.com",
];

async function resolveUserEmailFromSub(env, subscriptionId, resource) {
  // Primary: look up subscription ID in KV (stored when subscription was registered)
  if (subscriptionId) {
    for (const email of INBOXES) {
      const sub = await env.DAYA_KV.get(`mem:sub:${email}`, "json");
      if (sub?.subscriptionId === subscriptionId) return email;
    }
  }

  // Fallback: match email username against resource URL
  if (resource) {
    const lower = resource.toLowerCase();
    const matched = INBOXES.find(email => lower.includes(email.split("@")[0].toLowerCase()));
    if (matched) return matched;
  }

  return null;
}

// ── Backfill: process historical emails from all inboxes ──────────────────────
// Strategy: lazy scan — check dedup one at a time and stop once we have
// MAX_PER_RUN new emails. Process those sequentially. Self-chain if more remain.
//
// Designed for Cloudflare Workers Unbound (paid) plan:
//   - Real clock time: up to 15 minutes per invocation
//   - CPU time: up to 30 seconds (network I/O doesn't count — fine for backfill)
//   - Time budget set to 10 minutes as a safety net, well under the 15-min limit
//
// Subrequest budget per run (1000 limit):
//   2 inbox fetches + ~100 dedup KV reads + 50×4 email fetches + 50 KV marks ≈ 302
// Each run safely completes under budget. Self-chains until all emails are done.

// Process 15 emails per run (~1-2 min wall clock). User runs /backfill multiple times
// until hasMore is false. Runs synchronously in the request handler (no waitUntil) to
// avoid the 30s post-response window limit that applies even on Unbound paid plan.
const MAX_PER_RUN = 15;

export async function backfillEmails(env, limitPerInbox = 150) {
  console.log("Backfill: run started.");
  const inboxes = [
    "peterkimani@wearedaya.com",
    "procurement@wearedaya.com",
  ];

  // Step 1: Fetch message lists for all inboxes in parallel (2 subrequests)
  const listResults = await Promise.allSettled(
    inboxes.map(async inbox => {
      const messages = await fetchRecentMessages(env, inbox, limitPerInbox);
      console.log(`Backfill: found ${messages.length} messages in ${inbox}`);
      return { inbox, messages };
    })
  );

  // Flatten into one list
  const allMessages = [];
  for (const result of listResults) {
    if (result.status === "fulfilled") {
      for (const msg of result.value.messages) {
        allMessages.push({ inbox: result.value.inbox, msgMeta: msg });
      }
    } else {
      console.error(`Backfill: fetchRecentMessages failed: ${result.reason?.message}`);
    }
  }

  // Step 2: Bulk-load all processed message IDs into memory.
  // One KV list call per 1000 processed emails — vastly better than one KV get per email
  // (which would burn through Cloudflare's 1000-subrequest limit as the inbox grows).
  const processedIds = new Set();
  let kvCursor;
  do {
    const result = await env.DAYA_KV.list({ prefix: "mem:msg:", cursor: kvCursor, limit: 1000 });
    for (const key of result.keys) processedIds.add(key.name.slice("mem:msg:".length));
    kvCursor = result.list_complete ? null : result.cursor;
  } while (kvCursor);

  console.log(`Backfill: ${processedIds.size} already-processed emails loaded from KV.`);

  // Step 3: In-memory dedup scan — O(1) per check, no subrequest cost
  const toProcess = [];
  let totalSkipped = 0;
  for (const item of allMessages) {
    if (toProcess.length >= MAX_PER_RUN) break;
    if (processedIds.has(item.msgMeta.id)) {
      totalSkipped++;
    } else {
      toProcess.push(item);
    }
  }

  // Check if there might be more after this batch
  const scannedAll = toProcess.length + totalSkipped >= allMessages.length;

  console.log(`Backfill: ${toProcess.length} to process, ${totalSkipped} skipped, more=${!scannedAll}`);

  if (toProcess.length === 0) {
    console.log("Backfill complete: nothing new to process.");
    return { processed: 0, factsStored: 0, skipped: totalSkipped, hasMore: false };
  }

  // Step 3: Process sequentially
  const allFacts = [];
  for (let i = 0; i < toProcess.length; i++) {
    const { inbox, msgMeta } = toProcess[i];
    console.log(`Backfill: [${i + 1}/${toProcess.length}] ${msgMeta.subject || msgMeta.id}`);
    const rows = await fetchAndExtractFacts(env, inbox, msgMeta);
    if (rows !== null) {
      await markMessage(env.DAYA_KV, msgMeta.id);
      if (rows.length > 0) allFacts.push(...rows);
    }
  }

  // Step 4: Write all facts in one batch
  if (allFacts.length > 0) {
    await appendFacts(env, allFacts);
  }

  console.log(`Backfill: stored ${allFacts.length} facts this run.`);

  // Return stats so the caller can show progress and whether there are more emails
  return {
    processed: toProcess.length,
    factsStored: allFacts.length,
    skipped: totalSkipped,
    hasMore: !scannedAll,
  };
}

// ── Fetch + extract facts for one email (used by backfill batches) ────────────

async function fetchAndExtractFacts(env, inbox, msgMeta, maxAttachmentBytes = 4 * 1024 * 1024) {
  try {
    const msg = await fetchMessage(env, inbox, msgMeta.id);
    const { pdfs, docxTexts } = await fetchDocumentAttachments(env, inbox, msgMeta.id, msg.hasAttachments, maxAttachmentBytes);

    const { company, facts } = await extractEmailFacts(env, {
      from: msg.from,
      subject: msg.subject,
      body: msg.bodyText,
      date: msg.receivedAt,
      pdfs,
      docxTexts,
    });

    if (!company || facts.length === 0) {
      console.log(`Backfill: no useful facts — ${msg.subject}`);
      return [];
    }

    const sourceTag = [pdfs.length > 0 && "pdf", docxTexts.length > 0 && "docx"].filter(Boolean);
    const source = sourceTag.length > 0 ? `email+${sourceTag.join("+")}` : "email";
    const createdAt = new Date().toISOString();
    const rows = facts.map(fact => ({
      company,
      threadId: msg.conversationId,
      subject: msg.subject,
      sender: msg.from,
      emailDate: msg.receivedAt,
      fact,
      source,
      createdAt,
    }));

    console.log(`Backfill: extracted ${rows.length} facts for "${company}" — ${msg.subject}`);
    return rows;
  } catch (err) {
    console.error(`Backfill: failed for ${msgMeta.id} (${msgMeta.subject}): ${err.message}`);
    return null; // null = failure, don't mark as processed so it retries next run
  }
}
