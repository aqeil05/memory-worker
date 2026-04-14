// ─── daya-memory-worker ───────────────────────────────────────────────────────
// Standalone Cloudflare Worker for project memory:
//   - Independently monitors email inboxes via Graph webhooks
//   - Extracts facts (+ PDF attachments) via Claude Haiku → OneDrive Excel
//   - Powers a Telegram group bot (@DayaProjectBot) for project Q&A
//   - Handles Closed Won project setup links

import { handleEmailWebhook, backfillEmails, processEmail } from "./email-handler.js";
import { handleTelegramUpdate } from "./telegram.js";
import { registerSubscription, renewSubscriptions } from "./graph.js";
import { setupWorkbook, exportToExcel, getAllCompanies, appendFacts, mergeCompany, getActiveProjects, addActiveProject, archiveProject } from "./onedrive.js";
import { generateDailySummaries } from "./telegram-bot-query.js";
import { setAlias } from "./memory.js";
import { sendMessage } from "./notify.js";

const INBOXES = [
  "peterkimani@wearedaya.com",
  "procurement@wearedaya.com",
];

// ── Admin auth ────────────────────────────────────────────────────────────────
// Returns a 401 Response if INTERNAL_SECRET is set and the request doesn't match.
// If INTERNAL_SECRET is not configured, auth is skipped (open — set it in production).
function requireSecret(request, env) {
  if (!env.INTERNAL_SECRET) return null;
  const auth = request.headers.get("Authorization") || "";
  if (auth !== `Bearer ${env.INTERNAL_SECRET}`) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }
  return null;
}

const REQUIRED_SECRETS = [
  "ANTHROPIC_API_KEY",
  "AZURE_TENANT_ID",
  "AZURE_CLIENT_ID",
  "AZURE_CLIENT_SECRET",
  "TELEGRAM_MEMORY_BOT_TOKEN",
  "MEMORY_CLIENT_STATE",
  "MEMORY_WORKER_URL",
];

// ── Router ────────────────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    // Validate required secrets on startup
    const missing = REQUIRED_SECRETS.filter(k => !env[k]);
    if (missing.length) {
      console.error(`Missing secrets: ${missing.join(", ")}`);
      return new Response(`Missing secrets: ${missing.join(", ")}`, { status: 500 });
    }

    const url = new URL(request.url);
    const method = request.method;

    // POST /webhook — Graph email change notifications
    if (method === "POST" && url.pathname === "/webhook") {
      return handleEmailWebhook(request, env, ctx);
    }

    // POST /telegram — Telegram bot webhook (group messages)
    if (method === "POST" && url.pathname === "/telegram") {
      return handleTelegramUpdate(request, env, ctx);
    }

    // GET /setup — register Graph subscriptions for all inboxes
    if (method === "GET" && url.pathname === "/setup") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleSetup(env);
    }

    // GET /setup-telegram — register Telegram bot webhook
    if (method === "GET" && url.pathname === "/setup-telegram") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleSetupTelegram(env);
    }

    // GET /setup-db — create OneDrive Excel workbook (run once)
    if (method === "GET" && url.pathname === "/setup-db") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleSetupDb(env);
    }

    // GET /backfill?limit=30 — process historical emails into KV
    if (method === "GET" && url.pathname === "/backfill") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleBackfill(request, env, ctx);
    }

    // GET /reset-db — wipe all facts + message dedup so backfill re-extracts everything
    if (method === "GET" && url.pathname === "/reset-db") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleResetDb(env);
    }

    // GET /export-excel — rebuild OneDrive Excel from KV facts (call after backfill)
    if (method === "GET" && url.pathname === "/export-excel") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleExportExcel(env);
    }

    // GET /run-daily-summaries — manually trigger the 08:00 Qatar summary generation
    if (method === "GET" && url.pathname === "/run-daily-summaries") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleRunDailySummaries(env);
    }

    // GET /merge-company?from=14th+floor&into=malomatia+19th+floor
    // One-time fix: moves all facts from a stale/alias key into the canonical key, then deletes the old key.
    if (method === "GET" && url.pathname === "/merge-company") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleMergeCompany(url, env);
    }

    // GET /projects — list active project names (no auth)
    if (method === "GET" && url.pathname === "/projects") {
      const projects = await getActiveProjects(env);
      return new Response(JSON.stringify({ projects }, null, 2), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // GET /projects/add?name=X — add a project to the active list
    if (method === "GET" && url.pathname === "/projects/add") {
      const deny = requireSecret(request, env); if (deny) return deny;
      const name = (url.searchParams.get("name") || "").trim();
      if (!name) {
        return new Response(JSON.stringify({ ok: false, error: "?name= is required" }), {
          status: 400, headers: { "Content-Type": "application/json" },
        });
      }
      try {
        const result = await addActiveProject(env, name);
        return new Response(JSON.stringify({ ok: true, ...result }), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: err.message }), {
          status: 500, headers: { "Content-Type": "application/json" },
        });
      }
    }

    // GET /projects/archive?name=X — remove a project from the active list (facts stay in KV)
    if (method === "GET" && url.pathname === "/projects/archive") {
      const deny = requireSecret(request, env); if (deny) return deny;
      const name = (url.searchParams.get("name") || "").trim();
      if (!name) {
        return new Response(JSON.stringify({ ok: false, error: "?name= is required" }), {
          status: 400, headers: { "Content-Type": "application/json" },
        });
      }
      try {
        const result = await archiveProject(env, name);
        return new Response(JSON.stringify({ ok: true, ...result }), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: err.message }), {
          status: 500, headers: { "Content-Type": "application/json" },
        });
      }
    }

    // GET /companies — list all company keys in the database (useful for /link)
    if (method === "GET" && url.pathname === "/companies") {
      const companies = await getAllCompanies(env);
      return new Response(JSON.stringify({ companies }, null, 2), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // GET /health
    if (method === "GET" && url.pathname === "/health") {
      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },

  // ── Queue consumer: process emails with no 30s waitUntil constraint ──────
  // Each message = { userEmail, messageId } sent by the webhook handler.
  // processEmail throws on transient errors → queue auto-retries up to 3×.
  async queue(batch, env) {
    for (const msg of batch.messages) {
      const { userEmail, messageId } = msg.body;
      try {
        await processEmail(env, userEmail, messageId);
        msg.ack();
      } catch (err) {
        console.error(`Queue: processEmail failed for ${messageId} (will retry): ${err.message}`);
        msg.retry();
      }
    }
  },

  // ── Cron handlers ─────────────────────────────────────────────────────────
  // "0 3 * * *"    → 06:00 Qatar (UTC+3) — pre-generate & cache daily summaries
  // "0 */12 * * *" → every 12h           — renew Graph email subscriptions
  async scheduled(event, env, ctx) {
    if (event.cron === "0 3 * * *") {
      try {
        await generateDailySummaries(env);
      } catch (err) {
        console.error(`generateDailySummaries crashed: ${err.stack || err.message}`);
      }
    } else {
      try {
        await renewSubscriptions(env);
      } catch (err) {
        console.error(`renewSubscriptions crashed: ${err.stack || err.message}`);
      }
    }
  },
};

// ── GET /setup — register Graph webhook subscriptions for all inboxes ─────────

async function handleSetup(env) {
  if (!env.MEMORY_WORKER_URL) {
    return new Response("MEMORY_WORKER_URL secret not set", { status: 500 });
  }

  const results = [];
  for (const email of INBOXES) {
    try {
      const id = await registerSubscription(env, email);
      results.push({ email, subscriptionId: id, status: "ok" });
    } catch (err) {
      results.push({ email, status: "error", error: err.message });
    }
  }

  return new Response(JSON.stringify(results, null, 2), {
    headers: { "Content-Type": "application/json" },
  });
}

// ── GET /setup-telegram — register memory bot webhook with Telegram ───────────

async function handleSetupTelegram(env) {
  const webhookUrl = `${env.MEMORY_WORKER_URL}/telegram`;

  const res = await fetch(
    `https://api.telegram.org/bot${env.TELEGRAM_MEMORY_BOT_TOKEN}/setWebhook`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        url: webhookUrl,
        allowed_updates: ["message", "callback_query"],
      }),
    }
  );

  const json = await res.json();
  return new Response(JSON.stringify(json, null, 2), {
    status: json.ok ? 200 : 500,
    headers: { "Content-Type": "application/json" },
  });
}

// ── GET /backfill — process historical emails into OneDrive ───────────────────

async function handleExportExcel(env) {
  try {
    const result = await exportToExcel(env);
    return new Response(JSON.stringify({ ok: true, ...result }, null, 2), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(JSON.stringify({ ok: false, error: err.message }, null, 2), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
}

async function handleResetDb(env) {
  // Delete all fact keys, company index keys, and message dedup keys
  const toDelete = [];

  // List mem:co: company index keys (new per-company format) and derive fact keys
  let cursor;
  do {
    const result = await env.DAYA_KV.list({ prefix: "mem:co:", cursor, limit: 1000 });
    for (const key of result.keys) {
      toDelete.push(key.name); // mem:co:{company}
      const company = key.name.slice("mem:co:".length);
      toDelete.push(`mem:facts:${company}`);
    }
    cursor = result.list_complete ? null : result.cursor;
  } while (cursor);

  // Also delete legacy mem:companies key if it still exists
  toDelete.push("mem:companies");

  // List all mem:dirty: cache-invalidation flags
  cursor = undefined;
  do {
    const result = await env.DAYA_KV.list({ prefix: "mem:dirty:", cursor, limit: 1000 });
    for (const key of result.keys) toDelete.push(key.name);
    cursor = result.list_complete ? null : result.cursor;
  } while (cursor);

  // List all mem:msg: dedup keys
  cursor = undefined;
  do {
    const result = await env.DAYA_KV.list({ prefix: "mem:msg:", cursor, limit: 1000 });
    for (const key of result.keys) toDelete.push(key.name);
    cursor = result.list_complete ? null : result.cursor;
  } while (cursor);

  // Delete all in parallel
  await Promise.all(toDelete.map(k => env.DAYA_KV.delete(k)));

  console.log(`reset-db: deleted ${toDelete.length} KV keys`);
  return new Response(JSON.stringify({ ok: true, deleted: toDelete.length }), {
    headers: { "Content-Type": "application/json" },
  });
}

async function handleBackfill(request, env, ctx) {
  const url = new URL(request.url);
  const rawLimit = url.searchParams.get("limit");
  const limit = rawLimit ? parseInt(rawLimit, 10) : 2000;
  if (isNaN(limit) || limit <= 0) return new Response(JSON.stringify({ error: "Invalid limit parameter" }), { status: 400, headers: { "Content-Type": "application/json" } });
  const chatId = url.searchParams.get("chatId") || null;

  // Run synchronously — avoids the 30s post-response waitUntil window limit.
  // Processes up to 15 emails per call; re-run until hasMore is false.
  try {
    const result = await backfillEmails(env, limit);

    if (chatId) {
      const tgMsg = result.processed === 0
        ? `✅ Inbox up to date — ${result.skipped} emails already processed, nothing new.`
        : result.hasMore
          ? `⏳ Batch done: ${result.processed} new emails processed, ${result.factsStored} facts stored (${result.skipped} already done). More remain — run /bot backfill again.`
          : `✅ Backfill complete: ${result.processed} new emails processed, ${result.factsStored} facts stored (${result.skipped} already done).`;
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, tgMsg).catch(() => {});
    }

    return new Response(
      JSON.stringify({
        ok: true,
        ...result,
        message: result.hasMore
          ? `Processed ${result.processed} emails (${result.factsStored} facts stored, ${result.skipped} skipped). More remain — run /backfill again.`
          : `Backfill complete. Processed ${result.processed} emails, ${result.factsStored} facts stored.`,
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.error(`backfillEmails crashed: ${err.stack || err.message}`);
    if (chatId) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, `❌ Backfill failed: ${err.message}`).catch(() => {});
    }
    return new Response(
      JSON.stringify({ ok: false, error: err.message }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

// ── GET /run-daily-summaries — manually trigger the 08:00 Qatar summary job ───

async function handleRunDailySummaries(env) {
  try {
    const result = await generateDailySummaries(env);
    return new Response(JSON.stringify({ ok: true, ...result }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error(`generateDailySummaries crashed: ${err.stack || err.message}`);
    return new Response(JSON.stringify({ ok: false, error: err.message }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
}

// ── GET /merge-company — consolidate a stale/alias company key into a canonical one ──
// Usage: /merge-company?from=14th+floor&into=malomatia+19th+floor
// Safe to re-run — appendFacts deduplicates on (emailDate|fact) before writing.

async function handleMergeCompany(url, env) {
  const from = (url.searchParams.get("from") || "").toLowerCase().trim();
  const into = (url.searchParams.get("into") || "").toLowerCase().trim();

  if (!from || !into) {
    return new Response(JSON.stringify({ ok: false, error: "Both ?from= and ?into= are required" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }
  if (from === into) {
    return new Response(JSON.stringify({ ok: false, error: "?from and ?into must be different" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const result = await mergeCompany(env, from, into);
    await setAlias(from, into, env);
    const message = result.moved === 0
      ? `No facts found under "${from}" — alias set, nothing to merge.`
      : `Moved ${result.moved} facts from "${from}" → "${into}" and alias registered.`;
    return new Response(JSON.stringify({ ok: true, ...result, message }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(JSON.stringify({ ok: false, error: err.message }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
}

// ── GET /setup-db — create OneDrive Excel workbook ────────────────────────────

async function handleSetupDb(env) {
  try {
    const file = await setupWorkbook(env);
    return new Response(
      JSON.stringify({ ok: true, file: file.name, webUrl: file.webUrl }, null, 2),
      { headers: { "Content-Type": "application/json" } }
    );
  } catch (err) {
    return new Response(
      JSON.stringify({ ok: false, error: err.message }, null, 2),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
