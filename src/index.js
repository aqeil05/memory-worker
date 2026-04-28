// ─── daya-memory-worker ───────────────────────────────────────────────────────
// Standalone Cloudflare Worker for project memory:
//   - Independently monitors email inboxes via Graph webhooks
//   - Extracts facts (+ PDF attachments) via Claude Haiku → OneDrive Excel
//   - Powers a Telegram group bot (@DayaProjectBot) for project Q&A
//   - Handles Closed Won project setup links

import { handleEmailWebhook, backfillEmails, processEmail } from "./email-handler.js";
import { handleTelegramUpdate, runReportTask, runRefineTask } from "./telegram.js";
import { registerSubscription, renewSubscriptions } from "./graph.js";
import { setupWorkbook, exportToExcel, getAllCompanies, appendFacts, mergeCompany, getActiveProjects, addActiveProject, archiveProject } from "./onedrive.js";
import { generateDailySummaries, handleReport } from "./telegram-bot-query.js";
import { setAlias } from "./memory.js";
import { sendMessage, sendLongMessage, sendWithButtons, editMessage, escHtml } from "./notify.js";
import { setDraft } from "./dedup.js";

const INBOXES = [
  "peterkimani@wearedaya.com",
  "procurement@wearedaya.com",
];

// ── Admin auth ────────────────────────────────────────────────────────────────
// Returns a 401 Response if INTERNAL_SECRET is set and the request doesn't match.
// If INTERNAL_SECRET is not configured, auth is skipped (open — set it in production).
// Uses timing-safe comparison to prevent side-channel attacks.
function requireSecret(request, env) {
  if (!env.INTERNAL_SECRET) return null;
  const auth = request.headers.get("Authorization") || "";
  const expected = `Bearer ${env.INTERNAL_SECRET}`;
  const reject = () => new Response(JSON.stringify({ error: "Unauthorized" }), {
    status: 401,
    headers: { "Content-Type": "application/json" },
  });
  if (auth.length !== expected.length) return reject();
  const enc = new TextEncoder();
  const a = enc.encode(auth);
  const b = enc.encode(expected);
  // crypto.subtle.timingSafeEqual available in Workers runtime
  if (!crypto.subtle.timingSafeEqual(a, b)) return reject();
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
    // Optional query params: chatId, msgId — when provided, edits the Telegram message with live progress
    if (method === "GET" && url.pathname === "/run-daily-summaries") {
      const deny = requireSecret(request, env); if (deny) return deny;
      const chatId  = url.searchParams.get("chatId") || null;
      const msgId   = url.searchParams.get("msgId")  ? parseInt(url.searchParams.get("msgId"), 10) : null;
      const company = url.searchParams.get("company") || null;
      return handleRunDailySummaries(env, chatId, msgId, company);
    }

    // POST /run-report — background report generation, self-invoked from Telegram handler
    if (method === "POST" && url.pathname === "/run-report") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleRunReport(request, env, ctx);
    }

    // POST /run-summaries-batch — process a specific subset of companies, self-invoked
    // from the 04:00 Qatar cron. Body: { companies: string[] }
    if (method === "POST" && url.pathname === "/run-summaries-batch") {
      const deny = requireSecret(request, env); if (deny) return deny;
      return handleRunSummariesBatch(request, env);
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

  // ── Queue consumer ────────────────────────────────────────────────────────
  async queue(batch, env) {
    // Report jobs — no wall-clock limit in queue consumers (up to 15 min).
    // runReportTask has an internal try/catch that messages Telegram on failure,
    // so it never throws — always ack, never retry.
    if (batch.queue === "daya-report-queue") {
      for (const msg of batch.messages) {
        const body = msg.body;
        if (body.type === "refine") {
          await runRefineTask(env, body);
        } else {
          await runReportTask(env, body.chatId, body.topic, body.project);
        }
        msg.ack();
      }
      return;
    }

    // Email jobs — processEmail throws on transient errors → auto-retries up to 3×.
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
  // "0 1 * * *"    → 04:00 Qatar (UTC+3) — main daily summaries: split into 3
  //                   parallel batch invocations with 20s stagger
  // "0 3 * * *"    → 06:00 Qatar (UTC+3) — retry: re-runs for any companies
  //                   that failed in the 04:00 run (freshness skip handles the rest)
  // "0 */12 * * *" → every 12h           — renew Graph email subscriptions
  async scheduled(event, env, ctx) {
    if (event.cron === "0 1 * * *") {
      // Main run: split all companies into 3 batches and fire in parallel.
      // Each batch runs as an independent HTTP invocation (/run-summaries-batch)
      // with its own CPU budget, so a network blip affects only one batch and
      // the sequential processing is never constrained by a single 15-min limit.
      try {
        const allCompanies = await getAllCompanies(env);
        const size = Math.ceil(allCompanies.length / 3);
        const batches = [
          allCompanies.slice(0, size),
          allCompanies.slice(size, size * 2),
          allCompanies.slice(size * 2),
        ].filter(b => b.length > 0);

        console.log(`Daily summaries main run: ${allCompanies.length} companies → ${batches.length} batches`);
        batches.forEach((batch, idx) => {
          ctx.waitUntil(
            new Promise(r => setTimeout(r, idx * 20_000)) // 20s stagger between batches
              .then(() => fetch(`${env.MEMORY_WORKER_URL}/run-summaries-batch`, {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  "Authorization": `Bearer ${env.INTERNAL_SECRET}`,
                },
                body: JSON.stringify({ companies: batch }),
              }))
              .catch(err => console.error(`Summary batch ${idx} failed to fire: ${err.message}`))
          );
        });
      } catch (err) {
        console.error(`Daily summaries main run crashed: ${err.stack || err.message}`);
      }

    } else if (event.cron === "0 3 * * *") {
      // Retry run: skipFresh=true so only companies that failed (or were never
      // processed) in the 04:00 batch run are regenerated. Companies with a fresh
      // summary (< 2.5 hours old) are silently skipped.
      try {
        await generateDailySummaries(env, null, null, null, true);
      } catch (err) {
        console.error(`generateDailySummaries retry crashed: ${err.stack || err.message}`);
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

function summaryProgressBar(done, total, company) {
  const pct   = Math.round(done / total * 100);
  const filled = Math.round(done / total * 16);
  const bar   = "█".repeat(filled) + "░".repeat(16 - filled);
  return `🔄 <b>Regenerating summaries…</b>\n${bar} ${done}/${total} (${pct}%)\n📌 ${escHtml(company)}`;
}

async function handleRunDailySummaries(env, chatId = null, msgId = null, company = null) {
  const onProgress = (chatId && msgId)
    ? async (done, total, co) => {
        await editMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, msgId,
          summaryProgressBar(done, total, co));
      }
    : null;

  try {
    const result = await generateDailySummaries(env, onProgress, company);
    if (chatId && msgId) {
      const lines = [];
      if (result.cached.length)  lines.push(`• ${result.cached.length} updated`);
      if (result.skipped.length) lines.push(`• ${result.skipped.length} skipped (no recent activity)`);
      if (result.failed.length)  lines.push(`• ${result.failed.length} failed`);
      await editMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, msgId,
        `✅ <b>Summaries regenerated.</b>\n${lines.join("\n") || "• Nothing to update."}`
      ).catch(() => {});
    }
    return new Response(JSON.stringify({ ok: true, ...result }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error(`generateDailySummaries crashed: ${err.stack || err.message}`);
    if (chatId && msgId) {
      await editMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, msgId,
        `❌ <b>Summary regeneration failed:</b> ${escHtml(err.message)}`
      ).catch(() => {});
    }
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

// ── POST /run-summaries-batch — process a company subset (self-invoked) ─────────
// Called by the 04:00 Qatar cron handler for each of the 3 parallel batches.
// Runs synchronously in a fresh HTTP invocation with its own CPU budget.

async function handleRunSummariesBatch(request, env) {
  let companies;
  try {
    ({ companies } = await request.json());
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }
  if (!Array.isArray(companies) || companies.length === 0) {
    return new Response(JSON.stringify({ error: "companies must be a non-empty array" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  console.log(`run-summaries-batch: processing ${companies.length} companies`);
  try {
    const result = await generateDailySummaries(env, null, null, companies);
    console.log(`run-summaries-batch: done — ${result.cached.length} cached, ${result.skipped.length} skipped, ${result.failed.length} failed`);
    return new Response(JSON.stringify({ ok: true, ...result }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error(`run-summaries-batch crashed: ${err.stack || err.message}`);
    return new Response(JSON.stringify({ ok: false, error: err.message }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
}

// ── POST /run-report — background report generation (self-invoked) ────────────
// Returns 200 immediately so the HTTP request completes before Cloudflare's
// 30-second wall-clock timeout. The heavy Claude call runs inside ctx.waitUntil()
// which is IO-bound (near-zero CPU) and is not subject to the request timeout.

async function handleRunReport(request, env, ctx) {
  console.log("run-report: endpoint hit");
  let chatId, topic, project;
  try {
    ({ chatId, topic, project } = await request.json());
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  console.log(`run-report: parsed [${chatId}] topic="${topic}" project="${project?.company}"`);

  ctx.waitUntil((async () => {
    try {
      const { text: reportText, json } = await handleReport(env, chatId, topic, project);
      await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, reportText);

      if (json) {
        await setDraft(env.DAYA_KV, chatId, { type: "report", topic, project, json, iteration: 1 });
        await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "What would you like to do with this report?",
          [[
            { text: "📄 Export as Word", callback_data: "export_report" },
            { text: "📑 Export as PDF",  callback_data: "export_report_pdf" },
          ], [
            { text: "✏️ Refine", callback_data: "refine_report" },
          ]]
        );
      }

      console.log(`run-report done [${chatId}]`);
    } catch (err) {
      console.error(`run-report failed [${chatId}]: ${err.stack || err.message}`);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `❌ Report generation failed: ${escHtml(err.message)}`
      ).catch(() => {});
    }
  })());

  return new Response(JSON.stringify({ ok: true }), {
    headers: { "Content-Type": "application/json" },
  });
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
