// ─── Telegram Group Bot — Query Handler ──────────────────────────────────────
// Handles /bot questions, /bot summary, /bot report, and clarification follow-ups.
// Reads facts from OneDrive Excel → builds context → Claude → answer/report.

import { queryFacts, getRecentFacts, getAllProjectFacts, getAllCompanies, matchingCompanies } from "./onedrive.js";
import { getBotHistory, setBotHistory, setBotPending } from "./dedup.js";
import { escHtml, sendChatAction } from "./notify.js";
import { claudeFetch } from "./memory.js";

const CLAUDE_API = "https://api.anthropic.com/v1/messages";
const SONNET_MODEL = "claude-sonnet-4-6";
const HAIKU_MODEL = "claude-haiku-4-5-20251001";

// Walk brackets to extract the first complete JSON object from text, avoiding greedy regex.
function extractFirstJsonObject(text) {
  const start = text.indexOf("{");
  if (start === -1) return null;
  let depth = 0;
  for (let i = start; i < text.length; i++) {
    if (text[i] === "{") depth++;
    else if (text[i] === "}") {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
}

const SUMMARY_CACHE_PREFIX = "summary:cache:";
const SUMMARY_CACHE_TTL_S  = 25 * 60 * 60; // 25h — survives from 6AM generation to next day's cron
const SUMMARY_DAYS_WINDOW  = 10;
const SUMMARY_CHUNK_SIZE   = 40; // facts per Haiku chunk in two-pass summary

// Q&A retrieval thresholds
const QA_MAX_SHORTLIST = 120;  // max facts sent to Sonnet for Q&A
const REPORT_MAX_SHORTLIST = 200;  // broader than Q&A — reports need the full story

const QA_SYSTEM_PROMPT = `You are Daya Assistant, internal AI for Daya Interior Design (Doha, Qatar) — a fit-out, interior design, and project management company.

Answer project questions using only the facts provided from real project emails and documents.

Facts are grouped by email thread in chronological order. Each thread shows how a topic evolved over time — decisions changed, specs were revised, approvals were given. Use this chronology to give precise, accurate answers.

Rules:
- Answer ONLY from the provided facts. If the answer isn't there, say: "I don't have that in the project memory yet."
- If the question is ambiguous (e.g. "which ceiling?" when multiple are mentioned), respond with ONLY: [CLARIFY] followed by one short clarifying question ending with "?". Do not add any other text. Example: [CLARIFY] Which ceiling are you asking about — the office or the lobby?
- Be concise. Use bullet points with "•". Format dates as "15 Jun 2026". Currency in QAR.
- When facts in the same thread conflict (e.g. spec changed from single to double glazed), show the evolution — what was originally decided, what changed, and what the final outcome was.
- For report-style questions, present as a timeline with dates.
- Never speculate or invent information not present in the facts.`;

const SUMMARY_SYSTEM_PROMPT = `You are Daya Assistant for Daya Interior Design (Doha, Qatar).

Analyse the project facts from the past 10 days below and produce a structured JSON briefing.
Base everything strictly on the provided facts. Do not invent.
Focus on recent activity: current status, open actions, upcoming deadlines, and active issues.

Return ONLY valid JSON:
{
  "executive_summary": "3-5 sentences on current project status",
  "timeline": [{"date": "YYYY-MM-DD", "event": "what happened", "significance": "why it matters"}],
  "open_issues": [{"issue": "clear statement", "priority": "High/Medium/Low", "action_required": "specific action", "deadline": "or null"}],
  "cost_items": [{"description": "item/trade", "amount": "QAR X,XXX or TBC", "status": "Quoted/Approved/Disputed/Pending"}],
  "key_contacts": [{"name": "name or company", "role": "Client/Contractor/Consultant/Supplier"}],
  "risks": [{"risk": "description", "severity": "High/Medium/Low"}]
}

If a section has no data, use [].
Return ONLY valid JSON. No markdown fences, no explanation, no extra text.`;

// Stop words for keyword extraction
const STOP_WORDS = new Set([
  "a","an","the","is","it","in","on","at","to","for","of","and","or","but","with",
  "what","when","where","who","how","was","were","are","be","been","being","have",
  "has","had","do","does","did","will","would","could","should","may","might",
  "this","that","these","those","i","we","you","he","she","they","me","us","him",
  "her","them","my","our","your","his","its","their","about","from","by","which",
]);

// ── Q&A handler ───────────────────────────────────────────────────────────────

export async function handleBotQuery(env, chatId, question, project, isClarification = false) {
  const { company, label } = project;

  const history = await getBotHistory(env.DAYA_KV, chatId);

  // Fetch all facts once — totalCount is always the real project total so Claude
  // can answer "how many facts / how big is this project" accurately.
  const allFacts = await getAllProjectFacts(env, company).catch((err) => {
    console.error(`getAllProjectFacts failed for ${company}: ${err.message}`);
    return [];
  });
  const totalCount = allFacts.length;

  // Compute full date range from all facts so Claude always knows the true span,
  // even when only a shortlist is sent as context.
  const allDates = allFacts.map(r => r.emailDate?.slice(0, 10)).filter(Boolean).sort();
  const fullDateRange = allDates.length > 0 ? `${allDates[0]} to ${allDates[allDates.length - 1]}` : null;

  // Include cached daily summary (if available) so Sonnet has broad project awareness
  // even when the specific thread wasn't in the selected facts.
  const cachedSummary = await getCachedSummary(env, company).catch(() => null);
  let summaryContext = "";
  if (cachedSummary?.json) {
    const s = cachedSummary.json;
    const parts = [];
    if (s.executive_summary) parts.push(`Overview: ${s.executive_summary}`);
    if (s.open_issues?.length) parts.push(`Open issues:\n${s.open_issues.map(i => `• ${i.issue} [${i.priority}] — ${i.action_required}`).join("\n")}`);
    if (s.cost_items?.length) parts.push(`Cost items:\n${s.cost_items.map(c => `• ${c.description}: ${c.amount} (${c.status})`).join("\n")}`);
    if (s.risks?.length) parts.push(`Risks:\n${s.risks.map(r => `• ${r.risk} [${r.severity}]`).join("\n")}`);
    summaryContext = `\n\n=== Recent Activity Summary (generated ${cachedSummary.generatedAt || "recently"}) ===\n${parts.join("\n\n")}`;
  }

  const messages = [
    ...history,
    {
      role: "user",
      content: isClarification
        ? `(Clarification to previous question) ${question}`
        : question,
    },
  ];

  sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});

  // Progressive context reduction on 529 (overloaded): halve the fact budget each attempt
  // so a smaller prompt succeeds when the full-size one hits capacity.
  let data;
  const budgets = [QA_MAX_SHORTLIST, Math.floor(QA_MAX_SHORTLIST / 2), Math.floor(QA_MAX_SHORTLIST / 4)];
  for (let attempt = 0; attempt < budgets.length; attempt++) {
    const contextFacts = selectRelevantFacts(allFacts, question, budgets[attempt]);
    const context = buildContextBlock(label, contextFacts, contextFacts, [], totalCount, fullDateRange);

    const res = await claudeFetch(CLAUDE_API, {
      method: "POST",
      headers: {
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: SONNET_MODEL,
        max_tokens: 600,
        system: `${QA_SYSTEM_PROMPT}${summaryContext}\n\n${context}`,
        messages,
      }),
    });

    if (res.status === 529) {
      if (attempt < budgets.length - 1) {
        console.warn(`Claude 529 on Q&A (attempt ${attempt + 1}), retrying with ${budgets[attempt + 1]} facts...`);
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }
      return "The AI service is currently busy — please try again in a moment.";
    }

    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`Claude Sonnet error: ${res.status} ${errBody}`);
    }

    data = await res.json();
    break;
  }
  const answer = data.content[0].text.trim();

  const newHistory = [
    ...history,
    { role: "user", content: question },
    { role: "assistant", content: answer },
  ].slice(-10);

  await setBotHistory(env.DAYA_KV, chatId, newHistory);

  // Check for explicit [CLARIFY] prefix — only set pending state if Claude intentionally
  // signals a clarification is needed, not just because a response ends with "?".
  if (answer.startsWith("[CLARIFY]")) {
    const displayAnswer = answer.replace(/^\[CLARIFY\]\s*/, "").trim();
    await setBotPending(env.DAYA_KV, chatId, { type: "clarification", originalQuestion: question, project });
    return displayAnswer;
  }

  return answer;
}

// ── Summary handler ───────────────────────────────────────────────────────────

function getTenDayCutoff() {
  const d = new Date();
  d.setDate(d.getDate() - SUMMARY_DAYS_WINDOW);
  return d.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

async function getCachedSummary(env, company) {
  return env.DAYA_KV.get(`${SUMMARY_CACHE_PREFIX}${company}`, "json");
}

async function setCachedSummary(env, company, payload) {
  await env.DAYA_KV.put(
    `${SUMMARY_CACHE_PREFIX}${company}`,
    JSON.stringify(payload),
    { expirationTtl: SUMMARY_CACHE_TTL_S }
  );
}

// Pass 1 helper: summarise one chunk of facts with Haiku.
// Returns a plain-text bullet summary — not JSON — kept short so the Sonnet
// synthesis call receives only distilled signal, not raw fact volume.
async function summarizeChunkWithHaiku(env, label, chunkFacts, chunkIndex) {
  const factLines = chunkFacts
    .map(f =>
      `[${(f.emailDate || "").slice(0, 10)}] [${(f.subject || "").slice(0, 60)}] ${f.fact}`
    )
    .join("\n");

  const res = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: HAIKU_MODEL,
      max_tokens: 600,
      messages: [{
        role: "user",
        content:
          `Summarise the following project facts for ${label}.\n` +
          `Preserve ALL key details: exact dates, cost figures, deadlines, open actions, decisions, risks, and contacts.\n` +
          `Be concise but complete — do not omit numbers or action items.\n\n` +
          `FACTS (${chunkFacts.length}):\n${factLines}\n\n` +
          `Return a concise bullet-point summary.`,
      }],
    }),
  });

  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Claude Haiku chunk ${chunkIndex} error: ${res.status} ${errBody}`);
  }

  const data = await res.json();
  return data.content[0].text.trim();
}

async function callClaudeForSummary(env, label, facts) {
  // Two-pass flow to avoid a single oversized Sonnet prompt (root cause of 429s):
  //   Pass 1 — split facts into chunks of SUMMARY_CHUNK_SIZE, summarise each with Haiku in parallel.
  //   Pass 2 — send only the compact chunk summaries to Sonnet for final JSON synthesis.
  // This also ensures every fact in the 10-day window contributes to the briefing,
  // not just the last-two-per-thread preview that buildContextBlock() produces.

  const chunks = [];
  for (let i = 0; i < facts.length; i += SUMMARY_CHUNK_SIZE) {
    chunks.push(facts.slice(i, i + SUMMARY_CHUNK_SIZE));
  }

  // Pass 1: batched-parallel Haiku calls — 3 concurrent chunks per batch, 500ms between batches.
  // Full parallel fan-out was the root cause of 429s on large summaries; batching of 3 gives
  // ~3× speed-up while staying safely within Haiku's rate limit.
  const PARALLEL_BATCH = 3;
  const chunkSummaries = new Array(chunks.length);
  for (let i = 0; i < chunks.length; i += PARALLEL_BATCH) {
    if (i > 0) await new Promise(r => setTimeout(r, 500));
    const batchResults = await Promise.all(
      chunks.slice(i, i + PARALLEL_BATCH)
        .map((chunk, offset) => summarizeChunkWithHaiku(env, label, chunk, i + offset))
    );
    batchResults.forEach((r, j) => { chunkSummaries[i + j] = r; });
  }

  // Pass 2: Sonnet synthesis over compact summaries only
  const combinedSummaries = chunkSummaries
    .map((s, i) => `=== Batch ${i + 1} of ${chunks.length} ===\n${s}`)
    .join("\n\n");

  const res = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: SONNET_MODEL,
      max_tokens: 4096,
      system: SUMMARY_SYSTEM_PROMPT,
      messages: [{
        role: "user",
        content:
          `Project: ${label}\n` +
          `Facts window: last ${SUMMARY_DAYS_WINDOW} days ` +
          `(${facts.length} facts, pre-summarised in ${chunks.length} batch${chunks.length !== 1 ? "es" : ""}).\n\n` +
          `The following are pre-summarised batches of all project facts from this period:\n\n` +
          `${combinedSummaries}`,
      }],
    }),
  });

  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Claude Sonnet summary error: ${res.status} ${errBody}`);
  }

  const data = await res.json();
  const raw = data.content[0].text.trim();

  let json;
  try {
    // 1. Direct parse (Claude returned pure JSON)
    json = JSON.parse(raw);
  } catch {
    try {
      // 2. Strip markdown code fences: ```json { ... } ``` (greedy — must match outermost {})
      const codeBlock = raw.match(/```(?:json)?\s*(\{[\s\S]*\})\s*```/);
      if (codeBlock) {
        json = JSON.parse(codeBlock[1]);
      } else {
        // 3. Extract first complete { ... } from mixed text (bracket-walker, not greedy regex)
        const extracted = extractFirstJsonObject(raw);
        if (!extracted) throw new Error("No JSON object found in response");
        json = JSON.parse(extracted);
      }
    } catch (e2) {
      console.error(`Summary JSON parse failed for "${label}": ${e2.message} | Raw (first 300): ${raw.slice(0, 300)}`);
      throw new Error("Claude returned invalid JSON for summary");
    }
  }

  return json;
}

export async function handleSummary(env, chatId, project) {
  const { company, label } = project;

  // Exact cache lookup first (fast path — covers groups linked to exact company keys).
  const cached = await getCachedSummary(env, company);
  if (cached?.json) {
    const text = formatSummaryText(label, cached.json, cached.generatedAt);
    // Draft storage is handled by the caller (telegram.js) via setDraft() with full { type, project, json } wrapper
    return { text, json: cached.json };
  }

  // Fuzzy fallback: groups linked via alias or partial-word match may resolve to one
  // or more exact company keys whose caches were pre-generated by generateDailySummaries().
  // This mirrors the same fuzzy resolution used by getAllProjectFacts() for Q&A and reports.
  const matches = await matchingCompanies(env, company);
  for (const matchedCompany of matches) {
    if (matchedCompany === company) continue; // already tried above
    const fuzzyCached = await getCachedSummary(env, matchedCompany);
    if (fuzzyCached?.json) {
      const text = formatSummaryText(label, fuzzyCached.json, fuzzyCached.generatedAt);
      // Draft storage is handled by the caller (telegram.js) via setDraft() with full { type, project, json } wrapper
      return { text, json: fuzzyCached.json };
    }
  }

  return {
    text: `📊 <b>${escHtml(label)}</b>\n\nNo summary available yet — summaries are generated daily at 6:00 AM Qatar time.\nIf this is a new project, the first summary will appear tomorrow morning.`,
    json: null,
  };
}

// Called by the 08:00 Qatar cron — pre-generates and caches summaries for all companies.
export async function generateDailySummaries(env) {
  const companies = await getAllCompanies(env);
  const cutoff = getTenDayCutoff();
  const cached = [], skipped = [], failed = [];

  for (const company of companies) {
    try {
      const allFacts = await getAllProjectFacts(env, company);
      const recent = allFacts.filter(f => (f.emailDate || "").slice(0, 10) >= cutoff);
      if (!recent.length) {
        await env.DAYA_KV.delete(`${SUMMARY_CACHE_PREFIX}${company}`);
        skipped.push(company);
        continue;
      }

      // Skip regeneration when cache is fresh and no new emails have arrived.
      // appendFacts() sets mem:dirty:{company} on every new email ingestion.
      const isDirty = await env.DAYA_KV.get(`mem:dirty:${company}`);
      const existingCache = await getCachedSummary(env, company);
      if (existingCache?.json && !isDirty) {
        // Rewrite the cache entry to reset its TTL. Without this, a summary generated
        // at 06:00 on day N would expire around 07:00 on day N+1 — about an hour after
        // the next cron runs — because the 25h TTL counts from original creation, not
        // from when the cron last ran. Rewriting here keeps the summary alive continuously.
        await setCachedSummary(env, company, existingCache);
        skipped.push(company);
        continue;
      }

      const json = await callClaudeForSummary(env, company, recent);
      await setCachedSummary(env, company, { json, generatedAt: new Date().toISOString() });
      // Clear the dirty flag now that the cache is up to date.
      await env.DAYA_KV.delete(`mem:dirty:${company}`);
      console.log(`Daily summary cached for "${company}" (${recent.length} facts)`);
      cached.push({ company, facts: recent.length });
    } catch (err) {
      console.error(`Daily summary failed for "${company}": ${err.message}`);
      failed.push({ company, error: err.message });
    } finally {
      // Always pace inter-company requests — including after failures — to avoid burst 429s.
      await new Promise(r => setTimeout(r, 1500));
    }
  }

  return { cached, skipped, failed };
}

// ── Report context builder (thread-grouped, chronological, all facts in full) ──

// Simpler than buildContextBlock — no condensed section, all selected facts are
// relevant and shown in full detail. Threads sorted by earliest date so Sonnet
// traces the issue from first mention to most recent.
function buildReportContextBlock(topic, label, facts, totalCount) {
  if (facts.length === 0) {
    return `=== ${label} — Project Memory ===\nNo relevant facts found for topic: ${topic}`;
  }

  const threadMap = new Map();
  for (const r of facts) {
    const tid = r.threadId || `solo_${r.emailDate || r.createdAt}`;
    if (!threadMap.has(tid)) threadMap.set(tid, { subject: r.subject, facts: [] });
    threadMap.get(tid).facts.push(r);
  }

  for (const thread of threadMap.values()) {
    thread.facts.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));
  }

  // Sort threads by earliest date — chronological issue evolution
  const sortedThreads = [...threadMap.entries()].sort(([, a], [, b]) => {
    const aDate = a.facts[0]?.emailDate || "";
    const bDate = b.facts[0]?.emailDate || "";
    return aDate.localeCompare(bDate);
  });

  const allDates = facts.map(r => r.emailDate?.slice(0, 10)).filter(Boolean).sort();
  const dateRange = allDates.length > 0 ? `${allDates[0]} to ${allDates[allDates.length - 1]}` : null;

  let block = `=== ${label} — Project Memory ===\n`;
  block += `Topic: ${topic}\n`;
  block += `${facts.length} relevant facts across ${sortedThreads.length} email thread${sortedThreads.length !== 1 ? "s" : ""}`;
  if (totalCount && totalCount > facts.length) block += ` (selected from ${totalCount} total)`;
  block += ".\n";
  if (dateRange) block += `Date range: ${dateRange}.\n`;

  for (const [, thread] of sortedThreads) {
    block += formatThread(thread);
  }

  return block;
}

// ── Report handler (thread-based selection → Sonnet narrative) ────────────────

const REPORT_SYSTEM_PROMPT = `You are a professional report writer for Daya Interior Design (Doha, Qatar) — a fit-out, interior design, and project management company.

Write formal issue/delay reports suitable for clients, contract administrators, or legal use.

Rules:
- Base all content strictly on the provided facts. Do not invent or speculate.
- Trace the issue chronologically — show what was originally decided, what changed, who was responsible, and the current position.
- Use specific dates, sender names, and email subjects when citing evidence.
- Use formal language appropriate for contract correspondence.
- Currency in QAR. Dates as "DD Mon YYYY".
- The narrative should be detailed — trace every step of the issue with dates and attribution.
- Recommendations must be actionable and specific, not generic advice.

Return ONLY valid JSON with this exact structure:
{
  "executive_summary": "3-5 sentences summarising the issue and current status",
  "background": "2-4 sentences of project context relevant to this issue",
  "narrative": "Detailed chronological account (8-15 sentences) tracing the issue's full evolution with specific dates, senders, and decisions at each step",
  "evidence": [{"date": "YYYY-MM-DD", "sender": "name or email", "subject": "email subject", "excerpt": "most relevant sentence from the email", "attribution": "Client/Supplier/Daya — brief explanation of this item's significance"}],
  "timeline": [{"date": "YYYY-MM-DD", "event": "what happened", "significance": "why it matters"}],
  "impact": "3-5 sentences on programme and cost impact, including any quantified delays or cost figures from the evidence",
  "recommendations": ["specific actionable recommendation 1", "specific actionable recommendation 2"],
  "conclusion": "3-5 sentences with a clear closing position"
}

No markdown fences, no explanation, no extra text. Return ONLY the JSON object.`;

export async function handleReport(env, chatId, topic, project) {
  const { company, label } = project;

  const allFacts = await getAllProjectFacts(env, company);
  if (allFacts.length === 0) {
    return { text: `📋 No facts recorded yet for this project.`, json: null };
  }

  // Select relevant facts using thread-based scoring (same as Q&A, broader budget)
  const selected = selectRelevantFacts(allFacts, topic, REPORT_MAX_SHORTLIST);

  if (selected.length < 3) {
    return {
      text: `📋 I couldn't find relevant facts for "<b>${escHtml(topic)}</b>" in the project memory. Try a different search term.`,
      json: null,
    };
  }

  // Include cached daily summary for broader project awareness
  const cachedSummary = await getCachedSummary(env, company).catch(() => null);
  let summaryContext = "";
  if (cachedSummary?.json) {
    const s = cachedSummary.json;
    const parts = [];
    if (s.executive_summary) parts.push(`Project overview: ${s.executive_summary}`);
    if (s.open_issues?.length) parts.push(`Open issues:\n${s.open_issues.map(i => `• ${i.issue} [${i.priority}]`).join("\n")}`);
    if (s.risks?.length) parts.push(`Risks:\n${s.risks.map(r => `• ${r.risk} [${r.severity}]`).join("\n")}`);
    summaryContext = `\n\n=== Project Context (generated ${cachedSummary.generatedAt || "recently"}) ===\n${parts.join("\n\n")}`;
  }

  const contextBlock = buildReportContextBlock(topic, label, selected, allFacts.length);

  sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});

  const narrativeRes = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: SONNET_MODEL,
      max_tokens: 3000,
      system: `${REPORT_SYSTEM_PROMPT}${summaryContext}`,
      messages: [{
        role: "user",
        content: `Topic: ${topic}\nProject: ${label}\n\n${contextBlock}`,
      }],
    }),
  });

  if (!narrativeRes.ok) throw new Error(`Claude Sonnet report error: ${narrativeRes.status}`);

  const narrativeData = await narrativeRes.json();
  let json;
  try {
    const raw = narrativeData.content[0].text.trim();
    const codeBlock = raw.match(/```(?:json)?\s*(\{[\s\S]*\})\s*```/);
    json = JSON.parse(codeBlock ? codeBlock[1] : (extractFirstJsonObject(raw) ?? raw));
  } catch {
    throw new Error("Claude returned invalid JSON for report");
  }

  return { text: formatReportText(topic, label, json), json };
}

// ── Regenerate report with user feedback ──────────────────────────────────────

export async function regenerateReport(env, chatId, topic, project, originalJson, feedback) {
  const { label } = project;

  const res = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: SONNET_MODEL,
      max_tokens: 3000,
      messages: [{
        role: "user",
        content:
          `You wrote this issue report for project "${label}", topic "${topic}":\n` +
          `${JSON.stringify(originalJson, null, 2)}\n\n` +
          `Please revise it based on this feedback: ${feedback}\n\n` +
          `Return ONLY the revised JSON with the same structure, including executive_summary, background, narrative, evidence, timeline, impact, recommendations, and conclusion fields.`,
      }],
    }),
  });

  if (!res.ok) throw new Error(`Claude regenerate error: ${res.status}`);

  const data = await res.json();
  let json;
  try {
    const extracted = extractFirstJsonObject(data.content[0].text.trim());
    json = JSON.parse(extracted ?? data.content[0].text.trim());
  } catch {
    throw new Error("Claude returned invalid JSON for regenerated report");
  }

  return { text: formatReportText(topic, label, json), json };
}

// ── Formatting helpers ────────────────────────────────────────────────────────

function formatSummaryText(label, json, generatedAt = null) {
  const parts = [`📊 <b>${escHtml(label)} — Project Briefing</b>\n`];

  if (json.executive_summary) {
    parts.push(escHtml(json.executive_summary));
    parts.push("");
  }

  if (json.timeline?.length > 0) {
    parts.push("📅 <b>Key Timeline:</b>");
    for (const t of json.timeline) {
      parts.push(`• ${escHtml(t.date || "")} — ${escHtml(t.event || "")}`);
    }
    parts.push("");
  }

  if (json.open_issues?.length > 0) {
    parts.push("⚠️ <b>Open Issues:</b>");
    for (const i of json.open_issues) {
      const dot = i.priority === "High" ? "🔴" : i.priority === "Medium" ? "🟡" : "🟢";
      parts.push(`${dot} ${escHtml(i.issue || "")} — ${escHtml(i.action_required || "")}`);
    }
    parts.push("");
  }

  if (json.cost_items?.length > 0) {
    parts.push("💰 <b>Cost Items:</b>");
    for (const c of json.cost_items) {
      parts.push(`• ${escHtml(c.description || "")}: ${escHtml(c.amount || "")} (${escHtml(c.status || "")})`);
    }
    parts.push("");
  }

  if (json.key_contacts?.length > 0) {
    parts.push("👥 <b>Key Contacts:</b>");
    for (const c of json.key_contacts) {
      parts.push(`• ${escHtml(c.name || "")} — ${escHtml(c.role || "")}`);
    }
    parts.push("");
  }

  if (json.risks?.length > 0) {
    parts.push("🚨 <b>Risks:</b>");
    for (const r of json.risks) {
      parts.push(`• [${escHtml(r.severity || "")}] ${escHtml(r.risk || "")}`);
    }
    parts.push("");
  }

  if (generatedAt) {
    const d = new Date(generatedAt);
    const stamp = d.toLocaleString("en-GB", {
      timeZone: "Asia/Qatar",
      day: "2-digit", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit", hour12: false,
    });
    parts.push(`<i>Last generated: ${stamp} (Qatar time)</i>`);
  }

  return parts.join("\n");
}

function formatReportText(topic, label, json) {
  const parts = [
    `📋 <b>Issue Report: ${escHtml(topic)}</b>`,
    `Project: ${escHtml(label)}`,
    "",
    `<b>Executive Summary:</b>`,
    escHtml(json.executive_summary || ""),
    "",
    `<b>Background:</b>`,
    escHtml(json.background || ""),
    "",
    `<b>Issue Narrative:</b>`,
    escHtml(json.narrative || ""),
    "",
  ];

  if (json.evidence?.length > 0) {
    parts.push(`📧 <b>Evidence (${json.evidence.length} emails):</b>`);
    for (const e of json.evidence) {
      parts.push(`• [${escHtml(e.date || "")}] ${escHtml(e.sender || "")} — "<i>${escHtml(e.subject || "")}</i>"`);
      parts.push(`  "${escHtml(e.excerpt || "")}"`);
      parts.push(`  → ${escHtml(e.attribution || "")}`);
    }
    parts.push("");
  }

  if (json.timeline?.length > 0) {
    parts.push(`📅 <b>Timeline:</b>`);
    for (const t of json.timeline) {
      parts.push(`• ${escHtml(t.date || "")} — ${escHtml(t.event || "")}`);
      if (t.significance) parts.push(`  <i>${escHtml(t.significance)}</i>`);
    }
    parts.push("");
  }

  parts.push(`<b>Impact:</b>`);
  parts.push(escHtml(json.impact || ""));
  parts.push("");

  if (json.recommendations?.length > 0) {
    parts.push(`💡 <b>Recommendations:</b>`);
    json.recommendations.forEach((r, i) => {
      parts.push(`${i + 1}. ${escHtml(r)}`);
    });
    parts.push("");
  }

  parts.push(`<b>Conclusion:</b>`);
  parts.push(escHtml(json.conclusion || ""));
  parts.push("");
  parts.push(`<i>Without Prejudice — Daya Interior Design</i>`);

  return parts.join("\n");
}

// ── Context builder for Q&A (thread-grouped, chronological) ──────────────────

// Returns all meaningful terms from the question (not just the first one).
function extractKeywords(question) {
  const words = question.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/);
  return words.filter(w => w.length > 2 && !STOP_WORDS.has(w));
}

// Score and select facts by email thread.
// Groups all facts into their source threads, scores each thread by keyword relevance
// + recency, then fills the budget with complete threads in score order.
// This keeps thread context intact (MOM 008's 6 facts arrive together, not split apart)
// and handles temporal queries ("latest", "most recent") correctly via recency weighting.
function selectRelevantFacts(allFacts, question, maxCount = QA_MAX_SHORTLIST) {
  const keywords = extractKeywords(question);
  const isTemporalQuery = /\b(latest|most recent|last|recent|newest|current)\b/i.test(question);
  const now = Date.now();

  // Group facts into threads, preserving insertion order within each thread
  const threads = new Map();
  for (const fact of allFacts) {
    const tid = fact.threadId || `solo_${fact.emailDate || fact.createdAt}`;
    if (!threads.has(tid)) threads.set(tid, []);
    threads.get(tid).push(fact);
  }

  // Sort facts within each thread chronologically
  for (const facts of threads.values()) {
    facts.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));
  }

  // Score each thread
  const questionLower = question.toLowerCase();
  const scoredThreads = [];
  for (const [tid, facts] of threads.entries()) {
    let kwScore = 0;
    for (const fact of facts) {
      const text = (fact.fact || "").toLowerCase();
      const subj = (fact.subject || "").toLowerCase();
      for (const kw of keywords) {
        if (text.includes(kw)) kwScore += 2;
        if (subj.includes(kw)) kwScore += 1;
      }
      // Tag bonus: +1 per fact whose tags appear verbatim in the question.
      // e.g. "what is the cost?" boosts facts tagged ["cost"] even if "cost" isn't in the fact text.
      // Graceful on old untagged facts — fact.tags will be undefined, optional chain short-circuits.
      if (fact.tags?.some(tag => questionLower.includes(tag))) kwScore += 1;
    }

    const latestDate = new Date(facts[facts.length - 1]?.emailDate || 0).getTime();
    const ageDays = (now - latestDate) / 86400000;
    const recencyScore = Math.max(0, 3 * (1 - ageDays / 365));
    const recencyWeight = isTemporalQuery ? 2.5 : 1;

    scoredThreads.push({ tid, facts, score: kwScore + recencyScore * recencyWeight, kwScore });
  }

  // Sort threads by score descending; require at least one keyword hit to include
  scoredThreads.sort((a, b) => b.score - a.score);

  // Greedily fill budget with complete threads
  const selected = [];
  for (const thread of scoredThreads) {
    if (thread.kwScore === 0) continue;
    if (selected.length >= maxCount) break;
    const remaining = maxCount - selected.length;
    // Take all facts if they fit; otherwise take the most recent facts from the thread
    const toAdd = thread.facts.length <= remaining
      ? thread.facts
      : thread.facts.slice(-remaining);
    selected.push(...toAdd);
  }

  // Fallback: almost nothing keyword-matched → return most recent facts
  if (selected.length < 5) {
    return allFacts.slice(-maxCount);
  }

  // Return chronologically sorted so Sonnet sees a clear timeline
  return selected.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));
}

function buildContextBlock(label, merged, relevant, recent, totalCount, fullDateRange = null) {
  if (merged.length === 0) {
    return `=== ${label} — Project Memory ===\nNo facts on record yet.`;
  }

  const threadMap = new Map();
  for (const r of merged) {
    const tid = r.threadId || "no-thread";
    if (!threadMap.has(tid)) threadMap.set(tid, { subject: r.subject, facts: [] });
    threadMap.get(tid).facts.push(r);
  }

  for (const thread of threadMap.values()) {
    thread.facts.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));
  }

  const relevantThreadIds = new Set(relevant.map(r => r.threadId || "no-thread"));
  const sortedThreads = [...threadMap.entries()].sort(([aId], [bId]) => {
    return (relevantThreadIds.has(aId) ? 0 : 1) - (relevantThreadIds.has(bId) ? 0 : 1);
  });

  const displayFacts = merged.length;
  const realTotal = totalCount ?? displayFacts;
  const totalThreads = threadMap.size;

  // Use full date range from all facts when available (large projects shortlist facts,
  // so computing from merged alone would give a narrow recent window).
  const _dates = merged.map(r => r.emailDate?.slice(0, 10)).filter(Boolean).sort();
  const dateRange = fullDateRange ?? (_dates.length > 0 ? `${_dates[0]} to ${_dates[_dates.length - 1]}` : null);

  let block = `=== ${label} — Project Memory ===\n${realTotal} facts across ${totalThreads} email thread${totalThreads !== 1 ? "s" : ""}.\n`;
  if (dateRange) block += `Email date range: ${dateRange}.\n`;

  const relevantSection = sortedThreads.filter(([id]) => relevantThreadIds.has(id));
  const otherSection = sortedThreads.filter(([id]) => !relevantThreadIds.has(id));

  if (relevantSection.length > 0) {
    block += "\nRelevant threads (full detail):\n";
    for (const [, thread] of relevantSection) {
      block += formatThread(thread);
    }
  }

  if (otherSection.length > 0) {
    // Non-relevant threads: show the 2 most recent facts + a header.
    // This gives Claude real content to reason from without flooding it with
    // all 1200 facts. For deeper detail on any thread, user asks a specific question.
    block += "\nOther project activity (most recent facts shown — ask a specific question for full detail):\n";
    for (const [, thread] of otherSection) {
      block += formatThreadCondensed(thread);
    }
  }

  return block;
}

function formatThread(thread) {
  const subject = thread.subject || "No subject";
  const facts = thread.facts;
  const dateRange = facts.length > 1
    ? `${facts[0].emailDate?.slice(0, 10) || "?"} to ${facts[facts.length - 1].emailDate?.slice(0, 10) || "?"}`
    : facts[0]?.emailDate?.slice(0, 10) || "unknown date";

  let out = `\nThread: "${subject}" (${facts.length} email${facts.length !== 1 ? "s" : ""} — ${dateRange})\n`;
  for (const r of facts) {
    const date = r.emailDate ? r.emailDate.slice(0, 10) : "?";
    out += `  → [${date}] ${r.fact}\n`;
  }
  return out;
}

// Condensed thread view for non-relevant threads: header + last 2 facts only.
// Keeps Claude informed about what happened in every thread without sending
// every individual fact — dramatically reduces context size for broad queries.
function formatThreadCondensed(thread) {
  const subject = thread.subject || "No subject";
  const facts = thread.facts; // already sorted asc
  const dateRange = facts.length > 1
    ? `${facts[0].emailDate?.slice(0, 10) || "?"} to ${facts[facts.length - 1].emailDate?.slice(0, 10) || "?"}`
    : facts[0]?.emailDate?.slice(0, 10) || "unknown date";

  let out = `\nThread: "${subject}" (${facts.length} fact${facts.length !== 1 ? "s" : ""} — ${dateRange})\n`;
  const preview = facts.slice(-2); // last 2 facts (most recent)
  for (const r of preview) {
    const date = r.emailDate ? r.emailDate.slice(0, 10) : "?";
    out += `  → [${date}] ${r.fact}\n`;
  }
  if (facts.length > 2) {
    out += `  … (${facts.length - 2} earlier facts — ask specifically to retrieve all)\n`;
  }
  return out;
}

// ── Timeline — topic lifecycle tracer ────────────────────────────────────────
// Traces one item (e.g. "timber door", "marble flooring") from first mention
// through revisions, approvals, procurement, and installation.
// Uses Haiku to select and sequence relevant facts; returns a clean event chain.
//
// Usage:
//   /bot timeline timber door    → traces timber door lifecycle
//   /bot timeline                → prompts for a topic (pending state)

const TIMELINE_SYSTEM_PROMPT = `You trace the lifecycle of a specific item or topic within a fit-out / interior design project, using facts extracted from real project emails.

Given a topic and relevant facts, produce a clean chronological event timeline showing how that item progressed — from initial specification or BOQ, through design iterations, revisions, client approvals, procurement, installation, defects, and closeout.

If the topic is ambiguous and clearly refers to multiple distinct items (e.g. "door" when there are timber doors AND steel doors AND a glass sliding door), respond with ONLY:
[CLARIFY] <one short clarifying question listing the options>

Otherwise, return ONLY valid JSON — no markdown, no extra text:
{
  "topic": "Item name as understood (e.g. Timber Veneer Doors — Level 3)",
  "events": [
    {
      "date": "YYYY-MM-DD",
      "phase": "BOQ | Design | Revision | Approval | Procurement | Installation | Defect | Closeout | Update",
      "summary": "One concise sentence. Include QAR amounts, names, or decision-makers where available."
    }
  ],
  "current_status": "One sentence on the current position of this item."
}

Rules:
- Base everything strictly on the provided facts — do not invent events.
- Use the email date for any fact where no specific date is mentioned.
- If fewer than 2 facts are clearly relevant, return: {"topic": "...", "events": [], "current_status": "Insufficient data on this topic."}`;

// Phase → emoji mapping for clean display
const PHASE_ICON = {
  boq:          "📋",
  design:       "📐",
  revision:     "✏️",
  approval:     "✅",
  procurement:  "📦",
  installation: "🔧",
  defect:       "⚠️",
  closeout:     "🏁",
  update:       "📝",
};

function phaseIcon(phase = "") {
  return PHASE_ICON[(phase || "").toLowerCase()] || "•";
}

// "YYYY-MM-DD" → "15 Mar 2024"
function tlFmt(dateStr) {
  if (!dateStr || dateStr < "2000-01-01") return "—";
  const d = new Date(dateStr + "T00:00:00Z");
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
}

export async function handleTimeline(env, project, topic) {
  const { company, label } = project;

  const allFacts = await getAllProjectFacts(env, company);
  if (allFacts.length === 0) {
    return { text: `📅 No facts recorded yet for <b>${escHtml(label)}</b>.`, clarify: false };
  }

  // Use the same keyword-scored fact selection as Q&A — topic is the "question"
  const selected = selectRelevantFacts(allFacts, topic, 120);

  if (selected.length === 0) {
    return {
      text: `📅 No facts found relating to "<b>${escHtml(topic)}</b>" in <b>${escHtml(label)}</b>.\n\nTry a different keyword or run <code>/bot timeline</code> to choose a new topic.`,
      clarify: false,
    };
  }

  // Build a compact fact list for Haiku (date + text only — no subject/thread noise)
  const factLines = selected
    .map((f, i) => `[${i + 1}] ${(f.emailDate || "").slice(0, 10)} — ${f.fact}`)
    .join("\n");

  const res = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: HAIKU_MODEL,
      max_tokens: 1200,
      system: TIMELINE_SYSTEM_PROMPT,
      messages: [{
        role: "user",
        content: `Project: ${label}\nTopic: ${topic}\n\nFacts (${selected.length} selected from ${allFacts.length} total):\n${factLines}`,
      }],
    }),
  });

  if (!res.ok) throw new Error(`Claude Haiku timeline error: ${res.status}`);

  const data = await res.json();
  const raw = (data.content?.[0]?.text || "").trim();

  // Clarification requested — caller will show the question and re-arm pending state
  if (raw.startsWith("[CLARIFY]")) {
    return { text: raw.replace(/^\[CLARIFY\]\s*/, "").trim(), clarify: true };
  }

  // Parse JSON response
  let json;
  try {
    const cleaned = raw.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();
    json = JSON.parse(extractFirstJsonObject(cleaned) ?? cleaned);
  } catch {
    throw new Error("Claude returned invalid JSON for timeline");
  }

  if (!json.events?.length) {
    return {
      text: `📅 <b>${escHtml(json.topic || topic)}</b>\n\n<i>${escHtml(json.current_status || "Insufficient data on this topic.")}</i>`,
      clarify: false,
    };
  }

  // Format as clean Telegram HTML event chain
  const lines = [
    `📅 <b>${escHtml(json.topic || topic)}</b>`,
    `<i>Project: ${escHtml(label)}</i>`,
    "",
  ];

  for (const ev of json.events) {
    const icon = phaseIcon(ev.phase);
    const dateStr = tlFmt(ev.date);
    lines.push(`${icon} <b>${escHtml(ev.phase || "Update")}</b>  ·  <i>${dateStr}</i>`);
    lines.push(`   ${escHtml(ev.summary || "")}`);
    lines.push("");
  }

  lines.push(`📌 <b>Current Status</b>`);
  lines.push(`   ${escHtml(json.current_status || "—")}`);

  return { text: lines.join("\n"), clarify: false };
}
