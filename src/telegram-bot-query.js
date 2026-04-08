// ─── Telegram Group Bot — Query Handler ──────────────────────────────────────
// Handles /bot questions, /bot summary, /bot report, and clarification follow-ups.
// Reads facts from OneDrive Excel → builds context → Claude → answer/report.

import { queryFacts, getRecentFacts, getAllProjectFacts, getAllCompanies, matchingCompanies } from "./onedrive.js";
import { getBotHistory, setBotHistory, setBotPending } from "./dedup.js";
import { escHtml } from "./notify.js";
import { claudeFetch } from "./memory.js";

const CLAUDE_API = "https://api.anthropic.com/v1/messages";
const SONNET_MODEL = "claude-sonnet-4-5";
const HAIKU_MODEL = "claude-haiku-4-5";

const SUMMARY_CACHE_PREFIX = "summary:cache:";
const SUMMARY_CACHE_TTL_S  = 25 * 60 * 60; // 25h — survives from 6AM generation to next day's cron
const SUMMARY_DAYS_WINDOW  = 10;
const SUMMARY_CHUNK_SIZE   = 40; // facts per Haiku chunk in two-pass summary

// Q&A retrieval thresholds
const QA_LARGE_THRESHOLD = 80;  // fact count above which Haiku shortlist path activates
const QA_RECENCY_TAIL    = 10;  // most-recent facts always included in large-project shortlist
const QA_MAX_SHORTLIST   = 40;  // cap on facts sent to Sonnet for Q&A

const QA_SYSTEM_PROMPT = `You are Daya Assistant, internal AI for Daya Interior Design (Doha, Qatar) — a fit-out, interior design, and project management company.

Answer project questions using only the facts provided from real project emails and documents.

Facts are grouped by email thread in chronological order. Each thread shows how a topic evolved over time — decisions changed, specs were revised, approvals were given. Use this chronology to give precise, accurate answers.

Rules:
- Answer ONLY from the provided facts. If the answer isn't there, say: "I don't have that in the project memory yet."
- If the question is ambiguous (e.g. "which ceiling?" when multiple are mentioned), respond with ONLY: [CLARIFY] followed by one short clarifying question ending with "?". Do not add any other text. Example: [CLARIFY] Which ceiling are you asking about — the office or the lobby?
- Be concise. Use bullet points with "•". Format dates as "15 Jun 2026". Currency in AED.
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
  "cost_items": [{"description": "item/trade", "amount": "AED X,XXX or TBC", "status": "Quoted/Approved/Disputed/Pending"}],
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

  let contextFacts;
  let relevantFacts;

  if (totalCount > QA_LARGE_THRESHOLD) {
    // Large project: build a compact fact index, ask Haiku for relevant indices,
    // then keep a recency tail so the most recent activity is always present.
    const indices = await shortlistQAFactsWithHaiku(env, question, allFacts).catch((err) => {
      console.warn(`Haiku Q&A shortlist failed, falling back to recency tail: ${err.message}`);
      return [];
    });

    // Always include the most-recent QA_RECENCY_TAIL facts.
    const recencySet = new Set();
    for (let i = Math.max(0, totalCount - QA_RECENCY_TAIL); i < totalCount; i++) {
      recencySet.add(i);
    }

    // Reserve the recency tail first — these indices cannot be dropped by the cap.
    const tailIndices = [...recencySet].sort((a, b) => a - b);

    // Fill remaining budget from Haiku-selected indices, excluding already-reserved tail.
    const remainingBudget = QA_MAX_SHORTLIST - tailIndices.length;
    const haikuIndices = indices
      .filter(i => Number.isInteger(i) && i >= 0 && i < totalCount && !recencySet.has(i))
      .slice(0, remainingBudget);

    // Merge, de-duplicate, restore chronological order.
    const finalIndices = [...new Set([...haikuIndices, ...tailIndices])].sort((a, b) => a - b);
    contextFacts = finalIndices.map(i => allFacts[i]);

    // All shortlisted facts are considered relevant — show every thread at full detail.
    relevantFacts = contextFacts;
  } else {
    // Small project: send all facts; promote threads that match any question keyword.
    contextFacts = allFacts;
    const keywords = extractKeywords(question);
    relevantFacts = keywords.length > 0
      ? allFacts.filter(r => keywords.some(kw => r.fact.toLowerCase().includes(kw)))
      : [];
  }

  const context = buildContextBlock(label, contextFacts, relevantFacts, [], totalCount);

  const messages = [
    ...history,
    {
      role: "user",
      content: isClarification
        ? `(Clarification to previous question) ${question}`
        : question,
    },
  ];

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
      system: `${QA_SYSTEM_PROMPT}\n\n${context}`,
      messages,
    }),
  });

  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Claude Sonnet error: ${res.status} ${errBody}`);
  }

  const data = await res.json();
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

  // Pass 1: sequential Haiku calls with inter-chunk delay to avoid burst rate limiting.
  // Parallel fan-out was the root cause of 429s on large summaries.
  const chunkSummaries = [];
  for (let idx = 0; idx < chunks.length; idx++) {
    if (idx > 0) await new Promise(r => setTimeout(r, 500));
    chunkSummaries.push(await summarizeChunkWithHaiku(env, label, chunks[idx], idx));
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
        // 3. Extract outermost { ... } from mixed text
        const match = raw.match(/\{[\s\S]*\}/);
        if (!match) throw new Error("No JSON object found in response");
        json = JSON.parse(match[0]);
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
    const text = formatSummaryText(label, cached.json);
    await env.DAYA_KV.put(`tg:bot:draft:${chatId}`, JSON.stringify(cached.json),
      { expirationTtl: 2 * 60 * 60 });
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
      const text = formatSummaryText(label, fuzzyCached.json);
      await env.DAYA_KV.put(`tg:bot:draft:${chatId}`, JSON.stringify(fuzzyCached.json),
        { expirationTtl: 2 * 60 * 60 });
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

// ── Report handler (two-step: Haiku shortlist → Sonnet narrative) ─────────────

export async function handleReport(env, chatId, topic, project) {
  const { company, label } = project;

  const facts = await getAllProjectFacts(env, company);
  if (facts.length === 0) {
    return {
      text: `📋 No facts recorded yet for this project.`,
      json: null,
    };
  }

  // Step 1: Shortlist relevant facts via Claude Haiku
  const factIndex = facts
    .map((f, i) =>
      `[${i}] [${(f.emailDate || "").slice(0, 10)}] [${(f.subject || "").slice(0, 50)}] ${(f.fact || "").slice(0, 120)}`
    )
    .join("\n");

  const shortlistRes = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: HAIKU_MODEL,
      max_tokens: 300,
      messages: [{
        role: "user",
        content:
          `From the project fact list below, identify ALL facts that serve as evidence for this issue.\n` +
          `Topic: ${topic}\n\n` +
          `Include facts showing: decisions made, specs changed, delays, approvals, costs, contradictions.\n\n` +
          `Return ONLY JSON: { "relevant_indices": [list of integers] }\n\n` +
          `FACT LIST:\n${factIndex}`,
      }],
    }),
  });

  if (!shortlistRes.ok) throw new Error(`Claude Haiku shortlist error: ${shortlistRes.status}`);

  const shortlistData = await shortlistRes.json();
  let indices = [];
  try {
    const match = shortlistData.content[0].text.trim().match(/\{[\s\S]*\}/);
    indices = JSON.parse(match[0]).relevant_indices || [];
  } catch {
    indices = [];
  }

  const shortlisted = indices
    .filter(i => Number.isInteger(i) && i >= 0 && i < facts.length)
    .map(i => facts[i]);

  if (shortlisted.length === 0) {
    return {
      text: `📋 I couldn't find relevant facts for "<b>${escHtml(topic)}</b>" in the project memory. Try a different search term.`,
      json: null,
    };
  }

  // Step 2: Generate report narrative via Claude Sonnet
  const evidenceBlock = shortlisted
    .map(f =>
      `FROM: ${f.sender}\nDATE: ${(f.emailDate || "").slice(0, 10)}\nSUBJECT: ${f.subject}\nFACT: ${f.fact}`
    )
    .join("\n\n---\n\n");

  const narrativeRes = await claudeFetch(CLAUDE_API, {
    method: "POST",
    headers: {
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: SONNET_MODEL,
      max_tokens: 1400,
      messages: [{
        role: "user",
        content:
          `You are a professional report writer for Daya Interior Design (Doha, Qatar).\n` +
          `Topic: ${topic}\nProject: ${label}\n\n` +
          `Based on the evidence below, write a formal issue/delay report.\n\n` +
          `Return ONLY valid JSON:\n` +
          `{\n` +
          `  "executive_summary": "2-3 sentences",\n` +
          `  "background": "1-2 sentences of context",\n` +
          `  "narrative": "4-6 sentences tracing the issue chronologically with specific dates",\n` +
          `  "evidence": [{"date": "YYYY-MM-DD", "sender": "email", "subject": "subject", "excerpt": "most relevant sentence", "attribution": "Client/Supplier/Daya — explanation"}],\n` +
          `  "impact": "2-3 sentences on programme/cost impact",\n` +
          `  "conclusion": "2-3 sentences"\n` +
          `}\n\n` +
          `Use formal language suitable for client or contract administrator.\n\n` +
          `EVIDENCE:\n${evidenceBlock}`,
      }],
    }),
  });

  if (!narrativeRes.ok) throw new Error(`Claude Sonnet narrative error: ${narrativeRes.status}`);

  const narrativeData = await narrativeRes.json();
  let json;
  try {
    const match = narrativeData.content[0].text.trim().match(/\{[\s\S]*\}/);
    json = JSON.parse(match ? match[0] : narrativeData.content[0].text);
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
      max_tokens: 1400,
      messages: [{
        role: "user",
        content:
          `You wrote this issue report for project "${label}", topic "${topic}":\n` +
          `${JSON.stringify(originalJson, null, 2)}\n\n` +
          `Please revise it based on this feedback: ${feedback}\n\n` +
          `Return ONLY the revised JSON with the same structure.`,
      }],
    }),
  });

  if (!res.ok) throw new Error(`Claude regenerate error: ${res.status}`);

  const data = await res.json();
  let json;
  try {
    const match = data.content[0].text.trim().match(/\{[\s\S]*\}/);
    json = JSON.parse(match ? match[0] : data.content[0].text);
  } catch {
    throw new Error("Claude returned invalid JSON for regenerated report");
  }

  return { text: formatReportText(topic, label, json), json };
}

// ── Formatting helpers ────────────────────────────────────────────────────────

function formatSummaryText(label, json) {
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

  parts.push(`<b>Impact:</b>`);
  parts.push(escHtml(json.impact || ""));
  parts.push("");
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

// Ask Haiku to identify relevant fact indices for a Q&A question.
// Mirrors the shortlist step used by handleReport but scoped to Q&A.
async function shortlistQAFactsWithHaiku(env, question, facts) {
  const factIndex = facts
    .map((f, i) =>
      `[${i}] [${(f.emailDate || "").slice(0, 10)}] [${(f.subject || "").slice(0, 50)}] ${(f.fact || "").slice(0, 120)}`
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
      max_tokens: 400,
      messages: [{
        role: "user",
        content:
          `From the project fact list below, identify ALL facts that are relevant to answering this question.\n` +
          `Question: ${question}\n\n` +
          `Consider: direct mentions, related topics, context clues, and recent updates.\n` +
          `Return ONLY JSON: { "relevant_indices": [list of integers] }\n\n` +
          `FACT LIST:\n${factIndex}`,
      }],
    }),
  });

  if (!res.ok) throw new Error(`Claude Haiku Q&A shortlist error: ${res.status}`);

  const data = await res.json();
  try {
    const match = data.content[0].text.trim().match(/\{[\s\S]*\}/);
    return JSON.parse(match[0]).relevant_indices || [];
  } catch {
    return [];
  }
}

function buildContextBlock(label, merged, relevant, recent, totalCount) {
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

  // Compute date range so Claude can answer earliest/latest questions directly
  // without having to search through hundreds of individual facts.
  const dates = merged.map(r => r.emailDate?.slice(0, 10)).filter(Boolean).sort();
  const dateRange = dates.length > 0 ? `${dates[0]} to ${dates[dates.length - 1]}` : null;

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
