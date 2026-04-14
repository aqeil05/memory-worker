// ─── Claude Haiku — Email Fact Extraction ────────────────────────────────────
// Extracts company name + key facts from email body and any PDF attachments.
// Returns: { company: string, facts: { text: string, tags: string[] }[] }
// tags — one or more of: cost, timeline, decision, contact, risk, material, approval

import { getActiveProjects } from "./onedrive.js";

const CLAUDE_API = "https://api.anthropic.com/v1/messages";
const HAIKU_MODEL = "claude-haiku-4-5-20251001";

const FACT_RULES = `Extract:
1. The PROJECT name this email relates to.
   Rules:
   - Always prefer the CLIENT or BUILDING name over a bare floor/level reference. "Malomatia 19th floor" is correct; "14th floor" alone is NOT — pair it with the building name.
   - Never use a standalone floor or level descriptor (e.g. "14th floor", "level 3", "ground floor") as the full project name. If only a floor reference appears in the subject, search the email body or sender domain for the client/building name and combine them.
   - Use only lowercase (e.g. "malomatia 19th floor", "al fardan villa").
   - If truly no project name is identifiable, use the main client or supplier company name instead.
2. 3 to 7 key facts useful for answering future project questions.

Focus on: dates, deadlines, warranties, payments, decisions, scope changes, contacts, site visits, approvals, handovers, defects, materials, specifications, quantities, prices.
Skip pleasantries, greetings, and irrelevant filler.
Each fact = one concise sentence. Include who said or decided it where relevant.

Return ONLY valid JSON, no markdown fences:
{
  "company": "project name in lowercase",
  "facts": [
    {"text": "fact one sentence.", "tags": ["cost"]},
    {"text": "fact two sentence.", "tags": ["timeline", "decision"]}
  ]
}
Tag each fact with 1-2 of: cost, timeline, decision, contact, risk, material, approval.

If you cannot identify a project or extract meaningful facts, return:
{ "company": "", "facts": [] }`;

function buildSystemPrompt(activeProjects) {
  const header = `You extract key facts from business emails and documents for an interior fit-out company's project memory system.
Company context: Daya Interior Design, Doha Qatar — fit-out, interior design, project management, carpentry.`;

  if (!activeProjects || activeProjects.length === 0) {
    return `${header}\n\n${FACT_RULES}`;
  }

  const projectList = activeProjects.map((p, i) => `${i + 1}. ${p}`).join("\n");
  return `${header}

ACTIVE PROJECTS — if this email clearly relates to one of these, use that exact name (as written below, already lowercase). If none clearly match, extract the best project/client name from the email content.
${projectList}

${FACT_RULES}`;
}

// ── Company name alias map ─────────────────────────────────────────────────────
// Maps known variations → canonical project key stored in KV.
// Add new aliases here whenever a project is found to be split across multiple keys.
// Keys must be lowercase. Applied AFTER Claude extraction as a safety net.

const COMPANY_ALIASES = {
  // Nabina
  "nabina ceramic":                   "nabina",
  "nabina holding":                   "nabina",
  "nabina interiors":                 "nabina",
  "nabina interiors co":              "nabina",
  "nabina interiors co / malomatia":  "nabina",
  "nabina interiors malomatia":       "nabina",
  "nabina interiors office":          "nabina",

  // MCIT
  "mcit 14th floor":                              "mcit",
  "mcit 19 floor":                                "mcit",
  "mcit 19th floor":                              "mcit",
  "mcit operations centre":                       "mcit",
  "mcit operation center":                        "mcit",
  "mcit operation center at al brooq tower":      "mcit",
  "mcit operation centre 14th floor al borooq tower": "mcit",
  "mcit operations centre 14th floor brooq tower":"mcit",
  "mcit water leakage 16th 15th 14th floors":     "mcit",

  // Singapore Embassy
  "singapore embassy fit-out work":               "singapore embassy",
  "singapore embassy meeting room":               "singapore embassy",

  // Malomatia 19th floor variations
  "malomatia":           "malomatia 19th floor",
  "malomatia 19":        "malomatia 19th floor",
  "malomatia 19 floor":  "malomatia 19th floor",
  "malomatia 19th":      "malomatia 19th floor",
  "malomatia qatar":     "malomatia 19th floor",
  "p18174":              "malomatia 19th floor",
  "14th floor":          "malomatia 19th floor",

  // Villaggio Starlink kiosk
  "villaggio kiosk":                      "villaggio starlink",
  "villaggio kiosk - mall shop":          "villaggio starlink",
  "villaggio kiosk - new fitout":         "villaggio starlink",
  "villaggio kiosk starlink retail stand":"villaggio starlink",
  "villaggio starlink kiosk":             "villaggio starlink",
  "villaggio mall kiosk":                 "villaggio starlink",

  // QSTP kiosk / web summit booth
  "qstp booth re installation":                           "qstp kiosk",
  "qstp booth re-installation":                           "qstp kiosk",
  "qstp booth re-installation (web summit tree stand)":   "qstp kiosk",
  "qstp booth re-installation rayyan":                    "qstp kiosk",
  "qstp booth re-installation tech2":                     "qstp kiosk",
  "qstp kiosk at web summit 2026":                        "qstp kiosk",
  "qstp stand at qatar web summit":                       "qstp kiosk",
  "qstp stand for qatar web summit":                      "qstp kiosk",
  "qstp web summit booth and tree reinstallation":        "qstp kiosk",
  "qstp web summit booth at qstp rayyan":                 "qstp kiosk",
  "qstp web summit booth re-installation tech2":          "qstp kiosk",
  "qstp web summit tree stand at qstp rayyan":            "qstp kiosk",
  "qstp web summit":                                      "qstp kiosk",
  "qstp booth":                                           "qstp kiosk",

  // Daya workshop — B3 building complex (warehouse/workshop units)
  "b3 a05 warehouse":    "daya workshop",
  "b3 series buildings": "daya workshop",
  "b3-a5-37":            "daya workshop",
  "b3-a5-37,38":         "daya workshop",
  "b3-a5-38":            "daya workshop",
  "b3-a5-39":            "daya workshop",
  "b3-a5-40":            "daya workshop",
  "b5a538":              "daya workshop",
  "b3 workshop":         "daya workshop",
  "daya warehouse":      "daya workshop",
};

export function normalizeCompany(name) {
  const lower = (name || "").toLowerCase().trim();
  // Exact alias match only — do not use startsWith/endsWith to avoid rewriting
  // distinct project names (e.g. "malomatia office" must not become "malomatia 19th floor").
  if (COMPANY_ALIASES[lower]) return COMPANY_ALIASES[lower];
  return lower;
}

// ── Dynamic KV alias helpers ──────────────────────────────────────────────────
// Stored as mem:alias:{lowercased_source} → "canonical name" (no TTL, permanent).
// Used alongside the hardcoded COMPANY_ALIASES map — hardcoded = fast path,
// KV = dynamic extension added via /bot alias or /bot merge.

export async function resolveAlias(name, env) {
  const afterHardcoded = normalizeCompany(name);
  // If the hardcoded map already changed the name, trust it — skip KV read.
  if (afterHardcoded !== (name || "").toLowerCase().trim()) return afterHardcoded;
  const kvAlias = await env.DAYA_KV.get(`mem:alias:${afterHardcoded}`);
  if (kvAlias) {
    console.log(`resolveAlias (KV): "${afterHardcoded}" → "${kvAlias}"`);
    return kvAlias;
  }
  return afterHardcoded;
}

export async function setAlias(source, target, env) {
  const s = (source || "").toLowerCase().trim();
  const t = (target || "").toLowerCase().trim();
  if (!s || !t) throw new Error("setAlias: source and target must be non-empty");
  await env.DAYA_KV.put(`mem:alias:${s}`, t);
}

export async function listAliases(env) {
  const result = await env.DAYA_KV.list({ prefix: "mem:alias:" });
  const aliases = [];
  for (const key of result.keys) {
    const source = key.name.slice("mem:alias:".length);
    const target = await env.DAYA_KV.get(key.name);
    aliases.push({ source, target });
  }
  return aliases;
}

// ── Shared Claude fetch helper — retries 429 and 5xx with Retry-After / backoff ─

export async function claudeFetch(url, options) {
  let res;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      res = await fetch(url, options);
    } catch (networkErr) {
      // Network failure (DNS, timeout, connection refused) — treat as retryable
      console.warn(`Claude network error (attempt ${attempt + 1}/3): ${networkErr.message}`);
      if (attempt === 2) throw networkErr;
      await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 1000));
      continue;
    }
    // 429 = rate limited; 5xx (including 529 = overloaded) = server error — all retryable
    const shouldRetry = res.status === 429 || res.status >= 500;
    if (!shouldRetry) break;
    if (attempt === 2) break; // exhausted retries — fall through to caller
    const waitMs = res.status === 429
      ? Math.min(parseInt(res.headers.get("retry-after") || "5", 10), 10) * 1000
      : Math.pow(2, attempt) * 1000; // 1s, 2s exponential for 5xx
    console.warn(`Claude ${res.status}, retrying in ${waitMs / 1000}s... (attempt ${attempt + 1}/3)`);
    await new Promise(r => setTimeout(r, waitMs));
  }
  return res;
}

// ── Main export ───────────────────────────────────────────────────────────────

export async function extractEmailFacts(env, { from, subject, body, date, pdfs = [], docxTexts = [] }) {
  const activeProjects = await getActiveProjects(env);
  const systemPrompt = buildSystemPrompt(activeProjects);
  const activeSet = new Set(activeProjects);

  // Build content array:
  //   1. PDFs as native document blocks (Claude reads them directly)
  //   2. Word docs as extracted text blocks
  //   3. Email body text
  const content = [
    ...pdfs.map(pdf => ({
      type: "document",
      source: {
        type: "base64",
        media_type: "application/pdf",
        data: pdf.contentBytes,
      },
      title: pdf.filename || "attachment.pdf",
    })),
    ...docxTexts.map(d => ({
      type: "text",
      text: `=== Document: ${d.filename} ===\n${d.text}`,
    })),
    {
      type: "text",
      text: (() => {
        const MAX_BODY = 3000;
        const truncated = body.length > MAX_BODY;
        if (truncated) console.warn(`Email body truncated: ${body.length} chars → ${MAX_BODY} | Subject: ${subject}`);
        return `FROM: ${from}
DATE: ${date}
SUBJECT: ${subject}
${truncated ? "[NOTE: Email body was truncated due to length — extract facts from the visible portion only]\n" : ""}
${body.slice(0, MAX_BODY)}`;
      })(),
    },
  ];

  const payload = JSON.stringify({
    model: HAIKU_MODEL,
    max_tokens: 600,
    system: systemPrompt,
    messages: [{ role: "user", content }],
  });

  const headers = {
    "x-api-key": env.ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json",
  };

  const res = await claudeFetch(CLAUDE_API, { method: "POST", headers, body: payload });

  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Claude API error: ${res.status} ${errBody}`);
  }

  const data = await res.json();
  const raw = data?.content?.[0]?.text?.trim();
  if (!raw) {
    console.warn("extractEmailFacts: empty or unexpected response from Claude");
    return { company: "", facts: [] };
  }
  const cleaned = raw.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();

  try {
    const parsed = JSON.parse(cleaned);
    const rawCompany = (parsed.company || "").toLowerCase().trim();
    // If Claude matched an active project, use it directly — no alias resolution needed.
    // Otherwise fall through to the existing alias/normalization pipeline.
    const company = activeSet.has(rawCompany)
      ? rawCompany
      : await resolveAlias(rawCompany, env);
    if (company !== rawCompany) {
      console.log(`resolveAlias: "${rawCompany}" → "${company}"`);
    }
    return {
      company,
      // Accept both old string format and new {text, tags} object format for graceful migration.
      // New emails produce objects; any legacy strings (e.g. from partial rollouts) are normalised here.
      facts: Array.isArray(parsed.facts)
        ? parsed.facts
            .map(f => typeof f === "string"
              ? { text: f.trim(), tags: [] }
              : { text: (f.text || "").trim(), tags: Array.isArray(f.tags) ? f.tags : [] })
            .filter(f => f.text)
        : [],
    };
  } catch {
    console.warn(`extractEmailFacts JSON parse failed. Raw: ${raw}`);
    return { company: "", facts: [] };
  }
}
