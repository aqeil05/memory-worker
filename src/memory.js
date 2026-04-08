// ─── Claude Haiku — Email Fact Extraction ────────────────────────────────────
// Extracts company name + key facts from email body and any PDF attachments.
// Returns: { company: string, facts: string[] }

const CLAUDE_API = "https://api.anthropic.com/v1/messages";
const HAIKU_MODEL = "claude-haiku-4-5";

const SYSTEM_PROMPT = `You extract key facts from business emails and documents for an interior fit-out company's project memory system.
Company context: Daya Interior Design, Doha Qatar — fit-out, interior design, project management, carpentry.

Extract:
1. The PROJECT name this email relates to. Look for it in the subject line — it is usually the client or building name, often followed by a floor or unit reference (e.g. "Malomatia 19th floor", "Al Fardan Villa", "QU Student Housing", "B3-A5-37").
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
  "facts": ["fact one.", "fact two.", "fact three."]
}

If you cannot identify a project or extract meaningful facts, return:
{ "company": "", "facts": [] }`;

// ── Company name alias map ─────────────────────────────────────────────────────
// Maps known variations → canonical project key stored in KV.
// Add new aliases here whenever a project is found to be split across multiple keys.
// Keys must be lowercase. Applied AFTER Claude extraction as a safety net.

const COMPANY_ALIASES = {
  // Malomatia 19th floor variations
  "malomatia":           "malomatia 19th floor",
  "malomatia 19":        "malomatia 19th floor",
  "malomatia 19 floor":  "malomatia 19th floor",
  "malomatia 19th":      "malomatia 19th floor",
  "malomatia qatar":     "malomatia 19th floor",
  "p18174":              "malomatia 19th floor",
  "14th floor":          "malomatia 19th floor",
};

export function normalizeCompany(name) {
  const lower = (name || "").toLowerCase().trim();
  // Exact alias match only — do not use startsWith/endsWith to avoid rewriting
  // distinct project names (e.g. "malomatia office" must not become "malomatia 19th floor").
  if (COMPANY_ALIASES[lower]) return COMPANY_ALIASES[lower];
  return lower;
}

// ── Shared Claude fetch helper — retries 429 and 5xx with Retry-After / backoff ─

export async function claudeFetch(url, options) {
  let res;
  for (let attempt = 0; attempt < 3; attempt++) {
    res = await fetch(url, options);
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
      text: `FROM: ${from}
DATE: ${date}
SUBJECT: ${subject}

${body.slice(0, 3000)}`,
    },
  ];

  const payload = JSON.stringify({
    model: HAIKU_MODEL,
    max_tokens: 400,
    system: SYSTEM_PROMPT,
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
    const company = normalizeCompany(rawCompany);
    if (company !== rawCompany) {
      console.log(`normalizeCompany: "${rawCompany}" → "${company}"`);
    }
    return {
      company,
      facts: Array.isArray(parsed.facts) ? parsed.facts.filter(f => typeof f === "string" && f.trim()) : [],
    };
  } catch {
    console.warn(`extractEmailFacts JSON parse failed. Raw: ${raw}`);
    return { company: "", facts: [] };
  }
}
