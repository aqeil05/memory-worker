// ─── OneDrive / Graph Excel API ──────────────────────────────────────────────
// Facts are stored in Cloudflare KV (fast, reliable).
// OneDrive Excel is a human-readable export — call GET /export-excel to sync.
// Groups sheet columns: ChatID, Company, Label, CreatedAt

import { getAccessToken } from "./graph.js";
import { setGroupProject, getGroupProject } from "./dedup.js";

const GRAPH_BASE = "https://graph.microsoft.com/v1.0";

export { getGroupProject };

// ── URL builders ──────────────────────────────────────────────────────────────

function driveRoot(userEmail) {
  return `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/drive/root`;
}

// ── Setup: create workbook with Facts and Groups tables (run once) ────────────

export async function setupWorkbook(env) {
  const token = await getAccessToken(env);
  const userEmail = env.ONEDRIVE_USER_EMAIL;
  const filePath = env.ONEDRIVE_FILE_PATH;

  // Create folder structure first so OneDrive is clearly organised
  await createFolders(env, token, userEmail);

  // Upload the pre-built xlsx binary to OneDrive
  const uploadUrl = `${driveRoot(userEmail)}:/${encodeURIComponent(filePath)}:/content`;
  const xlsx = buildXlsxWithData([]); // empty on first setup; call /export-excel to populate

  const res = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
    body: xlsx,
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`setupWorkbook upload failed: ${res.status} ${body}`);
  }

  const file = await res.json();
  return { id: file.id, name: file.name, webUrl: file.webUrl };
}

// ── Append facts to KV (primary storage) ─────────────────────────────────────
// Facts are stored per-company in KV: mem:facts:{company} → JSON array
// This is fast, reliable, and avoids the Graph Excel workbook API entirely.

export async function appendFacts(env, factRows) {
  if (!factRows.length) return;

  // Group incoming rows by company
  const byCompany = {};
  for (const f of factRows) {
    const co = (f.company || "unknown").toLowerCase().trim();
    if (!byCompany[co]) byCompany[co] = [];
    byCompany[co].push({
      company: co,
      threadId: f.threadId || "",
      subject: f.subject || "",
      sender: f.sender || "",
      emailDate: f.emailDate || "",
      fact: f.fact || "",
      tags: Array.isArray(f.tags) ? f.tags : [],
      source: f.source || "email",
      createdAt: f.createdAt || new Date().toISOString(),
    });
  }

  // Write each company's new facts into KV (read → deduplicate → append → write)
  for (const [company, rows] of Object.entries(byCompany)) {
    const key = `mem:facts:${company}`;
    const existing = await env.DAYA_KV.get(key, "json") || [];

    // Deduplicate: skip incoming rows whose (emailDate|fact) combo already exists.
    // Prevents duplicates if backfill is re-run or a webhook fires twice for the same email.
    const existingKeys = new Set(existing.map(r => `${r.emailDate}|${r.fact}`));
    const newRows = rows.filter(r => !existingKeys.has(`${r.emailDate}|${r.fact}`));

    if (newRows.length === 0) {
      // Still track the company even if all rows were dupes (idempotent)
      await trackCompany(env, company);
      continue;
    }

    const merged = [...existing, ...newRows];
    if (merged.length > 500) {
      console.warn(`appendFacts: project "${company}" now has ${merged.length} facts — consider archiving old data`);
    }
    await env.DAYA_KV.put(key, JSON.stringify(merged));
    await env.DAYA_KV.put(`mem:dirty:${company}`, "1");
    await trackCompany(env, company);
  }
}

// ── Merge company ─────────────────────────────────────────────────────────────
// Moves all facts from `from` key → `into` key (with dedup), then deletes stale keys.
// Safe to re-run — appendFacts is idempotent on (emailDate|fact).

export async function mergeCompany(env, from, into) {
  from = (from || "").toLowerCase().trim();
  into = (into || "").toLowerCase().trim();
  if (!from || !into) throw new Error("Both from and into are required");
  if (from === into) throw new Error("from and into must be different");

  const fromFacts = await env.DAYA_KV.get(`mem:facts:${from}`, "json") || [];
  if (fromFacts.length === 0) {
    return { moved: 0, from, into };
  }

  const retagged = fromFacts.map(f => ({ ...f, company: into }));
  await appendFacts(env, retagged);

  const deleteResults = await Promise.allSettled([
    env.DAYA_KV.delete(`mem:facts:${from}`),
    env.DAYA_KV.delete(`mem:co:${from}`),
    env.DAYA_KV.delete(`summary:cache:${from}`),
  ]);
  const failed = deleteResults.filter(r => r.status === "rejected");
  if (failed.length > 0) {
    console.error(`mergeCompany: ${failed.length} KV delete(s) failed for "${from}":`, failed.map(r => r.reason?.message));
  }

  console.log(`mergeCompany: moved ${fromFacts.length} facts from "${from}" → "${into}"`);
  return { moved: fromFacts.length, from, into };
}

// ── KV fact readers ────────────────────────────────────────────────────────────
// getKVFacts uses fuzzy word matching so "/link Malomatia 19th Floor" finds
// facts stored under "malomatia", "malomatia qatar", etc.
// Matching logic:
//   1. Exact match on the full normalised company string (fast path)
//   2. If nothing found, check every stored company key — if any word with 3+
//      characters is shared between the query and the stored key, include it.

async function getKVFacts(env, company) {
  const normalized = company.toLowerCase().trim();

  const allCompanies = await getAllCompanies(env);

  // Exact-first path: if the company key exists verbatim in the database, treat it as
  // authoritative and return only that project's facts. This prevents fuzzy merging from
  // pulling in related-but-distinct projects (e.g. "malomatia office" must not return
  // "malomatia 19th floor" facts when "malomatia office" is a stored key).
  if (allCompanies.includes(normalized)) {
    return await env.DAYA_KV.get(`mem:facts:${normalized}`, "json") || [];
  }

  // Fuzzy fallback: only used when no exact key exists in the database.
  // Find all stored companies that share meaningful words with the query.
  const queryWords = new Set(normalized.split(/\s+/).filter(w => w.length >= 3));

  const matched = allCompanies.filter(co => {
    const coWords = co.split(/\s+/);
    return coWords.some(w => w.length >= 3 && queryWords.has(w)) ||
      [...queryWords].some(w => co.includes(w));
  });

  if (!matched.length) return [];

  // Merge and deduplicate facts from all fuzzy-matched company keys
  const seen = new Set();
  const merged = [];
  for (const co of matched) {
    const rows = await env.DAYA_KV.get(`mem:facts:${co}`, "json") || [];
    for (const row of rows) {
      const key = `${row.emailDate}|${row.fact}`;
      if (!seen.has(key)) { seen.add(key); merged.push(row); }
    }
  }
  return merged;
}

// Returns the list of company keys that would match a given company string —
// used by /link to tell the admin what was found in the database, and by
// handleSummary() to locate pre-generated caches for fuzzy-linked groups.
// Mirrors the exact-first resolution semantics of getKVFacts() so that all
// callers see consistent behaviour:
//   1. If the company key exists verbatim, return only that key (authoritative).
//   2. Otherwise return all fuzzy-matched keys from the current mem:co:* index.
export async function matchingCompanies(env, company) {
  const normalized = company.toLowerCase().trim();
  const allCompanies = await getAllCompanies(env);

  // Exact-first rule: mirrors getKVFacts() — if the linked key exists verbatim,
  // return only it so callers don't broaden to fuzzy candidates unnecessarily.
  if (allCompanies.includes(normalized)) {
    return [normalized];
  }

  // Fuzzy fallback: only when no exact key exists in the current index.
  const queryWords = new Set(normalized.split(/\s+/).filter(w => w.length >= 3));
  return allCompanies.filter(co => {
    const coWords = co.split(/\s+/);
    return coWords.some(w => w.length >= 3 && queryWords.has(w)) ||
      [...queryWords].some(w => co.includes(w));
  });
}

export async function queryFacts(env, company, keyword, limit = 20) {
  const rows = await getKVFacts(env, company);
  const kwLower = keyword?.toLowerCase();
  return rows
    .filter(r => !kwLower || r.fact.toLowerCase().includes(kwLower))
    .slice(-limit)
    .reverse();
}

export async function getRecentFacts(env, company, limit = 15) {
  const rows = await getKVFacts(env, company);
  return rows.slice(-limit).reverse();
}

export async function getAllProjectFacts(env, company) {
  const rows = await getKVFacts(env, company);
  return rows.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));
}

// ── Company index (for /export-excel) ────────────────────────────────────────
// Uses per-company KV keys (mem:co:{company} = "1") instead of a single JSON
// array. KV put is idempotent — eliminates the read-check-write race condition
// that caused duplicates when two emails for a new company were processed concurrently.

async function trackCompany(env, company) {
  await env.DAYA_KV.put(`mem:co:${company}`, "1");
}

export async function getAllCompanies(env) {
  // Try the fast company index first
  const coList = await env.DAYA_KV.list({ prefix: "mem:co:" });
  if (coList.keys.length > 0) {
    return coList.keys.map(k => k.name.slice("mem:co:".length));
  }
  // Fallback: scan mem:facts: directly (handles missing index)
  const factsList = await env.DAYA_KV.list({ prefix: "mem:facts:" });
  return factsList.keys.map(k => k.name.slice("mem:facts:".length));
}

// ── Active project list ───────────────────────────────────────────────────────
// mem:projects → JSON array of canonical lowercase project names.
// Used by extractEmailFacts to guide Claude's project matching so new emails
// always land under a canonical key instead of a freeform invented name.

export async function getActiveProjects(env) {
  const raw = await env.DAYA_KV.get("mem:projects", "json");
  return Array.isArray(raw) ? raw : [];
}

export async function addActiveProject(env, name) {
  const normalized = name.toLowerCase().trim();
  if (!normalized) throw new Error("Project name cannot be empty");
  const current = await getActiveProjects(env);
  if (current.includes(normalized)) return { added: false, name: normalized };
  await env.DAYA_KV.put("mem:projects", JSON.stringify([...current, normalized].sort()));
  return { added: true, name: normalized };
}

export async function archiveProject(env, name) {
  const normalized = name.toLowerCase().trim();
  const current = await getActiveProjects(env);
  const filtered = current.filter(p => p !== normalized);
  if (filtered.length === current.length) return { removed: false, name: normalized };
  await env.DAYA_KV.put("mem:projects", JSON.stringify(filtered));
  return { removed: true, name: normalized };
}

// ── Upload a file to OneDrive/Memory Worker/Reports/ and return webUrl ─────────

export async function uploadReport(env, filename, fileBytes) {
  const token = await getAccessToken(env);
  const reportPath = `Memory Worker/Reports/${filename}`;
  const uploadUrl = `${driveRoot(env.ONEDRIVE_USER_EMAIL)}:/${encodeURIComponent(reportPath)}:/content`;

  const res = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    },
    body: fileBytes,
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`uploadReport failed: ${res.status} ${body}`);
  }

  const file = await res.json();
  return file.webUrl;
}

// ── Export all KV facts to OneDrive Excel (call GET /export-excel) ────────────
// Builds a fresh xlsx with all current facts embedded — no workbook API needed.

export async function exportToExcel(env) {
  const companies = await getAllCompanies(env);
  const allFacts = [];
  for (const co of companies) {
    const rows = await getKVFacts(env, co);
    allFacts.push(...rows);
  }

  // Sort by emailDate oldest → newest
  allFacts.sort((a, b) => (a.emailDate || "").localeCompare(b.emailDate || ""));

  const xlsx = buildXlsxWithData(allFacts);
  const token = await getAccessToken(env);
  const uploadUrl = `${driveRoot(env.ONEDRIVE_USER_EMAIL)}:/${encodeURIComponent(env.ONEDRIVE_FILE_PATH)}:/content`;

  const res = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
    body: xlsx,
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`exportToExcel upload failed: ${res.status} ${body}`);
  }

  const file = await res.json();
  return { factCount: allFacts.length, companyCount: companies.length, webUrl: file.webUrl };
}

// ── Link a Telegram group to a project ────────────────────────────────────────

export async function linkGroup(env, chatId, company, label) {
  // Store in KV for fast lookup on every /bot message
  await setGroupProject(env.DAYA_KV, chatId, { company, label });
}

// ── Create OneDrive folder structure ─────────────────────────────────────────
// Creates "Memory Worker/" and "Memory Worker/Reports/" before uploading files.
// Uses conflictBehavior "replace" so this is safe to call multiple times.

async function createFolders(env, token, userEmail) {
  const base = `${GRAPH_BASE}/users/${encodeURIComponent(userEmail)}/drive`;

  // 1. Create "Memory Worker" at the drive root
  const r1 = await fetch(`${base}/root/children`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      name: "Memory Worker",
      folder: {},
      "@microsoft.graph.conflictBehavior": "replace",
    }),
  });
  if (!r1.ok) {
    const body = await r1.text();
    throw new Error(`createFolders: failed to create "Memory Worker/" — ${r1.status} ${body}`);
  }

  // 2. Create "Reports" inside "Memory Worker"
  const r2 = await fetch(`${base}/root:/Memory Worker:/children`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      name: "Reports",
      folder: {},
      "@microsoft.graph.conflictBehavior": "replace",
    }),
  });
  if (!r2.ok) {
    const body = await r2.text();
    throw new Error(`createFolders: failed to create "Memory Worker/Reports/" — ${r2.status} ${body}`);
  }
}

// ─── xlsx builder with embedded data rows ────────────────────────────────────
// Generates the xlsx file directly from fact data — no workbook API needed.
// Uses inline strings for data rows (avoids shared strings complexity).

function escXml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function buildXlsxWithData(factRows) {
  const enc = new TextEncoder();

  const COLS = ["Company", "ThreadID", "Subject", "Sender", "EmailDate", "Fact", "Source", "CreatedAt"];
  const N = factRows.length;
  const tableRef = `A1:H${N + 1}`;

  // Header row uses shared strings (indices 0–7)
  const headerXml = `<row r="1">${COLS.map((_, i) =>
    `<c r="${String.fromCharCode(65 + i)}1" t="s"><v>${i}</v></c>`
  ).join("")}</row>`;

  // Data rows use inline strings
  const dataXml = factRows.map((f, ri) => {
    const rn = ri + 2;
    const vals = [f.company, f.threadId, f.subject, f.sender, f.emailDate, f.fact, f.source, f.createdAt];
    return `<row r="${rn}">${vals.map((v, ci) =>
      `<c r="${String.fromCharCode(65 + ci)}${rn}" t="inlineStr"><is><t>${escXml(v)}</t></is></c>`
    ).join("")}</row>`;
  }).join("");

  const sheet1 = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" ` +
    `xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">` +
    `<sheetData>${headerXml}${dataXml}</sheetData>` +
    `<tableParts count="1"><tablePart r:id="rId1"/></tableParts></worksheet>`;

  const table1 = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<table xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" ` +
    `id="1" name="Facts" displayName="Facts" ref="${tableRef}" totalsRowShown="0">` +
    `<tableColumns count="8">` +
    COLS.map((c, i) => `<tableColumn id="${i + 1}" name="${c}"/>`).join("") +
    `</tableColumns></table>`;

  const xmlFiles = [
    { name: "[Content_Types].xml", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/tables/table1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/></Types>` },
    { name: "_rels/.rels", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>` },
    { name: "xl/workbook.xml", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Facts" sheetId="1" r:id="rId1"/></sheets></workbook>` },
    { name: "xl/_rels/workbook.xml.rels", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>` },
    { name: "xl/worksheets/sheet1.xml", content: sheet1 },
    { name: "xl/worksheets/_rels/sheet1.xml.rels", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/table" Target="../tables/table1.xml"/></Relationships>` },
    { name: "xl/tables/table1.xml", content: table1 },
    { name: "xl/styles.xml", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>` },
    { name: "xl/sharedStrings.xml", content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="8" uniqueCount="8"><si><t>Company</t></si><si><t>ThreadID</t></si><si><t>Subject</t></si><si><t>Sender</t></si><si><t>EmailDate</t></si><si><t>Fact</t></si><si><t>Source</t></si><si><t>CreatedAt</t></si></sst>` },
  ];

  return buildZip(xmlFiles.map(f => ({ name: f.name, data: enc.encode(f.content) })));
}

// ─── Minimal xlsx ZIP builder (headers only — for /setup-db initial upload) ───

function buildMinimalXlsx() {
  const enc = new TextEncoder();

  const xmlFiles = [
    {
      name: "[Content_Types].xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/tables/table1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml"/><Override PartName="/xl/tables/table2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/></Types>`,
    },
    {
      name: "_rels/.rels",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
    },
    {
      name: "xl/workbook.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Facts" sheetId="1" r:id="rId1"/><sheet name="Groups" sheetId="2" r:id="rId2"/></sheets></workbook>`,
    },
    {
      name: "xl/_rels/workbook.xml.rels",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/><Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>`,
    },
    {
      // Facts sheet: Company(0) ThreadID(1) Subject(2) Sender(3) EmailDate(4) Fact(5) Source(6) CreatedAt(7)
      name: "xl/worksheets/sheet1.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c><c r="C1" t="s"><v>2</v></c><c r="D1" t="s"><v>3</v></c><c r="E1" t="s"><v>4</v></c><c r="F1" t="s"><v>5</v></c><c r="G1" t="s"><v>6</v></c><c r="H1" t="s"><v>7</v></c></row></sheetData><tableParts count="1"><tablePart r:id="rId1"/></tableParts></worksheet>`,
    },
    {
      // Groups sheet: ChatID(8) Company(0) Label(9) CreatedAt(7)
      name: "xl/worksheets/sheet2.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheetData><row r="1"><c r="A1" t="s"><v>8</v></c><c r="B1" t="s"><v>0</v></c><c r="C1" t="s"><v>9</v></c><c r="D1" t="s"><v>7</v></c></row></sheetData><tableParts count="1"><tablePart r:id="rId1"/></tableParts></worksheet>`,
    },
    {
      name: "xl/worksheets/_rels/sheet1.xml.rels",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/table" Target="../tables/table1.xml"/></Relationships>`,
    },
    {
      name: "xl/worksheets/_rels/sheet2.xml.rels",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/table" Target="../tables/table2.xml"/></Relationships>`,
    },
    {
      name: "xl/tables/table1.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><table xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" id="1" name="Facts" displayName="Facts" ref="A1:H1" totalsRowShown="0"><tableColumns count="8"><tableColumn id="1" name="Company"/><tableColumn id="2" name="ThreadID"/><tableColumn id="3" name="Subject"/><tableColumn id="4" name="Sender"/><tableColumn id="5" name="EmailDate"/><tableColumn id="6" name="Fact"/><tableColumn id="7" name="Source"/><tableColumn id="8" name="CreatedAt"/></tableColumns></table>`,
    },
    {
      name: "xl/tables/table2.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><table xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" id="2" name="Groups" displayName="Groups" ref="A1:D1" totalsRowShown="0"><tableColumns count="4"><tableColumn id="1" name="ChatID"/><tableColumn id="2" name="Company"/><tableColumn id="3" name="Label"/><tableColumn id="4" name="CreatedAt"/></tableColumns></table>`,
    },
    {
      name: "xl/styles.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>`,
    },
    {
      // sharedStrings indices: 0=Company 1=ThreadID 2=Subject 3=Sender 4=EmailDate 5=Fact 6=Source 7=CreatedAt 8=ChatID 9=Label
      name: "xl/sharedStrings.xml",
      content: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="10" uniqueCount="10"><si><t>Company</t></si><si><t>ThreadID</t></si><si><t>Subject</t></si><si><t>Sender</t></si><si><t>EmailDate</t></si><si><t>Fact</t></si><si><t>Source</t></si><si><t>CreatedAt</t></si><si><t>ChatID</t></si><si><t>Label</t></si></sst>`,
    },
  ];

  return buildZip(xmlFiles.map(f => ({ name: f.name, data: enc.encode(f.content) })));
}

// ── ZIP builder (store method — no compression) ───────────────────────────────

function buildZip(files) {
  const enc = new TextEncoder();

  // CRC-32 table
  const crcTable = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    crcTable[i] = c;
  }

  function crc32(data) {
    let crc = 0xffffffff;
    for (const byte of data) crc = crcTable[(crc ^ byte) & 0xff] ^ (crc >>> 8);
    return (crc ^ 0xffffffff) >>> 0;
  }

  function u16(n) { return [n & 0xff, (n >> 8) & 0xff]; }
  function u32(n) { return [n & 0xff, (n >> 8) & 0xff, (n >> 16) & 0xff, (n >> 24) & 0xff]; }

  const entries = [];
  let localOffset = 0;

  for (const file of files) {
    const nameBytes = enc.encode(file.name);
    const data = file.data;
    const crc = crc32(data);
    const size = data.length;

    const localHeader = new Uint8Array([
      0x50, 0x4b, 0x03, 0x04,   // local file header signature
      ...u16(20),                 // version needed
      ...u16(0),                  // flags
      ...u16(0),                  // compression: stored
      ...u16(0),                  // last mod time
      ...u16(0),                  // last mod date
      ...u32(crc),
      ...u32(size),               // compressed size (same as uncompressed for stored)
      ...u32(size),               // uncompressed size
      ...u16(nameBytes.length),
      ...u16(0),                  // extra field length
      ...nameBytes,
    ]);

    const centralHeader = new Uint8Array([
      0x50, 0x4b, 0x01, 0x02,   // central directory signature
      ...u16(20),                 // version made by
      ...u16(20),                 // version needed
      ...u16(0),                  // flags
      ...u16(0),                  // compression: stored
      ...u16(0),                  // last mod time
      ...u16(0),                  // last mod date
      ...u32(crc),
      ...u32(size),
      ...u32(size),
      ...u16(nameBytes.length),
      ...u16(0),                  // extra field length
      ...u16(0),                  // file comment length
      ...u16(0),                  // disk number start
      ...u16(0),                  // internal attributes
      ...u32(0),                  // external attributes
      ...u32(localOffset),        // offset of local header
      ...nameBytes,
    ]);

    entries.push({ localHeader, data, centralHeader });
    localOffset += localHeader.length + data.length;
  }

  const centralDirOffset = localOffset;
  const centralDirSize = entries.reduce((s, e) => s + e.centralHeader.length, 0);

  const eocd = new Uint8Array([
    0x50, 0x4b, 0x05, 0x06,          // end of central directory signature
    ...u16(0),                         // disk number
    ...u16(0),                         // disk with central dir
    ...u16(files.length),              // records on this disk
    ...u16(files.length),              // total records
    ...u32(centralDirSize),
    ...u32(centralDirOffset),
    ...u16(0),                         // comment length
  ]);

  const totalSize = entries.reduce((s, e) => s + e.localHeader.length + e.data.length, 0)
    + centralDirSize + eocd.length;

  const zip = new Uint8Array(totalSize);
  let pos = 0;

  for (const { localHeader, data } of entries) {
    zip.set(localHeader, pos); pos += localHeader.length;
    zip.set(data, pos); pos += data.length;
  }
  for (const { centralHeader } of entries) {
    zip.set(centralHeader, pos); pos += centralHeader.length;
  }
  zip.set(eocd, pos);

  return zip;
}
