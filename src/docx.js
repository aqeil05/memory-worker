// ─── Word Document Builder ────────────────────────────────────────────────────
// Generates valid .docx binaries using the same ZIP+OOXML approach as onedrive.js.
// Styling: navy (#1B2A4A) headings, gold (#C9A84C) table headers, alternating rows.

// ── Public exports ─────────────────────────────────────────────────────────────

// json = { executive_summary, timeline[], open_issues[], cost_items[], key_contacts[], risks[] }
export function buildSummaryDocx(label, json) {
  const body = [
    h1(`${label} — Project Briefing`),
    emptyPara(),
    h2("Executive Summary"),
    para(json.executive_summary || ""),
    emptyPara(),
  ];

  if (json.timeline?.length > 0) {
    body.push(h2("Key Timeline"));
    body.push(table(
      ["Date", "Event", "Significance"],
      json.timeline.map(t => [t.date || "", t.event || "", t.significance || ""])
    ));
    body.push(emptyPara());
  }

  if (json.open_issues?.length > 0) {
    body.push(h2("Open Issues"));
    body.push(table(
      ["Issue", "Priority", "Action Required", "Deadline"],
      json.open_issues.map(i => [i.issue || "", i.priority || "", i.action_required || "", i.deadline || ""])
    ));
    body.push(emptyPara());
  }

  if (json.cost_items?.length > 0) {
    body.push(h2("Cost Items"));
    body.push(table(
      ["Description", "Amount", "Status"],
      json.cost_items.map(c => [c.description || "", c.amount || "", c.status || ""])
    ));
    body.push(emptyPara());
  }

  if (json.key_contacts?.length > 0) {
    body.push(h2("Key Contacts"));
    body.push(table(
      ["Name / Company", "Role"],
      json.key_contacts.map(c => [c.name || "", c.role || ""])
    ));
    body.push(emptyPara());
  }

  if (json.risks?.length > 0) {
    body.push(h2("Risks"));
    body.push(table(
      ["Risk", "Severity"],
      json.risks.map(r => [r.risk || "", r.severity || ""])
    ));
    body.push(emptyPara());
  }

  return buildDocx(body.join(""));
}

// json = { executive_summary, background, narrative, evidence[], impact, conclusion }
export function buildReportDocx(topic, label, json) {
  const date = new Date().toISOString().slice(0, 10);

  const body = [
    h1(`Issue Report: ${topic}`),
    para(`Project: ${label}`),
    para(`Date: ${date}`),
    emptyPara(),
    h2("Executive Summary"),
    para(json.executive_summary || ""),
    emptyPara(),
    h2("Background"),
    para(json.background || ""),
    emptyPara(),
    h2("Issue Narrative"),
    para(json.narrative || ""),
    emptyPara(),
  ];

  if (json.evidence?.length > 0) {
    body.push(h2(`Evidence (${json.evidence.length} emails)`));
    body.push(table(
      ["Date", "Sender", "Subject", "Excerpt", "Attribution"],
      json.evidence.map(e => [e.date || "", e.sender || "", e.subject || "", e.excerpt || "", e.attribution || ""])
    ));
    body.push(emptyPara());
  }

  body.push(h2("Impact"));
  body.push(para(json.impact || ""));
  body.push(emptyPara());
  body.push(h2("Conclusion"));
  body.push(para(json.conclusion || ""));
  body.push(emptyPara());
  body.push(paraItalic("Without Prejudice — Daya Interior Design"));

  return buildDocx(body.join(""));
}

// ── OOXML element builders (inline formatting — no style deps) ─────────────────

function h1(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="120"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:b/><w:sz w:val="32"/><w:color w:val="1B2A4A"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function h2(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:before="240" w:after="80"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:b/><w:sz w:val="24"/><w:color w:val="1B2A4A"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function para(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="80"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:sz w:val="20"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function paraItalic(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="80"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:i/><w:sz w:val="20"/><w:color w:val="888888"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function emptyPara() {
  return `<w:p><w:pPr><w:spacing w:after="0"/></w:pPr></w:p>`;
}

const TABLE_BORDERS =
  `<w:tblBorders>` +
  `<w:top w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:left w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:bottom w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:right w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:insideH w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:insideV w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `</w:tblBorders>`;

function table(headers, rows) {
  const headerRow =
    `<w:tr>` +
    headers.map(h =>
      `<w:tc>` +
      `<w:tcPr><w:shd w:val="clear" w:color="auto" w:fill="1B2A4A"/></w:tcPr>` +
      `<w:p><w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:b/><w:sz w:val="18"/><w:color w:val="FFFFFF"/></w:rPr>` +
      `<w:t xml:space="preserve">${escXml(h)}</w:t></w:r></w:p></w:tc>`
    ).join("") +
    `</w:tr>`;

  const dataRows = rows.map((row, rowIdx) => {
    const fill = rowIdx % 2 === 0 ? "F5F5F5" : "FFFFFF";
    return `<w:tr>` +
      row.map(cell =>
        `<w:tc>` +
        `<w:tcPr><w:shd w:val="clear" w:color="auto" w:fill="${fill}"/></w:tcPr>` +
        `<w:p><w:r><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:sz w:val="18"/></w:rPr>` +
        `<w:t xml:space="preserve">${escXml(cell)}</w:t></w:r></w:p></w:tc>`
      ).join("") +
      `</w:tr>`;
  }).join("");

  return `<w:tbl>` +
    `<w:tblPr><w:tblW w:w="0" w:type="auto"/>${TABLE_BORDERS}` +
    `<w:tblCellMar><w:top w:w="80" w:type="dxa"/><w:left w:w="120" w:type="dxa"/><w:bottom w:w="80" w:type="dxa"/><w:right w:w="120" w:type="dxa"/></w:tblCellMar>` +
    `</w:tblPr>` +
    `<w:tblGrid>${headers.map(() => "<w:gridCol/>").join("")}</w:tblGrid>` +
    headerRow + dataRows +
    `</w:tbl>`;
}

function escXml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// ── Core docx builder ─────────────────────────────────────────────────────────

function buildDocx(bodyXml) {
  const enc = new TextEncoder();

  const CONTENT_TYPES = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
    `<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
    `<Default Extension="xml" ContentType="application/xml"/>` +
    `<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>` +
    `<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>` +
    `<Override PartName="/word/settings.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml"/>` +
    `</Types>`;

  const RELS = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
    `<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>` +
    `</Relationships>`;

  const DOC_RELS = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
    `<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>` +
    `<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings" Target="settings.xml"/>` +
    `</Relationships>`;

  const DOCUMENT = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">` +
    `<w:body>${bodyXml}` +
    `<w:sectPr><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"/></w:sectPr>` +
    `</w:body></w:document>`;

  const STYLES = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">` +
    `<w:docDefaults><w:rPrDefault><w:rPr>` +
    `<w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/><w:sz w:val="20"/>` +
    `</w:rPr></w:rPrDefault></w:docDefaults>` +
    `<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/></w:style>` +
    `</w:styles>`;

  const SETTINGS = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:settings xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">` +
    `<w:defaultTabStop w:val="720"/>` +
    `</w:settings>`;

  const files = [
    { name: "[Content_Types].xml", data: enc.encode(CONTENT_TYPES) },
    { name: "_rels/.rels", data: enc.encode(RELS) },
    { name: "word/document.xml", data: enc.encode(DOCUMENT) },
    { name: "word/_rels/document.xml.rels", data: enc.encode(DOC_RELS) },
    { name: "word/styles.xml", data: enc.encode(STYLES) },
    { name: "word/settings.xml", data: enc.encode(SETTINGS) },
  ];

  return buildZip(files);
}

// ── ZIP builder (store method — no compression, same as onedrive.js) ──────────

function buildZip(files) {
  const enc = new TextEncoder();

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
      0x50, 0x4b, 0x03, 0x04,
      ...u16(20), ...u16(0), ...u16(0), ...u16(0), ...u16(0),
      ...u32(crc), ...u32(size), ...u32(size),
      ...u16(nameBytes.length), ...u16(0),
      ...nameBytes,
    ]);

    const centralHeader = new Uint8Array([
      0x50, 0x4b, 0x01, 0x02,
      ...u16(20), ...u16(20), ...u16(0), ...u16(0), ...u16(0), ...u16(0),
      ...u32(crc), ...u32(size), ...u32(size),
      ...u16(nameBytes.length), ...u16(0), ...u16(0), ...u16(0), ...u16(0), ...u32(0),
      ...u32(localOffset),
      ...nameBytes,
    ]);

    entries.push({ localHeader, data, centralHeader });
    localOffset += localHeader.length + data.length;
  }

  const centralDirOffset = localOffset;
  const centralDirSize = entries.reduce((s, e) => s + e.centralHeader.length, 0);

  const eocd = new Uint8Array([
    0x50, 0x4b, 0x05, 0x06,
    ...u16(0), ...u16(0),
    ...u16(files.length), ...u16(files.length),
    ...u32(centralDirSize), ...u32(centralDirOffset),
    ...u16(0),
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
