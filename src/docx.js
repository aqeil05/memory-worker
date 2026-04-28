// ─── Word Document Builder ────────────────────────────────────────────────────
// Generates valid .docx binaries using raw OOXML + ZIP (no external deps).
// Styling: Century Gothic, navy/gold palette, A4, 1080 DXA margins.

// ── Public exports ─────────────────────────────────────────────────────────────

// json = { executive_summary, timeline[], open_issues[], cost_items[], key_contacts[], risks[] }
export function buildSummaryDocx(label, json) {
  const title = `${label} — Project Briefing`;
  const body = [
    h1(title),
    emptyPara(),
    h2("Executive Summary"),
    para(json.executive_summary || ""),
    emptyPara(),
  ];

  if (json.timeline?.length > 0) {
    body.push(h2("Key Timeline"));
    body.push(table(
      ["Date", "Event", "Significance"],
      json.timeline.map(t => [t.date || "", t.event || "", t.significance || ""]),
      { colWidths: [1446, 4096, 3818] }
    ));
    body.push(emptyPara());
  }

  if (json.open_issues?.length > 0) {
    body.push(h2("Open Issues"));
    body.push(table(
      ["Issue", "Priority", "Action Required", "Deadline"],
      json.open_issues.map(i => [i.issue || "", i.priority || "", i.action_required || "", i.deadline || ""]),
      {
        colWidths: [3373, 964, 3677, 1346],
        colorCell: (colIdx, value) => {
          if (colIdx !== 1) return null;
          const v = String(value).toLowerCase();
          if (v === "high")   return "FDECEA";
          if (v === "medium") return "FFF8E7";
          if (v === "low")    return "EAF4EA";
          return null;
        },
      }
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

  return buildDocx(body.join(""), title);
}

// json = {
//   header, progress_snapshot[], executive_summary,
//   timeline[], impact_assessment[], decisions_required[],
//   party_actions[], commercial_summary[]
// }
// Also handles legacy format: timeline_narrative (string), commercial_summary (string), impact detail field
export function buildReportDocx(topic, label, json, diagramPngBytes = null) {
  const h = json.header || {};
  const reportDate = h.date || new Date().toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });

  const body = [];

  // 1. Title Band
  body.push(titleBand(topic, label, reportDate, h.status, h.prepared_by));
  body.push(emptyPara());

  // 2. Progress Snapshot
  if (json.progress_snapshot?.length > 0) {
    body.push(h2("Progress Snapshot"));
    body.push(progressSnapshot(json.progress_snapshot));
    body.push(emptyPara());
  }

  // 3. Executive Summary
  body.push(h2("Executive Summary"));
  const execParas = (json.executive_summary || "").split(/\n\n+/);
  for (const p of execParas) {
    if (p.trim()) body.push(para(p.trim()));
  }
  body.push(emptyPara());

  // 4. Timeline of Events
  body.push(h2("Timeline of Events"));
  if (Array.isArray(json.timeline) && json.timeline.length > 0) {
    body.push(timelineTable(json.timeline));
  } else if (json.timeline_narrative) {
    // Backward compat: legacy string format
    const tParas = json.timeline_narrative.split(/\n\n+/);
    for (const p of tParas) if (p.trim()) body.push(para(p.trim()));
  }
  body.push(emptyPara());

  // 4b. Visual Diagram (if generated via refine)
  if (diagramPngBytes) {
    body.push(h2("Visual Diagram"));
    body.push(inlineImage("rId5"));
    body.push(emptyPara());
  }

  // 5. Current Impact Assessment
  if (json.impact_assessment?.length > 0) {
    body.push(h2("Current Impact Assessment"));
    body.push(impactTable(json.impact_assessment));
    body.push(emptyPara());
  }

  // 6. Decisions Required
  if (json.decisions_required?.length > 0) {
    body.push(h2("Decisions Required"));
    body.push(decisionsTable(json.decisions_required));
    body.push(emptyPara());
  }

  // 7. What Is Needed From Each Party
  if (json.party_actions?.length > 0) {
    body.push(h2("What Is Needed From Each Party"));
    body.push(partyActionsTable(json.party_actions));
    body.push(emptyPara());
  }

  // 8. Commercial Summary
  body.push(h2("Commercial Summary"));
  if (Array.isArray(json.commercial_summary) && json.commercial_summary.length > 0) {
    body.push(commercialTable(json.commercial_summary));
  } else if (typeof json.commercial_summary === "string" && json.commercial_summary) {
    body.push(para(json.commercial_summary));
  }
  body.push(emptyPara());

  return buildDocx(body.join(""), `${topic} — ${label}`, reportDate, diagramPngBytes);
}

// ── Report-specific OOXML section builders ─────────────────────────────────────

// Full-width navy title band: large white title + gold subtitle line
function titleBand(title, project, date, status, preparedBy) {
  const subtitle = [
    `Project: ${project}`,
    date ? `Date: ${date}` : null,
    status ? `Status: ${status}` : null,
    `Prepared By: ${preparedBy || "Daya Interior Design"}`,
  ].filter(Boolean).join("   |   ");

  const noBorders =
    `<w:tblBorders>` +
    `<w:top w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `<w:left w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `<w:bottom w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `<w:right w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `<w:insideH w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `<w:insideV w:val="none" w:sz="0" w:space="0" w:color="auto"/>` +
    `</w:tblBorders>`;

  return `<w:tbl>` +
    `<w:tblPr>` +
    `<w:tblW w:w="9360" w:type="dxa"/>` +
    noBorders +
    `<w:tblCellMar><w:top w:w="280" w:type="dxa"/><w:left w:w="360" w:type="dxa"/><w:bottom w:w="280" w:type="dxa"/><w:right w:w="360" w:type="dxa"/></w:tblCellMar>` +
    `</w:tblPr>` +
    `<w:tblGrid><w:gridCol w:w="9360"/></w:tblGrid>` +
    `<w:tr>` +
    `<w:tc>` +
    `<w:tcPr><w:tcW w:w="9360" w:type="dxa"/><w:shd w:val="clear" w:color="auto" w:fill="1B2A4A"/></w:tcPr>` +
    // Title
    `<w:p><w:pPr><w:spacing w:before="80" w:after="80"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="72"/><w:color w:val="FFFFFF"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(title)}</w:t></w:r></w:p>` +
    // Subtitle
    `<w:p><w:pPr><w:spacing w:before="0" w:after="100"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:sz w:val="20"/><w:color w:val="C9A84C"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(subtitle)}</w:t></w:r></w:p>` +
    `</w:tc>` +
    `</w:tr>` +
    `</w:tbl>`;
}

// Single-row 4-cell KPI snapshot
// kpis: [{ label, value, color: "green"|"red"|"blue" }]
function progressSnapshot(kpis) {
  const colorMap = { green: "2E9E6B", red: "C0392B", blue: "3A6BC7" };
  const cellW = 2340; // 9360 / 4
  const slice = kpis.slice(0, 4);

  const cells = slice.map((kpi, i) => {
    const fill = i % 2 === 0 ? "FFFFFF" : "EEF1F7";
    const valColor = colorMap[kpi.color] || "1B2A4A";
    return `<w:tc>` +
      `<w:tcPr><w:tcW w:w="${cellW}" w:type="dxa"/><w:shd w:val="clear" w:color="auto" w:fill="${fill}"/>` +
      `<w:tcMar><w:top w:w="80" w:type="dxa"/><w:left w:w="140" w:type="dxa"/><w:bottom w:w="80" w:type="dxa"/><w:right w:w="140" w:type="dxa"/></w:tcMar>` +
      `</w:tcPr>` +
      `<w:p><w:pPr><w:jc w:val="center"/><w:spacing w:before="100" w:after="40"/></w:pPr>` +
      `<w:r><w:rPr><w:rFonts ${FONT}/><w:sz w:val="18"/><w:color w:val="888888"/></w:rPr>` +
      `<w:t xml:space="preserve">${escXml(kpi.label || "")}</w:t></w:r></w:p>` +
      `<w:p><w:pPr><w:jc w:val="center"/><w:spacing w:before="0" w:after="100"/></w:pPr>` +
      `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="36"/><w:color w:val="${valColor}"/></w:rPr>` +
      `<w:t xml:space="preserve">${escXml(kpi.value || "")}</w:t></w:r></w:p>` +
      `</w:tc>`;
  });

  const gridCols = slice.map(() => `<w:gridCol w:w="${cellW}"/>`).join("");

  return `<w:tbl>` +
    `<w:tblPr><w:tblW w:w="9360" w:type="dxa"/>${TABLE_BORDERS}</w:tblPr>` +
    `<w:tblGrid>${gridCols}</w:tblGrid>` +
    `<w:tr>${cells.join("")}</w:tr>` +
    `</w:tbl>`;
}

// Timeline table: Date | Event | Impact | Status (status cell colour-coded)
// rows: [{ date, event, impact, status }]
function timelineTable(rows) {
  const colWidths = [1300, 2960, 3600, 1500];
  const headers = ["Date", "Event", "Impact", "Status"];

  const statusFill = (status) => {
    const s = String(status).toLowerCase();
    if (s.includes("resolved") && !s.includes("partial")) return "EAF4EA";
    if (s.includes("partial") || s.includes("escalated"))  return "FFF8E7";
    if (s.includes("missed") || s.includes("at risk") || s.includes("unresolved")) return "FDECEA";
    return null;
  };

  return table(headers, rows.map(r => [r.date || "", r.event || "", r.impact || "", r.status || ""]), {
    colWidths,
    boldCols: [0],
    colorCell: (colIdx, value) => colIdx === 3 ? statusFill(value) : null,
  });
}

// Impact assessment table: Severity | Issue | Consequence (7 days) | Last Recorded
// rows: [{ severity, issue, consequence, last_recorded, detail }]
function impactTable(rows) {
  const colWidths = [900, 2560, 4200, 1700];
  const headers = ["Severity", "Issue", "Consequence if Unresolved (7 days)", "Last Recorded"];

  const sevFill = (severity) => {
    const s = String(severity).toLowerCase();
    if (s.includes("critical")) return "FDECEA";
    if (s.includes("high"))     return "FFF8E7";
    if (s.includes("medium"))   return "FFFDE7";
    return null;
  };

  const fmtSev = (severity) => {
    if (/critical/i.test(severity)) return "\uD83D\uDD34 Critical";
    if (/high/i.test(severity))     return "\uD83D\uDFE0 High";
    if (/medium/i.test(severity))   return "\uD83D\uDFE1 Medium";
    return severity;
  };

  return table(
    headers,
    rows.map(r => [
      fmtSev(r.severity || ""),
      r.issue || "",
      r.consequence || r.detail || "",   // backward compat
      r.last_recorded || "",
    ]),
    {
      colWidths,
      rowColors: rows.map(r => sevFill(r.severity || "")),
    }
  );
}

// Decisions required table: Decision | Owner | Deadline | Consequence of Delay
// Deadline column always red-shaded
function decisionsTable(rows) {
  const colWidths = [2700, 2200, 1260, 3200];
  const headers = ["Decision Required", "Owner", "Deadline", "Consequence of Delay"];

  return table(
    headers,
    rows.map(r => [r.decision || "", r.owner || "", r.deadline || "", r.delay_consequence || ""]),
    {
      colWidths,
      colorCell: (colIdx) => colIdx === 2 ? "FDECEA" : null,
    }
  );
}

// Party actions table: Party | Contact(s) | Actions Required
function partyActionsTable(rows) {
  const colWidths = [1700, 2160, 5500];
  const headers = ["Party", "Contact(s)", "Actions Required"];

  return table(
    headers,
    rows.map(r => [r.party || "", r.contacts || "", r.actions || ""]),
    { colWidths }
  );
}

// Commercial summary table: Item | Value / Reference | Notes / Risk
// Flagged rows (expired quotes, unsigned PCerts, etc.) shaded red
function commercialTable(rows) {
  const colWidths = [2800, 2200, 4360];
  const headers = ["Item", "Value / Reference", "Notes / Risk"];

  return table(
    headers,
    rows.map(r => [r.item || "", r.value_ref || "", r.notes_risk || ""]),
    {
      colWidths,
      rowColors: rows.map(r => r.flagged ? "FDECEA" : null),
    }
  );
}

// ── Shared OOXML element builders ──────────────────────────────────────────────

const FONT = `w:ascii="Century Gothic" w:hAnsi="Century Gothic"`;

function h1(text) {
  return `<w:p>` +
    `<w:pPr><w:jc w:val="center"/><w:spacing w:after="120"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="36"/><w:color w:val="1B2A4A"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function h2(text) {
  // Bold navy, 26pt, gold bottom border
  return `<w:p>` +
    `<w:pPr>` +
    `<w:pBdr><w:bottom w:val="single" w:sz="6" w:space="1" w:color="C9A84C"/></w:pBdr>` +
    `<w:spacing w:before="320" w:after="100"/>` +
    `</w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="26"/><w:color w:val="1B2A4A"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function h3(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:before="160" w:after="40"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="22"/><w:color w:val="1B2A4A"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

// Paragraph with a bold label run followed by a regular value run
function paraLabel(label, value) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="40"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="20"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(label)} </w:t></w:r>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:sz w:val="20"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(value)}</w:t></w:r></w:p>`;
}

function para(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="80" w:line="276" w:lineRule="auto"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:sz w:val="20"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function paraItalic(text) {
  return `<w:p>` +
    `<w:pPr><w:spacing w:after="80" w:line="276" w:lineRule="auto"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:i/><w:sz w:val="20"/><w:color w:val="888888"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(text)}</w:t></w:r></w:p>`;
}

function emptyPara() {
  return `<w:p><w:pPr><w:spacing w:after="0"/></w:pPr></w:p>`;
}

const TABLE_BORDERS =
  `<w:tblBorders>` +
  `<w:top    w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:left   w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:bottom w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:right  w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:insideH w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `<w:insideV w:val="single" w:sz="4" w:space="0" w:color="CCCCCC"/>` +
  `</w:tblBorders>`;

// options: {
//   colWidths?:  number[],
//   colorCell?:  (colIdx, value) => string|null   — per-cell fill override
//   rowColors?:  (string|null)[]                  — per-row fill override (lower priority than colorCell)
//   boldCols?:   number[]                         — column indices to render bold
// }
function table(headers, rows, options = {}) {
  const { colWidths = null, colorCell = null, rowColors = null, boldCols = [] } = options;
  const totalWidth = colWidths ? colWidths.reduce((a, b) => a + b, 0) : 0;

  const tblW = colWidths
    ? `<w:tblW w:w="${totalWidth}" w:type="dxa"/>`
    : `<w:tblW w:w="0" w:type="auto"/>`;

  const gridCols = colWidths
    ? colWidths.map(w => `<w:gridCol w:w="${w}"/>`).join("")
    : headers.map(() => "<w:gridCol/>").join("");

  const tcW = (colIdx) =>
    colWidths ? `<w:tcW w:w="${colWidths[colIdx]}" w:type="dxa"/>` : "";

  const CELL_MAR =
    `<w:tcMar>` +
    `<w:top w:w="80" w:type="dxa"/>` +
    `<w:left w:w="140" w:type="dxa"/>` +
    `<w:bottom w:w="80" w:type="dxa"/>` +
    `<w:right w:w="140" w:type="dxa"/>` +
    `</w:tcMar>`;

  const headerRow =
    `<w:tr>` +
    headers.map((hdr, i) =>
      `<w:tc>` +
      `<w:tcPr>${tcW(i)}<w:shd w:val="clear" w:color="auto" w:fill="1B2A4A"/>${CELL_MAR}</w:tcPr>` +
      `<w:p><w:r><w:rPr><w:rFonts ${FONT}/><w:b/><w:sz w:val="20"/><w:color w:val="FFFFFF"/></w:rPr>` +
      `<w:t xml:space="preserve">${escXml(hdr)}</w:t></w:r></w:p></w:tc>`
    ).join("") +
    `</w:tr>`;

  const dataRows = rows.map((row, rowIdx) => {
    const defaultFill = rowIdx % 2 === 0 ? "FFFFFF" : "EEF1F7";
    const rowOverride = rowColors ? rowColors[rowIdx] : null;

    return `<w:tr>` +
      row.map((cell, colIdx) => {
        const cellOverride = colorCell ? colorCell(colIdx, cell) : null;
        const fill = cellOverride || rowOverride || defaultFill;
        const bold = boldCols.includes(colIdx);
        return `<w:tc>` +
          `<w:tcPr>${tcW(colIdx)}<w:shd w:val="clear" w:color="auto" w:fill="${fill}"/>${CELL_MAR}</w:tcPr>` +
          `<w:p><w:r><w:rPr><w:rFonts ${FONT}/>${bold ? "<w:b/>" : ""}<w:sz w:val="20"/></w:rPr>` +
          `<w:t xml:space="preserve">${escXml(cell)}</w:t></w:r></w:p></w:tc>`;
      }).join("") +
      `</w:tr>`;
  }).join("");

  return `<w:tbl>` +
    `<w:tblPr>${tblW}${TABLE_BORDERS}</w:tblPr>` +
    `<w:tblGrid>${gridCols}</w:tblGrid>` +
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

// ── Inline image OOXML (16:9, full content width = 6188000 × 3481000 EMU) ─────

function inlineImage(relId) {
  const cx = 6188000, cy = 3481000;
  const WP  = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing";
  const A   = "http://schemas.openxmlformats.org/drawingml/2006/main";
  const PIC = "http://schemas.openxmlformats.org/drawingml/2006/picture";
  return (
    `<w:p><w:r><w:drawing>` +
    `<wp:inline xmlns:wp="${WP}" distT="0" distB="0" distL="0" distR="0">` +
    `<wp:extent cx="${cx}" cy="${cy}"/>` +
    `<wp:effectExtent l="0" t="0" r="0" b="0"/>` +
    `<wp:docPr id="1" name="Diagram"/>` +
    `<a:graphic xmlns:a="${A}">` +
    `<a:graphicData uri="${PIC}">` +
    `<pic:pic xmlns:pic="${PIC}">` +
    `<pic:nvPicPr><pic:cNvPr id="0" name="Diagram"/><pic:cNvPicPr/></pic:nvPicPr>` +
    `<pic:blipFill><a:blip r:embed="${relId}"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill>` +
    `<pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="${cx}" cy="${cy}"/></a:xfrm>` +
    `<a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr>` +
    `</pic:pic></a:graphicData></a:graphic>` +
    `</wp:inline></w:drawing></w:r></w:p>`
  );
}

// ── Core docx builder ─────────────────────────────────────────────────────────

function buildDocx(bodyXml, title, reportDate = "", diagramPngBytes = null) {
  const enc = new TextEncoder();
  const safeTitle = escXml(title || "");
  const safeDate  = escXml(reportDate || new Date().toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }));

  const W   = "http://schemas.openxmlformats.org/wordprocessingml/2006/main";
  const R   = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
  const PKG = "http://schemas.openxmlformats.org/package/2006";

  const CONTENT_TYPES =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Types xmlns="${PKG}/content-types">` +
    `<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
    `<Default Extension="xml" ContentType="application/xml"/>` +
    `<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>` +
    `<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>` +
    `<Override PartName="/word/settings.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml"/>` +
    `<Override PartName="/word/header1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml"/>` +
    `<Override PartName="/word/footer1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml"/>` +
    (diagramPngBytes ? `<Override PartName="/word/media/diagram.png" ContentType="image/png"/>` : "") +
    `</Types>`;

  const RELS =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Relationships xmlns="${PKG}/relationships">` +
    `<Relationship Id="rId1" Type="${R}/officeDocument" Target="word/document.xml"/>` +
    `</Relationships>`;

  const DOC_RELS =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<Relationships xmlns="${PKG}/relationships">` +
    `<Relationship Id="rId1" Type="${R}/styles" Target="styles.xml"/>` +
    `<Relationship Id="rId2" Type="${R}/settings" Target="settings.xml"/>` +
    `<Relationship Id="rId3" Type="${R}/header" Target="header1.xml"/>` +
    `<Relationship Id="rId4" Type="${R}/footer" Target="footer1.xml"/>` +
    (diagramPngBytes ? `<Relationship Id="rId5" Type="${R}/image" Target="media/diagram.png"/>` : "") +
    `</Relationships>`;

  const DOCUMENT =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:document xmlns:w="${W}" xmlns:r="${R}">` +
    `<w:body>${bodyXml}` +
    `<w:sectPr>` +
    `<w:pgSz w:w="11906" w:h="16838" w:orient="portrait"/>` +
    `<w:pgMar w:top="1080" w:right="1080" w:bottom="1080" w:left="1080" w:header="709" w:footer="709" w:gutter="0"/>` +
    `<w:headerReference w:type="default" r:id="rId3"/>` +
    `<w:footerReference w:type="default" r:id="rId4"/>` +
    `</w:sectPr>` +
    `</w:body></w:document>`;

  const STYLES =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:styles xmlns:w="${W}">` +
    `<w:docDefaults><w:rPrDefault><w:rPr>` +
    `<w:rFonts ${FONT}/><w:sz w:val="20"/>` +
    `</w:rPr></w:rPrDefault></w:docDefaults>` +
    `<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/></w:style>` +
    `</w:styles>`;

  const SETTINGS =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:settings xmlns:w="${W}">` +
    `<w:defaultTabStop w:val="720"/>` +
    `</w:settings>`;

  const HEADER =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:hdr xmlns:w="${W}">` +
    `<w:p>` +
    `<w:pPr><w:jc w:val="right"/><w:spacing w:after="0"/></w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:sz w:val="18"/><w:color w:val="888888"/></w:rPr>` +
    `<w:t xml:space="preserve">${safeTitle}${safeDate ? "  |  " + safeDate : ""}</w:t></w:r>` +
    `</w:p>` +
    `</w:hdr>`;

  // Footer: top border + italic grey "Without Prejudice" line
  const footerText = `Without Prejudice \u2014 Daya Interior Design | Prepared: ${safeDate}`;
  const FOOTER =
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
    `<w:ftr xmlns:w="${W}">` +
    `<w:p>` +
    `<w:pPr>` +
    `<w:jc w:val="center"/>` +
    `<w:spacing w:before="80" w:after="0"/>` +
    `<w:pBdr><w:top w:val="single" w:sz="4" w:space="2" w:color="CCCCCC"/></w:pBdr>` +
    `</w:pPr>` +
    `<w:r><w:rPr><w:rFonts ${FONT}/><w:i/><w:sz w:val="16"/><w:color w:val="888888"/></w:rPr>` +
    `<w:t xml:space="preserve">${escXml(footerText)}</w:t></w:r>` +
    `</w:p>` +
    `</w:ftr>`;

  const files = [
    { name: "[Content_Types].xml",          data: enc.encode(CONTENT_TYPES) },
    { name: "_rels/.rels",                  data: enc.encode(RELS) },
    { name: "word/document.xml",            data: enc.encode(DOCUMENT) },
    { name: "word/_rels/document.xml.rels", data: enc.encode(DOC_RELS) },
    { name: "word/styles.xml",              data: enc.encode(STYLES) },
    { name: "word/settings.xml",            data: enc.encode(SETTINGS) },
    { name: "word/header1.xml",             data: enc.encode(HEADER) },
    { name: "word/footer1.xml",             data: enc.encode(FOOTER) },
    ...(diagramPngBytes ? [{ name: "word/media/diagram.png", data: new Uint8Array(diagramPngBytes) }] : []),
  ];

  return buildZip(files);
}

// ── ZIP builder (store method — no compression) ───────────────────────────────

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
