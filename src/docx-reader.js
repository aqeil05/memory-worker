// ─── Word Document Text Extractor ─────────────────────────────────────────────
// Extracts plain text from a .docx binary (Uint8Array).
// A .docx is a ZIP archive — we parse the ZIP to find word/document.xml,
// decompress it (DEFLATE or STORE), strip XML tags, and return plain text.
// No external libraries needed — Cloudflare Workers support DecompressionStream.

const MAX_TEXT_LENGTH = 6000; // chars sent to Claude (token budget)

// ── Main export ───────────────────────────────────────────────────────────────

export async function extractDocxText(bytes) {
  try {
    const xml = await extractFileFromZip(bytes, "word/document.xml");
    if (!xml) return null;
    const text = stripXmlTags(xml);
    return text.slice(0, MAX_TEXT_LENGTH);
  } catch (err) {
    console.warn(`extractDocxText failed: ${err.message}`);
    return null;
  }
}

// ── ZIP parser ────────────────────────────────────────────────────────────────
// Reads the End-of-Central-Directory record to locate the central directory,
// then walks central directory entries to find the target filename.
// Handles both STORE (method 0) and DEFLATE (method 8) compressed entries.

async function extractFileFromZip(bytes, targetName) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  // Find End-of-Central-Directory signature (0x06054b50) scanning from the end.
  // EOCD is at least 22 bytes; scan last 65557 bytes to handle ZIP comments.
  const EOCD_SIG = 0x06054b50;
  let eocdOffset = -1;
  const scanStart = Math.max(0, bytes.length - 65557);
  for (let i = bytes.length - 22; i >= scanStart; i--) {
    if (view.getUint32(i, true) === EOCD_SIG) {
      eocdOffset = i;
      break;
    }
  }
  if (eocdOffset === -1) throw new Error("EOCD not found — not a valid ZIP");

  const cdOffset = view.getUint32(eocdOffset + 16, true); // central directory offset
  const cdSize   = view.getUint32(eocdOffset + 12, true); // central directory size

  // Walk central directory entries
  const CD_SIG = 0x02014b50;
  let pos = cdOffset;
  while (pos < cdOffset + cdSize) {
    if (view.getUint32(pos, true) !== CD_SIG) break;

    const compression    = view.getUint16(pos + 10, true);
    const compressedSize = view.getUint32(pos + 20, true);
    const filenameLen    = view.getUint16(pos + 28, true);
    const extraLen       = view.getUint16(pos + 30, true);
    const commentLen     = view.getUint16(pos + 32, true);
    const localOffset    = view.getUint32(pos + 42, true);

    const filenameBytes = bytes.slice(pos + 46, pos + 46 + filenameLen);
    const filename = new TextDecoder().decode(filenameBytes);

    pos += 46 + filenameLen + extraLen + commentLen;

    if (filename !== targetName) continue;

    // Found — jump to local file header to get the actual data offset
    const LOCAL_SIG = 0x04034b50;
    if (view.getUint32(localOffset, true) !== LOCAL_SIG) {
      throw new Error(`Local file header signature mismatch for ${filename}`);
    }
    const localFilenameLen = view.getUint16(localOffset + 26, true);
    const localExtraLen    = view.getUint16(localOffset + 28, true);
    const dataOffset = localOffset + 30 + localFilenameLen + localExtraLen;

    const compressed = bytes.slice(dataOffset, dataOffset + compressedSize);

    if (compression === 0) {
      // STORE — data is raw, no compression
      return new TextDecoder("utf-8", { fatal: false }).decode(compressed);
    } else if (compression === 8) {
      // DEFLATE — decompress using raw deflate stream
      const decompressed = await decompressDeflateRaw(compressed);
      return new TextDecoder("utf-8", { fatal: false }).decode(decompressed);
    } else {
      throw new Error(`Unsupported ZIP compression method: ${compression}`);
    }
  }

  return null; // file not found in ZIP
}

// ── DEFLATE decompression using Cloudflare's DecompressionStream ──────────────

async function decompressDeflateRaw(compressed) {
  const ds = new DecompressionStream("deflate-raw");
  const writer = ds.writable.getWriter();
  const reader = ds.readable.getReader();

  writer.write(compressed);
  writer.close();

  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }

  const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

// ── XML tag stripper ──────────────────────────────────────────────────────────
// Removes all XML tags and collapses whitespace.
// Adds spaces between paragraph/run boundaries so words don't merge.

function stripXmlTags(xml) {
  return xml
    .replace(/<\/w:p>/g, "\n")     // paragraph end → newline
    .replace(/<\/w:r>/g, " ")      // run end → space (prevents word merging)
    .replace(/<[^>]+>/g, "")       // strip all remaining tags
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/[ \t]+/g, " ")       // collapse horizontal whitespace
    .replace(/\n{3,}/g, "\n\n")    // max 2 consecutive newlines
    .trim();
}
