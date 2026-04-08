// ─── Telegram Notifications ───────────────────────────────────────────────────

export function escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

export async function sendMessage(botToken, chatId, text) {
  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "HTML",
    }),
  });
  const json = await res.json();
  if (!json.ok) {
    console.error("Telegram sendMessage error:", JSON.stringify(json));
  }
  return json;
}

// Find a safe split point that doesn't cut inside an HTML tag.
// Prefers double-newline, then single newline, then scans backwards from max
// to avoid splitting inside <b>, <code>, <i>, etc.
function findSafeSplit(text, max) {
  // Prefer splitting on a paragraph break or line break
  let splitAt = text.lastIndexOf("\n\n", max);
  if (splitAt >= max / 2) return splitAt;
  splitAt = text.lastIndexOf("\n", max);
  if (splitAt >= max / 2) return splitAt;

  // Hard cut fallback — scan backwards to avoid cutting inside an HTML tag
  splitAt = max;
  while (splitAt > max / 2) {
    const lastOpen = text.lastIndexOf("<", splitAt);
    const lastClose = text.lastIndexOf(">", splitAt);
    if (lastOpen <= lastClose) break; // not inside a tag at this position
    splitAt = lastOpen - 1; // step back before the unclosed tag
  }
  return splitAt > 0 ? splitAt : max;
}

// Split message across multiple sends if > 4000 chars (Telegram limit is 4096)
export async function sendLongMessage(botToken, chatId, text) {
  const MAX = 4000;
  if (text.length <= MAX) {
    await sendMessage(botToken, chatId, text);
    return;
  }

  const parts = [];
  let remaining = text;
  while (remaining.length > MAX) {
    const splitAt = findSafeSplit(remaining, MAX);
    parts.push(remaining.slice(0, splitAt).trim());
    remaining = remaining.slice(splitAt).trim();
  }
  if (remaining) parts.push(remaining);

  for (const part of parts) {
    await sendMessage(botToken, chatId, part);
  }
}

// Send message with inline keyboard buttons
// buttons: [[{text, callback_data}]] — 2D array (rows × columns)
export async function sendWithButtons(botToken, chatId, text, buttons) {
  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "HTML",
      reply_markup: { inline_keyboard: buttons },
    }),
  });
  const json = await res.json();
  if (!json.ok) {
    console.error("Telegram sendWithButtons error:", JSON.stringify(json));
  }
  return json;
}

// Acknowledge a callback_query (required within 10s of receiving it)
export async function answerCallback(botToken, callbackQueryId, text = "") {
  const res = await fetch(`https://api.telegram.org/bot${botToken}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: callbackQueryId, text }),
  });
  const json = await res.json();
  if (!json.ok) {
    console.error("Telegram answerCallback error:", JSON.stringify(json));
  }
  return json;
}
