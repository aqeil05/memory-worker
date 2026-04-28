// ─── Telegram Group Bot Handler ───────────────────────────────────────────────
// Handles updates from the memory bot (@DayaProjectBot) in group/supergroup chats.
//
// Commands:
//   /link {project name} — link group to a project; auto-pins the query menu
//   /bot                 — show 4-button query menu [❓ Q&A][📋 Summary][📅 Timeline][📊 Report]
//   /bot pin             — re-send and pin the query menu
//
// Button flows (tap → bot prompts → user types → bot responds):
//   ❓ Q&A       → "Ask your question:"         → answer
//   📋 Summary   → generates immediately         → [📥 Export as Word]
//   📅 Timeline  → "What item should I trace?"  → timeline
//   📊 Report    → "What should the report cover?" → report → [📥 Export] [✏️ Refine]
//
// Admin commands (still type directly):
//   /bot companies · /bot merge · /bot backfill · /bot projects
//   /bot addproject · /bot archiveproject · /bot alias · /bot alias list

import { handleBotQuery, handleSummary, handleReport, regenerateReport, handleTimeline, generateDiagramForFeedback } from "./telegram-bot-query.js";
import { linkGroup, getGroupProject, uploadReport, downloadItemAsPdf, uploadPdfReport, matchingCompanies, getAllCompanies, mergeCompany, getActiveProjects, addActiveProject, archiveProject } from "./onedrive.js";
import { normalizeCompany, isValidCompanyName, setAlias, listAliases } from "./memory.js";
import { buildSummaryDocx, buildReportDocx } from "./docx.js";
import {
  getBotPending, setBotPending, deleteBotPending,
  getDraft, setDraft, deleteDraft,
  getRefinePending, setRefinePending, deleteRefinePending,
  getActiveMode, setActiveMode, deleteActiveMode,
  setCancelFlag, checkAndClearCancelFlag,
  isRateLimited, incrementRateLimit,
} from "./dedup.js";
import { sendMessage, sendLongMessage, sendWithButtons, answerCallback, escHtml, sendChatAction, pinMessage, sendDocument, sendPhoto } from "./notify.js";

// ── Entry point ───────────────────────────────────────────────────────────────

export async function handleTelegramUpdate(request, env, ctx) {
  let update;
  try {
    update = await request.json();
  } catch {
    return new Response("Bad request", { status: 400 });
  }

  // Handle inline keyboard button taps
  if (update.callback_query) {
    const cq = update.callback_query;
    const chatId = String(cq.message?.chat?.id);
    const data = cq.data || "";
    if (chatId && data) {
      ctx.waitUntil(
        handleCallbackQuery(env, ctx, chatId, data, cq.id)
          .catch(err => console.error(`handleCallbackQuery failed [${chatId}]: ${err.stack || err.message}`))
      );
    }
    return new Response("OK");
  }

  // Handle text messages in group/supergroup chats only
  const msg = update.message;
  if (!msg?.text) return new Response("OK");

  const chatType = msg.chat?.type;
  if (chatType !== "group" && chatType !== "supergroup") return new Response("OK");

  const chatId = String(msg.chat.id);
  const text = msg.text.trim();

  ctx.waitUntil(
    handleGroupMessage(env, ctx, chatId, text)
      .catch(err => console.error(`handleGroupMessage failed [${chatId}]: ${err.stack || err.message}`))
  );
  return new Response("OK");
}

// ── Message dispatcher ────────────────────────────────────────────────────────

async function handleGroupMessage(env, ctx, chatId, text) {
  try {
    // "cancel" clears all pending/active state and kills in-flight queue jobs
    if (text.trim().toLowerCase() === "cancel") {
      await Promise.all([
        deleteBotPending(env.DAYA_KV, chatId),
        deleteDraft(env.DAYA_KV, chatId),
        deleteRefinePending(env.DAYA_KV, chatId),
        deleteActiveMode(env.DAYA_KV, chatId),
        setCancelFlag(env.DAYA_KV, chatId),
      ]);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "✅ Cancelled. All pending operations cleared.");
      return;
    }

    if (text.startsWith("/link") || text.startsWith("/start")) {
      await handleLink(env, chatId, text);
      return;
    }

    if (text.startsWith("/bot") || text.startsWith("/bot@")) {
      await handleBotCommand(env, ctx, chatId, text);
      return;
    }

    // Refine-pending (Refine button tap) takes priority over active mode
    const refinePending = await getRefinePending(env.DAYA_KV, chatId);
    if (refinePending) {
      await handleRefineText(env, chatId, text, refinePending);
      return;
    }

    // Active mode: all plain messages handled by the current mode until /bot resets
    const modeState = await getActiveMode(env.DAYA_KV, chatId);
    if (modeState) {
      await handleActiveMode(env, ctx, chatId, text, modeState);
      return;
    }

    // Legacy one-shot pending states (clarification from Q&A, guided merge)
    const pending = await getBotPending(env.DAYA_KV, chatId);
    if (pending?.type === "clarification") {
      await handleClarification(env, chatId, text, pending);
      return;
    }
    if (pending?.type === "report") {
      await handleReportTopic(env, ctx, chatId, text, pending.project);
      return;
    }
    if (pending?.type === "timeline") {
      await handleTimelineTopic(env, chatId, text, pending.project);
      return;
    }
    if (pending?.type === "merge") {
      await handleMergeStep(env, chatId, text, pending);
      return;
    }

    if (pending?.type === "proj_add") {
      await deleteBotPending(env.DAYA_KV, chatId);
      const name = text.trim().toLowerCase();
      const result = await addActiveProject(env, name);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        result.added
          ? `✅ Added <code>${escHtml(name)}</code> to active projects.\n\nFuture emails will be matched against it.`
          : `ℹ️ <code>${escHtml(name)}</code> is already in the active project list.`);
      return;
    }

    if (pending?.type === "alias_add") {
      await deleteBotPending(env.DAYA_KV, chatId);
      const m = text.match(/^(.+?)\s*(?:→|->)\s*(.+)$/);
      if (!m) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Format not recognised. Use: <code>source → canonical</code>\n\nTap <b>🔗 Aliases → ➕ Add alias</b> to try again.");
        return;
      }
      const source = m[1].toLowerCase().trim();
      const target = m[2].toLowerCase().trim();
      await setAlias(source, target, env);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `✅ Alias set: <code>${escHtml(source)}</code> → <code>${escHtml(target)}</code>\n\n` +
        `Future emails matching "<b>${escHtml(source)}</b>" will route to "<b>${escHtml(target)}</b>".`);
      return;
    }

    // Everything else: ignore silently
  } catch (err) {
    console.error(`handleGroupMessage error (chatId ${chatId}): ${err.message}`);
  }
}

// ── /bot command router ───────────────────────────────────────────────────────

async function handleBotCommand(env, ctx, chatId, text) {
  // Immediate typing indicator — shows "Bot is typing..." before any slow operations.
  sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});

  const question = text.replace(/\/bot(?:@\S+)?/, "").trim();
  const lowerQ = question.toLowerCase();

  if (!question) {
    await deleteActiveMode(env.DAYA_KV, chatId);
    const project = await getGroupProject(env.DAYA_KV, chatId);
    await sendQueryMenu(env, chatId, project?.label ?? null);
    return;
  }

  // ── DB management commands — no project link required ──────────────────────

  // /bot companies — list all company keys with fact counts
  if (lowerQ === "companies") {
    const companies = await getAllCompanies(env);
    if (companies.length === 0) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📂 No companies in the database yet. Run <code>/bot backfill</code> first.");
      return;
    }
    const lines = [];
    for (const co of companies.sort()) {
      const facts = await env.DAYA_KV.get(`mem:facts:${co}`, "json") || [];
      lines.push(`• <code>${escHtml(co)}</code> — ${facts.length} facts`);
    }
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Companies in database (${companies.length}):</b>\n` + lines.join("\n"));
    return;
  }

  // /bot alias list — show all dynamic KV aliases
  if (lowerQ === "alias list") {
    const aliases = await listAliases(env);
    if (aliases.length === 0) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📋 No custom aliases set yet.\n\nUse <code>/bot alias source name → canonical name</code> to add one.");
      return;
    }
    const lines = aliases.map(a => `• <code>${escHtml(a.source)}</code> → <code>${escHtml(a.target)}</code>`);
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Custom aliases (${aliases.length}):</b>\n` + lines.join("\n"));
    return;
  }

  // /bot alias {source} → {target}  (also accepts ->)
  const aliasMatch = question.match(/^alias\s+(.+?)\s*(?:→|->)\s*(.+)$/i);
  if (aliasMatch) {
    const source = aliasMatch[1].toLowerCase().trim();
    const target = aliasMatch[2].toLowerCase().trim();
    if (!source || !target) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "⚠️ Usage: <code>/bot alias source name → canonical name</code>");
      return;
    }
    await setAlias(source, target, env);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `✅ Alias set: <code>${escHtml(source)}</code> → <code>${escHtml(target)}</code>\n\n` +
      `Future emails matching "<b>${escHtml(source)}</b>" will be routed to "<b>${escHtml(target)}</b>".`);
    return;
  }

  // /bot projects — list active project names
  if (lowerQ === "projects") {
    const projects = await getActiveProjects(env);
    if (projects.length === 0) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📂 No active projects configured yet.\n\nUse <code>/bot addproject project name</code> to add one.");
      return;
    }
    const lines = [];
    for (const p of projects) {
      const facts = await env.DAYA_KV.get(`mem:facts:${p}`, "json") || [];
      lines.push(`• <code>${escHtml(p)}</code> — ${facts.length} facts`);
    }
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Active projects (${projects.length}):</b>\n` + lines.join("\n"));
    return;
  }

  // /bot addproject {name} — add to active project list
  const addProjectMatch = question.match(/^addproject\s+(.+)$/i);
  if (addProjectMatch) {
    const name = addProjectMatch[1].toLowerCase().trim();
    const result = await addActiveProject(env, name);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      result.added
        ? `✅ Added <code>${escHtml(name)}</code> to the active project list.\n\nFuture emails will be matched against it.`
        : `ℹ️ <code>${escHtml(name)}</code> is already in the active project list.`);
    return;
  }

  // /bot archiveproject {name} — remove from active project list (facts stay in KV)
  const archiveProjectMatch = question.match(/^archiveproject\s+(.+)$/i);
  if (archiveProjectMatch) {
    const name = archiveProjectMatch[1].toLowerCase().trim();
    const result = await archiveProject(env, name);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      result.removed
        ? `✅ <code>${escHtml(name)}</code> removed from active projects.\n\nExisting facts are preserved — new emails will no longer route here.`
        : `⚠️ <code>${escHtml(name)}</code> was not found in the active project list.`);
    return;
  }

  // /bot merge — guided two-step merge flow
  if (lowerQ === "merge") {
    const companies = await getAllCompanies(env);
    if (companies.length === 0) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📂 No companies in the database yet.");
      return;
    }
    const list = companies.sort().map(c => `• <code>${escHtml(c)}</code>`).join("\n");
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Merge companies — Step 1 of 2</b>\n\nWhich company should be merged FROM (the duplicate to remove)?\n\n` +
      `<b>Available companies:</b>\n${list}\n\n` +
      `Reply with the exact company name to merge FROM.`);
    await setBotPending(env.DAYA_KV, chatId, { type: "merge", step: 1 });
    return;
  }

  // /bot admin — show admin action menu
  if (lowerQ === "admin") {
    await sendAdminMenu(env, chatId);
    return;
  }

  // /bot pin — (re)send and pin the query menu
  if (lowerQ === "pin") {
    const proj = await getGroupProject(env.DAYA_KV, chatId);
    const menuMsgId = await sendQueryMenu(env, chatId, proj?.label ?? null);
    if (menuMsgId) {
      await pinMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, menuMsgId);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📌 Menu pinned. If it didn't appear at the top, make sure the bot is a group admin with <b>Pin Messages</b> permission.");
    }
    return;
  }

  // ── Project-required commands ───────────────────────────────────────────────

  const project = await getGroupProject(env.DAYA_KV, chatId);
  if (!project) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ This group isn't linked to a project yet. Use the setup link from the Closed Won flow.");
    return;
  }

  // /bot timeline [topic] — trace lifecycle of one item (timber door, marble flooring, etc.)
  if (lowerQ === "timeline" || lowerQ.startsWith("timeline ")) {
    const topic = lowerQ.startsWith("timeline ") ? question.slice("timeline ".length).trim() : "";
    if (!topic) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📅 <b>What item should I trace?</b>\n\n" +
        "Reply with the topic, e.g.:\n" +
        "• <i>timber door</i>\n" +
        "• <i>marble flooring</i>\n" +
        "• <i>CEO office glass partition</i>"
      );
      await setBotPending(env.DAYA_KV, chatId, { type: "timeline", project });
      return;
    }
    await handleTimelineTopic(env, chatId, topic, project);
    return;
  }

  // Rate-limit all Claude-powered /bot commands (soft cap: 20 per chat per hour).
  // Admin commands above (/bot companies, /bot merge, etc.) are exempt — they don't call Claude.
  if (await isRateLimited(env.DAYA_KV, chatId)) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⏸ Slow down — this group has sent 20 AI requests in the past hour. Try again soon.");
    return;
  }
  await incrementRateLimit(env.DAYA_KV, chatId);

  // /bot summary — full project briefing
  if (lowerQ === "summary") {
    const { text: summaryText, json } = await handleSummary(env, chatId, project);
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, summaryText);

    if (json) {
      await setDraft(env.DAYA_KV, chatId, { type: "summary", project, json });
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "Export this briefing?",
        [[
          { text: "📄 Export as Word", callback_data: "export_summary" },
          { text: "📑 Export as PDF",  callback_data: "export_summary_pdf" },
        ]]
      );
    }
    return;
  }

  // /bot backfill — trigger backfill of recent emails
  if (lowerQ === "backfill") {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⏳ Backfill started — processing up to 150 emails per inbox across 2 inboxes.\n\n" +
      "Already-processed emails are skipped. Run again to continue if needed."
    );
    // Dispatch to our own /backfill endpoint so it runs in its own context.
    // ctx.waitUntil ensures the fetch is sent before this worker context terminates.
    ctx.waitUntil(
      fetch(`${env.MEMORY_WORKER_URL}/backfill?chatId=${encodeURIComponent(chatId)}`).catch(() => {})
    );
    return;
  }

  // /bot report — prompt for topic
  if (lowerQ === "report") {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "📋 <b>What should the report cover?</b>\n\n" +
      "Reply with the topic, e.g.:\n" +
      "• <i>delay in CEO office glass door</i>\n" +
      "• <i>marble flooring decision</i>\n" +
      "• <i>client approval for lighting changes</i>"
    );
    await setBotPending(env.DAYA_KV, chatId, { type: "report", project });
    return;
  }

  // Regular Q&A
  const answer = await handleBotQuery(env, chatId, question, project, false);
  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, escHtml(answer));
}

// ── Timeline topic received (follow-up after /bot timeline or clarification) ──

async function handleTimelineTopic(env, chatId, topic, project) {
  await deleteBotPending(env.DAYA_KV, chatId);

  sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});

  const { text, clarify } = await handleTimeline(env, project, topic);

  if (clarify) {
    // Claude needs clarification — show the question and re-arm pending so next
    // message retries handleTimeline with the user's refined topic
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, escHtml(text));
    await setBotPending(env.DAYA_KV, chatId, { type: "timeline", project });
    return;
  }

  await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, text);
}

// ── Shared report background task ────────────────────────────────────────────
// Called by the Cloudflare Queue consumer in index.js — no wall-clock limit.
// Exported so index.js can import it for the queue handler.

export async function runReportTask(env, chatId, topic, project) {
  console.log(`runReportTask start [${chatId}] topic="${topic}" project="${project?.company}"`);
  try {
    const { text: reportText, json } = await handleReport(env, chatId, topic, project);
    if (await checkAndClearCancelFlag(env.DAYA_KV, chatId)) {
      console.log(`runReportTask cancelled [${chatId}]`);
      return;
    }
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
          { text: "❌ Cancel",  callback_data: "cancel_report" },
        ]]
      );
    }
    console.log(`runReportTask done [${chatId}]`);
  } catch (err) {
    console.error(`runReportTask failed [${chatId}]: ${err.stack || err.message}`);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `❌ Report generation failed: ${escHtml(err.message)}`
    ).catch(() => {});
  }
}

// ── Queue consumer: refine existing report with user feedback ─────────────────
// Runs without wall-clock limit in the queue consumer (up to 15 min).

export async function runRefineTask(env, { chatId, topic, project, json, feedback, iteration, diagramMermaid }) {
  // Keep typing indicator alive every 4s for the full duration of Claude streaming
  sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
  const typingInterval = setInterval(() => {
    sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
  }, 4000);
  try {
    const result = await regenerateReport(env, chatId, topic, project, json, feedback);
    const { text: reportText, json: newJson } = result;
    if (await checkAndClearCancelFlag(env.DAYA_KV, chatId)) {
      console.log(`runRefineTask cancelled [${chatId}]`);
      return;
    }
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, reportText);
    if (newJson) {
      await setDraft(env.DAYA_KV, chatId, {
        type: "report", topic, project, json: newJson, iteration: iteration + 1,
        ...(diagramMermaid ? { diagram: diagramMermaid } : {}),
      });
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "What would you like to do with this report?",
        [[
          { text: "📄 Export as Word", callback_data: "export_report" },
          { text: "📑 Export as PDF",  callback_data: "export_report_pdf" },
        ], [
          { text: "✏️ Refine", callback_data: "refine_report" },
          { text: "❌ Cancel",  callback_data: "cancel_report" },
        ]]
      );
    }
  } catch (err) {
    console.error(`runRefineTask failed [${chatId}]: ${err.stack || err.message}`);
    await setDraft(env.DAYA_KV, chatId, { type: "report", topic, project, json, iteration });
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ Refine failed — tap <b>✏️ Refine</b> again to retry.").catch(() => {});
  } finally {
    clearInterval(typingInterval);
  }
}

// ── Report topic received (follow-up after /bot report) ───────────────────────

async function handleReportTopic(env, ctx, chatId, topic, project) {
  await deleteBotPending(env.DAYA_KV, chatId);
  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
    `⏳ Generating report on "<b>${escHtml(topic)}</b>"...`
  );
  await env.REPORT_QUEUE.send({ chatId, topic, project });
}

// ── Guided merge flow ─────────────────────────────────────────────────────────

async function handleMergeStep(env, chatId, text, pending) {
  const userInput = text.trim().toLowerCase();

  if (pending.step === 1) {
    const companies = await getAllCompanies(env);
    if (!companies.includes(userInput)) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `⚠️ Company "<b>${escHtml(userInput)}</b>" not found.\n\n` +
        `Reply with an exact name, or run <code>/bot companies</code> to see all options.`);
      await setBotPending(env.DAYA_KV, chatId, { type: "merge", step: 1 });
      return;
    }
    await setBotPending(env.DAYA_KV, chatId, { type: "merge", step: 2, from: userInput });
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Merge companies — Step 2 of 2</b>\n\n` +
      `Merge <code>${escHtml(userInput)}</code> INTO which canonical company?\n\n` +
      `Reply with the canonical company name (the one to keep).`);
    return;
  }

  if (pending.step === 2) {
    const { from } = pending;
    const into = userInput;
    if (from === into) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "⚠️ FROM and INTO cannot be the same. Reply with a different canonical name.");
      await setBotPending(env.DAYA_KV, chatId, { type: "merge", step: 2, from });
      return;
    }
    await deleteBotPending(env.DAYA_KV, chatId);
    await setDraft(env.DAYA_KV, chatId, { type: "merge_confirm", from, into });
    await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `<b>Confirm merge:</b>\n\n` +
      `Move all facts from <code>${escHtml(from)}</code> → <code>${escHtml(into)}</code>\n` +
      `An alias will also be added so future emails auto-route correctly.\n\n` +
      `⚠️ This cannot be undone (except by merging back).`,
      [[
        { text: "✅ Yes, merge", callback_data: "merge_confirm" },
        { text: "❌ Cancel", callback_data: "merge_cancel" },
      ]]
    );
  }
}

// ── Inline button handler ─────────────────────────────────────────────────────

async function handleCallbackQuery(env, ctx, chatId, data, callbackQueryId) {
  try {
    await answerCallback(env.TELEGRAM_MEMORY_BOT_TOKEN, callbackQueryId);

    if (data === "export_summary") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft?.json) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Session expired — summaries are saved for 2 hours. Run <code>/bot summary</code> to generate a fresh one.");
        return;
      }
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Generating Word document...");
      const docx = buildSummaryDocx(draft.project.label, draft.json);
      const filename = `${safeName(draft.project.label)}_Briefing_${today()}.docx`;
      // Upload to OneDrive for archival, skipping if this exact summary was already uploaded
      const hash = summaryHash(draft.json);
      const dedupKey = `summary:onedrive:${safeName(draft.project.label)}:${hash}`;
      if (!(await env.DAYA_KV.get(dedupKey))) {
        const { id: itemId } = await uploadReport(env, filename, docx);
        await env.DAYA_KV.put(dedupKey, itemId, { expirationTtl: 90000 });
      }
      await sendDocument(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, docx,
        filename, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
      return;
    }

    if (data === "export_summary_pdf") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft?.json) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Session expired — summaries are saved for 2 hours. Run <code>/bot summary</code> to generate a fresh one.");
        return;
      }
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Generating PDF...");
      const docx = buildSummaryDocx(draft.project.label, draft.json);
      const docxFilename = `${safeName(draft.project.label)}_Briefing_${today()}.docx`;
      // Reuse existing OneDrive item if this summary was already uploaded; otherwise upload now
      const hash = summaryHash(draft.json);
      const dedupKey = `summary:onedrive:${safeName(draft.project.label)}:${hash}`;
      let itemId = await env.DAYA_KV.get(dedupKey);
      if (!itemId) {
        const { id } = await uploadReport(env, docxFilename, docx);
        itemId = id;
        await env.DAYA_KV.put(dedupKey, itemId, { expirationTtl: 90000 });
      }
      const pdfBytes = await downloadItemAsPdf(env, itemId);
      const pdfFilename = `${safeName(draft.project.label)}_Briefing_${today()}.pdf`;
      await sendDocument(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, pdfBytes, pdfFilename, "application/pdf");
      return;
    }

    if (data === "export_report") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft?.json) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Session expired — reports are saved for 2 hours. Run <code>/bot report</code> to generate a fresh one.");
        return;
      }
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Generating Word document...");
      let diagramPng = null;
      if (draft.diagram) {
        try {
          const encoded = btoa(unescape(encodeURIComponent(draft.diagram)));
          const imgRes = await fetch(`https://mermaid.ink/img/${encoded}`);
          if (imgRes.ok) diagramPng = await imgRes.arrayBuffer();
        } catch { /* non-fatal — export without diagram */ }
      }
      const docx = buildReportDocx(draft.topic, draft.project.label, draft.json, diagramPng);
      const topicSlug = safeName(draft.topic || "report").slice(0, 30);
      const filename = `${safeName(draft.project.label)}_Report_${topicSlug}_${today()}.docx`;
      await uploadReport(env, filename, docx);
      await sendDocument(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, docx,
        filename, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
      return;
    }

    if (data === "export_report_pdf") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft?.json) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Session expired — reports are saved for 2 hours. Run <code>/bot report</code> to generate a fresh one.");
        return;
      }
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Generating PDF...");
      let diagramPng = null;
      if (draft.diagram) {
        try {
          const encoded = btoa(unescape(encodeURIComponent(draft.diagram)));
          const imgRes = await fetch(`https://mermaid.ink/img/${encoded}`);
          if (imgRes.ok) diagramPng = await imgRes.arrayBuffer();
        } catch { /* non-fatal — export without diagram */ }
      }
      const docx = buildReportDocx(draft.topic, draft.project.label, draft.json, diagramPng);
      const topicSlug = safeName(draft.topic || "report").slice(0, 30);
      const docxFilename = `${safeName(draft.project.label)}_Report_${topicSlug}_${today()}.docx`;
      const { id: itemId } = await uploadReport(env, docxFilename, docx);
      const pdfBytes = await downloadItemAsPdf(env, itemId);
      const pdfFilename = `${safeName(draft.project.label)}_Report_${topicSlug}_${today()}.pdf`;
      await sendDocument(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, pdfBytes, pdfFilename, "application/pdf");
      return;
    }

    if (data === "refine_report") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft?.json) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ No report to refine — run <code>/bot report</code> again.");
        return;
      }
      await setRefinePending(env.DAYA_KV, chatId, {
        topic: draft.topic,
        project: draft.project,
        json: draft.json,
        iteration: draft.iteration || 1,
      });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "✏️ <b>What should be added or changed?</b>\n\n" +
        "Describe the changes, e.g. \"emphasise the cost impact\" or \"add more detail about the delay timeline\"."
      );
      return;
    }

    if (data === "cancel_report") {
      await deleteDraft(env.DAYA_KV, chatId);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "🗑️ Report discarded. Run <code>/bot report</code> to start a new one.");
      return;
    }

    if (data === "merge_confirm") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft || draft.type !== "merge_confirm") {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Merge session expired. Run <code>/bot merge</code> again.");
        return;
      }
      const { from, into } = draft;
      await deleteDraft(env.DAYA_KV, chatId);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `⏳ Merging <code>${escHtml(from)}</code> → <code>${escHtml(into)}</code>...`);
      try {
        const result = await mergeCompany(env, from, into);
        await setAlias(from, into, env);
        const movedMsg = result.moved === 0
          ? `No facts were found under "<b>${escHtml(from)}</b>" (already empty).`
          : `${result.moved} facts moved to "<b>${escHtml(into)}</b>".`;
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          `✅ <b>Merge complete.</b>\n\n${movedMsg}\n` +
          `Alias added: future emails matching "<b>${escHtml(from)}</b>" will route to "<b>${escHtml(into)}</b>".`);
      } catch (mergeErr) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          `❌ Merge failed: ${escHtml(mergeErr.message)}`);
      }
      return;
    }

    if (data === "merge_cancel") {
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (draft?.type === "merge_confirm") await deleteDraft(env.DAYA_KV, chatId);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "❌ Merge cancelled.");
      return;
    }

    if (data === "mode:summary") {
      const project = await getGroupProject(env.DAYA_KV, chatId);
      if (!project) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ This group isn't linked to a project yet. Use <code>/link Project Name</code> first.");
        return;
      }
      if (await isRateLimited(env.DAYA_KV, chatId)) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⏸ Slow down — this group has sent 20 AI requests in the past hour. Try again soon.");
        return;
      }
      await incrementRateLimit(env.DAYA_KV, chatId);
      sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
      const { text: summaryText, json } = await handleSummary(env, chatId, project);
      await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, summaryText);
      if (json) {
        await setDraft(env.DAYA_KV, chatId, { type: "summary", project, json });
        await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "Export this briefing?",
          [[
            { text: "📄 Export as Word", callback_data: "export_summary" },
            { text: "📑 Export as PDF",  callback_data: "export_summary_pdf" },
          ]]);
      }
      // Summary is one-shot — show the menu again for the next action
      await sendQueryMenu(env, chatId, project.label);
      return;
    }

    if (data === "mode:qa") {
      const project = await getGroupProject(env.DAYA_KV, chatId);
      if (!project) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ This group isn't linked to a project yet. Use <code>/link Project Name</code> first.");
        return;
      }
      await setActiveMode(env.DAYA_KV, chatId, { mode: "qa", project });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "❓ <b>Q&A mode active.</b>\n\nAsk anything about this project — just type freely.\n<i>Type <code>/bot</code> to switch modes.</i>");
      return;
    }

    if (data === "mode:timeline") {
      const project = await getGroupProject(env.DAYA_KV, chatId);
      if (!project) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ This group isn't linked to a project yet. Use <code>/link Project Name</code> first.");
        return;
      }
      await setActiveMode(env.DAYA_KV, chatId, { mode: "timeline", project });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📅 <b>Timeline mode active.</b>\n\nWhat item should I trace? e.g.:\n• <i>timber door</i>\n• <i>marble flooring</i>\n<i>Type <code>/bot</code> to switch modes.</i>");
      return;
    }

    if (data === "mode:report") {
      const project = await getGroupProject(env.DAYA_KV, chatId);
      if (!project) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ This group isn't linked to a project yet. Use <code>/link Project Name</code> first.");
        return;
      }
      await setActiveMode(env.DAYA_KV, chatId, { mode: "report", project });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📊 <b>Report mode active.</b>\n\nWhat should the report cover? e.g.:\n• <i>delay in CEO office glass door</i>\n• <i>marble flooring decision</i>\n<i>Type <code>/bot</code> to switch modes.</i>");
      return;
    }

    // ── Admin menu ────────────────────────────────────────────────────────────

    if (data === "admin:open") {
      await sendAdminMenu(env, chatId);
      return;
    }

    if (data === "admin:projects") {
      const projects = await getActiveProjects(env);
      let listText;
      if (projects.length === 0) {
        listText = "📂 No active projects yet.";
      } else {
        const lines = [];
        for (const p of projects) {
          const facts = await env.DAYA_KV.get(`mem:facts:${p}`, "json") || [];
          lines.push(`• <code>${escHtml(p)}</code> — ${facts.length} facts`);
        }
        listText = `<b>Active projects (${projects.length}):</b>\n` + lines.join("\n");
      }
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, listText, [
        [
          { text: "🗄 Archive",   callback_data: "proj:show_archive"   },
          { text: "📂 Unarchive", callback_data: "proj:show_unarchive" },
        ],
        [{ text: "➕ Add project", callback_data: "proj:add" }],
      ]);
      return;
    }

    if (data === "proj:show_archive") {
      const projects = await getActiveProjects(env);
      if (projects.length === 0) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "📂 No active projects to archive.");
        return;
      }
      const buttons = projects.sort().map(p => ([{
        text: p.length > 40 ? p.slice(0, 38) + "…" : p,
        callback_data: `proj:archive:${p}`.slice(0, 63),
      }]));
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "🗄 <b>Select a project to archive:</b>", buttons);
      return;
    }

    if (data === "proj:show_unarchive") {
      const [all, active] = await Promise.all([getAllCompanies(env), getActiveProjects(env)]);
      const archived = all.filter(c => !active.includes(c)).sort();
      if (archived.length === 0) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "📂 No archived projects found.");
        return;
      }
      const buttons = archived.map(p => ([{
        text: p.length > 40 ? p.slice(0, 38) + "…" : p,
        callback_data: `proj:unarchive:${p}`.slice(0, 63),
      }]));
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📂 <b>Select a project to unarchive:</b>", buttons);
      return;
    }

    if (data === "admin:merge") {
      const companies = await getAllCompanies(env);
      if (companies.length === 0) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "📂 No companies in the database yet.");
        return;
      }
      if (companies.length > 20) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          `📋 ${companies.length} companies — too many for buttons.\n\nUse <code>/bot merge</code> to type company names directly.`);
        return;
      }
      const sorted = companies.sort();
      const grid = [];
      for (let i = 0; i < sorted.length; i += 2) {
        const row = [{ text: sorted[i], callback_data: `merge:from:${sorted[i]}`.slice(0, 63) }];
        if (sorted[i + 1]) row.push({ text: sorted[i + 1], callback_data: `merge:from:${sorted[i + 1]}`.slice(0, 63) });
        grid.push(row);
      }
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "<b>Merge companies — Step 1 of 2</b>\n\nSelect the company to merge <b>FROM</b> (the duplicate to remove):",
        grid);
      return;
    }

    if (data === "admin:backfill") {
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "📥 <b>Backfill emails</b>\n\nSync up to 150 emails per inbox (2 inboxes). Already-processed emails are skipped.",
        [[
          { text: "✅ Start backfill", callback_data: "backfill:confirm" },
          { text: "❌ Cancel",         callback_data: "backfill:cancel"  },
        ]]);
      return;
    }

    if (data === "admin:aliases") {
      const aliases = await listAliases(env);
      let text;
      if (aliases.length === 0) {
        text = "📋 No custom aliases set yet.";
      } else {
        const lines = aliases.map(a => `• <code>${escHtml(a.source)}</code> → <code>${escHtml(a.target)}</code>`);
        text = `<b>Custom aliases (${aliases.length}):</b>\n` + lines.join("\n");
      }
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, text,
        [[{ text: "➕ Add alias", callback_data: "alias:add" }]]);
      return;
    }

    if (data === "admin:summaries") {
      const total = (await getAllCompanies(env)).length;
      const bar   = "░".repeat(16);
      const initRes = await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `🔄 <b>Regenerating summaries…</b>\n${bar} 0/${total} (0%)`);
      const msgId = initRes?.result?.message_id;
      const params = new URLSearchParams({ chatId });
      if (msgId) params.set("msgId", String(msgId));
      ctx.waitUntil(
        fetch(`${env.MEMORY_WORKER_URL}/run-daily-summaries?${params}`, {
          headers: { Authorization: `Bearer ${env.INTERNAL_SECRET}` },
        }).catch(() => {})
      );
      return;
    }

    // ── Project actions ───────────────────────────────────────────────────────

    if (data.startsWith("proj:archive:")) {
      const name = data.slice("proj:archive:".length);
      const result = await archiveProject(env, name);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        result.removed
          ? `✅ <code>${escHtml(name)}</code> archived.\n\nFacts are preserved — new emails will no longer route here.`
          : `⚠️ <code>${escHtml(name)}</code> was not in the active project list.`);
      return;
    }

    if (data.startsWith("proj:unarchive:")) {
      const name = data.slice("proj:unarchive:".length);
      const result = await addActiveProject(env, name);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        result.added
          ? `✅ <code>${escHtml(name)}</code> restored to active projects.\n\nNew emails will now route here.`
          : `ℹ️ <code>${escHtml(name)}</code> is already in the active project list.`);
      return;
    }

    if (data === "proj:add") {
      await setBotPending(env.DAYA_KV, chatId, { type: "proj_add" });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "➕ <b>Add project</b>\n\nReply with the project name to add to the active list.");
      return;
    }

    // ── Merge button flow ─────────────────────────────────────────────────────

    if (data.startsWith("merge:from:")) {
      const from = data.slice("merge:from:".length);
      const companies = await getAllCompanies(env);
      const intoList = companies.sort().filter(c => c !== from);
      if (intoList.length === 0) {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ No other companies to merge into.");
        return;
      }
      await setDraft(env.DAYA_KV, chatId, { type: "merge_from_selected", from });
      const grid = [];
      for (let i = 0; i < intoList.length; i += 2) {
        const row = [{ text: intoList[i], callback_data: `merge:into:${intoList[i]}`.slice(0, 63) }];
        if (intoList[i + 1]) row.push({ text: intoList[i + 1], callback_data: `merge:into:${intoList[i + 1]}`.slice(0, 63) });
        grid.push(row);
      }
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `<b>Merge companies — Step 2 of 2</b>\n\nMerge <code>${escHtml(from)}</code> INTO which company?`,
        grid);
      return;
    }

    if (data.startsWith("merge:into:")) {
      const into = data.slice("merge:into:".length);
      const draft = await getDraft(env.DAYA_KV, chatId);
      if (!draft || draft.type !== "merge_from_selected") {
        await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
          "⚠️ Merge session expired. Tap <b>🔀 Merge</b> to start again.");
        return;
      }
      const { from } = draft;
      await deleteDraft(env.DAYA_KV, chatId);
      await setDraft(env.DAYA_KV, chatId, { type: "merge_confirm", from, into });
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `<b>Confirm merge:</b>\n\n` +
        `Move all facts from <code>${escHtml(from)}</code> → <code>${escHtml(into)}</code>\n` +
        `An alias will also be added so future emails auto-route correctly.\n\n` +
        `⚠️ This cannot be undone (except by merging back).`,
        [[
          { text: "✅ Yes, merge", callback_data: "merge_confirm" },
          { text: "❌ Cancel",     callback_data: "merge_cancel"  },
        ]]);
      return;
    }

    // ── Backfill confirm/cancel ───────────────────────────────────────────────

    if (data === "backfill:confirm") {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "⏳ Backfill started — processing up to 150 emails per inbox across 2 inboxes.\n\nAlready-processed emails are skipped. Run again to continue if needed.");
      ctx.waitUntil(
        fetch(`${env.MEMORY_WORKER_URL}/backfill?chatId=${encodeURIComponent(chatId)}`).catch(() => {})
      );
      return;
    }

    if (data === "backfill:cancel") {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "❌ Backfill cancelled.");
      return;
    }

    // ── Alias add ─────────────────────────────────────────────────────────────

    if (data === "alias:add") {
      await setBotPending(env.DAYA_KV, chatId, { type: "alias_add" });
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "🔗 <b>Add alias</b>\n\nReply in the format:\n<code>source name → canonical name</code>\n\n" +
        "Example: <code>malomatia 14th floor → malomatia 19th floor</code>");
      return;
    }

  } catch (err) {
    console.error(`handleCallbackQuery error (chatId ${chatId}, data ${data}): ${err.message}`);
    try {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "⚠️ Something went wrong. Please try again.");
    } catch {}
  }
}

// ── Refine feedback received (follow-up after Refine button) ──────────────────

async function handleRefineText(env, chatId, feedback, refinePending) {
  await deleteRefinePending(env.DAYA_KV, chatId);

  const { topic, project, json, iteration } = refinePending;

  const effectiveFeedback = refinePending.awaitingClarification
    ? `${refinePending.originalFeedback}\n\nAdditional context provided by user:\n${feedback}`
    : feedback;

  const isVisual = /\b(visual|diagram|chart|graph|gantt|visuali[sz]|demo|illustrat|timeline\s+diagram)\b/i.test(effectiveFeedback);

  let diagramMermaid = null;
  if (isVisual) {
    // Typing indicator covers diagram generation — the queue consumer handles report typing
    sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
    const typingInterval = setInterval(() => {
      sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
    }, 4000);
    try {
      const { imageBytes, mermaidCode } = await generateDiagramForFeedback(env, topic, project, json, effectiveFeedback);
      await sendPhoto(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, imageBytes, "📊 <b>Visual Diagram</b>");
      diagramMermaid = mermaidCode;
    } catch (diagErr) {
      console.error(`Diagram generation error: ${diagErr.message}`);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⚠️ Could not generate diagram — continuing with text refinement.");
    } finally {
      clearInterval(typingInterval);
    }
  }

  // Enqueue text refinement — queue consumer runs without wall-clock limit
  try {
    await env.REPORT_QUEUE.send({
      type: "refine", chatId, topic, project, json, feedback: effectiveFeedback, iteration, diagramMermaid,
    });
  } catch (queueErr) {
    console.error(`Failed to enqueue refine [${chatId}]: ${queueErr.message}`);
    await setDraft(env.DAYA_KV, chatId, { type: "report", topic, project, json, iteration });
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ Refine failed — tap <b>✏️ Refine</b> again to retry.");
  }
}

// ── Active mode dispatcher ────────────────────────────────────────────────────

async function handleActiveMode(env, ctx, chatId, text, modeState) {
  try {
    sendChatAction(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId).catch(() => {});
    if (modeState.mode === "qa")           await handleModeQA(env, chatId, text, modeState);
    else if (modeState.mode === "timeline") await handleModeTimeline(env, chatId, text, modeState);
    else if (modeState.mode === "report")   await handleModeReport(env, ctx, chatId, text, modeState);
  } catch (err) {
    console.error(`handleActiveMode error (chatId ${chatId}): ${err.message}`);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ Something went wrong. Type <code>/bot</code> to reset.");
  }
}

async function handleModeQA(env, chatId, text, modeState) {
  if (await isRateLimited(env.DAYA_KV, chatId)) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⏸ Slow down — this group has sent 20 AI requests in the past hour. Try again soon.");
    return;
  }
  await incrementRateLimit(env.DAYA_KV, chatId);
  await setActiveMode(env.DAYA_KV, chatId, modeState);  // refresh TTL
  const answer = await handleBotQuery(env, chatId, text, modeState.project, false);
  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, escHtml(answer));
}

async function handleModeTimeline(env, chatId, text, modeState) {
  if (await isRateLimited(env.DAYA_KV, chatId)) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⏸ Slow down — this group has sent 20 AI requests in the past hour. Try again soon.");
    return;
  }
  await incrementRateLimit(env.DAYA_KV, chatId);

  if (!modeState.topic) {
    // First message — trace this item
    const updatedMode = { ...modeState, topic: text };
    await setActiveMode(env.DAYA_KV, chatId, updatedMode);
    const { text: result, clarify } = await handleTimeline(env, modeState.project, text);
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, result);
    if (clarify) {
      // Claude asked for clarification — revert so next message retries as a new item
      await setActiveMode(env.DAYA_KV, chatId, modeState);
    }
  } else {
    // Follow-up — Q&A in the context of the tracked item
    await setActiveMode(env.DAYA_KV, chatId, modeState);  // refresh TTL
    const answer = await handleBotQuery(env, chatId,
      `Re: ${modeState.topic} — ${text}`,
      modeState.project, false);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, escHtml(answer));
  }
}

async function handleModeReport(env, ctx, chatId, text, modeState) {
  console.log(`handleModeReport [${chatId}] topic="${text}" project="${modeState.project?.company}"`);
  if (await isRateLimited(env.DAYA_KV, chatId)) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⏸ Slow down — this group has sent 20 AI requests in the past hour. Try again soon.");
    return;
  }
  await incrementRateLimit(env.DAYA_KV, chatId);

  if (!modeState.topic) {
    // First message — enqueue report job. Queue consumer runs with no wall-clock limit.
    const updatedMode = { ...modeState, topic: text };
    await setActiveMode(env.DAYA_KV, chatId, updatedMode);
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      `⏳ Generating report on "<b>${escHtml(text)}</b>"...`);
    await env.REPORT_QUEUE.send({ chatId, topic: text, project: modeState.project });
  } else {
    // Subsequent message — refine the current report using the draft as source of truth
    const draft = await getDraft(env.DAYA_KV, chatId);
    if (!draft?.json) {
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "⚠️ Report session expired. What topic should the new report cover?");
      await setActiveMode(env.DAYA_KV, chatId, { mode: "report", project: modeState.project });
      return;
    }
    await setActiveMode(env.DAYA_KV, chatId, modeState);  // refresh TTL
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Refining report...");
    await env.REPORT_QUEUE.send({
      type: "refine",
      chatId,
      topic: draft.topic,
      project: draft.project,
      json: draft.json,
      feedback: text,
      iteration: draft.iteration || 1,
      diagramMermaid: null,
    });
  }
}

// ── Query menu — 4 inline buttons for the main query commands ────────────────

async function sendQueryMenu(env, chatId, projectLabel) {
  const header = projectLabel
    ? `🏗 <b>Daya Assistant — ${escHtml(projectLabel)}</b>\n\nTap to get started:`
    : `🏗 <b>Daya Assistant</b>\n\nTap to get started:`;
  const res = await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, header, [
    [
      { text: "❓ Q&A",      callback_data: "mode:qa" },
      { text: "📋 Summary",  callback_data: "mode:summary" },
    ],
    [
      { text: "📅 Timeline", callback_data: "mode:timeline" },
      { text: "📊 Report",   callback_data: "mode:report" },
    ],
    [
      { text: "⚙️ Admin",    callback_data: "admin:open" },
    ],
  ]);
  return res?.result?.message_id;
}

async function sendAdminMenu(env, chatId) {
  await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
    "⚙️ <b>Admin</b>\n\nSelect an action:",
    [
      [
        { text: "📁 Projects", callback_data: "admin:projects" },
        { text: "🔀 Merge",    callback_data: "admin:merge"    },
      ],
      [
        { text: "📥 Backfill", callback_data: "admin:backfill" },
        { text: "🔗 Aliases",  callback_data: "admin:aliases"  },
      ],
      [
        { text: "🔄 Regenerate summaries", callback_data: "admin:summaries" },
      ],
    ]
  );
}

// ── /link handler — admin types "/link Project Name" to link group to project ──

async function handleLink(env, chatId, text) {
  // Support both /link and /start (in case Telegram sends /start when bot is added)
  const label = text.replace(/^\/(link|start)(?:@\S+)?/, "").trim();

  if (!label) {
    const companies = await getAllCompanies(env);
    const companyList = companies.length > 0
      ? companies.sort().map(c => `• <code>${escHtml(c)}</code>`).join("\n")
      : "  <i>No projects found yet — run a backfill first.</i>";

    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "👋 I'm <b>Daya Assistant</b>.\n\n" +
      "To link this group to a project, type:\n" +
      "<code>/link Project Name</code>\n\n" +
      `<b>Available projects (${companies.length}):</b>\n${companyList}\n\n` +
      "Example: <code>/link Malomatia 19th Floor</code>"
    );
    return;
  }

  // Validate company name before any processing
  if (!isValidCompanyName(label)) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ Invalid project name — only letters, numbers, spaces, and basic punctuation are allowed.");
    return;
  }

  // Normalize to canonical company key (applies alias map — e.g. "14th floor" → "malomatia 19th floor").
  // Only exact alias matches cause a key change; partial word matches are intentionally NOT normalised
  // so that distinct names like "Malomatia office" are preserved as-is.
  const rawLower = label.toLowerCase().trim();
  const company = normalizeCompany(rawLower);
  // Replace the display label only when an exact alias was matched (company key changed).
  const displayLabel = company !== rawLower
    ? company.replace(/\b\w/g, c => c.toUpperCase())
    : label;

  await linkGroup(env, chatId, company, displayLabel);

  // Show which company keys in the database matched — helps admin confirm it's correct.
  // getKVFacts() uses exact-first lookup: if the linked key exists verbatim, only that
  // project's facts will be served (no fuzzy cross-project merging).
  const matches = await matchingCompanies(env, company);
  const hasExactMatch = matches.includes(company);

  let matchInfo;
  if (matches.length === 0) {
    matchInfo =
      `⚠️ No emails found yet for <b>${escHtml(label)}</b>.\n` +
      `Run <code>/bot backfill</code> to sync emails first, or check the spelling matches what's in the emails.`;
  } else if (hasExactMatch) {
    matchInfo =
      `📂 Exact project match found: <code>${escHtml(company)}</code>\n` +
      `Only this project's facts will be served — no cross-project data leakage.`;
  } else {
    const matchList = matches.map(m => `• <code>${escHtml(m)}</code>`).join("\n");
    matchInfo =
      `📂 No exact project key found — facts will be drawn from fuzzy-matched projects:\n${matchList}\n` +
      `To isolate data, run a backfill first so an exact key is created for <code>${escHtml(company)}</code>.`;
  }

  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
    `✅ <b>Group linked to: ${escHtml(displayLabel)}</b>\n\n` +
    `${matchInfo}`
  );

  // Send the query menu and pin it so it stays accessible at the top of the group
  const menuMsgId = await sendQueryMenu(env, chatId, displayLabel);
  if (menuMsgId) {
    await pinMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, menuMsgId);
  }
}

// ── Clarification follow-up (existing Q&A thread) ─────────────────────────────

async function handleClarification(env, chatId, text, pending) {
  await deleteBotPending(env.DAYA_KV, chatId);
  const answer = await handleBotQuery(env, chatId, text, pending.project, true);
  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, escHtml(answer));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function safeName(str) {
  return String(str || "").replace(/[^a-z0-9]/gi, "_").replace(/_+/g, "_").replace(/^_|_$/g, "");
}

function today() {
  // Include HH-MM so repeated exports never try to overwrite an open OneDrive file (423 resourceLocked)
  return new Date().toISOString().slice(0, 16).replace("T", "_").replace(":", "-");
}

// djb2 hash of the summary JSON — used to detect duplicate OneDrive uploads for the same summary version
function summaryHash(json) {
  const str = JSON.stringify(json);
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h << 5) + h) ^ str.charCodeAt(i);
  return (h >>> 0).toString(36);
}
