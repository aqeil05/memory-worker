// ─── Telegram Group Bot Handler ───────────────────────────────────────────────
// Handles updates from the memory bot (@DayaProjectBot) in group/supergroup chats.
//
// Commands:
//   /link {project name} — link this group to a project (admin runs once)
//   /bot {question}      — Q&A against project memory
//   /bot summary         — generate project briefing → [📥 Export as Word]
//   /bot report          — prompt for topic → generate report → [📥 Export] [✏️ Refine]
//
// Follow-ups (no /bot prefix needed):
//   - After clarification question from bot → continues Q&A thread
//   - After /bot report → captures topic
//   - After Refine button → captures feedback text

import { handleBotQuery, handleSummary, handleReport, regenerateReport } from "./telegram-bot-query.js";
import { linkGroup, getGroupProject, uploadReport, matchingCompanies, getAllCompanies, mergeCompany, getActiveProjects, addActiveProject, archiveProject } from "./onedrive.js";
import { normalizeCompany, setAlias, listAliases } from "./memory.js";
import { buildSummaryDocx, buildReportDocx } from "./docx.js";
import {
  getBotPending, setBotPending, deleteBotPending,
  getDraft, setDraft, deleteDraft,
  getRefinePending, setRefinePending, deleteRefinePending,
} from "./dedup.js";
import { sendMessage, sendLongMessage, sendWithButtons, answerCallback, escHtml, sendChatAction } from "./notify.js";

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
        handleCallbackQuery(env, chatId, data, cq.id)
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
    if (text.startsWith("/link")) {
      await handleLink(env, chatId, text);
      return;
    }

    if (text.startsWith("/start")) {
      await handleLink(env, chatId, text);
      return;
    }

    if (text.startsWith("/bot") || text.startsWith("/bot@")) {
      await handleBotCommand(env, ctx, chatId, text);
      return;
    }

    // Refine-pending: waiting for feedback text after Refine button tap
    const refinePending = await getRefinePending(env.DAYA_KV, chatId);
    if (refinePending) {
      await handleRefineText(env, chatId, text, refinePending);
      return;
    }

    // Bot-pending: waiting for clarification reply or report topic
    const pending = await getBotPending(env.DAYA_KV, chatId);
    if (pending?.type === "clarification") {
      await handleClarification(env, chatId, text, pending);
      return;
    }
    if (pending?.type === "report") {
      await handleReportTopic(env, chatId, text, pending.project);
      return;
    }
    if (pending?.type === "merge") {
      await handleMergeStep(env, chatId, text, pending);
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
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "💬 Please include your question after <code>/bot</code>\n" +
      "Examples:\n" +
      "• <code>/bot when is the site visit?</code>\n" +
      "• <code>/bot summary</code>\n" +
      "• <code>/bot report</code>\n" +
      "• <code>/bot projects</code>\n" +
      "• <code>/bot addproject lux tower</code>\n" +
      "• <code>/bot archiveproject lux tower</code>\n" +
      "• <code>/bot companies</code>\n" +
      "• <code>/bot alias old name → canonical name</code>\n" +
      "• <code>/bot merge</code>");
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

  // ── Project-required commands ───────────────────────────────────────────────

  const project = await getGroupProject(env.DAYA_KV, chatId);
  if (!project) {
    await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "⚠️ This group isn't linked to a project yet. Use the setup link from the Closed Won flow.");
    return;
  }

  // /bot summary — full project briefing
  if (lowerQ === "summary") {
    const { text: summaryText, json } = await handleSummary(env, chatId, project);
    await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, summaryText);

    if (json) {
      await setDraft(env.DAYA_KV, chatId, { type: "summary", project, json });
      await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        "Export this briefing as a Word document?",
        [[{ text: "📥 Export as Word", callback_data: "export_summary" }]]
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

// ── Report topic received (follow-up after /bot report) ───────────────────────

async function handleReportTopic(env, chatId, topic, project) {
  await deleteBotPending(env.DAYA_KV, chatId);

  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
    `⏳ Generating report on "<b>${escHtml(topic)}</b>"...`
  );

  const { text: reportText, json } = await handleReport(env, chatId, topic, project);
  await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, reportText);

  if (json) {
    await setDraft(env.DAYA_KV, chatId, { type: "report", topic, project, json, iteration: 1 });
    await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "What would you like to do with this report?",
      [[
        { text: "📥 Export as Word", callback_data: "export_report" },
        { text: "✏️ Refine", callback_data: "refine_report" },
      ]]
    );
  }
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

async function handleCallbackQuery(env, chatId, data, callbackQueryId) {
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
      const webUrl = await uploadReport(env, filename, docx);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `📄 <b>Word document ready:</b>\n<a href="${webUrl}">${escHtml(filename)}</a>`
      );
      await deleteDraft(env.DAYA_KV, chatId);
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
      const docx = buildReportDocx(draft.topic, draft.project.label, draft.json);
      const topicSlug = safeName(draft.topic || "report").slice(0, 30);
      const filename = `${safeName(draft.project.label)}_Report_${topicSlug}_${today()}.docx`;
      const webUrl = await uploadReport(env, filename, docx);
      await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
        `📄 <b>Word document ready:</b>\n<a href="${webUrl}">${escHtml(filename)}</a>`
      );
      await deleteDraft(env.DAYA_KV, chatId);
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

  await sendMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, "⏳ Refining report...");

  const { text: reportText, json: newJson } = await regenerateReport(env, chatId, topic, project, json, feedback);
  await sendLongMessage(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId, reportText);

  if (newJson) {
    await setDraft(env.DAYA_KV, chatId, {
      type: "report", topic, project, json: newJson, iteration: iteration + 1,
    });
    await sendWithButtons(env.TELEGRAM_MEMORY_BOT_TOKEN, chatId,
      "What would you like to do with this report?",
      [[
        { text: "📥 Export as Word", callback_data: "export_report" },
        { text: "✏️ Refine", callback_data: "refine_report" },
      ]]
    );
  }
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
    `${matchInfo}\n\n` +
    `Try:\n` +
    `• <code>/bot summary</code> — project briefing\n` +
    `• <code>/bot report</code> — generate a formal issue report\n` +
    `• <code>/bot your question</code> — ask anything`
  );
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
  return new Date().toISOString().slice(0, 10);
}
