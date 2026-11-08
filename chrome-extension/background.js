// Click'n'Load: Seiten sprechen mit http://127.0.0.1:9666. Wir leiten diese Aufrufe auf den MediaButler-Server um.
const RULE_ID = 1;

async function cfg() {
  const c = await chrome.storage.sync.get({ server: "", apiKey: "", user: "", pass: "" });
  c.server = (c.server || "").replace(/\/+$/, "");
  return c;
}

function authHeaders(c) {
  const h = [];
  if (c.apiKey) h.push({ header: "X-API-Key", operation: "set", value: c.apiKey });
  else if (c.user) h.push({ header: "Authorization", operation: "set", value: "Basic " + btoa(unescape(encodeURIComponent(c.user + ":" + c.pass))) });
  return h;
}

async function applyRules() {
  const c = await cfg();
  const remove = [RULE_ID, RULE_ID + 1];
  if (!c.server) return chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds: remove });
  const headers = authHeaders(c);
  const mk = (id, regex, sub) => ({
    id, priority: 1,
    action: { type: "redirect", redirect: { regexSubstitution: c.server + sub },
      ...(headers.length ? {} : {}) },
    condition: { regexFilter: regex, resourceTypes: ["xmlhttprequest", "other", "script", "main_frame", "sub_frame", "image"] }
  });
  // 1) Click'n'Load-Endpunkte (flash/…) und jdcheck.js
  const r1 = mk(RULE_ID, "^https?://(127\\.0\\.0\\.1|localhost):9666/flash(/.*)?$", "/api/cnl\\2");
  const r2 = mk(RULE_ID + 1, "^https?://(127\\.0\\.0\\.1|localhost):9666/jdcheck\\.js.*$", "/api/cnl/jdcheck.js");
  await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds: remove, addRules: [r1, r2] });
  // Authentifizierung: Redirect und Header-Änderung sind in einer Regel nicht kombinierbar, daher zweite Regel für den Zielserver
  if (headers.length) {
    const host = new URL(c.server).host;
    await chrome.declarativeNetRequest.updateDynamicRules({
      removeRuleIds: [RULE_ID + 2],
      addRules: [{ id: RULE_ID + 2, priority: 1,
        action: { type: "modifyHeaders", requestHeaders: headers },
        condition: { urlFilter: "||" + host + "/api/cnl", resourceTypes: ["xmlhttprequest", "other", "script", "main_frame", "sub_frame", "image"] } }]
    });
  } else {
    await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds: [RULE_ID + 2] });
  }
}

async function sendLinks(links, pkg) {
  const c = await cfg();
  if (!c.server) { notify("MediaButler", "Bitte zuerst im Plugin die Server-Adresse eintragen."); return; }
  const headers = { "Content-Type": "application/json", ...Object.fromEntries(authHeaders(c).map(h => [h.header, h.value])) };
  try {
    const r = await fetch(c.server + "/api/dl/add", { method: "POST", headers, body: JSON.stringify({ links, package: pkg || "" }) });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const j = await r.json();
    notify("MediaButler", j.added ? j.added + " Link(s) hinzugefügt" : "Keine neuen Links");
  } catch (e) { notify("MediaButler: Fehler", String(e.message || e)); }
}

function notify(title, message) {
  chrome.notifications.create({ type: "basic", iconUrl: "icon.png", title, message });
}

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({ id: "mb-link", title: "An MediaButler senden", contexts: ["link"] });
  chrome.contextMenus.create({ id: "mb-sel", title: "Markierte Links an MediaButler senden", contexts: ["selection"] });
  applyRules();
});
chrome.runtime.onStartup.addListener(applyRules);
chrome.storage.onChanged.addListener(applyRules);

chrome.contextMenus.onClicked.addListener((info) => {
  if (info.menuItemId === "mb-link" && info.linkUrl) sendLinks([info.linkUrl]);
  if (info.menuItemId === "mb-sel" && info.selectionText) sendLinks(info.selectionText.match(/https?:\/\/[^\s"'<>]+/g) || []);
});

chrome.runtime.onMessage.addListener((msg, _s, reply) => {
  if (msg.type === "send") sendLinks(msg.links, msg.pkg).then(() => reply({ ok: true }));
  if (msg.type === "reload") applyRules().then(() => reply({ ok: true }));
  return true;
});


// ddownload-Cookies (Login hat ein Captcha) aus dem Browser an MediaButler schicken
const CK_HOSTS = ["ddownload.com"];
let ckTimer = null;

async function sendCookies(notifyOk) {
  const c = await cfg();
  if (!c.server) { if (notifyOk) notify("MediaButler", "Bitte zuerst die Server-Adresse eintragen."); return; }
  const headers = { "Content-Type": "application/json", ...Object.fromEntries(authHeaders(c).map(h => [h.header, h.value])) };
  for (const host of CK_HOSTS) {
    const list = await chrome.cookies.getAll({ domain: host });
    if (!list.some(k => k.name === "xfss" || k.name === "login")) { if (notifyOk) notify("MediaButler", "Bei " + host + " nicht angemeldet."); continue; }
    const hdr = list.map(k => k.name + "=" + k.value).join("; ");
    try {
      const r = await fetch(c.server + "/api/dl/cookies", { method: "POST", headers, body: JSON.stringify({ host, cookies: hdr }) });
      if (!r.ok) throw new Error("HTTP " + r.status);
      if (notifyOk) notify("MediaButler", "Cookies von " + host + " gesendet.");
    } catch (e) { if (notifyOk) notify("MediaButler: Fehler", String(e.message || e)); }
  }
}

chrome.cookies.onChanged.addListener(ch => {
  if (!CK_HOSTS.some(h => (ch.cookie.domain || "").replace(/^\./, "").endsWith(h))) return;
  clearTimeout(ckTimer);
  ckTimer = setTimeout(() => sendCookies(false), 5000);
});

chrome.runtime.onMessage.addListener(msg => { if (msg && msg.type === "sendCookies") sendCookies(true); });
