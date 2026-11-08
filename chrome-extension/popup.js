const $ = (i) => document.getElementById(i);
chrome.storage.sync.get({ server: "", apiKey: "", user: "", pass: "" }).then((c) => { for (const k in c) $(k).value = c[k]; });
$("save").onclick = async () => {
  await chrome.storage.sync.set({ server: $("server").value.trim(), apiKey: $("apiKey").value.trim(), user: $("user").value.trim(), pass: $("pass").value });
  $("m").textContent = "Gespeichert. Click'n'Load wird jetzt umgeleitet.";
};
$("send").onclick = () => {
  const links = ($("links").value.match(/https?:\/\/[^\s"'<>]+/g) || []);
  chrome.runtime.sendMessage({ type: "send", links }, () => { $("links").value = ""; $("m").textContent = "Gesendet."; });
};
