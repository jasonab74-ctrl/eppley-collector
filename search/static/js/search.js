async function loadCSV(path) {
  const res = await fetch(path);
  const text = await res.text();
  const lines = text.split("\n").slice(1);
  const data = [];
  for (let line of lines) {
    if (!line.trim()) continue;
    const parts = line.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
    const [source, file, title, url, year, journal, content] = parts;
    data.push({
      source: source?.replaceAll('"', ''),
      title: title?.replaceAll('"', ''),
      url: url?.replaceAll('"', ''),
      year: year?.replaceAll('"', ''),
      journal: journal?.replaceAll('"', ''),
      text: content?.replaceAll('"', '')
    });
  }
  return data;
}

async function initSearch() {
  const dataset = await loadCSV("../output/eppley_master.csv");
  const options = {
    includeScore: true,
    threshold: 0.4,
    keys: ["title", "text", "journal", "source"]
  };
  const fuse = new Fuse(dataset, options);

  const input = document.getElementById("searchBox");
  const resultsDiv = document.getElementById("results");

  input.addEventListener("input", () => {
    const q = input.value.trim();
    resultsDiv.innerHTML = "";
    if (!q) return;
    const results = fuse.search(q).slice(0, 40);
    results.forEach(r => {
      const d = r.item;
      const el = document.createElement("article");
      el.innerHTML = `
        <h3><a href="${d.url}" target="_blank">${d.title || "(no title)"}</a></h3>
        <p class="meta">${d.source || ""} ${d.year || ""}</p>
        <p>${(d.text || "").substring(0, 240)}...</p>
      `;
      resultsDiv.appendChild(el);
    });
  });
}

window.addEventListener("DOMContentLoaded", initSearch);