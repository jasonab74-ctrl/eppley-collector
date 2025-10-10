(function(){
  // Base-path computation so JSON fetches work on GitHub Pages project sites and custom domains.
  function computeBase(){
    const hostIsGh = location.hostname.endsWith('github.io');
    const parts = location.pathname.split('/').filter(Boolean);
    if (hostIsGh && parts.length>0) return '/' + parts[0]; // e.g., /sports-app-project
    return ''; // custom domain or user site
  }
  const BASE = computeBase();

  const $ = (sel,root=document)=>root.querySelector(sel);
  const $$ = (sel,root=document)=>Array.from(root.querySelectorAll(sel));
  const el = (tag, cls)=>{const n=document.createElement(tag); if(cls) n.className=cls; return n;};

  // Drawer
  const hamb = $('#hamburger');
  const drawer = $('#drawer');
  hamb.addEventListener('click',()=>drawer.classList.toggle('open'));
  drawer.addEventListener('click', (e)=>{ if(e.target.tagName==='A') drawer.classList.remove('open'); });

  async function fetchJSON(path){
    try {
      const res = await fetch(BASE + path, {cache:'no-store'});
      if(!res.ok) throw new Error(res.status+" "+res.statusText);
      return await res.json();
    } catch (e) {
      console.warn('Fetch failed:', path, e.message);
      return null;
    }
  }

  function renderFilters(list){
    const wrap = $('#filters');
    wrap.innerHTML='';
    (list||['all']).forEach((f,i)=>{
      const b = el('button','chip'+(i===0?' active':'')); b.textContent=f; b.dataset.filter=f; wrap.appendChild(b);
    });
  }
  function applyFilter(which){
    $$('.chip').forEach(c=>c.classList.toggle('active', c.dataset.filter===which));
    $$('#newsGrid .item').forEach(card=>{
      const show = (which==='all' || card.dataset.source===which);
      card.style.display = show? 'flex':'none';
    });
  }

  // Headlines
  function renderNews(items){
    const grid = $('#newsGrid'); const empty = $('#newsEmpty'); grid.innerHTML='';
    if(!items || !items.length){ empty.textContent='No headlines yet.'; return; }
    empty.remove();
    items.forEach(it=>{
      const card = el('article','item');
      card.dataset.source = it.source || 'all';
      const a = el('a'); a.href = it.url; a.target = '_blank'; a.rel='noopener'; a.textContent = it.title || 'Untitled';
      const meta = el('div','meta'); meta.textContent = `${it.source||'—'} • ${it.published||''}`.trim();
      card.append(a, meta);
      grid.appendChild(card);
    });
  }

  // Rankings
  function renderRankings(data){
    if(!data) return;
    if(data.ap_top25){ $('#apRank').textContent = data.ap_top25.rank ?? '—'; $('#apDate').textContent = data.ap_top25.poll_date ?? '—'; }
    if(data.kenpom){ $('#kpRank').textContent = data.kenpom.rank ?? '—'; $('#kpDate').textContent = data.kenpom.as_of ?? '—'; }
  }

  // Schedule
  function renderSchedule(rows){
    const list = $('#schedList'); const empty=$('#schedEmpty'); list.innerHTML='';
    if(!rows || !rows.length){ empty.style.display='block'; return; }
    empty.style.display='none';
    rows.forEach(g=>{
      const li = el('li','card');
      li.innerHTML = `<div class="k">${g.date}</div><div class="v">${g.opponent}</div><div class="s">${g.home?'Home':'Away'} • ${g.time||''}`;
      list.appendChild(li);
    });
  }

  // Insider links
  function renderInsiders(src){
    const list = $('#insiderList'); const empty=$('#insiderEmpty'); list.innerHTML='';
    const rows = (src && src.insiders) || [];
    if(!rows.length){ empty.style.display='block'; return; }
    empty.style.display='none';
    rows.forEach(x=>{
      const li = el('li','card');
      li.innerHTML = `<a href="${x.url}" target="_blank" rel="noopener">${x.name}</a>`;
      list.appendChild(li);
    });
  }

  // Roster + brand
  function renderRoster(team){
    const tb = $('#rosterTable tbody'); const empty=$('#rosterEmpty'); tb.innerHTML='';
    const r = (team && team.roster) || [];
    if(!r.length){ empty.style.display='block'; return; }
    empty.style.display='none';
    r.forEach(p=>{
      const tr = el('tr');
      tr.innerHTML = `<td>${p.num||''}</td><td>${p.name||''}</td><td>${p.pos||''}</td><td>${p.class||''}</td><td>${p.ht||''}</td>`;
      tb.appendChild(tr);
    });
    if(team){
      document.documentElement.style.setProperty('--brand', team.primary || '#CEB888');
      $('#teamName').textContent = team.team || 'Team Hub';
      $('#teamTag').textContent = (team.slug||'') + ' • mobile-first hub';
    }
  }

  // Videos (optional: items with type:"video")
  function renderVideos(items){
    const list = $('#videoList'); const empty=$('#videoEmpty'); list.innerHTML='';
    const vids = (items||[]).filter(x=>x.type==='video');
    if(!vids.length){ empty.style.display='block'; return; }
    empty.style.display='none';
    vids.forEach(v=>{
      const li = el('li','card');
      li.innerHTML = `<a href="${v.url}" target="_blank" rel="noopener">${v.title||'Video'}</a><div class="meta">${v.published||''}</div>`;
      list.appendChild(li);
    });
  }

  // Init
  (async function init(){
    // Load JSON (same-origin)
    const [sources, widgets, sched, team, items] = await Promise.all([
      fetchJSON('/static/sources.json'),
      fetchJSON('/static/widgets.json'),
      fetchJSON('/static/schedule.json'),
      fetchJSON('/static/team.json'),
      fetchJSON('/static/teams/purdue-mbb/items.json')
    ]);

    renderFilters((sources && sources.filters)||['all','official','insiders','national']);
    renderInsiders(sources);
    renderRankings(widgets);
    renderSchedule(sched);
    renderRoster(team);
    renderNews(items);
    renderVideos(items);

    // Filter clicks
    $('#filters').addEventListener('click', (e)=>{
      const b = e.target.closest('.chip'); if(!b) return; applyFilter(b.dataset.filter);
    });

    const stamp = (widgets && widgets.as_of) || (team && team.as_of) || new Date().toISOString().slice(0,10);
    $('#freshness').textContent = `Data updated: ${stamp}`;
  })();
})();
