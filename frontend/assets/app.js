/* ============================================================
   MOSAIC — frontend app
   ============================================================ */
const API = ""; // same origin
const $ = s => document.querySelector(s);
const esc = s => String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
async function api(path, opts){
  const r = await fetch(API + path, opts);
  const ct = r.headers.get('content-type')||'';
  const data = ct.includes('application/json') ? await r.json() : await r.text();
  if(!r.ok){
    const msg = (data && data.error) ? data.error : `${path} → ${r.status}`;
    throw new Error(msg);
  }
  return data;
}
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

const NAV = [
  {sec:"Platform Layers", items:[
    {id:"connect", num:"1", tt:"Connectivity", sub:"Data off the floor"},
    {id:"ingest", num:"2", tt:"Ingest & Store", sub:"Capture & historize"},
    {id:"contextualize", num:"3", tt:"Contextualize", sub:"The centrepiece", core:true},
    {id:"visualize", num:"4", tt:"Visualize", sub:"Dashboards on meaning"},
    {id:"ragkb", num:"5", tt:"Build RAG KB", sub:"CAPA · Index · OEM · Regulatory · SOP"},
    {id:"intelligence", num:"6", tt:"SCADA Intelligence Platform", sub:"SCADA multi-agent RAG", core:true},
    {id:"govern", num:"7", tt:"Govern & Harden", sub:"Zero-trust"},
    {id:"copilot", num:"8", tt:"SCADA Co-pilot Chatbot", sub:"AI chatbot"},
  ]},
];
const FLAT = NAV.flatMap(s=>s.items);

let PARAMS = [];  // loaded from API

const PIPE = {
  STEPS: [
    {id:'connect', n:1, label:'Connectivity'},
    {id:'ingest', n:2, label:'Ingest & Store'},
    {id:'contextualize', n:3, label:'Contextualize'},
    {id:'visualize', n:4, label:'Visualize'},
    {id:'ragkb', n:5, label:'Build RAG KB'},
    {id:'intelligence', n:6, label:'SCADA Intel'},
    {id:'govern', n:7, label:'Govern'},
    {id:'copilot', n:8, label:'Co-pilot'},
  ],
  reading: null,
  readings: null,
  event: null,
};
function pipeIdx(id){ return PIPE.STEPS.findIndex(s=>s.id===id); }
function pipeTrack(id){
  const i = pipeIdx(id);
  if(i<0) return '';
  return `<div class="pipe-track">${PIPE.STEPS.map((s,k)=>`
    <button type="button" class="pipe-dot ${k===i?'on':k<i?'done':''}" onclick="nav('${s.id}')">
      <b>${s.n}</b><span>${s.label}</span>
    </button>${k<PIPE.STEPS.length-1?'<i></i>':''}`).join('')}</div>`;
}
function nextPipe(id){
  const i = pipeIdx(id);
  if(i<0 || i>=PIPE.STEPS.length-1) return '';
  const n = PIPE.STEPS[i+1];
  return `<div class="callout teal pipe-next"><span class="cic">→</span>
    <div class="cx"><b>Next: Step ${n.n} · ${n.label}</b>
      <div class="note" style="margin:4px 0 8px">This reading stays with you through the stack.</div>
      <button type="button" class="btn primary sm" onclick="nav('${n.id}')">Continue to ${n.label}</button>
    </div></div>`;
}
function paramOf(rd){
  if(!rd) return null;
  return (PARAMS.find(p=>p.tag===rd.tag)||{}).id || null;
}
function takeConnectBatch(r){
  const batch = r.stream || r.readings || (r.reading ? [r.reading] : []);
  if(!batch.length) return;
  PIPE.readings = batch;
  PIPE.reading = batch[batch.length-1];
  PIPE.event = null;
}
function mergeConnectBatch(r){
  const batch = r.readings || (r.reading ? [r.reading] : []);
  if(!batch.length) return;
  PIPE.readings = (PIPE.readings || []).concat(batch);
  PIPE.reading = batch[batch.length-1];
  PIPE.event = null;
}

function buildNav(){
  let html='';
  NAV.forEach(s=>{
    html+=`<div class="nav-sec">${s.sec}</div>`;
    s.items.forEach(it=>{
      html+=`<div class="nav-item ${it.core?'core':''}" id="nav-${it.id}" onclick="nav('${it.id}')">
        <div class="ni-num">${it.num}</div>
        <div class="ni-tx"><div class="ni-tt">${it.tt}</div><div class="ni-sub">${it.sub}</div></div>
        ${it.core?'<div class="ni-core">CORE</div>':''}</div>`;
    });
  });
  $('#nav').innerHTML=html;
}
function nav(id){
  document.querySelectorAll('.nav-item').forEach(el=>el.classList.toggle('active', el.id==='nav-'+id));
  render(id); window.scrollTo(0,0);
}
function stepbar(id){
  const pi = pipeIdx(id);
  if(pi>=0){
    const prev = pi>0
      ? `<button class="btn ghost sm" onclick="nav('${PIPE.STEPS[pi-1].id}')">← Step ${PIPE.STEPS[pi-1].n} ${esc(PIPE.STEPS[pi-1].label)}</button>`
      : `<button class="btn ghost sm" onclick="nav('home')">← Home</button>`;
    const nxt = pi<PIPE.STEPS.length-1
      ? `<button class="btn primary sm" onclick="nav('${PIPE.STEPS[pi+1].id}')">Step ${PIPE.STEPS[pi+1].n} ${esc(PIPE.STEPS[pi+1].label)} →</button>`
      : `<button class="btn amber sm" onclick="nav('home')">⌂ Home</button>`;
    return `<div class="stepbar">${prev}${nxt}</div>`;
  }
  const i = FLAT.findIndex(x=>x.id===id);
  const prev = i>0 ? `<button class="btn ghost sm" onclick="nav('${FLAT[i-1].id}')">← ${esc(FLAT[i-1].tt)}</button>`:'<span></span>';
  const next = i<FLAT.length-1 ? `<button class="btn primary sm" onclick="nav('${FLAT[i+1].id}')">${esc(FLAT[i+1].tt)} →</button>`:'<span class="note">end</span>';
  return `<div class="stepbar">${prev}${next}</div>`;
}
function render(id){
  const fn = VIEWS[id];
  const isHome = id === 'home';
  document.body.classList.toggle('home-mode', isHome);
  const bar = isHome ? '' : stepbar(id);
  $('#main').innerHTML = `<div class="view${isHome?' view-home':''}">${fn ? fn() : 'Not found'}${bar}</div>`;
  if(AFTER[id]) AFTER[id]();
}

/* ============================================================
   VIEWS
   ============================================================ */
const VIEWS = {};
const AFTER = {};

/* ---------- HOME ---------- */
function startMosaic(){
  const card = document.getElementById('arch-step-1');
  if(card){
    document.querySelectorAll('.arch-node.armed').forEach(el=>el.classList.remove('armed'));
    card.classList.add('armed');
  }
  setTimeout(()=>nav('connect'), 650);
}
function archCard(step, name, blurb, kind, chips){
  const core = kind === 'core';
  const cross = kind === 'cross';
  const fill = (chips||[]).map(c=>`<span>${c}</span>`).join('');
  return `<div class="arch-node ${core?'core':''} ${cross?'arch-gov-bar':''}" id="arch-step-${step}">
    <div class="an-idx">${String(step).padStart(2,'0')}</div>
    <div class="an-body">
      <div class="an-name"><span class="an-step">Step ${step}</span> ${name}${core?' <span class="chip core">CORE</span>':''}</div>
      <div class="an-role">${blurb}</div>
      ${fill?`<div class="an-fill">${fill}</div>`:''}
    </div>
    ${cross?'<span class="an-tag">CROSS-CUTTING</span>':''}
  </div>`;
}
VIEWS.home = () => `
  <div class="home-screen">
    <section class="home-brand">
      <div class="eyebrow"><span class="n">MOSAIC</span> Manufacturing Intelligence</div>
      <h1>MOSAIC</h1>
      <p class="home-expand">Manufacturing Operations Stream Aggregation, Integration &amp; Contextualization</p>
      <p class="home-bg">A manufacturing intelligence platform that turns raw SCADA telemetry — temperature, pH, humidity, pressure, conductivity — into contextualized, business-meaningful events, then into governed action through SENTRA. Shop-floor signals are joined to MES, SAP and the asset model so a number becomes a fact about a batch, an asset and a spec.</p>
      <div class="home-photo">
        <img src="/app/assets/mosaic-hero.png" alt="Plant operations and control-room intelligence">
      </div>
      <button type="button" class="btn amber home-start" onclick="startMosaic()">Start MOSAIC</button>
    </section>

    <section class="home-arch" aria-label="Architecture">
      <div class="home-sec-lbl">Architecture</div>
      <div class="arch-gov">
        ${archCard(6,'Govern &amp; Harden',
          'Zero-trust across the stack: who may act, what may execute, and a tamper-evident record of what was done.',
          'cross', ['RBAC','OPA policy','QA e-signature','SHA-256 audit'])}
        <div class="arch-fork">
          ${archCard(5,'SCADA Intelligence Platform',
            'Consumes Gold events — not raw tags. Perceive, diagnose, retrieve SOP/CAPA/OEM, then act or escalate.',
            'core', ['perceive','diagnose','RAG','reason','govern'])}
          ${archCard(4,'Visualize',
            'Dashboards in business terms: asset, batch, product and spec — plus pipeline health.',
            '', ['KPIs','excursions','historian','Gold store'])}
        </div>
        <div class="arch-merge"><span class="arch-gold">Gold · contextualized events</span></div>
        ${archCard(3,'Contextualize',
          'Keyed-lookup chain. A number becomes a fact about this asset, this batch, this product, this spec.',
          'core', ['tag →','asset →','batch →','product →','spec'])}
        ${archCard(2,'Ingest &amp; Store',
          'Land the stream durably: event log, time-series historian (Bronze), Medallion lake (Gold).',
          '', ['Kafka','TimescaleDB','MinIO','Bronze','Gold'])}
        ${archCard(1,'Connectivity',
          'Read sensors and PLCs via SCADA; publish every signal to one address in the Unified Namespace.',
          '', ['OPC-UA','MQTT','UNS','{tag, value, ts}'])}
        <div class="arch-node arch-ot-card">
          <div class="an-idx">00</div>
          <div class="an-body">
            <div class="an-name"><span class="an-step">Step 0</span> Edge / OT</div>
            <div class="an-role">On-prem snapshot the layers join. Five parameters: temperature, pH, humidity, pressure, conductivity.</div>
            <div class="arch-sources">
              <div class="arch-src"><div class="sn">SAP</div><div class="sd">master · spec</div></div>
              <div class="arch-src"><div class="sn">MES</div><div class="sd">batch · phase</div></div>
              <div class="arch-src"><div class="sn">RDBMS</div><div class="sd">calibration</div></div>
              <div class="arch-src"><div class="sn">Files</div><div class="sd">SOP · logs</div></div>
              <div class="arch-src"><div class="sn">Sensors</div><div class="sd">raw signal</div></div>
              <div class="arch-src"><div class="sn">SCADA</div><div class="sd">supervisory</div></div>
            </div>
          </div>
        </div>
      </div>
    </section>

    <footer class="home-powered">
      <span class="home-powered-lbl">Powered by · Tools &amp; Tech Stack</span>
      <div class="home-stack">
        <span>Python</span><span>FastAPI</span><span>OPC-UA</span><span>MQTT</span>
        <span>Kafka</span><span>TimescaleDB</span><span>Apache Flink</span>
        <span>Grafana</span><span>Ollama</span><span>Qdrant</span>
        <span>LangGraph</span><span>Keycloak</span><span>OPA</span>
      </div>
    </footer>
  </div>`;
AFTER.home = () => {};

/* ---------- generic phase content ---------- */
const PHASE_CONTENT = {
  ingest: {
    icon:"🌊", eyebrow:"PHASE 2", tagline:"Capture the stream and store it durably", core:false,
    def:"The <b>Ingest &amp; Store</b> layer carries telemetry into a durable event log and lands it in fit-for-purpose stores — a time-series historian and a Medallion data lake (Bronze → Silver → Gold).",
    tools:[["🌊","Redpanda / Kafka","Event streaming backbone"],["⏱️","TimescaleDB","Time-series historian"],["🪣","MinIO","S3-compatible data lake"],["🦆","DuckDB + Parquet","Query Medallion tables"]],
    contrib:"Together they capture the Connectivity stream, land it durably, and hand a fit-for-purpose Gold lake to Contextualize.",
    demo:"ingest"
  },
  visualize: {
    icon:"📊", eyebrow:"PHASE 4", tagline:"Business-meaningful dashboards, not raw tags", core:false,
    def:"The <b>Visualize / Observe</b> layer puts contextualized data in front of people in business terms. It covers process observability (is the plant healthy?) and platform observability (is the pipeline healthy?).",
    tools:[["📈","Grafana","Operational + platform dashboards"],["📊","Apache Superset","BI & exploration"],["🔍","Metabase","Self-serve BI"],["🔔","Prometheus","Metrics & alerting"]],
    contrib:"Together they turn contextualized Gold events into process and platform observability — plant health and pipeline health, not raw tags.",
    demo:"visualize"
  },
  govern: {
    icon:"🛡️", eyebrow:"PHASE 7", tagline:"Trust by design — nothing escapes oversight", core:false,
    def:"The <b>Govern &amp; Harden</b> layer wraps everything in zero-trust: identity, least-privilege access, policy checks on agent actions, and a hash-chained immutable audit. It runs across every layer.",
    tools:[["🔑","Keycloak","Identity & RBAC"],["⚖️","Open Policy Agent","Action policy gate"],["📜","Postgres WORM","Hash-chained audit"],["🧱","OT/IT Segmentation","Purdue zones + DMZ"]],
    contrib:"Together they wrap every MOSAIC action in zero-trust: authenticate who may act, allow or deny the action, write an immutable audit, and keep OT off the IT network so nothing escapes oversight.",
    demo:"govern"
  },
};
function phaseView(id){
  const c = PHASE_CONTENT[id];
  const toolLine = c.tools.map(([,n,d])=>`<b>${esc(n)}</b> — ${esc(d)}`).join(' · ');
  return `
  ${pipeTrack(id)}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow ${c.core?'core':''}"><span class="n">${c.eyebrow}</span> Platform Layer</div></div>
    <h1>${c.icon} &nbsp;${esc(FLAT.find(x=>x.id===id).tt)}</h1>
    <p class="lead">${c.tagline}</p>
  </header>
  <div class="page-brief"><span class="lbl">Tools that build this page</span>${toolLine}. ${c.contrib}</div>
  <div class="def"><span class="lbl">What this layer does</span>${c.def}</div>
  <div class="card"><h3><span class="ic">▶️</span> Live demo <span class="tag">calls the MOSAIC API</span></h3><div id="demoArea"></div></div>`;
}
VIEWS.connect = () => `
  <div class="connect-page layer-page">
  ${pipeTrack('connect')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow"><span class="n">PHASE 1</span> Foundation</div></div>
    <h1>Connectivity</h1>
    <p class="lead">Get every shop-floor tag onto <b>MQTT</b> (Message Queuing Telemetry Transport). Two independent tracks — <b>OPC-UA</b> (Open Platform Communications Unified Architecture) simulator and <b>uploaded file</b> — never mixed.</p>
  </header>
  <div class="page-brief"><span class="lbl">What you build</span>SCADA source → edge collector → Unified Namespace. No PLC required. Simulator generates live tags over OPC-UA (Open Platform Communications Unified Architecture); Excel/CSV/JSON replays your rows onto MQTT (Message Queuing Telemetry Transport) for Temperature, pH, Pressure, Conductivity and Humidity.</div>

  <div class="card tight src-card">
    <div class="src-head">
      <h3><span class="ic">🗂️</span> Shop-floor source</h3>
      <span class="tag" id="cSrcTag">simulated</span>
    </div>
    <div class="src-actions" id="cSrcToggle">
      <button type="button" class="btn sm primary" id="cSimBtn">Use simulator</button>
      <button type="button" class="btn sm ghost" id="cUpBtn">Show upload box</button>
      <a class="btn sm ghost" href="/api/connect/sample.csv" download="mosaic-floor-sample.csv">↓ CSV</a>
      <a class="btn sm ghost" href="/api/connect/sample.xlsx" download="mosaic-floor-sample.xlsx">↓ Excel</a>
      <button type="button" class="btn save sm" id="cSaveBtn">Save to PostgreSQL</button>
      <button type="button" class="btn sm delete hidden" id="cDelBtn">Delete</button>
    </div>
    <div id="cUploadBox" class="dropzone c-drop">
      <div class="c-drop-ic">⬇</div>
      <div class="c-drop-t">Upload Files only</div>
      <div class="note" style="margin:0">Drag an Excel, CSV or JSON file from File Explorer onto this box. There is no Open dialog — that Windows lock error cannot appear here.</div>
      <div class="c-drop-actions">
        <button type="button" class="btn sm amber" id="cLoadDiskBtn">Load from MOSAIC folders</button>
      </div>
      <div class="note">Expected columns: <code>tag,value,unit,timestamp</code> · tags <code>TT-1202B</code> <code>AT-3401</code> <code>PT-2201</code> <code>CT-5501</code> <code>MT-6601</code></div>
    </div>
    <p class="src-hint" id="cSaveHint">Upload a file first, then save. Next visit loads it automatically.</p>
    <div id="cSrcStatus" class="note"></div>
  </div>

  <div class="card tight" id="cWalk">
    <h3><span class="ic">▶️</span> Walk the four hops <span class="tag">serial · 1 → 4</span></h3>
    <div class="hop-map four">
      <div class="hop-map-item">
        <b>1</b>
        <div>
          <div class="hop-map-tt">OPC-UA server</div>
          <div class="hop-map-role">Open Platform Communications Unified Architecture</div>
          <p>Live tags exist. Nothing on MQTT yet.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>2</b>
        <div>
          <div class="hop-map-tt">Mosquitto (MQTT)</div>
          <div class="hop-map-role">Message Queuing Telemetry Transport</div>
          <p>Broker listening. Address space only.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>3</b>
        <div>
          <div class="hop-map-tt">Node-RED</div>
          <div class="hop-map-role">Collector — publish</div>
          <p>Values enter <code>plant/…</code> or <code>plant/file/…</code>.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>4</b>
        <div>
          <div class="hop-map-tt">Subscribe</div>
          <div class="hop-map-role">Proof — confirm stream</div>
          <p>All tags on the wire. Ingest takes this batch next.</p>
        </div>
      </div>
    </div>
    <div class="c-ctrl">
      <label class="fl">Parameter</label>
      <select class="inp" id="cParam"></select>
      <label class="fl" id="cZoneLbl">Scenario</label>
      <select class="inp" id="cZone">
        <option value="">natural mix</option>
        <option value="control">in-spec</option>
        <option value="alarm">alarm</option>
        <option value="trip">trip</option>
      </select>
      <button type="button" class="btn ghost sm" id="cReset">↺ Reset hops</button>
    </div>
    <div id="cSteps"></div>
    <div id="cDone"></div>
    <div id="cNext"></div>
  </div>
  </div>`;
VIEWS.ingest = () => `
  <div class="ingest-page layer-page">
  ${pipeTrack('ingest')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow"><span class="n">PHASE 2</span> Platform Layer</div></div>
    <h1>Ingest &amp; Store</h1>
    <p class="lead">Capture the Connectivity stream and land it in fit-for-purpose stores — a time-series historian and a Medallion data lake (<span class="zone-pill bronze">Bronze</span> → <span class="zone-pill silver">Silver</span> → <span class="zone-pill gold">Gold</span>).</p>
  </header>
  <div class="def"><span class="lbl">What this layer does</span>
    Connectivity proved the floor is on MQTT. This layer consumes <b>every confirmed datapoint</b> from that stream — not one tag — and writes the batch to
    <b>Redpanda / Kafka</b>, dual-writes <b>Bronze</b> into <b>TimescaleDB</b> and <b>MinIO</b>, then <b>DuckDB</b> transforms Bronze → Silver → Gold lake.
    Lake Gold is <em>not</em> Layer 3’s Flink-contextualized events.
  </div>

  <div class="card tight" id="iWalk">
    <h3><span class="ic">▶️</span> Walk the five hops <span class="tag">serial · Connectivity → 1 → 5</span></h3>
    <p class="note" style="margin:0 0 8px">Each hop depends on the one before it. Hop 1 consumes the full Connectivity stream. You cannot skip ahead.</p>
    <div id="iHandoff"></div>
    <div class="hop-map five">
      <div class="hop-map-item">
        <b>1</b>
        <div>
          <div class="hop-map-tt">Redpanda / Kafka</div>
          <div class="hop-map-role">Event log</div>
          <p>Produce every MQTT reading to <code>scada.telemetry</code>. <b>Contribution:</b> ordered, replayable log.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>2</b>
        <div>
          <div class="hop-map-tt">TimescaleDB</div>
          <div class="hop-map-role">Bronze historian</div>
          <p>INSERT the Kafka batch as-is. <b>Contribution:</b> last-N / by-tag queries. Still raw.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>3</b>
        <div>
          <div class="hop-map-tt">MinIO</div>
          <div class="hop-map-role">Bronze lake</div>
          <p>Same batch as immutable Parquet. <b>Contribution:</b> replayable landing zone.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>4</b>
        <div>
          <div class="hop-map-tt">DuckDB + Parquet</div>
          <div class="hop-map-role">Silver</div>
          <p>CAST types, canonical units, drop BAD. <b>Contribution:</b> conformed. No MES/SAP yet.</p>
        </div>
      </div>
      <div class="hop-map-arrow" aria-hidden="true">→</div>
      <div class="hop-map-item">
        <b>5</b>
        <div>
          <div class="hop-map-tt">DuckDB + MinIO</div>
          <div class="hop-map-role">Gold lake</div>
          <p>Curated telemetry table for Flink. <b>Contribution:</b> lake Gold — not contextualized events.</p>
        </div>
      </div>
    </div>
    <div class="i-reset-row">
      <button type="button" class="btn ghost sm" id="iReset">↺ Reset hops</button>
      <span class="note" style="margin:0">Clears this layer’s hop lock. The Connectivity batch stays.</span>
    </div>
    <div id="iSteps"></div>
    <div id="iDone"></div>
    <div id="iNext"></div>
  </div>
  </div>`;
VIEWS.visualize = () => `
  <div class="viz-page layer-page">
  ${pipeTrack('visualize')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow"><span class="n">PHASE 4</span> Platform Layer</div></div>
    <h1>Visualize</h1>
    <a class="btn primary sm" id="vXlsx" href="/api/visualize/excel">↓ Download excel file</a>
    <p class="lead">Every trip and alarm observation is one <b>reading_id</b>. That reading’s tag joins Asset, MES, SAP and RDBMS — <b>reading_id → tag → asset → batch → product → spec</b>.</p>
  </header>
  <div class="page-brief"><span class="lbl">What this layer does</span>Process observability on the merged contextualized dataset. Grain: <b>reading_id</b>. Then mapping keys: <b>tag</b> (Asset, RDBMS) · <b>asset + timestamp</b> (MES) · <b>product</b> (SAP).</div>
  <div id="vKpis"></div>
  <div class="viz-keys">
    <span class="viz-key"><b>Grain</b> reading_id</span>
    <span class="viz-key"><b>Asset</b> match on tag</span>
    <span class="viz-key"><b>MES</b> match on asset + ts</span>
    <span class="viz-key"><b>SAP</b> match on product</span>
    <span class="viz-key"><b>RDBMS</b> match on tag</span>
  </div>
  <div class="card tight" id="vTripCard">
    <h3><span class="ic">⚠️</span> Trip — merged contextualized dataset <span class="tag" id="vTripTag">all parameters</span></h3>
    <div id="vTrip"></div>
  </div>
  <div class="card tight" id="vAlarmCard">
    <h3><span class="ic">🔔</span> Alarm — merged contextualized dataset <span class="tag" id="vAlarmTag">all parameters</span></h3>
    <div id="vAlarm"></div>
  </div>
  </div>`;
VIEWS.govern = () => {
  const c = PHASE_CONTENT.govern;
  const toolLine = c.tools.map(([,n,d])=>`<b>${esc(n)}</b> — ${esc(d)}`).join(' · ');
  return `
  <div class="govern-page layer-page">
  ${pipeTrack('govern')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow"><span class="n">PHASE 7</span> Platform Layer</div></div>
    <h1>Govern &amp; Harden</h1>
    <p class="lead">${c.tagline}</p>
  </header>
  <div class="page-brief"><span class="lbl">Tools that build this page</span>${toolLine}. ${c.contrib}</div>
  <div class="def"><span class="lbl">What this layer does</span>${c.def}</div>

  <div class="c-ctrl gov-ctrl">
    <label class="fl">Parameter</label>
    <select class="inp" id="gParam"></select>
    <label class="fl">Zone</label>
    <select class="inp" id="gZone">
      <option value="alarm" selected>alarm</option>
      <option value="trip">trip</option>
      <option value="control">in-spec</option>
    </select>
    <label class="fl">Role</label>
    <select class="inp" id="gRole">
      <option value="qa" selected>QA</option>
      <option value="operator">operator</option>
      <option value="shift_lead">shift lead</option>
    </select>
  </div>

  <div class="gov-console" id="gConsole">
    <section class="gov-panel" id="gPolicy">
      <div class="gov-hd">
        <h3>⚖️ Policy decision</h3>
        <span class="gov-meta" id="gPolicyMeta">—</span>
      </div>
      <div class="gov-kv" id="gPolicyKv"></div>
      <div class="gov-banner" id="gBanner"></div>
    </section>

    <section class="gov-panel" id="gSign">
      <div class="gov-hd">
        <h3>✍️ Apply e-signature</h3>
        <span class="gov-meta">21 CFR Part 11</span>
      </div>
      <div class="gov-fields">
        <label class="gov-fl">Signer (QA / Production)
          <input class="gov-inp" id="gSigner" type="text" placeholder="e.g. A. Sharma, QA" autocomplete="off">
        </label>
        <label class="gov-fl">Meaning of signature
          <input class="gov-inp" id="gMeaning" type="text" placeholder="e.g. Approved — corrective action" autocomplete="off">
        </label>
      </div>
      <div class="gov-actions">
        <button type="button" class="btn gov-commit" id="gCommit" disabled>✍️ Sign &amp; commit</button>
        <button type="button" class="btn gov-reject" id="gReject" disabled>❌ Reject</button>
      </div>
    </section>

    <section class="gov-panel" id="gAudit">
      <div class="gov-hd">
        <h3>🔒 WORM audit trail</h3>
        <span class="gov-meta">hash-chained</span>
      </div>
      <div class="gov-term" id="gTerm"><span class="d">// loading audit…</span></div>
    </section>
  </div>
  <div id="gNext"></div>
  </div>`;
};

VIEWS.copilot = () => `
  <div class="copilot-page layer-page">
  ${pipeTrack('copilot')}
  <header class="page-head copilot-head">
    <div class="page-kicker"><div class="eyebrow"><span class="n">STEP 8 · MANUAL ASSIST</span></div></div>
    <h1>SCADA Co-pilot Chatbot</h1>
    <p class="lead">Open this chatbot when the automated <b>SCADA Intelligence Platform</b> is unclear. Select a parameter agent to inspect its last trip/alarm remedy, or use <b>Human Escalation</b> when that agent cannot ground a near-perfect answer.</p>
  </header>
  <div class="page-brief"><span class="lbl">How to use</span>Pick an agent → review the last automated remedy → ask one of the five suggested questions (or your own). Answers come from RAG first (CAPA · Master Index · OEM · Regulatory · SOP). The LLM is used only if RAG cannot answer — you will see a notice. Request a chart at any time. Every turn is stored in PostgreSQL.</div>

  <div class="card tight cp-toolbar">
    <label class="fl">Scenario</label>
    <select class="inp" id="cpZone">
      <option value="trip">Trip</option>
      <option value="alarm">Alarm</option>
    </select>
    <div class="cp-rag-chips" title="Retrieval sources">
      <span>CAPA</span><span>Master Index</span><span>OEM</span><span>Regulatory</span><span>SOP</span>
    </div>
    <span class="note" id="cpPgHint" style="margin:0 0 0 auto">PostgreSQL chat log</span>
  </div>

  <div class="cp-tabs" id="cpTabs" role="tablist"></div>

  <div class="cp-grid">
    <div class="cp-col">
      <div class="card tight cp-si-wrap">
        <h3><span class="ic">📋</span> Last SCADA Intelligence remedy</h3>
        <div id="cpSi"></div>
      </div>
      <div class="card tight cp-queries">
        <h3><span class="ic">💡</span> <span id="cpQLbl">Top expected queries</span></h3>
        <div class="cp-q-list" id="cpQList"></div>
      </div>
    </div>
    <div class="cp-col">
      <div class="card tight cp-chat">
        <div class="cp-chat-hd">
          <span class="cp-agent-ic" id="cpAgentIc">🌡️</span>
          <div>
            <div class="cp-agent-nm" id="cpAgentNm">Temperature Agent</div>
            <div class="cp-agent-sub" id="cpAgentSub">Parameter agent · RAG first</div>
          </div>
          <span class="cp-badge">SCADA Co-pilot</span>
        </div>
        <div class="cp-log" id="cpLog"></div>
        <div class="cp-compose">
          <input class="cp-inp" id="cpInput" type="text" placeholder="Type a question, or ask for a chart…" autocomplete="off">
          <button type="button" class="btn sm ghost" id="cpChartBtn">Chart</button>
          <button type="button" class="btn sm amber" id="cpSend">Ask</button>
        </div>
      </div>
    </div>
  </div>
  </div>`;

AFTER.connect = () => demoConnect();
AFTER.ingest = () => demoIngest();
AFTER.visualize = () => demoVisualize();
AFTER.govern = () => demoGovern();
AFTER.copilot = () => demoCopilot();

/* ---------- phase demos (live API) ---------- */
const CONNECT_TRACKS = [
  {id:'simulator', label:'OPC-UA simulator'},
  {id:'uploaded', label:'Uploaded dataset'},
];
const CONNECT_HOPS = [
  {n:1, title:'Install an OPC-UA (Open Platform Communications Unified Architecture) server and expose tags',
   sim:{btn:'▶ Start OPC-UA server', tool:'OPC-UA simulator', stack:'Python asyncua / FreeOpcUa / open62541',
        why:'This tab only uses the built-in generator. Tags appear on <code>opc.tcp://127.0.0.1:4840</code> with live simulated values — never from your file. Nothing is ingested yet.'},
   up:{btn:'▶ Expose uploaded tags from file', tool:'File replay', stack:'every row in the Excel / CSV / JSON — not the OPC-UA generator',
       why:'This tab exposes <b>every datapoint</b> in the file you uploaded (or loaded from PostgreSQL). If nothing is loaded yet, hop 1 uses the sample Excel. The OPC-UA simulator is not used here.',
       stream:true}},
  {n:2, title:'Stand up Mosquitto — the MQTT (Message Queuing Telemetry Transport) Unified Namespace hub',
   sim:{btn:'▶ Start Mosquitto broker', tool:'Mosquitto', stack:'docker run -p 1883:1883 eclipse-mosquitto',
        why:'Start the MQTT hub for the <b>simulator</b> track. Collectors on this tab publish here; the uploaded tab has its own hop-2. You should see it listening, not tag values yet.'},
   up:{btn:'▶ Start Mosquitto for uploaded stream', tool:'Mosquitto', stack:'topics plant/file/{filename}/{tag} · every file row',
       why:'Stand up MQTT for the <b>uploaded file</b> only. Every datapoint in the spreadsheet is registered in the Unified Namespace — not simulated OPC-UA tags. Complete hop 1 on this same tab first.'}},
  {n:3, title:'Node-RED reads OPC-UA (Open Platform Communications Unified Architecture) and publishes each tag to MQTT (Message Queuing Telemetry Transport)',
   sim:{btn:'▶ Deploy Node-RED flow', tool:'Node-RED', stack:'opcua-in → mqtt-out  ·  Telegraf as alternative',
        why:'The edge collector reads <b>simulated</b> OPC-UA tags and publishes each to a hierarchical topic such as plant/line3/BR-12/temp. Uploaded rows are not used on this tab.'},
   up:{btn:'▶ Deploy Node-RED for uploaded tags', tool:'Node-RED', stack:'file-in → mqtt-out  ·  every file row',
       why:'The edge collector publishes <b>every row</b> from your file to plant/file/{filename}/{tag}. Simulated OPC-UA values are not used on this tab.'}},
  {n:4, title:'Subscribe and confirm live values are arriving',
   sim:{btn:'▶ Subscribe to plant/#', tool:'MQTT client', stack:'mosquitto_sub -t \'plant/#\'',
        why:'Prove the simulator is on the wire. Subscribe and watch <b>generated</b> values stream in. That stream is what Ingest will capture next if you stay on this track.'},
   up:{btn:'▶ Subscribe to plant/file/#', tool:'MQTT client', stack:'mosquitto_sub -t \'plant/file/#\' · full file',
       why:'Prove the uploaded file is on the wire. Subscribe to <code>plant/file/#</code> and confirm <b>every datapoint</b> from the spreadsheet — never generated OPC-UA values. That full batch is what Ingest captures next.'}},
];
function hopCopy(h, src){ return src === 'uploaded' ? h.up : h.sim; }

function connectLogHtml(log){
  if(!log || !log.length) return `<span class="d">// run this hop to see its output</span>`;
  return log.map(line=>{
    const cls = /✓|listening|live stream|Server started|flow deployed|Streaming |hub ready/.test(line) ? 'p'
      : /\[mqtt\]|\[node\]|\[Node-RED\]|\[opcua-in\]|\[file-in\]|\[file row|\[stream\]|\[UNS\]/.test(line) ? 'o'
      : /docker|mosquitto_sub/.test(line) ? 'w' : 'd';
    return `<div><span class="${cls}">${esc(line)}</span></div>`;
  }).join('');
}

let connectStreamTimer = null;
function stopConnectStream(){
  if(connectStreamTimer){ clearInterval(connectStreamTimer); connectStreamTimer = null; }
  const b = $('#cStreamBtn');
  if(b) b.textContent = '▶ Stream uploaded dataset';
}

async function demoConnect(){
  stopConnectStream();
  const hopsHost = $('#cSteps');
  if(!hopsHost) return;

  const paneHtml = (h, src) => {
    const c = hopCopy(h, src);
    const stream = src === 'uploaded' && c.stream
      ? `<button type="button" class="btn amber sm" id="cStreamBtn">▶ Stream uploaded dataset</button>` : '';
    return `
      <div class="hop-pane${src==='simulator'?'':' hidden'}" data-track="${src}">
        <div class="cstep-tool"><span class="chip free">${esc(c.tool)}</span><span class="note" style="margin:0">${esc(c.stack)}</span></div>
        <p class="cstep-why">${c.why}</p>
        <div class="cstep-actions">
          <button type="button" class="btn primary sm cstep-btn" data-step="${h.n}" data-source="${src}">${c.btn}</button>
          ${stream}
        </div>
        <div class="demo-box"><div class="term" id="cOut-${h.n}-${src}"><span class="d">// run this hop on this tab to see its output</span></div></div>
      </div>`;
  };

  hopsHost.innerHTML = CONNECT_HOPS.map(h=>`
    <div class="cstep" id="cstep-${h.n}">
      <div class="cstep-hd">
        <b>${h.n}</b>
        <div>
          <div class="cstep-tt">${esc(h.title)}</div>
          <div class="hop-tabs" role="tablist">
            ${CONNECT_TRACKS.map((t,i)=>`
              <button type="button" class="hop-tab${i===0?' active':''}" role="tab"
                data-step="${h.n}" data-track="${t.id}">${esc(t.label)}</button>`).join('')}
          </div>
        </div>
      </div>
      ${paneHtml(h,'simulator')}
      ${paneHtml(h,'uploaded')}
    </div>`).join('');

  $('#cParam').innerHTML =
    `<option value="all">All 5 parameters</option>` +
    PARAMS.map(p=>`<option value="${p.id}">${p.short} (${p.tag})</option>`).join('');

  let floorStatus = {row_count:0, has_upload:false};
  let hopDone = {simulator:0, uploaded:0};

  const paintSource = (st) => {
    floorStatus = st || floorStatus;
    const has = !!(st.row_count > 0 || st.has_upload);
    const tag = $('#cSrcTag');
    if(tag) tag.textContent = has ? `uploaded · ${st.row_count} rows · simulator ready` : 'simulator ready';
    $('#cSimBtn').classList.toggle('primary', !has);
    $('#cSimBtn').classList.toggle('ghost', has);
    $('#cUpBtn').classList.toggle('primary', has);
    $('#cUpBtn').classList.toggle('ghost', !has);
    const by = st.by_param || {};
    const breakdown = PARAMS.map(p=>`${p.short}: ${by[p.id]||0}`).join(' · ');
    $('#cSrcStatus').innerHTML = has
      ? `<span class="note ok">${st.persisted?'Loaded from PostgreSQL':'File ready'}: <b>${esc(st.filename||'upload')}</b> — <b>${st.row_count}</b> rows. ${esc(breakdown)}. ${st.persisted?'No need to re-upload. ':''}${(st.parse && st.parse.skipped) ? esc(st.parse.skipped + ' rows skipped. ') : ''}Use the <b>Uploaded dataset</b> tab on each hop. The simulator tab stays independent.</span>`
      : `<span class="note">Simulator tabs work now. Drag a file onto <b>Upload Files only</b>, then <b>Save to PostgreSQL</b>.</span>`;
    const sb = $('#cStreamBtn');
    if(sb){
      sb.disabled = false;
      sb.title = has ? 'Replay the uploaded file at 1 Hz (this tab only)' : 'Loads the sample floor file, then streams it at 1 Hz';
    }
    const save = $('#cSaveBtn');
    const hint = $('#cSaveHint');
    const del = $('#cDelBtn');
    if(save){
      save.disabled = !(st.row_count > 0);
      save.classList.remove('ghost','primary');
      save.classList.add('save');
      if(st.persisted){
        save.textContent = '✓ Saved in PostgreSQL';
        if(hint){ hint.textContent = `${st.filename} is stored. It will load automatically next time.`; hint.classList.add('ok'); }
      } else if(st.row_count > 0){
        save.textContent = 'Save to PostgreSQL';
        if(hint){ hint.textContent = `${st.row_count} rows in memory. Click Save so you do not re-upload.`; hint.classList.remove('ok'); }
      } else {
        save.textContent = 'Save to PostgreSQL';
        if(hint){ hint.textContent = 'Upload a file first, then save. Next visit loads it automatically.'; hint.classList.remove('ok'); }
      }
    }
    if(del){
      del.classList.toggle('hidden', !st.persisted);
      del.disabled = !st.persisted;
    }
    paintLocks();
  };

  const paintLocks = () => {
    const has = !!(floorStatus.row_count > 0 || floorStatus.has_upload);
    CONNECT_HOPS.forEach(h=>{
      const el = $(`#cstep-${h.n}`);
      if(!el) return;
      const maxN = Math.max(hopDone.simulator, hopDone.uploaded);
      el.classList.toggle('done', h.n <= maxN);
      el.classList.toggle('open', h.n === hopDone.simulator+1 || h.n === hopDone.uploaded+1);
      CONNECT_TRACKS.forEach(t=>{
        const n = hopDone[t.id] || 0;
        const pane = el.querySelector(`.hop-pane[data-track="${t.id}"]`);
        const tab = el.querySelector(`.hop-tab[data-track="${t.id}"]`);
        const waitFile = t.id==='uploaded' && !has && h.n > 1;
        if(pane){
          pane.classList.toggle('locked', h.n > n+1 || waitFile);
          pane.querySelectorAll('.cstep-btn').forEach(btn => {
            btn.disabled = h.n > n+1 || waitFile;
          });
        }
        if(tab){
          tab.classList.toggle('ready', h.n === n+1 && (t.id!=='uploaded' || has || h.n===1));
          tab.classList.toggle('complete', h.n <= n);
        }
      });
    });
    const sb = $('#cStreamBtn');
    if(sb){
      sb.disabled = false;
      sb.title = has ? 'Replay the uploaded file at 1 Hz (this tab only)' : 'Loads the sample floor file, then streams it at 1 Hz';
    }
    const finished = hopDone.simulator >= 4 || hopDone.uploaded >= 4;
    if(finished){
      const which = [
        hopDone.simulator>=4 ? 'OPC-UA simulator' : null,
        hopDone.uploaded>=4 ? 'uploaded dataset' : null,
      ].filter(Boolean).join(' and ');
      $('#cDone').innerHTML = `<div class="callout green"><span class="cic">✓</span>
        <div class="cx"><b>Done when:</b> you can watch raw sensor values stream in on <code>plant/#</code>
        (${esc(which)}). That track is ready for Ingest. The other tab is independent — finish it if you want both.</div></div>`;
      $('#cNext').innerHTML = '';
    } else {
      $('#cDone').innerHTML = '';
      $('#cNext').innerHTML = '';
    }
  };

  const jsonPost = (path, body) => api(path, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(body||{})
  });

  const loadSampleFloor = async () => {
    const res = await fetch('/api/connect/sample.xlsx');
    if(!res.ok) throw new Error('Could not fetch the sample Excel file');
    const blob = await res.blob();
    const fd = new FormData();
    fd.append('file', new File([blob], 'mosaic-floor-sample.xlsx', {
      type: blob.type || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }));
    const st = await api('/api/connect/upload', {method:'POST', body: fd});
    paintSource(st);
    clearTrack('uploaded');
    hopsHost.querySelectorAll('.hop-tab[data-track="uploaded"]').forEach(t=>t.click());
    return st;
  };

  const ensureFloorFile = async () => {
    let st = floorStatus;
    try { st = await api('/api/connect/status'); paintSource(st); }
    catch(e){ /* keep last known status */ }
    if(st.row_count > 0 || st.has_upload) return st;
    throw new Error('Upload a file first, then run the uploaded-dataset hops.');
  };

  const clearTrack = (src) => {
    if(src === 'uploaded') stopConnectStream();
    hopDone[src] = 0;
    CONNECT_HOPS.forEach(h=>{
      const t = $(`#cOut-${h.n}-${src}`);
      if(t) t.innerHTML = '<span class="d">// run this hop on this tab to see its output</span>';
    });
    paintLocks();
  };
  const clearHops = () => {
    stopConnectStream();
    hopDone = {simulator:0, uploaded:0};
    CONNECT_HOPS.forEach(h=>{
      CONNECT_TRACKS.forEach(t=>{
        const el = $(`#cOut-${h.n}-${t.id}`);
        if(el) el.innerHTML = '<span class="d">// run this hop on this tab to see its output</span>';
      });
    });
    PIPE.reading=null; PIPE.readings=null; PIPE.event=null;
    paintLocks();
  };

  const appendStream = (html) => {
    const term = $('#cOut-1-uploaded'); if(!term) return;
    const hold = term.querySelectorAll('div');
    const keep = Array.from(hold).slice(-36).map(el=>el.outerHTML).join('');
    term.innerHTML = keep + html;
    term.parentElement.scrollTop = term.parentElement.scrollHeight;
  };

  const streamTick = async () => {
    const r = await jsonPost('/api/connect/stream/tick', {param: $('#cParam').value, source:'uploaded'});
    mergeConnectBatch(r);
    appendStream(connectLogHtml(r.log));
    hopDone.uploaded = Math.max(hopDone.uploaded, 1);
    paintLocks();
    return r;
  };

  hopsHost.querySelectorAll('.hop-tab').forEach(tab=>{
    tab.onclick = () => {
      const step = tab.dataset.step;
      const src = tab.dataset.track;
      const card = $(`#cstep-${step}`);
      card.querySelectorAll('.hop-tab').forEach(b=>b.classList.toggle('active', b===tab));
      card.querySelectorAll('.hop-pane').forEach(p=>p.classList.toggle('hidden', p.dataset.track!==src));
      if(src==='uploaded' && !(floorStatus.row_count > 0 || floorStatus.has_upload)){
        $('#cUploadBox')?.scrollIntoView({block:'nearest'});
      }
    };
  });

  try {
    const st = await api('/api/connect/status');
    if(st.track_done){
      hopDone.simulator = st.track_done.simulator || 0;
      hopDone.uploaded = st.track_done.uploaded || 0;
    }
    paintSource(st);
  }
  catch(e){ $('#cSrcStatus').textContent = 'Could not reach /api/connect/status'; }
  paintLocks();

  const sendFloorFile = async (file) => {
    if(!file) return;
    $('#cSrcStatus').innerHTML = `<span class="spin"></span> reading ${esc(file.name)}…`;
    try{
      const buf = await file.arrayBuffer();
      const copy = new File([buf], file.name, {type: file.type || 'application/octet-stream'});
      const fd = new FormData();
      fd.append('file', copy, file.name);
      const st = await api('/api/connect/upload', {method:'POST', body: fd});
      paintSource(st);
      clearTrack('uploaded');
      hopsHost.querySelectorAll('.hop-tab[data-track="uploaded"]').forEach(t=>t.click());
    }catch(err){
      const msg = String(err && err.message || err);
      const locked = /notreadable|not allowed|locked|in use|abort|could not be read/i.test(msg);
      $('#cSrcStatus').innerHTML = locked
        ? `<span class="note" style="color:var(--coral)">Could not read <b>${esc(file.name)}</b>. Close it in Excel, then drag it onto the box.</span>`
        : `<span class="note" style="color:var(--coral)">${esc(msg)}</span>`;
    }
  };

  $('#cSimBtn').onclick = async () => {
    paintSource(await jsonPost('/api/connect/simulate'));
    clearTrack('simulator');
  };
  $('#cUpBtn').onclick = () => {
    const box = $('#cUploadBox');
    if(box){ box.classList.remove('hidden'); box.scrollIntoView({block:'nearest'}); }
  };
  $('#cLoadDiskBtn').onclick = async () => {
    const btn = $('#cLoadDiskBtn');
    btn.disabled = true;
    $('#cSrcStatus').innerHTML = `<span class="spin"></span> loading from MOSAIC folders…`;
    try{
      const st = await jsonPost('/api/connect/load-local');
      paintSource(st);
      clearTrack('uploaded');
      hopsHost.querySelectorAll('.hop-tab[data-track="uploaded"]').forEach(t=>t.click());
    }catch(err){
      $('#cSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
    }
    btn.disabled = false;
  };
  $('#cSaveBtn').onclick = async () => {
    const btn = $('#cSaveBtn');
    btn.disabled = true;
    $('#cSrcStatus').innerHTML = `<span class="spin"></span> saving to PostgreSQL…`;
    try{
      const st = await jsonPost('/api/connect/save');
      paintSource(st);
    }catch(err){
      $('#cSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
      btn.disabled = false;
    }
  };
  $('#cDelBtn').onclick = async () => {
    const name = floorStatus.filename || 'the stored dataset';
    if(!confirm(`Permanently delete ${name} from PostgreSQL, MOSAIC memory, and the ingest lake?\n\nThis cannot be undone.`)) return;
    const btn = $('#cDelBtn');
    btn.disabled = true;
    $('#cSrcStatus').innerHTML = `<span class="spin"></span> deleting from PostgreSQL…`;
    try{
      const st = await jsonPost('/api/connect/delete');
      hopDone.uploaded = 0;
      paintSource(st);
      clearTrack('uploaded');
      PIPE.reading=null; PIPE.readings=null; PIPE.event=null;
      $('#cSrcStatus').innerHTML = `<span class="note ok">${esc(st.message || 'Permanently deleted.')}</span>`;
    }catch(err){
      $('#cSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
      btn.disabled = false;
    }
  };
  const dropBox = $('#cUploadBox');
  if(dropBox){
    ['dragenter','dragover'].forEach(kind => {
      dropBox.addEventListener(kind, (e) => {
        e.preventDefault(); e.stopPropagation();
        dropBox.classList.add('over');
      });
    });
    ['dragleave','drop'].forEach(kind => {
      dropBox.addEventListener(kind, (e) => {
        e.preventDefault(); e.stopPropagation();
        if(kind === 'dragleave' && dropBox.contains(e.relatedTarget)) return;
        dropBox.classList.remove('over');
      });
    });
    dropBox.addEventListener('drop', (e) => {
      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      sendFloorFile(f);
    });
  };
  $('#cReset').onclick = async () => {
    await jsonPost('/api/connect/reset');
    clearHops();
  };
  $('#cParam').onchange = () => $('#cReset').click();

  const streamBtn = $('#cStreamBtn');
  if(streamBtn){
    streamBtn.onclick = async () => {
      if(connectStreamTimer){ stopConnectStream(); return; }
      const term = $('#cOut-1-uploaded');
      try{
        const st = await ensureFloorFile();
        term.innerHTML = connectLogHtml([
          `[stream] uploaded track · ${st.filename} (${st.row_count} rows)`,
          `[stream] 1 Hz replay · ${$('#cParam').value==='all'?'all 5 parameters':$('#cParam').value}`,
          `[stream] simulator tab is not used`,
        ]);
        streamBtn.textContent = '■ Stop stream';
        await streamTick();
        connectStreamTimer = setInterval(() => {
          streamTick().catch(err => {
            stopConnectStream();
            appendStream(`<div><span class="w">${esc(err.message)}</span></div>`);
          });
        }, 1000);
      }catch(err){
        term.innerHTML = `<span class="w">${esc(err.message)}</span>`;
        stopConnectStream();
      }
    };
  }

  hopsHost.querySelectorAll('.cstep-btn').forEach(btn=>{
    btn.onclick = async () => {
      const step = Number(btn.dataset.step);
      const source = btn.dataset.source || 'simulator';
      if(step===1 && source==='uploaded') stopConnectStream();
      const term = $(`#cOut-${step}-${source}`);
      term.innerHTML = `<span class="spin"></span> running hop ${step} (${source})…`;
      try{
        if(source==='uploaded') await ensureFloorFile();
        const r = await jsonPost(`/api/connect/step/${step}`, {
          param: $('#cParam').value,
          zone: source === 'simulator' ? ($('#cZone').value || null) : null,
          source
        });
        term.innerHTML = connectLogHtml(r.log);
        takeConnectBatch(r);
        hopDone[source] = Math.max(hopDone[source]||0, r.step);
        if(r.track_done){
          hopDone.simulator = Math.max(hopDone.simulator, r.track_done.simulator||0);
          hopDone.uploaded = Math.max(hopDone.uploaded, r.track_done.uploaded||0);
        }
        paintLocks();
      }catch(err){
        term.innerHTML = `<span class="w">${esc(err.message)}</span>`;
      }
    };
  });
}
const INGEST_XLSX = {3:'bronze', 4:'silver', 5:'gold'};
const INGEST_HOPS = [
  {n:1, title:'Consume Connectivity and produce to Redpanda / Kafka',
   btn:'▶ Produce to scada.telemetry', tool:'Redpanda / Kafka',
   stack:'topic scada.telemetry · key = tag · offset WAL',
   why:'MQTT proved the floor is on the wire; it is not a store. This hop takes the Connectivity reading and produces it to a durable, ordered log so historian and lake can both consume the same offset.'},
  {n:2, title:'Land Bronze in TimescaleDB — the operational historian',
   btn:'▶ INSERT into historian', tool:'TimescaleDB',
   stack:'hypertable historian_bronze · consume Kafka offset',
   why:'Bronze historian is the raw Kafka payload inserted as-is. Use it for last-N and by-tag queries on the plant floor. No unit cleanup, no quality drop, no MES join — that comes later.'},
  {n:3, title:'Land Bronze in MinIO — immutable Parquet landing zone',
   btn:'▶ Write bronze parquet', tool:'MinIO',
   stack:'s3://mosaic-lake/bronze/dt=…/tag=…/offset=….parquet',
   why:'Dual-write: hop 2 was queryable time-series; hop 3 is the lake. Same payload, immutable object. Silver and Gold are rebuilt from these files — never from a mutated historian row.'},
  {n:4, title:'Transform Bronze → Silver with DuckDB + Parquet',
   btn:'▶ Run Silver transform', tool:'DuckDB + Parquet',
   stack:'CAST · canonical unit · TRY_CAST timestamp · drop BAD',
   why:'DuckDB reads the Bronze Parquet and writes Silver. Types are fixed, units become canonical (°C, pH, kPa…), timestamps become TIMESTAMP, BAD quality is dropped. Still no batch, product, or spec — that is Contextualize.'},
  {n:5, title:'Curate Silver → Gold lake for Contextualize',
   btn:'▶ Publish Gold lake table', tool:'DuckDB + MinIO',
   stack:'grain=reading · ready_for=contextualize · context_status=pending',
   why:'Gold lake is a curated telemetry table Flink will consume. It is not Layer 3’s contextualized Gold events (those join MES / SAP / asset model). Fit-for-purpose store, then hand off.'},
];
function ingestLogHtml(log){
  if(!log || !log.length) return `<span class="d">// run this hop to see its output</span>`;
  return log.map(line=>{
    const cls = /✓|hypertable|connected|PUT  s3:/.test(line) ? 'p'
      : /\[kafka\]|\[redpanda\]|\[timescaledb\]|\[minio\]|\[duckdb\]|\[payload\]|\[sql\]/.test(line) ? 'o'
      : /\[transform\]|\[schema\]|\[contract\]|\[consumer\]|\[wal\]/.test(line) ? 'w'
      : /\[role\]|production:/.test(line) ? 'd' : 'd';
    return `<div><span class="${cls}">${esc(line)}</span></div>`;
  }).join('');
}
function previewTable(rows){
  if(!rows || !rows.length) return '';
  const cols = Object.keys(rows[0]);
  const head = cols.map(c=>`<th>${esc(c)}</th>`).join('');
  const body = rows.map(r=>`<tr>${cols.map(c=>`<td>${esc(r[c])}</td>`).join('')}</tr>`).join('');
  return `<div class="preview-wrap"><table class="preview-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
async function demoIngest(){
  const hopsHost = $('#iSteps');
  if(!hopsHost) return;
  let hopDone = 0;

  const batchOf = () => {
    if(PIPE.readings && PIPE.readings.length) return PIPE.readings;
    if(PIPE.reading) return [PIPE.reading];
    return [];
  };

  const paintHandoff = () => {
    const el = $('#iHandoff');
    if(!el) return;
    const batch = batchOf();
    if(batch.length){
      const tags = [...new Set(batch.map(r=>r.tag).filter(Boolean))];
      const rows = batch.slice(0, 12).map(r=>`
        <div class="handoff-pt"><b>${esc(r.tag)}</b> ${esc(r.value)} ${esc(r.unit||'')}</div>`).join('');
      const more = batch.length > 12 ? `<div class="hs">… ${batch.length - 12} more</div>` : '';
      el.innerHTML = `<div class="handoff">
        <div class="hh">From Connectivity (hop 4 stream) · ${batch.length} datapoint${batch.length===1?'':'s'} · ${tags.length} tag${tags.length===1?'':'s'}</div>
        <div class="handoff-pts">${rows}</div>
        ${more}
      </div>`;
    } else {
      el.innerHTML = `<div class="handoff missing">
        <div class="hh">Waiting on Connectivity</div>
        <div class="hv">No MQTT-confirmed stream is carried yet.</div>
        <div class="hs">Finish Connectivity hop 4, or pull every live tag from that layer — Ingest will not invent sensor values.</div>
        <button type="button" class="btn sm primary" id="iPull" style="margin-top:8px">▶ Pull all Connectivity readings</button>
      </div>`;
      const pull = $('#iPull');
      if(pull) pull.onclick = async () => {
        try{
          let batch = [];
          try{
            const st = await api('/api/connect/status');
            if(st.last_stream && st.last_stream.length) batch = st.last_stream;
          }catch(e){}
          if(!batch.length){
            const em = await api('/api/emit');
            batch = em.readings || [];
          }
          if(!batch.length) throw new Error('Connectivity returned no readings');
          PIPE.readings = batch;
          PIPE.reading = batch[batch.length-1];
          PIPE.event = null;
          paintHandoff();
          paintLocks();
        }catch(err){
          el.insertAdjacentHTML('beforeend', `<div class="note" style="color:var(--coral)">${esc(err.message)}</div>`);
        }
      };
    }
  };

  hopsHost.innerHTML = INGEST_HOPS.map(h=>`
    <div class="cstep" id="istep-${h.n}">
      <div class="cstep-hd">
        <b>${h.n}</b>
        <div>
          <div class="cstep-tt">${esc(h.title)}</div>
          <div class="cstep-tool"><span class="chip free">${esc(h.tool)}</span><span class="note" style="margin:0">${esc(h.stack)}</span></div>
        </div>
      </div>
      <p class="cstep-why">${esc(h.why)}</p>
      <div class="cstep-actions">
        <button type="button" class="btn primary sm istep-btn" data-step="${h.n}">${esc(h.btn)}</button>
        ${INGEST_XLSX[h.n] ? `<a class="btn sm ghost i-xlsx hidden" id="iXlsx-${h.n}" href="/api/ingest/excel/${INGEST_XLSX[h.n]}">↓ Download ${INGEST_XLSX[h.n][0].toUpperCase()+INGEST_XLSX[h.n].slice(1)} Excel</a>` : ''}
      </div>
      <div class="demo-box"><div class="term" id="iOut-${h.n}"><span class="d">// run this hop to see its output</span></div></div>
      <div id="iPrev-${h.n}"></div>
    </div>`).join('');

  const paintLocks = () => {
    const hasBatch = batchOf().length > 0;
    INGEST_HOPS.forEach(h=>{
      const el = $(`#istep-${h.n}`);
      if(!el) return;
      el.classList.remove('open','done','locked');
      const btn = el.querySelector('.istep-btn');
      const needReading = h.n===1 && !hasBatch;
      const xlsx = $(`#iXlsx-${h.n}`);
      if(xlsx) xlsx.classList.toggle('hidden', h.n > hopDone);
      if(h.n <= hopDone){
        el.classList.add('done');
        if(btn) btn.disabled = false;
      } else if(h.n === hopDone + 1 && !needReading){
        el.classList.add('open');
        if(btn) btn.disabled = false;
      } else {
        el.classList.add('locked');
        if(btn) btn.disabled = true;
      }
    });
    if(hopDone >= 5){
      $('#iDone').innerHTML = `<div class="callout green"><span class="cic">✓</span>
        <div class="cx"><b>Medallion complete.</b> The full Connectivity batch is in Kafka, TimescaleDB Bronze, MinIO Bronze, DuckDB Silver and Gold lake.
        Layer 3 (Contextualize) will join these Gold lake rows to MES / SAP / spec — that is a different Gold.</div></div>`;
    } else {
      $('#iDone').innerHTML = '';
      $('#iNext').innerHTML = '';
    }
  };

  const jsonPost = (path, body) => api(path, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(body||{})
  });

  $('#iReset').onclick = async () => {
    hopDone = 0;
    try{ await jsonPost('/api/ingest/reset'); }catch(e){}
    INGEST_HOPS.forEach(h=>{
      const t = $(`#iOut-${h.n}`);
      if(t) t.innerHTML = '<span class="d">// run this hop to see its output</span>';
      const p = $(`#iPrev-${h.n}`);
      if(p) p.innerHTML = '';
    });
    paintLocks();
  };

  hopsHost.querySelectorAll('.istep-btn').forEach(btn=>{
    btn.onclick = async () => {
      const step = Number(btn.dataset.step);
      const term = $(`#iOut-${step}`);
      const prev = $(`#iPrev-${step}`);
      term.innerHTML = `<span class="spin"></span> running hop ${step}…`;
      if(prev) prev.innerHTML = '';
      try{
        const batch = batchOf();
        if(!batch.length) throw new Error('No Connectivity stream — finish hop 4 there, or pull all readings above.');
        const r = await jsonPost(`/api/ingest/step/${step}`, {readings: batch});
        term.innerHTML = ingestLogHtml(r.log);
        if(prev) prev.innerHTML = previewTable(r.preview || r.snapshot);
        hopDone = (r.steps_done != null) ? r.steps_done : Math.max(hopDone, r.step || step);
        refreshRail();
        paintLocks();
      }catch(err){
        term.innerHTML = `<span class="w">${esc(err.message)}</span>`;
      }
    };
  });

  try{
    if(!batchOf().length){
      const st = await api('/api/connect/status');
      if(st.last_stream && st.last_stream.length){
        PIPE.readings = st.last_stream;
        PIPE.reading = st.last_stream[st.last_stream.length-1];
      }
    }
  }catch(e){}

  paintHandoff();
  paintLocks();
}
async function demoVisualize(){
  const kpis = $('#vKpis');
  const tripEl = $('#vTrip');
  const alarmEl = $('#vAlarm');
  if(kpis) kpis.innerHTML = `<span class="spin"></span> joining trip &amp; alarm observations across all parameters…`;
  if(tripEl) tripEl.innerHTML = '';
  if(alarmEl) alarmEl.innerHTML = '';
  let board;
  try{
    board = await api('/api/visualize/merged');
  }catch(err){
    if(kpis) kpis.innerHTML = `<div class="handoff missing"><div class="hv">Could not load merged dataset</div><div class="hs">${esc(err.message)}</div></div>`;
    return;
  }
  const trip = (board.trip && board.trip.events) || [];
  const alarm = (board.alarm && board.alarm.events) || [];
  const pl = board.platform || {};
  const params = board.parameters || [];
  const paramN = params.length;
  if(kpis){
    kpis.innerHTML = `<div class="grid g3 viz-kpis">
      <div class="tile"><div class="td" style="color:var(--slate)">Trip observations</div><div class="viz-kpi-n" style="color:#c45c26">${trip.length}</div><div class="td">one row per reading_id</div></div>
      <div class="tile"><div class="td" style="color:var(--slate)">Alarm observations</div><div class="viz-kpi-n" style="color:var(--amber)">${alarm.length}</div><div class="td">${board.observation_rows||0} readings classified</div></div>
      <div class="tile"><div class="td" style="color:var(--slate)">Pipeline health</div><div class="viz-kpi-n" style="color:var(--green)">${esc(pl.status||'—')}</div><div class="td">lag ${pl.consumer_lag??0} · ${pl.throughput_msg_s??'—'} msg/s</div></div>
    </div>
    <div class="cx-counts viz-param-chips">${params.map(p=>`<span class="cx-chip">${esc(p.short||p.param)} · trip ${p.trip||0} · alarm ${p.alarm||0}</span>`).join('')}</div>`;
  }
  if($('#vTripTag')) $('#vTripTag').textContent = `${trip.length} rows · all parameters`;
  if($('#vAlarmTag')) $('#vAlarmTag').textContent = `${alarm.length} rows · all parameters`;
  if(tripEl) tripEl.innerHTML = paintVizTable(trip, 'trip');
  if(alarmEl) alarmEl.innerHTML = paintVizTable(alarm, 'alarm');
}
function paintVizTable(rows, zone){
  if(!rows || !rows.length){
    return `<div class="handoff missing"><div class="hv">No ${esc(zone)} observations</div><div class="hs">Finish Connectivity / Ingest so this page has floor or Gold-lake rows to join.</div></div>`;
  }
  const cell = v => esc(v==null || v==='' ? '—' : v);
  const body = rows.map(e=>{
    const stCls = e.status==='OK' ? 'ok' : (e.status==='OVER' ? 'over' : 'under');
    const ts = e.timestamp ? String(e.timestamp).replace('T',' ').replace('Z','') : '';
    const spec = Array.isArray(e.spec) ? `[${e.spec.join(', ')}]` : (e.spec || '—');
    const val = e.value==null ? '—' : `${e.value}${e.unit||''}`;
    const cal = e.calibration_age_days==null ? (e.probe_calibration_date || '—') : `${e.calibration_age_days}d`;
    return `<tr>
      <td>${cell(e.reading_id)}</td>
      <td>${cell(ts)}</td>
      <td>${cell(e.short||e.param)}</td>
      <td>${cell(e.tag)}</td>
      <td>${esc(val)}</td>
      <td><span class="cx-status ${stCls}">${cell(e.status)}</span></td>
      <td>${esc(spec)}</td>
      <td>${cell(e.asset)}</td>
      <td>${cell(e.asset_name)}</td>
      <td>${cell(e.line)}</td>
      <td>${cell(e.batch)}</td>
      <td>${cell(e.product)}</td>
      <td>${cell(e.phase)}</td>
      <td>${cell(e.operator_shift)}</td>
      <td>${cell(e.mes_window)}</td>
      <td>${cell(e.material_no)}</td>
      <td>${cell(e.family)}</td>
      <td>${cell(e.grade)}</td>
      <td>${cell(e.spec_source)}</td>
      <td>${cell(cal)}</td>
      <td>${cell(e.last_maintenance)}</td>
      <td>${cell(e.lab_note_ref)}</td>
    </tr>`;
  }).join('');
  return `<div class="cx-obs-wrap viz-table-wrap">
    <table class="preview-table viz-merged">
      <thead>
        <tr>
          <th>reading_id</th><th>timestamp</th><th>param</th><th>tag</th><th>value</th><th>status</th><th>spec</th>
          <th>asset</th><th>name</th><th>line</th>
          <th>batch</th><th>product</th><th>phase</th><th>shift</th><th>window</th>
          <th>material</th><th>family</th><th>grade</th><th>spec src</th>
          <th>cal</th><th>maint</th><th>lab</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  </div>`;
}
async function demoGovern(){
  const pid0 = paramOf(PIPE.reading) || 'ph';
  $('#gParam').innerHTML = PARAMS.map(p=>`<option value="${p.id}" ${p.id===pid0?'selected':''}>${p.short} (${p.tag})</option>`).join('');

  let board = {reading:null, event:null, policy:null};

  const fmtTs = (ts) => {
    if(!ts) return '';
    const s = String(ts).replace('T',' ').replace('Z','');
    return s.length >= 19 ? s.slice(11,19) : s;
  };
  const paintAudit = (entries) => {
    const el = $('#gTerm'); if(!el) return;
    const rows = (entries||[]).slice().reverse();
    if(!rows.length){
      el.innerHTML = `<span class="d">// no audit records yet</span>`;
      return;
    }
    el.innerHTML = rows.map(e=>{
      const param = (e.payload && e.payload.param) || '';
      const h = String(e.hash||'').slice(0,8);
      return `<div><span class="d">${esc(fmtTs(e.ts))}</span> <span class="o">${esc(e.actor)}.${esc(e.action)}</span> <span class="d">param=${esc(param)}</span> <span class="p">#${esc(h)}</span></div>`;
    }).join('');
    el.scrollTop = el.scrollHeight;
  };
  const paintPolicy = (pol) => {
    const p = pol || {};
    const meta = $('#gPolicyMeta');
    if(meta) meta.textContent = `${p.short||p.param||'—'} • ${p.asset||'—'}`;
    const zoneCls = p.zone==='alarm'||p.zone==='trip' ? 'warn' : '';
    const decCls = p.esign_required ? 'warn' : 'ok';
    $('#gPolicyKv').innerHTML = `
      <div class="gov-k">Event</div><div class="gov-v">${esc(p.event_id||'—')}</div>
      <div class="gov-k">${esc(p.short||'Rule')}</div><div class="gov-v">${esc(p.ph_rule||'—')}</div>
      <div class="gov-k">Zone</div><div class="gov-v ${zoneCls}">${esc(p.zone||'—')}</div>
      <div class="gov-k">Threshold rule</div><div class="gov-v">${esc(p.threshold_rule||'—')}</div>
      <div class="gov-k">P(breach)</div><div class="gov-v ${zoneCls}">${p.p_breach==null?'—':p.p_breach}</div>
      <div class="gov-k">Decision</div><div class="gov-v ${decCls}"><b>${esc(p.decision||'—')}</b></div>`;
    const ban = $('#gBanner');
    if(p.esign_required){
      ban.className = 'gov-banner on';
      ban.innerHTML = `<div class="gov-ban-t">✍️ QA E-SIGNATURE REQUIRED</div>
        <div class="gov-ban-s">This action is gated on a 21 CFR Part 11 electronic signature before it can commit.</div>`;
    } else {
      ban.className = 'gov-banner ok';
      ban.innerHTML = `<div class="gov-ban-t">✓ Policy permits autonomous commit</div>
        <div class="gov-ban-s">${esc(p.message||'SENTRA may act, then log to the WORM audit.')}</div>`;
    }
  };
  const syncBtns = () => {
    const ready = !!( $('#gSigner').value.trim() && $('#gMeaning').value.trim() );
    $('#gCommit').disabled = !ready;
    $('#gReject').disabled = !ready;
  };
  const loadBoard = async () => {
    const pid = $('#gParam').value;
    const zone = $('#gZone').value;
    try{
      board = await api(`/api/govern/evaluate?param=${encodeURIComponent(pid)}&zone=${encodeURIComponent(zone)}`);
      PIPE.reading = board.reading;
      PIPE.event = board.event;
      paintPolicy(board.policy);
    }catch(err){
      $('#gPolicyKv').innerHTML = `<div class="gov-k">Error</div><div class="gov-v warn">${esc(err.message)}</div>`;
    }
  };
  const loadAudit = async () => {
    try{
      const a = await api('/api/audit?n=40');
      paintAudit(a.entries||[]);
    }catch(err){
      $('#gTerm').innerHTML = `<span class="w">${esc(err.message)}</span>`;
    }
  };
  const submit = async (action) => {
    const rd = board.reading || PIPE.reading;
    if(!rd){ await loadBoard(); }
    const reading = board.reading || PIPE.reading;
    if(!reading) return;
    $('#gCommit').disabled = true;
    $('#gReject').disabled = true;
    try{
      const r = await api('/api/govern/commit', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          tag: reading.tag, value: reading.value, timestamp: reading.timestamp,
          actor_role: $('#gRole').value,
          signer: $('#gSigner').value.trim(),
          meaning: $('#gMeaning').value.trim(),
          action
        })
      });
      PIPE.event = r.event;
      if(r.policy) paintPolicy(r.policy);
      await loadAudit();
      const d = r.decision || {};
      if(d.committed){
        $('#gNext').innerHTML = `<div class="callout green pipe-next"><span class="cic">✓</span>
          <div class="cx"><b>Signed and committed.</b> Authenticated, policy-checked, written to the hash-chained WORM audit (#${esc(d.audit_seq)}).</div></div>`;
      } else if(d.mode==='REJECTED'){
        $('#gNext').innerHTML = `<div class="callout pipe-next"><span class="cic">✕</span>
          <div class="cx"><b>Rejected.</b> ${esc(d.reason||'Recorded in the WORM audit.')}</div></div>`;
      } else {
        $('#gNext').innerHTML = `<div class="callout pipe-next"><span class="cic">!</span>
          <div class="cx"><b>${esc(d.mode||'Blocked')}.</b> ${esc(d.reason||'')}</div></div>`;
      }
    }catch(err){
      $('#gNext').innerHTML = `<div class="note" style="color:var(--coral)">${esc(err.message)}</div>`;
    }
    syncBtns();
  };

  $('#gSigner').oninput = syncBtns;
  $('#gMeaning').oninput = syncBtns;
  $('#gParam').onchange = () => { loadBoard(); $('#gNext').innerHTML=''; };
  $('#gZone').onchange = () => { loadBoard(); $('#gNext').innerHTML=''; };
  $('#gCommit').onclick = () => submit('commit');
  $('#gReject').onclick = () => submit('reject');
  syncBtns();
  await loadBoard();
  await loadAudit();
}

function demoCopilot(){
  const AGENTS = [
    {id:'temp', ic:'🌡️', name:'Temperature', full:'Temperature Agent', role:'Parameter agent'},
    {id:'ph', ic:'🧪', name:'pH', full:'pH Agent', role:'Parameter agent'},
    {id:'press', ic:'⏲️', name:'Pressure', full:'Pressure Agent', role:'Parameter agent'},
    {id:'cond', ic:'💧', name:'Conductivity', full:'Conductivity Agent', role:'Parameter agent'},
    {id:'hum', ic:'☁️', name:'Humidity', full:'Humidity Agent', role:'Parameter agent'},
    {id:'escalation', ic:'🛡️', name:'Human Escalation', full:'Human Escalation Agent', role:'Override when RAG is not near-perfect'},
  ];
  let current = AGENTS[0];
  let questions = [];
  const history = {};
  let loading = false;

  const zone = () => ($('#cpZone') && $('#cpZone').value) || 'trip';
  const chartQuery = () => current.id === 'escalation'
    ? 'Show a chart of trip vs alarm observations across all parameters'
    : `Show a chart of recent ${current.name} trip vs alarm observations`;

  const turnsToMsgs = (turns) => {
    const msgs = [];
    (turns || []).forEach(t => {
      msgs.push({role:'user', text:t.query});
      msgs.push({
        role:'bot',
        text: t.answer || '',
        hits: t.hits || [],
        chart: t.chart || null,
        source: t.source,
        llm_fallback: !!t.llm_fallback,
        notice: t.llm_fallback
          ? 'RAG (CAPA, Master Index, OEM, Regulatory, SOP) did not return a confident match. This answer was generated with the LLM.'
          : null,
      });
    });
    return msgs;
  };

  const paintPg = (pg) => {
    const el = $('#cpPgHint');
    if(!el) return;
    if(pg && pg.ok){
      el.textContent = `PostgreSQL · ${pg.database || 'mosaic'} @ ${pg.host || 'localhost'}:${pg.port || ''} — every turn stored`;
      el.style.color = '';
    }else{
      el.textContent = 'PostgreSQL unreachable — turns will not persist until the floor database is up.';
      el.style.color = 'var(--coral)';
    }
  };

  const paintSi = (si) => {
    const box = $('#cpSi');
    if(!box) return;
    if(!si || !si.reading_id){
      box.innerHTML = `<div class="cp-si-empty">No automated ${esc(zone())} remedy yet for this agent. Run <b>SCADA Intelligence Platform</b> on this parameter’s ${esc(zone())} first — Co-pilot will then suggest questions from that interaction.</div>`;
      return;
    }
    const rem = si.remedy || {};
    const cites = (si.citations || []).slice(0,5).map(c =>
      `<span class="cp-cite">${esc(c.ref || c.type || 'src')}</span>`).join('');
    const val = (si.val == null || si.val === '') ? '—' : `${si.val}${si.unit || ''}`;
    box.innerHTML = `
      <div class="cp-si-card">
        <div class="cp-si-meta">
          <span><em>reading</em> ${esc(si.reading_id)}</span>
          <span><em>zone</em> ${esc(si.zone || zone())}</span>
          <span><em>tag</em> ${esc(si.tag || '—')} ${esc(val)}</span>
          <span><em>status</em> ${esc(si.status || '—')}</span>
        </div>
        <p class="cp-si-sum">${esc(rem.summary || 'Remedy recorded — open a suggested question to inspect CAPA / Master Index / OEM / Regulatory / SOP grounding.')}</p>
        ${rem.grounded_on ? `<div class="cp-si-ground">Grounded on ${esc(rem.grounded_on)}${rem.confidence != null ? ` · confidence ${esc(rem.confidence)}` : ''}</div>` : ''}
        ${cites ? `<div class="cp-cites">${cites}</div>` : ''}
      </div>`;
  };

  const paintQs = (qs) => {
    questions = (qs && qs.length) ? qs.slice(0,5) : [];
    const lbl = $('#cpQLbl');
    if(lbl) lbl.textContent = `Top expected ${current.name} ${zone()} queries`;
    if(!questions.length){
      $('#cpQList').innerHTML = `<div class="cp-si-empty">Questions will appear after SCADA Intelligence runs for this agent’s ${esc(zone())}.</div>`;
      return;
    }
    $('#cpQList').innerHTML = questions.map((q,i)=>
      `<button type="button" class="cp-q" data-i="${i}"><span class="cp-q-n">${i+1}</span><span>${esc(q)}</span></button>`).join('');
    $('#cpQList').querySelectorAll('.cp-q').forEach(b=>{
      b.onclick = () => ask(questions[Number(b.dataset.i)]);
    });
  };

  const paintLog = () => {
    const log = $('#cpLog');
    const rows = history[current.id] || [];
    if(!rows.length){
      log.innerHTML = `<div class="cp-ph">Review the last SCADA Intelligence remedy on the left, then pick a suggested question or type your own. Retrieval order: CAPA → Master Index → OEM → Regulatory → SOP. Human Escalation is used when a parameter agent cannot ground a near-perfect answer.</div>`;
      return;
    }
    log.innerHTML = rows.map(m=>{
      if(m.role==='user') return `<div class="cp-msg user"><div class="cp-bubble">${esc(m.text)}</div></div>`;
      let chart = '';
      if(m.chart && m.chart.length){
        const max = Math.max(...m.chart.map(c=>Number(c.value)||0), 1);
        chart = `<div class="cp-chart"><div class="cp-chart-hd">Trip vs alarm observations</div>${m.chart.map(c=>`
          <div class="cp-bar-row"><span>${esc(c.label)}</span>
            <i><b style="width:${Math.round(100*(Number(c.value)||0)/max)}%"></b></i>
            <em>${esc(c.value)}</em></div>`).join('')}</div>`;
      }
      const cites = (m.hits||[]).slice(0,6).map(h=>
        `<span class="cp-cite">${esc((h.type||'').toString().replace('_',' '))} · ${esc(h.ref||h.id||'src')}</span>`).join('');
      const notice = m.notice
        ? `<div class="cp-notice"><b>${m.llm_fallback ? 'LLM fallback' : 'Notice'}</b> — ${esc(m.notice)}</div>`
        : '';
      const src = m.source === 'llm'
        ? `<span class="cp-src llm">LLM</span>`
        : m.source === 'rag'
          ? `<span class="cp-src rag">RAG</span>`
          : '';
      const escBtn = m.suggest_escalation
        ? `<button type="button" class="cp-esc">Switch to Human Escalation Agent</button>`
        : '';
      return `<div class="cp-msg bot"><div class="cp-bubble">${src}${esc(m.text).replace(/\n/g,'<br>')}${chart}${notice}${cites?`<div class="cp-cites">${cites}</div>`:''}${escBtn}</div></div>`;
    }).join('');
    log.scrollTop = log.scrollHeight;
  };

  const loadContext = async () => {
    try{
      const r = await api(`/api/copilot/context?agent=${encodeURIComponent(current.id)}&zone=${encodeURIComponent(zone())}`);
      paintPg(r.postgres);
      paintSi(r.last_si);
      paintQs(r.questions);
      history[current.id] = turnsToMsgs(r.history);
      paintLog();
    }catch(err){
      paintSi(null);
      paintQs([]);
      $('#cpLog').innerHTML = `<div class="cp-ph">${esc(err.message || 'Could not load Co-pilot context.')}</div>`;
    }
  };

  const selectAgent = async (id) => {
    current = AGENTS.find(a=>a.id===id) || AGENTS[0];
    $('#cpTabs').querySelectorAll('.cp-tab').forEach(b=>b.classList.toggle('on', b.dataset.id===current.id));
    $('#cpAgentIc').textContent = current.ic;
    $('#cpAgentNm').textContent = current.full;
    const sub = $('#cpAgentSub');
    if(sub) sub.textContent = current.id === 'escalation'
      ? 'Override · used when a parameter agent cannot ground a near-perfect answer'
      : 'Parameter agent · answers from RAG first';
    await loadContext();
  };

  const ask = async (text, wantChart) => {
    const q = (text || $('#cpInput').value || '').trim();
    if(!q || loading) return;
    $('#cpInput').value = '';
    history[current.id] = history[current.id] || [];
    history[current.id].push({role:'user', text:q});
    const pending = {
      role:'bot',
      text:'Retrieving CAPA / Master Index / OEM / Regulatory / SOP…',
      hits:[],
    };
    history[current.id].push(pending);
    paintLog();
    loading = true;
    try{
      const r = await api('/api/copilot/chat', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          query:q, agent:current.id, zone:zone(),
          want_chart: !!wantChart || /chart|plot|distribution/i.test(q),
        })
      });
      pending.text = r.answer || 'No answer.';
      pending.hits = r.hits || [];
      pending.chart = r.chart || null;
      pending.source = r.source;
      pending.llm_fallback = !!r.llm_fallback;
      pending.notice = r.notice || null;
      pending.suggest_escalation = !!r.suggest_escalation;
      if(r.last_si) paintSi(r.last_si);
    }catch(err){
      pending.text = err.message || 'Co-pilot request failed.';
    }
    loading = false;
    paintLog();
  };

  $('#cpTabs').innerHTML = AGENTS.map(a=>
    `<button type="button" class="cp-tab${a.id==='escalation'?' esc':''}" data-id="${a.id}">
       <span class="cp-tab-ic">${a.ic}</span>
       <span class="cp-tab-nm">${esc(a.name)}</span>
     </button>`).join('');
  $('#cpTabs').querySelectorAll('.cp-tab').forEach(b=>{
    b.onclick = () => selectAgent(b.dataset.id);
  });
  $('#cpSend').onclick = () => ask();
  $('#cpChartBtn').onclick = () => ask(chartQuery(), true);
  $('#cpInput').onkeydown = (ev) => { if(ev.key==='Enter'){ ev.preventDefault(); ask(); } };
  $('#cpZone').onchange = () => loadContext();
  $('#cpLog').onclick = (ev) => {
    const btn = ev.target.closest('.cp-esc');
    if(btn) selectAgent('escalation');
  };
  selectAgent('temp');
}

/* ---------- CONTEXTUALIZE layer ---------- */
const CX_KINDS = [
  {id:'asset', ic:'🗺️', label:'Asset Model', key:'tag', hint:'tag, asset, name, unit, control_lo, control_hi, line'},
  {id:'mes',   ic:'🏭', label:'MES',         key:'asset', hint:'asset, batch, product, phase, start, end, operator_shift'},
  {id:'sap',   ic:'🗄️', label:'SAP',         key:'product', hint:'product, material_no, family, grade, equipment, spec_source'},
  {id:'rdbms', ic:'💾', label:'RDBMS / Files', key:'tag', hint:'tag, probe_calibration_date, calibration_age_days, last_maintenance, lab_note_ref'},
];
VIEWS.contextualize = () => `
  <div class="context-page layer-page">
  ${pipeTrack('contextualize')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow core"><span class="n">CORE · PHASE 3</span> Platform Layer</div></div>
    <h1>Contextualize</h1>
    <p class="lead">Gold-lake readings from Ingest are joined to Asset, MES, SAP and RDBMS. Grain: <b>reading_id</b> (one unique row). Mapping keys: <b>tag</b> (Asset, RDBMS) · <b>asset + timestamp</b> (MES) · <b>product</b> (SAP).</p>
  </header>
  <div class="page-brief"><span class="lbl">What this layer does</span>Select parameter and scenario, then Next Flink step or Auto-play. Consume → Enrich → Compute → Emit process every matching <b>reading_id</b> through the four sources. Map Gold Lake opens the unique merged dataset.</div>
  <div id="cxHandoff"></div>

  <div class="card tight src-card" id="cxIntegrate">
    <div class="cx-toolbar">
      <div class="cx-int-head">Data Source Integration — Asset, MES, SAP &amp; RDBMS/Files</div>
      <label class="btn amber cx-file-btn" id="cxUpLbl">Upload files
        <input type="file" id="cxMultiFile" multiple>
      </label>
    </div>
    <div class="save-row" id="cxPgBar">
      <span id="cxSaveHint" class="note" style="margin:0">Upload one or more files of any type (Excel, CSV, JSON, PDF, images, …). Each file becomes a source card.</span>
    </div>
    <div id="cxSlots" class="cx-slots"></div>
    <div id="cxSrcStatus" class="note"></div>
  </div>

  <div class="card tight core">
    <h3><span class="ic">⚙️</span> Flink join chain <span class="tag">observations → four sources</span></h3>
    <div class="c-ctrl">
      <label class="fl">Parameter</label><select class="inp" id="jcParam"></select>
      <label class="fl">Scenario</label>
      <select class="inp" id="jcZone">
        <option value="trip">excursion (trip)</option>
        <option value="alarm">alarm</option>
        <option value="control">in-spec (control)</option>
      </select>
    </div>
    <div id="cxCounts"></div>
    <div id="jcHost2"></div>
    <div id="cxMapSlot"></div>
    <div id="cxMerged"></div>
    <div id="cxNext"></div>
  </div>
  </div>`;
AFTER.contextualize = () => {
  const pid = paramOf(PIPE.reading) || 'temp';
  $('#jcParam').innerHTML = PARAMS.map(p=>`<option value="${p.id}" ${p.id===pid?'selected':''}>${p.short} (${p.tag})</option>`).join('');

  const KIND_ORDER = {asset:0, mes:1, sap:2, rdbms:3, other:4};
  const listSources = (st) => {
    const sources = (st && st.sources) || {};
    return Object.keys(sources).map(id => ({id, ...sources[id]})).sort((a,b)=>{
      const ka = KIND_ORDER[a.kind] ?? 9;
      const kb = KIND_ORDER[b.kind] ?? 9;
      if(ka !== kb) return ka - kb;
      return String(a.filename||a.id).localeCompare(String(b.filename||b.id));
    });
  };

  const slotHtml = (src) => {
    const loaded = !!src.loaded;
    const persisted = !!src.persisted;
    const name = src.filename ? esc(src.filename) : 'no file';
    const n = src.row_count || 0;
    const cols = (src.columns && src.columns.length) ? src.columns.join(', ') : (src.hint || '');
    const ext = src.extension || (src.is_file ? 'file' : 'table');
    const size = src.size_bytes ? ` · ${src.size_bytes} bytes` : '';
    const state = persisted
      ? `saved · ${src.is_file ? ext + size : n + ' rows'}`
      : (loaded ? (src.is_file ? `${ext}${size} in memory` : `${n} rows in memory`) : 'awaiting upload');
    const key = src.key ? `key: ${esc(src.key)}` : (src.is_file ? esc(ext) : '');
    const thumb = src.preview
      ? `<img class="cx-thumb" src="${esc(src.preview)}" alt="${name}">`
      : '';
    const colLine = src.is_file && !cols
      ? `<p class="hint cx-cols"><b>File</b> ${esc(src.content_type || ext)}${esc(size)}</p>`
      : `<p class="hint cx-cols"><b>${src.is_file ? 'Fields' : 'Columns'}</b> ${esc(cols || src.content_type || ext)}</p>`;
    return `<div class="cx-slot${loaded?' ready':''}${persisted?' saved':''}" data-slot="${esc(src.slot || src.id)}">
      <h4>${src.ic || '📄'} ${esc(src.label || src.kind || 'Source')} <span class="cx-key">${key}</span></h4>
      ${thumb}
      <p class="hint cx-desc">${esc(src.description || '')}</p>
      ${colLine}
      <div class="cx-slot-bar">
        <label class="btn sm ghost cx-file-btn">Replace
          <input type="file" class="cx-replace" data-slot="${esc(src.slot || src.id)}">
        </label>
        <button type="button" class="btn save sm cx-save" data-slot="${esc(src.slot || src.id)}" ${loaded?'':'disabled'}>Save</button>
        <button type="button" class="btn sm delete cx-del" data-slot="${esc(src.slot || src.id)}">Delete</button>
        <span class="cx-slot-state">${esc(state)} · ${name}</span>
      </div>
    </div>`;
  };

  const paintSources = (st) => {
    const rows = listSources(st);
    const box = $('#cxSlots');
    if(!rows.length){
      box.innerHTML = `<div class="cx-empty">No sources yet. Use <b>Upload files</b> — any extension is allowed (Excel, CSV, JSON, PDF, images, and others). Each file becomes a card; tabular files are classified as Asset, MES, SAP or RDBMS when the columns match.</div>`;
    } else {
      box.innerHTML = rows.map(slotHtml).join('');
    }
    const loadedN = rows.filter(s => s.loaded).length;
    const savedN = rows.filter(s => s.persisted).length;
    const hint = $('#cxSaveHint');
    const bar = $('#cxPgBar');
    if(hint){
      if(savedN && savedN === loadedN){
        hint.innerHTML = `<span class="note ok" style="margin:0"><b>✓ Saved in PostgreSQL</b> — ${savedN} source(s) stored. They reload automatically.</span>`;
        if(bar) bar.classList.add('on');
      } else if(loadedN){
        hint.textContent = `${loadedN} source card(s) in memory. Save each card to keep it in PostgreSQL.`;
        if(bar) bar.classList.remove('on');
      } else {
        hint.textContent = 'Upload one or more files of any type (Excel, CSV, JSON, PDF, images, …). Each file becomes a source card.';
        if(bar) bar.classList.remove('on');
      }
    }
    box.querySelectorAll('.cx-replace').forEach(inp=>{
      inp.onchange = async (ev) => {
        const f = ev.target.files && ev.target.files[0]; if(!f) return;
        const slot = inp.dataset.slot;
        $('#cxSrcStatus').innerHTML = `<span class="spin"></span> replacing ${esc(f.name)}…`;
        const fd = new FormData();
        fd.append('file', f, f.name);
        try{
          const next = await api(`/api/context/replace/${encodeURIComponent(slot)}`, {method:'POST', body: fd});
          $('#cxSrcStatus').innerHTML = `<span class="note ok">Replaced with <b>${esc(f.name)}</b>.</span>`;
          paintSources(next);
          prepareJoin();
        }catch(err){
          $('#cxSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
        }
      };
    });
    box.querySelectorAll('.cx-save').forEach(btn=>{
      btn.onclick = async () => {
        const slot = btn.dataset.slot;
        btn.disabled = true;
        $('#cxSrcStatus').innerHTML = `<span class="spin"></span> saving to PostgreSQL…`;
        try{
          const next = await api(`/api/context/save/${encodeURIComponent(slot)}`, {method:'POST'});
          $('#cxSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Saved.')}</span>`;
          paintSources(next);
          prepareJoin();
        }catch(err){
          $('#cxSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
          btn.disabled = false;
        }
      };
    });
    box.querySelectorAll('.cx-del').forEach(btn=>{
      btn.onclick = async () => {
        const slot = btn.dataset.slot;
        const card = rows.find(s => (s.slot||s.id) === slot);
        const name = (card && card.filename) || slot;
        if(!confirm(`Permanently delete ${name} from PostgreSQL?\n\nYou will need to upload this file again.`)) return;
        btn.disabled = true;
        $('#cxSrcStatus').innerHTML = `<span class="spin"></span> deleting from PostgreSQL…`;
        try{
          const next = await api(`/api/context/delete/${encodeURIComponent(slot)}`, {method:'POST'});
          $('#cxSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Deleted.')}</span>`;
          paintSources(next);
          prepareJoin();
        }catch(err){
          $('#cxSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
          btn.disabled = false;
        }
      };
    });
  };

  const paintHandoff = (lake) => {
    const el = $('#cxHandoff'); if(!el) return;
    if(lake && lake.ready){
      const pts = (lake.snapshot||[]).map(r=>
        `<div class="handoff-pt"><b>${esc(r.tag)}</b> ${esc(r.value)} ${esc(r.unit||'')} <span class="note" style="margin:0">${esc(r.param||'')}</span></div>`
      ).join('');
      el.innerHTML = `<div class="handoff">
        <div class="hv">Ingest Gold lake → Contextualize</div>
        <div class="hs">${lake.count} readings · ${lake.tags} tags · latest-per-tag snapshot used as Flink input</div>
        <div class="handoff-pts">${pts}</div>
      </div>`;
    } else {
      el.innerHTML = `<div class="handoff missing">
        <div class="hv">No Gold lake yet</div>
        <div class="hs">Finish Ingest hop 5 so this page can map live readings. Until then the join chain can still run on a Connectivity reading.</div>
      </div>`;
    }
  };

  const prepareJoin = async () => {
    if($('#cxMerged')) $('#cxMerged').innerHTML = '';
    if($('#cxMapSlot')) $('#cxMapSlot').innerHTML = '';
    await mountJoinChain('jcHost2', $('#jcParam').value, $('#jcZone').value);
    if($('#cxNext')) $('#cxNext').innerHTML = '';
  };

  $('#cxMultiFile').onchange = async (ev) => {
    const files = ev.target.files ? Array.from(ev.target.files) : [];
    if(!files.length) return;
    $('#cxSrcStatus').innerHTML = `<span class="spin"></span> reading ${files.length} file(s)…`;
    const fd = new FormData();
    files.forEach(f => fd.append('files', f, f.name));
    try{
      const next = await api('/api/context/upload', {method:'POST', body: fd});
      const extra = (next.errors && next.errors.length) ? ` · ${next.errors.join('; ')}` : '';
      $('#cxSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Loaded.')}${esc(extra)}</span>`;
      paintSources(next);
      prepareJoin();
    }catch(err){
      $('#cxSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
    }
    ev.target.value = '';
  };
  $('#jcParam').onchange = prepareJoin;
  $('#jcZone').onchange = prepareJoin;

  (async () => {
    try{
      const st = await api('/api/context/status');
      paintSources(st);
      paintHandoff(st.gold_lake);
    }catch(e){
      paintSources({});
      paintHandoff(null);
    }
    prepareJoin();
  })();
};

/* ---------- the join-chain widget (fed by the real API trace) ---------- */
function paintCounts(res, pid, zone){
  const countsEl = $('#cxCounts'); if(!countsEl) return;
  const evs = uniqueEvents((res && res.events) || (res && res.event ? [res.event] : []));
  const counts = (res && res.counts) || {};
  const p = PARAMS.find(x=>x.id===pid) || {};
  const tag = (res && res.tag) || p.tag || '';
  const zoneLbl = {trip:'trip', alarm:'alarm', control:'in-spec'}[zone] || zone;
  const total = counts.total || 0;
  countsEl.innerHTML = `<div class="cx-counts">
      <span><b>${esc(tag||p.short||pid)}</b> · ${total} observation${total===1?'':'s'}</span>
      <span class="cx-chip control">control ${counts.control||0}</span>
      <span class="cx-chip alarm">alarm ${counts.alarm||0}</span>
      <span class="cx-chip trip">trip ${counts.trip||0}</span>
      <span class="cx-chip on">${esc(zoneLbl)} to process · ${evs.length}</span>
    </div>`;
}
function uniqueEvents(evs){
  const seen = new Set();
  const out = [];
  const grain = (e) => {
    const tag = String(e.tag||'').trim();
    let raw = e.val!=null ? e.val : e.value;
    if(typeof raw === 'string') raw = raw.trim().replace(/%$/,'').replace(/,/g,'');
    const val = (raw==null || raw==='') ? '' : (Number.isFinite(Number(raw)) ? Number(raw).toFixed(4) : String(raw));
    let ts = String(e.timestamp||e.ts||'').replace('T',' ').replace(/Z/gi,'').trim();
    ts = ts.replace(/\s*UTC$/i,'').replace(/[+-]\d{2}:?\d{2}$/,'').trim();
    if(ts.includes('.')) ts = ts.split('.')[0];
    ts = ts.slice(0,19);
    return `${tag}|${val}|${ts}`;
  };
  (evs||[]).forEach(e=>{
    const k = grain(e);
    if(seen.has(k)) return;
    seen.add(k);
    out.push(e);
  });
  return out;
}
function paintMergedEvent(res, pid, zone){
  const el = $('#cxMerged'); if(!el) return;
  const evs = uniqueEvents((res && res.events) || (res && res.event ? [res.event] : []));
  const p = PARAMS.find(x=>x.id===pid) || {};
  const tag = (res && res.tag) || p.tag || '';
  const zoneLbl = {trip:'trip', alarm:'alarm', control:'in-spec'}[zone] || zone;
  if(res && res.error && !evs.length){
    el.innerHTML = `<div class="handoff missing"><div class="hv">No ${esc(zoneLbl)} observations</div><div class="hs">${esc(res.error)}</div></div>`;
    return;
  }
  if(!evs.length){ el.innerHTML = ''; return; }
  const cell = v => esc(v==null || v==='' ? '—' : v);
  const rows = evs.map(e=>{
    const stCls = e.status==='OK' ? 'ok' : (e.status==='OVER' ? 'over' : 'under');
    const ts = e.timestamp ? String(e.timestamp).replace('T',' ').replace('Z','') : '';
    const val = e.val==null && e.value==null ? '—' : `${e.val??e.value}${e.unit||''}`;
    return `<tr>
      <td>${cell(e.reading_id)}</td>
      <td>${cell(ts)}</td>
      <td>${cell(e.tag)}</td>
      <td>${esc(val)}</td>
      <td><span class="cx-status ${stCls}">${cell(e.status)}</span></td>
      <td>${cell(e.asset)}</td>
      <td>${cell(e.batch)}</td>
      <td>${cell(e.product)}</td>
      <td>${cell(e.material_no)}</td>
      <td>[${(e.spec||[]).join(', ')}]</td>
      <td>${e.calibration_age_days==null?'—':e.calibration_age_days+'d'}</td>
    </tr>`;
  }).join('');
  el.innerHTML = `<div class="cx-merged">
    <div class="cx-merged-hd">
      <div>
        <div class="cx-merged-k">Merged contextualized dataset</div>
        <div class="cx-merged-t">${esc(p.short||pid)} · ${esc(zoneLbl)} · <b>${evs.length}</b> unique observation${evs.length===1?'':'s'} (one row per tag + timestamp + value)</div>
      </div>
    </div>
    <div class="cx-obs-wrap">
      <table class="preview-table viz-merged">
        <thead><tr><th>reading_id</th><th>timestamp</th><th>tag</th><th>value</th><th>status</th><th>asset</th><th>batch</th><th>product</th><th>material</th><th>spec</th><th>cal</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  </div>`;
}
async function mountJoinChain(hostId, pid, zone){
  const host = $('#'+hostId); if(!host) return;
  const store = hostId === 'jcHost2';
  if(!store){
    host.innerHTML = `<div style="text-align:center;padding:12px"><span class="spin"></span> joining…</div>`;
    try{
      const res = await api(`/api/contextualize/scenario?param=${encodeURIComponent(pid)}&zone=${encodeURIComponent(zone||'trip')}&store=false`);
      const rd = res.reading || {tag: pid, value: null};
      renderJoinChain(host, rd, res);
    }catch(err){
      host.innerHTML = `<div class="note" style="color:var(--coral)">${esc(err.message)}</div>`;
    }
    return;
  }
  host.innerHTML = `<div style="text-align:center;padding:12px"><span class="spin"></span> pulling observations · joining ${esc(zone||'trip')}…</div>`;
  let res;
  try{
    res = await api(`/api/contextualize/observations?param=${encodeURIComponent(pid)}&zone=${encodeURIComponent(zone||'trip')}`);
  }catch(err){
    host.innerHTML = `<div class="note" style="color:var(--coral)">${esc(err.message)}</div>`;
    paintCounts({error: err.message}, pid, zone);
    return;
  }
  const rd = res.reading || {tag: res.tag || pid, value: null};
  PIPE.reading = rd;
  if(res.event) PIPE.event = res.event;
  paintCounts(res, pid, zone);
  if(!res.event){
    host.innerHTML = `<div class="note">Select a scenario that has observations, then press Next Flink step or Auto-play. ${esc(res.error||'')}</div>`;
    return;
  }
  renderJoinChain(host, rd, res);
}
function renderJoinChain(host, rd, res){
  const ev = res.event;
  const specTxt = ev && Array.isArray(ev.spec) ? ev.spec.join(', ') : '';
  const err = res.error ? `<div class="handoff missing" style="margin-bottom:8px"><div class="hv">Join incomplete</div><div class="hs">${esc(res.error)}</div></div>` : '';
  const gated = !!$('#cxMapSlot');
  const n = uniqueEvents((res.events && res.events.length) ? res.events : (ev?[ev]:[])).length || 1;
  const rid = (rd && rd.reading_id) || (ev && ev.reading_id) || 'reading_id';
  host.innerHTML = err + `
  <div class="jc-controls">
    <button class="btn amber sm" id="jcStep">▶ Next Flink step</button>
    <button class="btn primary sm" id="jcAuto">⏩ Auto-play</button>
    <button class="btn ghost sm" id="jcReset">↺ Reset</button>
    <span class="jc-ind" id="jcInd">step 0 / 4</span></div>
  ${gated ? `<p class="note jc-start-hint" id="jcHint">1–2. Parameter and scenario are set. 3. Press <b>Next Flink step</b> or <b>Auto-play</b>. Consume → Enrich → Compute → Emit then process all <b>${n}</b> unique reading_id${n===1?'':'s'} through Asset, MES, SAP and RDBMS. Map Gold Lake appears after Emit.</p>` : ''}
  <div id="jcPipeline" class="jc-pipeline${gated?' hidden':''}">
  <div class="jc-flinksteps">
    <div class="jc-fstep" id="jcf-0"><div class="fl">A</div><div class="fn">Consume</div><div class="fd">read signal</div></div>
    <div class="jc-fstep" id="jcf-1"><div class="fl">B</div><div class="fn">Enrich</div><div class="fd">keyed lookups</div></div>
    <div class="jc-fstep" id="jcf-2"><div class="fl">C</div><div class="fn">Compute</div><div class="fd">derive status</div></div>
    <div class="jc-fstep" id="jcf-3"><div class="fl">D</div><div class="fn">Emit</div><div class="fd">event out</div></div></div>
  <div class="jc-grid">
    <div>
      <div class="jc-carrier"><div class="jc-lbl">Record flowing through Flink — primary key reading_id</div><div class="jc-keys" id="jcKeys"></div></div>
      <div class="jc-lbl">Sources — matched by key (step B), one join per unique reading_id</div>
      <div class="jc-src" id="jcs-asset"><div class="jc-sh"><div class="jc-sic">🗺️</div><div><div class="jc-sn">Asset Model</div><div class="jc-sq">what is this tag?</div></div><div class="jc-skey">match on<br><b>tag</b></div></div>
        <div class="jc-sio"><div><span class="lk">lookup</span> <span class="q">asset_model["${rd.tag}"]</span></div><div><span class="ar">↳</span> <span class="rt" id="jcr-asset"></span></div></div></div>
      <div class="jc-src mes" id="jcs-mes"><div class="jc-sh"><div class="jc-sic">🏭</div><div><div class="jc-sn">MES <span class="jc-btime">+ TIME</span></div><div class="jc-sq">what's running now?</div></div><div class="jc-skey">match on<br><b>asset + ts</b></div></div>
        <div class="jc-sio"><div><span class="lk">lookup</span> <span class="q">mes["${ev?ev.asset:'?'}" @ ts]</span></div><div><span class="ar">↳</span> <span class="rt" id="jcr-mes"></span></div></div></div>
      <div class="jc-src" id="jcs-sap"><div class="jc-sh"><div class="jc-sic">🗄️</div><div><div class="jc-sn">SAP</div><div class="jc-sq">business meaning?</div></div><div class="jc-skey">match on<br><b>product</b></div></div>
        <div class="jc-sio"><div><span class="lk">lookup</span> <span class="q">sap_master["${ev?ev.product:'?'}"]</span></div><div><span class="ar">↳</span> <span class="rt" id="jcr-sap"></span></div></div></div>
      <div class="jc-src" id="jcs-rdbms"><div class="jc-sh"><div class="jc-sic">💾</div><div><div class="jc-sn">RDBMS / Files</div><div class="jc-sq">supporting context</div></div><div class="jc-skey">match on<br><b>tag</b></div></div>
        <div class="jc-sio"><div><span class="lk">lookup</span> <span class="q">calibration_db["${rd.tag}"]</span></div><div><span class="ar">↳</span> <span class="rt" id="jcr-rdbms"></span></div></div></div>
    </div>
    <div>
      <div class="jc-lbl">What Flink is doing</div>
      <div class="jc-narr"><div class="nt">Narration</div><div class="nb" id="jcNarr">Press <b>Next Flink step</b> or <b>Auto-play</b>. Consume → Enrich → Compute → Emit stay hidden until then.</div></div>
      <div class="jc-lbl">The event, assembling field by field</div>
      <div class="jc-ev"><div class="jc-et">▶ contextualized event</div>
        <div class="jc-ef on" id="jcf-rid"><span class="k">reading_id</span><span class="v">"${esc(rid)}"</span><span class="fr">primary key</span></div>
        <div class="jc-ef on" id="jcf-val"><span class="k">val</span><span class="v">${ev?ev.val:rd.value}</span><span class="fr">raw signal</span></div>
        <div class="jc-ef" id="jcf-asset"><span class="k">asset</span><span class="v">"${ev?ev.asset:''}"</span><span class="fr">← Asset Model</span></div>
        <div class="jc-ef" id="jcf-spec"><span class="k">spec</span><span class="v">[${specTxt}]</span><span class="fr">← Asset/SAP</span></div>
        <div class="jc-ef" id="jcf-batch"><span class="k">batch</span><span class="v">"${ev?ev.batch:''}"</span><span class="fr">← MES (temporal)</span></div>
        <div class="jc-ef" id="jcf-product"><span class="k">product</span><span class="v">"${ev?ev.product:''}"</span><span class="fr">← MES</span></div>
        <div class="jc-ef computed" id="jcf-status"><span class="k">status</span><span class="v">"${ev?ev.status:''}"</span><span class="fr">⚙ Flink computes</span></div></div>
    </div></div>
  <details class="cx-sql-wrap">
    <summary>Flink SQL — grain is reading_id; ON clauses are the lookup keys</summary>
    <div class="jc-sql"><span class="cm">-- unique row grain: reading_id · applied to every matching observation</span>
<span class="kw">SELECT</span> r.reading_id, r.value, a.asset, a.spec, m.batch, m.product, s.material
<span class="kw">FROM</span> readings r
<span class="kw">JOIN</span> asset_model a <span class="on">ON</span> <span id="sqlk1">r.tag = a.tag</span>                    <span class="cm">-- tag → asset</span>
<span class="kw">JOIN</span> mes_batches m <span class="on">ON</span> <span id="sqlk2">a.asset = m.asset <span class="kw">AND</span> r.ts <span class="kw">BETWEEN</span> m.start <span class="kw">AND</span> m.end</span>  <span class="cm">-- asset + TIME</span>
<span class="kw">JOIN</span> sap_master s <span class="on">ON</span> <span id="sqlk3">m.product = s.product</span>            <span class="cm">-- product</span>
<span class="kw">WHERE</span> r.scenario = '${esc(res.scenario||'trip')}'</div>
  </details>
  <div class="jc-final" id="jcFinal"><b>That's the mechanism.</b> Grain <code style="font-family:var(--mono);color:var(--cyan)">${esc(rid)}</code> — ${n} unique row(s) joined through Asset → MES → SAP → RDBMS. Map Gold Lake to open the full merged dataset.</div>
  </div>`;
  wireJoinChain(rd, res);
}

/* ---------- join-chain step logic (uses the real trace) ---------- */
function wireJoinChain(rd, res){
  const ev = res.event, trace = res.trace || [];
  const uniq = uniqueEvents((res.events && res.events.length) ? res.events : (ev?[ev]:[]));
  const n = uniq.length || 1;
  const nLbl = n===1 ? '1 unique reading_id' : `${n} unique reading_id rows`;
  const rid = (rd && rd.reading_id) || (ev && ev.reading_id) || '';
  const bAsset = trace.find(t=>t.sub==='B.1');
  const bMes   = trace.find(t=>t.sub==='B.2');
  const bSap   = trace.find(t=>t.sub==='B.3');
  const bRd    = trace.find(t=>t.sub==='B.4');
  if(bAsset) $('#jcr-asset').textContent = JSON.stringify(bAsset.returns);
  if(bMes)   $('#jcr-mes').textContent   = JSON.stringify({batch:bMes.returns.batch,product:bMes.returns.product});
  if(bSap)   $('#jcr-sap').textContent   = JSON.stringify({material_no:bSap.returns.material_no});
  if(bRd)    $('#jcr-rdbms').textContent = JSON.stringify(bRd.returns);

  const STEPS = [
    {flink:0, src:null, keys:[], fields:[], sql:null,
     narr:`<b>A — Consume.</b> Flink reads <b>${nLbl}</b> for this scenario. Grain is <code>reading_id</code>. First row: <code>{ reading_id:"${esc(rid)}", tag:"${rd.tag}", value:${rd.value} }</code>. All ${n} row(s) enter the same pipeline.`},
    {flink:1, src:'all', keys:ev?[{k:'asset',v:ev.asset},{k:'batch',v:ev.batch},{k:'product',v:ev.product}]:[],
     fields:['jcf-asset','jcf-spec','jcf-batch','jcf-product'], sql:'sqlk1',
     narr:`<b>B — Enrich.</b> Every unique reading_id is joined to all four sources: Asset on <code>tag</code>, MES on <code>asset + timestamp</code>, SAP on <code>product</code>, RDBMS on <code>tag</code>. ${n} row(s) × 4 lookups.`},
    {flink:2, src:null, keys:[], fields:['jcf-status'], sql:null,
     narr:`<b>C — Compute.</b> Each of the ${n} value(s) is compared to spec → status. Example: <b>${ev?ev.status:'?'}</b>${ev&&ev.delta?` by ${ev.delta}${ev.unit}`:''}.`},
    {flink:3, src:null, keys:[], fields:[], sql:null, done:true,
     narr:`<b>D — Emit.</b> Flink emits <b>${nLbl}</b> as contextualized events. Map Gold Lake is ready — it opens the unique merged dataset for this scenario.`},
  ];
  let i=0;
  const BASE=[{k:'reading_id',v:rid||'—',cls:''},{k:'tag',v:rd.tag,cls:''},{k:'timestamp',v:'ts',cls:'time'}];
  const pid = res.param || '';
  const zone = res.scenario || '';
  function keys(fresh){
    const all=[...BASE]; for(let s=0;s<i;s++) (STEPS[s].keys||[]).forEach(k=>all.push({k:k.k,v:k.v,cls:''}));
    $('#jcKeys').innerHTML = all.map((x,idx)=>`<span class="jc-key ${x.cls} ${fresh&&idx>=all.length-fresh?'fresh':''}"><span class="kt">${x.k}</span>${esc(x.v)}</span>`).join('');
  }
  function revealPipeline(){
    const pipe = $('#jcPipeline');
    if(pipe) pipe.classList.remove('hidden');
    const hint = $('#jcHint');
    if(hint) hint.classList.add('hidden');
  }
  function showMapGold(){
    const slot = $('#cxMapSlot'); if(!slot) return;
    slot.innerHTML = `<div class="cx-map-cta">
      <button type="button" class="btn primary" id="cxMapBtn">Map Gold Lake</button>
      <span class="note">${nLbl} joined through Asset → MES → SAP → RDBMS. Click to open the unique merged dataset (grain: reading_id).</span>
    </div>`;
    $('#cxMapBtn').onclick = () => {
      paintMergedEvent(res, pid, zone);
      const el = $('#cxMerged');
      if(el) el.scrollIntoView({behavior:'smooth', block:'nearest'});
    };
  }
  function hideMapGold(){
    if($('#cxMapSlot')) $('#cxMapSlot').innerHTML = '';
    if($('#cxMerged')) $('#cxMerged').innerHTML = '';
    if($('#cxNext')) $('#cxNext').innerHTML = '';
  }
  function step(){
    if(i>=STEPS.length) return; const st=STEPS[i];
    revealPipeline();
    for(let f=0;f<4;f++){const el=$('#jcf-'+f);el.classList.toggle('active',f===st.flink);el.classList.toggle('done',f<st.flink);}
    document.querySelectorAll('.jc-src').forEach(el=>el.classList.remove('hit'));
    if(st.src==='all'){
      document.querySelectorAll('.jc-src').forEach(el=>{
        el.classList.add('hit');
        setTimeout(()=>el&&el.classList.add('matched'),900);
      });
    } else if(st.src){
      const el=$('#jcs-'+st.src);el.classList.add('hit');setTimeout(()=>el&&el.classList.add('matched'),900);
    }
    $('#jcNarr').innerHTML=st.narr;
    (st.fields||[]).forEach(f=>$('#'+f)&&$('#'+f).classList.add('on'));
    document.querySelectorAll('.jc-sql .hl').forEach(el=>el.classList.remove('hl'));
    if(st.sql)$('#'+st.sql).classList.add('hl');
    i++; keys((st.keys||[]).length);
    $('#jcInd').textContent=`step ${Math.min(i,STEPS.length)} / ${STEPS.length}`;
    if(st.done){$('#jcFinal').classList.add('show');$('#jcStep').disabled=true;showMapGold();}
  }
  function reset(){
    i=0;
    hideMapGold();
    const pipe = $('#jcPipeline');
    if(pipe && $('#cxMapSlot')) pipe.classList.add('hidden');
    const hint = $('#jcHint');
    if(hint) hint.classList.remove('hidden');
    for(let f=0;f<4;f++){const el=$('#jcf-'+f);el.classList.remove('active','done');}
    document.querySelectorAll('.jc-src').forEach(el=>el.classList.remove('hit','matched'));
    document.querySelectorAll('.jc-ef').forEach(el=>{if(el.id!=='jcf-val' && el.id!=='jcf-rid')el.classList.remove('on');});
    document.querySelectorAll('.jc-sql .hl').forEach(el=>el.classList.remove('hl'));
    $('#jcFinal').classList.remove('show');$('#jcStep').disabled=false;
    $('#jcNarr').innerHTML=`Press <b>Next Flink step</b> or <b>Auto-play</b>. Consume → Enrich → Compute → Emit then process ${nLbl} through Asset, MES, SAP and RDBMS.`;
    $('#jcInd').textContent=`step 0 / ${STEPS.length}`; keys(0);
  }
  $('#jcStep').onclick=step; $('#jcReset').onclick=reset;
  $('#jcAuto').onclick=async()=>{reset();$('#jcStep').disabled=true;$('#jcAuto').disabled=true;
    while(i<STEPS.length){step();await sleep(1400);}$('#jcAuto').disabled=false;};
  reset();
}

/* ---------- BUILD RAG KB ---------- */
VIEWS.ragkb = () => `
  <div class="rag-page layer-page">
  ${pipeTrack('ragkb')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow core"><span class="n">CORE · PHASE 5</span> Platform Layer</div></div>
    <h1>Build RAG KB</h1>
    <p class="lead">Integrate CAPA, Master Index, OEM, Regulatory, SOP and any extra files. Each file becomes a card. <b>Save</b> stores it in PostgreSQL; <b>Start RAG pipeline</b> extracts, chunks, embeds and indexes for SENTRA.</p>
  </header>
  <div class="page-brief"><span class="lbl">What this layer does</span>Parse → chunk → embed → index. SENTRA’s retrieve step searches this index over SOP / CAPA / OEM / Master Index / Regulatory — and any extra documents you add.</div>
  <div class="note" id="ragStats"></div>

  <div class="card tight src-card">
    <div class="cx-toolbar">
      <div class="cx-int-head">Integrate your RAG Database</div>
      <label class="btn amber cx-file-btn" id="ragUpLbl">Upload files
        <input type="file" id="ragMultiFile" multiple>
      </label>
    </div>
    <div class="save-row" id="ragPgBar">
      <span id="ragSaveHint" class="note" style="margin:0">Upload one or more files of any type (PDF, Excel, images, …). Each file becomes a card with Replace, Save and Delete.</span>
    </div>
    <div id="ragSlots" class="cx-slots rag-slots"></div>
    <div class="rag-toolbar">
      <button type="button" class="btn amber" id="ragPipeBtn" disabled>▶ Start RAG pipeline</button>
    </div>
    <div id="ragSrcStatus" class="note"></div>
    <div id="ragJobs"></div>
  </div>
  </div>`;
AFTER.ragkb = () => {
  const KIND_ORDER = {capa:0, master_index:1, oem:2, regulatory:3, sop:4, other:5};
  const fmtBytes = (n) => {
    n = n || 0;
    if(n < 1024) return n + ' B';
    if(n < 1024*1024) return (n/1024).toFixed(1) + ' KB';
    return (n/1024/1024).toFixed(1) + ' MB';
  };
  const listSources = (st) => {
    const sources = (st && st.sources) || {};
    return Object.keys(sources).map(id => ({id, ...sources[id]})).sort((a,b)=>{
      const ka = KIND_ORDER[a.kind] ?? 9;
      const kb = KIND_ORDER[b.kind] ?? 9;
      if(ka !== kb) return ka - kb;
      return String(a.filename||a.id).localeCompare(String(b.filename||b.id));
    });
  };
  const paintStats = (st) => {
    const by = (st && st.by_type) || {};
    const el = $('#ragStats'); if(!el) return;
    el.innerHTML = `Index · <b>${(st && st.chunks) || 0}</b> chunks · SOP ${by.SOP||0} · CAPA ${by.CAPA||0} · OEM ${by.OEM||0} · Index ${by.MASTER_INDEX||0} · Reg ${by.REGULATORY||0}`;
  };
  const slotHtml = (src) => {
    const loaded = !!src.loaded;
    const persisted = !!src.persisted;
    const name = src.filename ? esc(src.filename) : 'no file';
    const size = src.bytes ? fmtBytes(src.bytes) : '';
    const cols = (src.columns && src.columns.length) ? src.columns.join(', ') : (src.content_type || src.extension || '');
    const state = persisted ? `saved${size?' · '+size:''}` : (loaded ? `in memory${size?' · '+size:''}` : 'awaiting upload');
    const thumb = src.preview ? `<img class="cx-thumb" src="${esc(src.preview)}" alt="${name}">` : '';
    return `<div class="cx-slot${loaded?' ready':''}${persisted?' saved':''}" data-slot="${esc(src.slot || src.id)}">
      <h4>${src.ic || '📄'} ${esc(src.label || src.kind || 'Source')} <span class="cx-key">${esc(src.tag || src.kind || '')}</span></h4>
      ${thumb}
      <p class="hint cx-desc">${esc(src.description || src.hint || '')}</p>
      <p class="hint cx-cols"><b>Columns</b> ${esc(cols)}</p>
      <div class="cx-slot-bar">
        <label class="btn sm ghost cx-file-btn">Replace
          <input type="file" class="rag-replace" data-slot="${esc(src.slot || src.id)}">
        </label>
        <button type="button" class="btn save sm rag-save" data-slot="${esc(src.slot || src.id)}" ${loaded?'':'disabled'}>Save</button>
        <button type="button" class="btn sm delete rag-del" data-slot="${esc(src.slot || src.id)}">Delete</button>
        <span class="cx-slot-state">${esc(state)} · ${name}</span>
      </div>
    </div>`;
  };
  const paintSources = (st) => {
    const rows = listSources(st);
    const box = $('#ragSlots');
    if(!rows.length){
      box.innerHTML = `<div class="cx-empty">No RAG sources yet. Use <b>Upload files</b> — PDF, Excel, images and other types are allowed. Each file becomes a card classified from its name (CAPA, Master Index, OEM, Regulatory, SOP, or additional).</div>`;
    } else {
      box.innerHTML = rows.map(slotHtml).join('');
    }
    const loadedN = rows.filter(s => s.loaded).length;
    const savedN = rows.filter(s => s.persisted).length;
    const pipe = $('#ragPipeBtn');
    const hint = $('#ragSaveHint');
    if(pipe) pipe.disabled = savedN === 0;
    if(hint){
      if(savedN && savedN === loadedN){
        hint.innerHTML = `<span class="note ok" style="margin:0"><b>✓ Saved in PostgreSQL</b> — ${savedN} document(s) stored. Start RAG pipeline to index them.</span>`;
      } else if(loadedN){
        hint.textContent = `${loadedN} card(s) in memory. Save each card to keep it in PostgreSQL, then start the pipeline.`;
      } else {
        hint.textContent = 'Upload one or more files of any type (PDF, Excel, images, …). Each file becomes a card with Replace, Save and Delete.';
      }
    }
    box.querySelectorAll('.rag-replace').forEach(inp=>{
      inp.onchange = async (ev) => {
        const f = ev.target.files && ev.target.files[0]; if(!f) return;
        const slot = inp.dataset.slot;
        $('#ragSrcStatus').innerHTML = `<span class="spin"></span> replacing ${esc(f.name)}…`;
        const fd = new FormData();
        fd.append('file', f, f.name);
        try{
          const next = await api(`/api/rag/replace/${encodeURIComponent(slot)}`, {method:'POST', body: fd});
          $('#ragSrcStatus').innerHTML = `<span class="note ok">Replaced with <b>${esc(f.name)}</b>.</span>`;
          paintSources(next); paintStats(next);
        }catch(err){
          $('#ragSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
        }
      };
    });
    box.querySelectorAll('.rag-save').forEach(btn=>{
      btn.onclick = async () => {
        const slot = btn.dataset.slot;
        btn.disabled = true;
        $('#ragSrcStatus').innerHTML = `<span class="spin"></span> saving to PostgreSQL…`;
        try{
          const next = await api(`/api/rag/save/${encodeURIComponent(slot)}`, {method:'POST'});
          $('#ragSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Saved.')}</span>`;
          paintSources(next); paintStats(next);
        }catch(err){
          $('#ragSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
          btn.disabled = false;
        }
      };
    });
    box.querySelectorAll('.rag-del').forEach(btn=>{
      btn.onclick = async () => {
        const slot = btn.dataset.slot;
        const card = rows.find(s => (s.slot||s.id) === slot);
        const name = (card && card.filename) || slot;
        if(!confirm(`Permanently delete ${name} from PostgreSQL?\n\nYou will need to upload this file again.`)) return;
        btn.disabled = true;
        $('#ragSrcStatus').innerHTML = `<span class="spin"></span> deleting from PostgreSQL…`;
        try{
          const next = await api(`/api/rag/delete/${encodeURIComponent(slot)}`, {method:'POST'});
          $('#ragSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Deleted.')}</span>`;
          paintSources(next); paintStats(next);
        }catch(err){
          $('#ragSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
          btn.disabled = false;
        }
      };
    });
  };
  const jobCard = (job) => {
    const steps = (job.steps||[]).map(s=>`
      <div class="rag-step">
        <div class="rag-dot">✓</div>
        <div><b>${esc(s.name)}</b><div class="note" style="margin:0">${esc(s.detail)}</div></div>
      </div>`).join('');
    return `<div class="rag-job">
      <div class="rag-job-hd"><b>${esc(job.filename)}</b><span class="note ok">✓ ${job.chunks} chunks · ${esc(job.type||'')}</span></div>
      ${steps}
    </div>`;
  };
  $('#ragMultiFile').onchange = async (ev) => {
    const files = ev.target.files ? Array.from(ev.target.files) : [];
    if(!files.length) return;
    $('#ragSrcStatus').innerHTML = `<span class="spin"></span> reading ${files.length} file(s)…`;
    const fd = new FormData();
    files.forEach(f => fd.append('files', f, f.name));
    try{
      const next = await api('/api/rag/upload', {method:'POST', body: fd});
      const extra = (next.errors && next.errors.length) ? ` · ${next.errors.join('; ')}` : '';
      $('#ragSrcStatus').innerHTML = `<span class="note ok">${esc(next.message || 'Loaded.')}${esc(extra)}</span>`;
      paintSources(next); paintStats(next);
    }catch(err){
      $('#ragSrcStatus').innerHTML = `<span class="note" style="color:var(--coral)">${esc(err.message)}</span>`;
    }
    ev.target.value = '';
  };
  $('#ragPipeBtn').onclick = async () => {
    const btn = $('#ragPipeBtn');
    btn.disabled = true;
    const host = $('#ragJobs');
    host.innerHTML = `<span class="spin"></span> extracting, chunking, embedding and indexing…`;
    $('#ragSrcStatus').innerHTML = '';
    try{
      const r = await api('/api/rag/pipeline', {method:'POST'});
      const cards = (r.jobs||[]).map(jobCard).join('');
      const fails = (r.errors||[]).map(e=>`<div class="handoff missing"><div class="hv">${esc(e.filename||e.kind)}</div><div class="hs">${esc(e.error)}</div></div>`).join('');
      host.innerHTML = cards + fails || '<span class="note">No files indexed.</span>';
      $('#ragSrcStatus').innerHTML = `<span class="note ok">${esc(r.message || 'Pipeline finished.')}</span>`;
      paintSources(r); paintStats(r);
    }catch(err){
      host.innerHTML = `<div class="handoff missing"><div class="hv">Pipeline failed</div><div class="hs">${esc(err.message)}</div></div>`;
      btn.disabled = false;
    }
  };
  (async () => {
    try{
      const st = await api('/api/rag/status');
      paintSources(st);
      paintStats(st);
    }catch(e){
      paintSources({});
      paintStats({});
    }
  })();
};

/* ---------- SENTRA ---------- */
VIEWS.intelligence = () => `
  <div class="sentra-page layer-page">
  ${pipeTrack('intelligence')}
  <header class="page-head">
    <div class="page-kicker"><div class="eyebrow core"><span class="n">CORE · PHASE 6</span> Platform Layer</div></div>
    <h1>SCADA Intelligence Platform</h1>
    <p class="lead">SCADA intelligence: pick a parameter and trip/alarm scenario, select a <b>reading_id</b>, decompose it in Flink, turn the event into retrieval queries, then activate the matching LLM agent for a cited RemedyCard.</p>
  </header>
  <div class="page-brief"><span class="lbl">What this layer does</span>perceive → diagnose → retrieve (RAG) → reason → govern. Grounded in SOP / CAPA / OEM / Master Index / Regulatory.</div>

  <div class="card tight">
    <h3><span class="ic">1–2</span> Parameter &amp; scenario</h3>
    <div class="c-ctrl">
      <label class="fl">Parameter</label><select class="inp" id="siParam"></select>
      <label class="fl">Scenario</label>
      <select class="inp" id="siZone">
        <option value="trip">Trip</option>
        <option value="alarm">Alarm</option>
      </select>
    </div>
  </div>

  <div class="card tight">
    <h3><span class="ic">3</span> Contextualized / merged data <span class="tag" id="siBoardTag">select parameter</span></h3>
    <div id="siBoard"></div>
  </div>

  <div class="card tight">
    <h3><span class="ic">4</span> Select a reading, then start Flink SQL decomposition</h3>
    <div class="si-tabs" id="siTabs">
      <button type="button" class="si-tab on" data-tab="row">Select row</button>
      <button type="button" class="si-tab" data-tab="rid">Select reading_id</button>
    </div>
    <div id="siPickRow" class="note">Click a row in the table above.</div>
    <div id="siPickRid" class="hidden" style="margin-top:8px">
      <label class="fl">reading_id</label>
      <select class="inp" id="siRid"></select>
    </div>
    <div class="si-toolbar">
      <button type="button" class="btn amber" id="siDecompBtn" disabled>▶ Start Flink SQL Decomposition</button>
      <span class="note" id="siPickHint" style="margin:0">Select a row or a reading_id first.</span>
    </div>
  </div>

  <div class="card tight" id="siFlinkCard">
    <h3><span class="ic">5</span> Live Flink Decomposition Pipeline Stepper</h3>
    <div id="siFlink"><span class="note">Decomposition starts after you select a reading.</span></div>
  </div>

  <div class="card tight" id="siQueryCard">
    <h3><span class="ic">6</span> Transform decomposed events into queries</h3>
    <div id="siQueries"><span class="note">Queries appear after Flink Emit.</span></div>
  </div>

  <div class="card tight" id="siAgentCard">
    <h3><span class="ic">7</span> Activate LLM multi-agent</h3>
    <div id="siAgents" class="si-agents"></div>
    <div class="si-toolbar">
      <button type="button" class="btn amber" id="siAgentBtn" disabled>▶ Activate agent</button>
      <span class="note" id="siAgentHint" style="margin:0">Finish Flink decomposition first.</span>
    </div>
    <div id="siAgentLog"></div>
  </div>

  <div class="card tight" id="siRemedyCard">
    <h3><span class="ic">8–9</span> RAG knowledge base retrieval &amp; synthesized RemedyCard</h3>
    <div id="siRemedy"><span class="note">Activate the agent to retrieve chunks and synthesise actionable steps.</span></div>
    <div id="sNext"></div>
  </div>
  </div>`;
AFTER.intelligence = () => {
  const SI = {param:null, zone:'trip', board:null, selected:null, decomp:null, tab:'row'};
  const pid0 = paramOf(PIPE.reading) || 'temp';
  $('#siParam').innerHTML = PARAMS.map(p=>`<option value="${p.id}" ${p.id===pid0?'selected':''}>${p.short}</option>`).join('');
  $('#siAgents').innerHTML = PARAMS.map(p=>`<button type="button" class="si-agent" data-id="${p.id}" disabled>${esc(p.short)}</button>`).join('');

  const resetDownstream = () => {
    SI.selected = null;
    SI.decomp = null;
    $('#siDecompBtn').disabled = true;
    $('#siAgentBtn').disabled = true;
    $('#siFlink').innerHTML = `<span class="note">Decomposition starts after you select a reading.</span>`;
    $('#siQueries').innerHTML = `<span class="note">Queries appear after Flink Emit.</span>`;
    $('#siAgentLog').innerHTML = '';
    $('#siRemedy').innerHTML = `<span class="note">Activate the agent to retrieve chunks and synthesise actionable steps.</span>`;
    if($('#sNext')) $('#sNext').innerHTML = '';
    $('#siPickHint').textContent = 'Select a row or a reading_id first.';
    $('#siAgentHint').textContent = 'Finish Flink decomposition first.';
    paintPick();
  };

  const paintPick = () => {
    const rid = SI.selected;
    const ids = (SI.board && SI.board.reading_ids) || [];
    $('#siRid').innerHTML = ids.map(id=>`<option value="${esc(id)}" ${id===rid?'selected':''}>${esc(id)}</option>`).join('') || '<option value="">—</option>';
    if(rid && ids.includes(rid)) $('#siRid').value = rid;
    $('#siDecompBtn').disabled = !rid;
    $('#siPickHint').textContent = rid ? `Selected ${rid}` : 'Select a row or a reading_id first.';
    $('#siPickRow').classList.toggle('hidden', SI.tab !== 'row');
    $('#siPickRid').classList.toggle('hidden', SI.tab !== 'rid');
    document.querySelectorAll('#siBoard tr[data-rid]').forEach(tr => {
      tr.classList.toggle('on', tr.getAttribute('data-rid') === rid);
    });
  };

  const paintBoard = (board) => {
    SI.board = board;
    const rows = (board && board.events) || [];
    const tag = $('#siBoardTag');
    if(tag) tag.textContent = `${board.name||board.param} · ${board.zone} · ${rows.length} row${rows.length===1?'':'s'}${board.fallback?' · scenario sample':''}`;
    if(!rows.length){
      $('#siBoard').innerHTML = `<div class="handoff missing"><div class="hv">No ${esc(board.zone||'')} observations</div><div class="hs">${esc(board.error||'Finish Connectivity / Ingest so this page has floor rows.')}</div></div>`;
      $('#siRid').innerHTML = '<option value="">—</option>';
      return;
    }
    const cell = v => esc(v==null || v==='' ? '—' : v);
    const body = rows.map(e=>{
      const stCls = e.status==='OK' ? 'ok' : (e.status==='OVER' ? 'over' : 'under');
      const ts = e.timestamp ? String(e.timestamp).replace('T',' ').replace('Z','') : '';
      const spec = Array.isArray(e.spec) ? `[${e.spec.join(', ')}]` : (e.spec || '—');
      const val = e.value==null ? '—' : `${e.value}${e.unit||''}`;
      return `<tr data-rid="${esc(e.reading_id||'')}">
        <td>${cell(e.reading_id)}</td><td>${cell(ts)}</td><td>${cell(e.tag)}</td>
        <td>${esc(val)}</td><td><span class="cx-status ${stCls}">${cell(e.status)}</span></td>
        <td>${cell(e.asset)}</td><td>${cell(e.batch)}</td><td>${cell(e.product)}</td>
        <td>${esc(spec)}</td><td>${cell(e.material_no)}</td>
      </tr>`;
    }).join('');
    $('#siBoard').innerHTML = `<div class="cx-obs-wrap viz-table-wrap">
      <table class="preview-table viz-merged si-table">
        <thead><tr><th>reading_id</th><th>timestamp</th><th>tag</th><th>value</th><th>status</th><th>asset</th><th>batch</th><th>product</th><th>spec</th><th>material</th></tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
    $('#siBoard').querySelectorAll('tr[data-rid]').forEach(tr => {
      tr.onclick = () => {
        SI.tab = 'row';
        document.querySelectorAll('.si-tab').forEach(b=>b.classList.toggle('on', b.dataset.tab==='row'));
        SI.selected = tr.getAttribute('data-rid');
        paintPick();
      };
    });
    if(!SI.selected && rows[0] && rows[0].reading_id){
      /* do not auto-select — user asked to select */
    }
    paintPick();
  };

  const loadBoard = async () => {
    SI.param = $('#siParam').value;
    SI.zone = $('#siZone').value;
    resetDownstream();
    $('#siBoard').innerHTML = `<span class="spin"></span> loading merged ${esc(SI.zone)} observations…`;
    try{
      const board = await api(`/api/sentra/board?param=${encodeURIComponent(SI.param)}&zone=${encodeURIComponent(SI.zone)}`);
      paintBoard(board);
    }catch(err){
      $('#siBoard').innerHTML = `<div class="handoff missing"><div class="hv">Could not load merged data</div><div class="hs">${esc(err.message)}</div></div>`;
    }
    document.querySelectorAll('.si-agent').forEach(b => {
      b.classList.toggle('active', b.dataset.id === SI.param);
    });
    const p = PARAMS.find(x=>x.id===SI.param);
    $('#siAgentBtn').textContent = `▶ Activate ${p?p.short:''} agent`;
  };

  const paintQueries = (queries) => {
    const qs = queries || [];
    if(!qs.length){ $('#siQueries').innerHTML = `<span class="note">No queries.</span>`; return; }
    $('#siQueries').innerHTML = qs.map(q=>`
      <div class="si-query ${esc(q.kind||'')}">
        <div class="si-ql">${esc(q.label)} · ${esc((q.kind||'').toUpperCase())}</div>
        <pre>${esc(q.text)}</pre>
      </div>`).join('');
  };

  const paintFlink = (dec) => {
    const ev = dec.event || {};
    const rd = dec.reading || {tag: ev.tag, value: ev.val};
    $('#siFlink').innerHTML = `
      <div class="jc-controls">
        <button class="btn amber sm" id="siStep">▶ Next Flink step</button>
        <button class="btn primary sm" id="siAuto">⏩ Auto-play</button>
        <button class="btn ghost sm" id="siReset">↺ Reset</button>
        <span class="jc-ind" id="siInd">step 0 / 4</span>
      </div>
      <div class="jc-flinksteps">
        <div class="jc-fstep" id="sif-0"><div class="fl">A</div><div class="fn">Consume</div><div class="fd">read signal</div></div>
        <div class="jc-fstep" id="sif-1"><div class="fl">B</div><div class="fn">Enrich</div><div class="fd">keyed lookups</div></div>
        <div class="jc-fstep" id="sif-2"><div class="fl">C</div><div class="fn">Compute</div><div class="fd">derive status</div></div>
        <div class="jc-fstep" id="sif-3"><div class="fl">D</div><div class="fn">Emit</div><div class="fd">event out</div></div>
      </div>
      <div class="jc-narr"><div class="nt">Narration</div><div class="nb" id="siNarr">Press <b>Next Flink step</b>. Signal arrives as tag + timestamp + reading_id.</div></div>
      <details class="cx-sql-wrap"><summary>Flink SQL — ON clauses are the key chain</summary>
        <div class="jc-sql"><span class="cm">-- decompose reading_id ${esc(ev.reading_id||'')}</span>
<span class="kw">SELECT</span> r.value, a.asset, a.spec, m.batch, m.product, s.material
<span class="kw">FROM</span> readings r
<span class="kw">JOIN</span> asset_model a <span class="on">ON</span> r.tag = a.tag
<span class="kw">JOIN</span> mes_batches m <span class="on">ON</span> a.asset = m.asset <span class="kw">AND</span> r.ts <span class="kw">BETWEEN</span> m.start <span class="kw">AND</span> m.end
<span class="kw">JOIN</span> sap_master s <span class="on">ON</span> m.product = s.product
<span class="kw">WHERE</span> r.reading_id = '${esc(ev.reading_id||'')}'</div>
      </details>`;
    const STEPS = [
      {f:0, narr:`<b>A — Consume.</b> Flink reads <code>${esc(rd.tag||ev.tag||'')}</code> · reading_id <code>${esc(ev.reading_id||'')}</code> · value ${esc(rd.value??ev.val)}.`},
      {f:1, narr:`<b>B — Enrich.</b> Keyed lookups: tag → Asset <b>${esc(ev.asset||'')}</b> → MES batch <b>${esc(ev.batch||'')}</b> → SAP product <b>${esc(ev.product||'')}</b> → RDBMS cal.`},
      {f:2, narr:`<b>C — Compute.</b> Value vs spec [${(ev.spec||[]).join(', ')}] → status <b>${esc(ev.status||'')}</b> · zone <b>${esc(ev.zone||'')}</b>.`},
      {f:3, narr:`<b>D — Emit.</b> Contextualized event for ${esc(ev.asset||'')} / ${esc(ev.param||'')} is emitted. Transform it into retrieval queries next.`, done:true},
    ];
    let i = 0;
    const paint = () => {
      const cur = i===0 ? null : STEPS[Math.min(i, STEPS.length)-1];
      for(let f=0;f<4;f++){
        const el = $('#sif-'+f); if(!el) continue;
        el.classList.toggle('active', !!cur && f===cur.f);
        el.classList.toggle('done', !!cur && f<cur.f);
      }
      $('#siInd').textContent = `step ${i} / ${STEPS.length}`;
    };
    const step = () => {
      if(i>=STEPS.length) return;
      const st = STEPS[i];
      i++;
      $('#siNarr').innerHTML = st.narr;
      paint();
      if(st.done){
        $('#siStep').disabled = true;
        paintQueries(dec.queries);
        $('#siAgentBtn').disabled = false;
        const p = PARAMS.find(x=>x.id===SI.param);
        $('#siAgentHint').textContent = `Ready to activate the ${p?p.short:''} LLM agent.`;
      }
    };
    $('#siStep').onclick = step;
    $('#siAuto').onclick = async () => {
      $('#siAuto').disabled = true;
      while(i<STEPS.length){ step(); await sleep(420); }
      $('#siAuto').disabled = false;
    };
    $('#siReset').onclick = () => {
      i = 0; paint();
      $('#siStep').disabled = false;
      $('#siNarr').innerHTML = `Press <b>Next Flink step</b>. Signal arrives as tag + timestamp + reading_id.`;
      $('#siQueries').innerHTML = `<span class="note">Queries appear after Flink Emit.</span>`;
      $('#siAgentBtn').disabled = true;
    };
    paint();
  };

  const paintRemedy = (r) => {
    const ag = r.agent || {};
    const ev = r.event || {};
    if(!ag.action_required){
      $('#siRemedy').innerHTML = `<div class="callout green"><span class="cic">✓</span><div class="cx"><b>${esc(ev.asset||'')} ${esc(ev.param||'')} in control band (${esc(ev.val)}${esc(ev.unit||'')}).</b> No action required.</div></div>`;
      return;
    }
    const g = ag.governance || {};
    const steps = (ag.remedy && ag.remedy.steps) || [];
    const hits = ag.hits || [];
    const kindCls = t => ({SOP:'sop',CAPA:'capa',OEM:'oem',MASTER_INDEX:'index',REGULATORY:'reg'}[t]||'sop');
    $('#siRemedy').innerHTML = `
      <div class="rcard">
        <div class="rcard-hd">
          <div>
            <div class="rcard-k">Grounded Remedy Plan — ${esc(ev.short||ev.param||'')} (${esc(ev.asset||'')})</div>
            <div class="note" style="margin:2px 0 0">Synthesized across ${hits.length} retrieved SOP / CAPA / OEM chunk(s). ${ag.llm&&ag.llm.used?esc(ag.llm.provider+' · '+ag.llm.model):'Local TF-IDF RAG (Qdrant swap-in)'}.</div>
          </div>
          <span class="rcard-badge">Cited &amp; verified</span>
        </div>
        <div class="rcard-grid">
          <div>
            <div class="rcard-sec">Actionable remediation steps</div>
            ${steps.map(s=>`<div class="rcard-step"><div class="n">${s.n}</div><div><div>${esc(s.text)}</div><span class="cite">${esc(s.cite)}</span></div></div>`).join('')||'<span class="note">No steps.</span>'}
            <div class="gov-badge ${esc(g.action||'')}">${esc(g.action||'')}${g.approver?' → '+esc(g.approver):''} · ${esc(g.control||'')}</div>
          </div>
          <div>
            <div class="rcard-sec">Retrieved knowledge base chunks (Qdrant vector search)</div>
            ${hits.map(h=>`<div class="rcard-chunk">
              <div class="rcard-ch"><span class="rag-kind ${kindCls(h.type)}">${esc(h.type)}</span><span class="rcard-score">Score: ${esc(h.score)}</span></div>
              <div class="rcard-ref">${esc(h.ref)}</div>
              <div class="rcard-tx">${esc((h.text||'').slice(0,280))}${(h.text||'').length>280?'…':''}</div>
            </div>`).join('')||'<span class="note">No chunks.</span>'}
          </div>
        </div>
      </div>`;
  };

  $('#siParam').onchange = loadBoard;
  $('#siZone').onchange = loadBoard;
  document.querySelectorAll('.si-tab').forEach(btn => {
    btn.onclick = () => {
      SI.tab = btn.dataset.tab;
      document.querySelectorAll('.si-tab').forEach(b=>b.classList.toggle('on', b===btn));
      paintPick();
    };
  });
  $('#siRid').onchange = () => {
    SI.selected = $('#siRid').value || null;
    paintPick();
  };
  $('#siDecompBtn').onclick = async () => {
    if(!SI.selected) return;
    $('#siDecompBtn').disabled = true;
    $('#siFlink').innerHTML = `<span class="spin"></span> decomposing ${esc(SI.selected)}…`;
    try{
      const dec = await api('/api/sentra/decompose', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({param:SI.param, zone:SI.zone, reading_id:SI.selected}),
      });
      SI.decomp = dec;
      PIPE.event = dec.event;
      paintFlink(dec);
    }catch(err){
      $('#siFlink').innerHTML = `<div class="handoff missing"><div class="hv">Decomposition failed</div><div class="hs">${esc(err.message)}</div></div>`;
      $('#siDecompBtn').disabled = false;
    }
  };
  $('#siAgentBtn').onclick = async () => {
    if(!SI.selected) return;
    $('#siAgentBtn').disabled = true;
    $('#siAgentLog').innerHTML = `<span class="spin"></span> activating ${esc((PARAMS.find(x=>x.id===SI.param)||{}).short||'')} agent · RAG retrieve…`;
    try{
      const r = await api('/api/sentra/agent', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({param:SI.param, zone:SI.zone, reading_id:SI.selected}),
      });
      PIPE.event = r.event;
      const ag = r.agent || {};
      $('#siAgentLog').innerHTML = (ag.steps||[]).map(s=>`<div class="agent-stage"><div class="as-ic">${({perceive:'👁️',diagnose:'🔬',retrieve:'📚',reason:'🧠',govern:'🛡️',assess:'✓'})[s.stage]||'•'}</div><div class="as-tx"><b>${esc(s.stage)}</b><div>${esc(s.detail)}</div></div></div>`).join('');
      paintRemedy(r);
      refreshRail();
      $('#siAgentBtn').disabled = false;
    }catch(err){
      $('#siAgentLog').innerHTML = `<div class="handoff missing"><div class="hv">Agent failed</div><div class="hs">${esc(err.message)}</div></div>`;
      $('#siAgentBtn').disabled = false;
    }
  };
  loadBoard();
};
function renderAgent(r){
  const ag=r.agent, ev=r.event;
  if(!ag.action_required){
    $('#sResult').innerHTML=`<div class="callout green"><span class="cic">✓</span><div class="cx"><b>${ev.asset} ${ev.param} in control band (${ev.val}${ev.unit}).</b> No action required.</div></div>`;
    return;
  }
  const g=ag.governance;
  $('#sResult').innerHTML=`
    <div class="callout amber" style="margin-top:0"><span class="cic">⚠️</span><div class="cx"><b>${ev.asset} · batch ${ev.batch} · ${ev.product}</b> — ${ev.param} ${ev.status} (${ev.val}${ev.unit}, spec [${ev.spec.join(', ')}])</div></div>
    ${ag.steps.map(s=>`<div class="agent-stage"><div class="as-ic">${({perceive:'👁️',diagnose:'🔬',retrieve:'📚',reason:'🧠',govern:'🛡️',assess:'✓'})[s.stage]||'•'}</div><div class="as-tx"><b>${s.stage}</b><div>${esc(s.detail)}</div></div></div>`).join('')}
    <div class="remedy"><div class="rt">▶ grounded remedy · confidence ${ag.remedy.confidence}</div><div class="rb">${esc(ag.remedy.summary)}</div>
      <div>${ag.citations.map(c=>`<span class="cite">${c.ref} · ${c.type}</span>`).join('')}</div>
      <div class="gov-badge ${g.action}">${g.action}${g.approver?' → '+g.approver:''} · ${esc(g.control)}</div></div>`;
}

/* ============================================================
   INIT
   ============================================================ */
async function refreshRail(){
  return;
}
async function init(){
  buildNav();
  try{
    const h=await api('/api/health'); $('#apiStatus').textContent='● API online'; $('#apiStatus').classList.remove('down');
    const p=await api('/api/parameters'); PARAMS=p.parameters;
  }catch(e){
    $('#apiStatus').textContent='● API offline'; $('#apiStatus').classList.add('down');
  }
  nav('home'); refreshRail();
}
init();
