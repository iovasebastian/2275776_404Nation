import React, { useEffect, useState } from 'react';

const API_BASE = (import.meta.env.VITE_API_BASE || 'http://localhost:8000').replace(/\/$/, '');
const RULES_KEY = 'mars-rules-local';

const SENSORS = [
  { id: 'greenhouse_temperature', label: 'greenhouse_temperature', unit: 'C' },
  { id: 'entrance_humidity', label: 'entrance_humidity', unit: '%' },
  { id: 'co2_hall', label: 'co2_hall', unit: 'ppm' },
  { id: 'hydroponic_ph', label: 'hydroponic', unit: 'pH' },
  { id: 'water_tank_level', label: 'water_tank_level', unit: '%' },
  { id: 'corridor_pressure', label: 'corridor_pressure', unit: 'kPa' },
  { id: 'air_quality_pm25', label: 'air_quality_pm2.5', unit: 'ug/m3' },
  { id: 'air_quality_voc', label: 'air_quality_voc', unit: 'ppb' }
];

const TOPICS = [
  'mars/telemetry/solar_array',
  'mars/telemetry/radiation',
  'mars/telemetry/life_support',
  'mars/telemetry/thermal_loop',
  'mars/telemetry/power_bus',
  'mars/telemetry/power_consumption',
  'mars/telemetry/airlock'
];

const ACTUATORS = ['cooling_fan', 'entrance_humidifier', 'hall_ventilation', 'habitat_heater'];
const OPERATORS = ['<', '<=', '=', '>', '>='];
const EMPTY_FORM = {
  id: '',
  sensor_name: 'greenhouse_temperature',
  operator: '>',
  value: '28',
  unit: 'C',
  actuator_name: 'cooling_fan',
  target_state: 'ON',
  enabled: true
};

const TOPIC_UNIT = {
  'mars/telemetry/solar_array': 'kW',
  'mars/telemetry/radiation': 'uSv/h',
  'mars/telemetry/life_support': '%',
  'mars/telemetry/thermal_loop': 'C',
  'mars/telemetry/power_bus': 'kW',
  'mars/telemetry/power_consumption': 'kW',
  'mars/telemetry/airlock': 'cycles/h'
};

const wsUrl = (path) => {
  const url = new URL(API_BASE + path);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
};

async function request(path, { method = 'GET', body } = {}) {
  const res = await fetch(API_BASE + path, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}`);
  return res.json();
}

const localRules = {
  load: () => {
    try {
      return JSON.parse(localStorage.getItem(RULES_KEY) || '[]');
    } catch {
      return [];
    }
  },
  save: (rules) => localStorage.setItem(RULES_KEY, JSON.stringify(rules))
};

const ruleBaseSig = (r) => [r.sensor_name, r.operator, Number(r.value), r.actuator_name].join('|');
const ruleSig = (r) => `${ruleBaseSig(r)}|${r.target_state}`;
const uniqueRules = (rules) => Array.from(new Map(rules.map((r) => [ruleSig(r), r])).values());

const sensorText = (d) => {
  if (!d) return '-';
  if (typeof d.value === 'number') return `${d.value} ${d.unit || ''}`.trim();
  if (typeof d.level_pct === 'number') return `${d.level_pct}%`;
  if (typeof d.pm25_ug_m3 === 'number') return `${d.pm25_ug_m3} ug/m3`;
  if (Array.isArray(d.measurements)) return d.measurements.map((m) => `${m.metric}:${m.value}${m.unit ? ` ${m.unit}` : ''}`).join(' | ');
  return '-';
};

const topicValue = (topic, d) => {
  if (!d) return null;
  if (topic.includes('radiation') || topic.includes('life_support')) return d.measurements?.[0]?.value ?? null;
  if (topic.includes('thermal_loop')) return d.temperature_c ?? null;
  if (topic.includes('airlock')) return d.cycles_per_hour ?? null;
  return d.power_kw ?? null;
};

function MiniChart({ title, unit, points }) {
  if (!points.length) return <div className="chart-panel"><h4>{title}</h4><div className="empty">Waiting for data...</div></div>;

  const w = 430;
  const h = 220;
  const pad = 28;
  const vals = points.map((p) => p.value);
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = max - min || 1;
  const pts = points.map((p, i) => ({
    ...p,
    x: pad + (i / Math.max(points.length - 1, 1)) * (w - pad * 2),
    y: h - pad - ((p.value - min) / range) * (h - pad * 2)
  }));
  const d = pts.map((p, i) => `${i ? 'L' : 'M'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');

  return (
    <div className="chart-panel">
      <h4>{title}</h4>
      <svg viewBox={`0 0 ${w} ${h}`} className="chart-svg">
        <rect x="0" y="0" width={w} height={h} className="chart-bg" rx="12" />
        <line x1={pad} y1={pad} x2={pad} y2={h - pad} className="axis" />
        <line x1={pad} y1={h - pad} x2={w - pad} y2={h - pad} className="axis" />
        <path d={d} className="line" />
        {pts.map((p, i) => (
          <g key={i}>
            <circle cx={p.x} cy={p.y} r="3" className="point" />
            <text x={p.x} y={p.y - 8} textAnchor="middle" className="point-label">{p.value.toFixed(2)}</text>
          </g>
        ))}
        <text x="8" y="14" className="meta">min: {min.toFixed(2)} {unit}</text>
        <text x="8" y={h - 8} className="meta">max: {max.toFixed(2)} {unit}</text>
      </svg>
    </div>
  );
}

export default function App() {
  const [page, setPage] = useState('operations');
  const [sensors, setSensors] = useState({});
  const [actuators, setActuators] = useState({});
  const [latest, setLatest] = useState({});
  const [series, setSeries] = useState(Object.fromEntries(TOPICS.map((t) => [t, []])));
  const [rules, setRules] = useState([]);
  const [remoteRules, setRemoteRules] = useState(true);
  const [form, setForm] = useState(EMPTY_FORM);
  const [dialog, setDialog] = useState({ open: false, mode: 'info', message: '', ruleId: '' });

  const canEdit = Boolean(form.id);

  const closeDialog = () => setDialog({ open: false, mode: 'info', message: '', ruleId: '' });
  const showInfo = (message) => setDialog({ open: true, mode: 'info', message, ruleId: '' });
  const confirmDelete = (ruleId) => setDialog({ open: true, mode: 'confirm', message: 'Are you sure you want to delete this rule?', ruleId });

  const loadSensors = async () => {
    const rows = await Promise.all(SENSORS.map(async (s) => {
      try {
        return [s.id, await request(`/api/sensors/${s.id}`)];
      } catch {
        return [s.id, { status: 'unavailable' }];
      }
    }));
    setSensors(Object.fromEntries(rows));
  };

  const loadActuators = async () => {
    const data = await request('/api/actuators');
    setActuators(data.actuators || {});
  };

  useEffect(() => {
    loadSensors().catch(() => { });
    loadActuators().catch(() => { });

    const conns = TOPICS.map((topic) => {
      const ws = new WebSocket(wsUrl(`/api/telemetry/ws?topic=${encodeURIComponent(topic)}`));
      ws.onmessage = (e) => {
        try {
          setLatest((prev) => ({ ...prev, [topic]: JSON.parse(e.data) }));
        } catch {
          // ignore malformed telemetry messages
        }
      };
      return ws;
    });

    return () => conns.forEach((x) => x.close());
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      loadSensors().catch(() => { });
      setSeries((prev) => {
        const next = { ...prev };
        for (const topic of TOPICS) {
          const value = topicValue(topic, latest[topic]);
          if (typeof value === 'number') next[topic] = [...(next[topic] || []), { ts: Date.now(), value }].slice(-20);
        }
        return next;
      });
    }, 5000);
    return () => clearInterval(timer);
  }, [latest]);

  useEffect(() => {
    const loadRules = async () => {
      try {
        const data = await request('/api/rules');  //Backend rule interface is available ->remoteRules==true
        setRules(uniqueRules(data.rules || []));
        setRemoteRules(true);
      } catch {
        setRules(uniqueRules(localRules.load()));  //Backend rule interface is unavailable ->remoteRules==false，enter localStorage mode.
        setRemoteRules(false);
      }
    };
    loadRules();
  }, []);

  const syncRules = (next, action, payload) => {
    setRules(next);
    if (remoteRules) {
      request('/api/rules/action', { method: 'POST', body: { action, payload } }).catch(() => { });
    } else {
      localRules.save(next);
    }
  };

  const setActuatorState = async (name, state) => {
    try {
      await request(`/api/actuators/${name}`, { method: 'POST', body: { state } });
      setActuators((prev) => ({ ...prev, [name]: state }));
    } catch {
      showInfo('Failed to switch actuator state.');
    }
  };

  const submitRule = (e) => {
    e.preventDefault();
    const candidate = {
      ...form,
      id: form.id || crypto.randomUUID(),
      value: Number(form.value),
      enabled: form.enabled !== false
    };

    const peerRules = rules.filter((r) => r.id !== candidate.id);
    if (peerRules.some((r) => ruleSig(r) === ruleSig(candidate))) {  //Determine rule conflicts
      showInfo('The rule already exists.');
      return;
    }
    if (peerRules.some((r) => ruleBaseSig(r) === ruleBaseSig(candidate) && r.target_state !== candidate.target_state)) {
      showInfo('This rule conflicts with the existing regulations.');
      return;
    }

    const next = uniqueRules(canEdit ? rules.map((r) => (r.id === candidate.id ? candidate : r)) : [...rules, candidate]);
    syncRules(next, canEdit ? 'update' : 'create', candidate);
    setForm(EMPTY_FORM);
  };

  const editRule = (r) => setForm({ ...r, value: String(r.value) });

  const toggleRule = (r) => {
    const nextRule = { ...r, enabled: !(r.enabled !== false) };
    syncRules(rules.map((x) => (x.id === r.id ? nextRule : x)), 'toggle', { id: r.id, enabled: nextRule.enabled });
  };

  const removeRule = (id) => {
    syncRules(rules.filter((r) => r.id !== id), 'delete', { id });
  };

  const onDialogConfirm = () => {
    if (dialog.mode === 'confirm' && dialog.ruleId) removeRule(dialog.ruleId);
    closeDialog();
  };

  return (
    <div className="page">
      <header className="top">
        <h1>Mars Base Dashboard and Control Console</h1>

        <div className="page-switch" role="tablist" aria-label="Content pages">
          <button
            className={page === 'operations' ? 'active-tab' : ''}
            title="Page 1: Sensors, actuators, and rules"
            onClick={() => setPage('operations')}
          >
            Operations Overview
          </button>
          <button
            className={page === 'telemetry' ? 'active-tab' : ''}
            title="Page 2: Real-time telemetry charts"
            onClick={() => setPage('telemetry')}
          >
            Telemetry Charts
          </button>
        </div>
      </header>

      {page === 'operations' && (
        <div key="operations" className="page-content">
          <section className="block row-block">
            <h2>REST Sensors</h2>
            <div className="sensor-grid">
              {SENSORS.map((s) => (
                <div className="sensor-card" key={s.id}>
                  <strong>{s.label}</strong>
                  <div className="sensor-value">{sensorText(sensors[s.id])}</div>
                  <small>Status: {sensors[s.id]?.status || '-'}</small>
                </div>
              ))}
            </div>
          </section>

          <section className="block row-block">
            <h2>Actuators</h2>
            <div className="actuator-grid">
              {ACTUATORS.map((name) => {
                const state = actuators[name] || 'OFF';
                return (
                  <div className="actuator-card" key={name}>
                    <strong>{name}</strong>
                    <div className="actuator-actions">
                      <div className={`toggle-switch ${state === 'ON' ? 'on' : 'off'}`}>
                        <button className={state === 'ON' ? 'selected' : ''} onClick={() => state !== 'ON' && setActuatorState(name, 'ON')}>ON</button>
                        <button className={state === 'OFF' ? 'selected' : ''} onClick={() => state !== 'OFF' && setActuatorState(name, 'OFF')}>OFF</button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          <section className="block row-block">
            <h2>Rules</h2>

            <form className="rule-form" onSubmit={submitRule}>
              <label>Sensor Name
                <select
                  value={form.sensor_name}
                  onChange={(e) => {
                    const sensor = SENSORS.find((s) => s.id === e.target.value);
                    setForm((x) => ({ ...x, sensor_name: e.target.value, unit: sensor?.unit || x.unit }));
                  }}
                >
                  {SENSORS.map((s) => <option key={s.id} value={s.id}>{s.label}</option>)}
                </select>
              </label>

              <label>Operator
                <select value={form.operator} onChange={(e) => setForm((x) => ({ ...x, operator: e.target.value }))}>
                  {OPERATORS.map((o) => <option key={o} value={o}>{o}</option>)}
                </select>
              </label>

              <label>Value
                <input type="number" value={form.value} onChange={(e) => setForm((x) => ({ ...x, value: e.target.value }))} required />
              </label>

              <label>Unit
                <input value={form.unit} onChange={(e) => setForm((x) => ({ ...x, unit: e.target.value }))} />
              </label>

              <label>Actuator Name
                <select value={form.actuator_name} onChange={(e) => setForm((x) => ({ ...x, actuator_name: e.target.value }))}>
                  {ACTUATORS.map((a) => <option key={a} value={a}>{a}</option>)}
                </select>
              </label>

              <label>Target State
                <select value={form.target_state} onChange={(e) => setForm((x) => ({ ...x, target_state: e.target.value }))}>
                  <option value="ON">ON</option>
                  <option value="OFF">OFF</option>
                </select>
              </label>

              <div className="rule-actions">
                <button className="rule-action-btn" type="submit">{canEdit ? 'Update Rule' : 'Create Rule'}</button>
                {canEdit && <button className="rule-action-btn" type="button" onClick={() => setForm(EMPTY_FORM)}>Cancel</button>}
              </div>
            </form>

            <div className="rules-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Condition</th>
                    <th>Action</th>
                    <th>Status</th>
                    <th>Operations</th>
                  </tr>
                </thead>
                <tbody>
                  {rules.map((r) => (
                    <tr key={r.id}>
                      <td>{r.id}</td>
                      <td>{`IF ${r.sensor_name} ${r.operator} ${r.value} ${r.unit || ''}`}</td>
                      <td>{`THEN set ${r.actuator_name} to ${r.target_state}`}</td>
                      <td>{r.enabled === false ? 'Disabled' : 'Enabled'}</td>
                      <td>
                        <button onClick={() => editRule(r)}>Edit</button>
                        <button onClick={() => toggleRule(r)}>{r.enabled === false ? 'Enable' : 'Disable'}</button>
                        <button onClick={() => confirmDelete(r.id)}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      )}

      {page === 'telemetry' && (
        <div key="telemetry" className="page-content">
          <section className="block">
            <h2>Telemetry (Stream)</h2>
            <div className="charts-grid">
              {TOPICS.map((t) => <MiniChart key={t} title={t} unit={TOPIC_UNIT[t]} points={series[t] || []} />)}
            </div>
          </section>
        </div>
      )}

      {dialog.open && (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card">
            <h3>{dialog.mode === 'confirm' ? 'Confirm Action' : 'Notice'}</h3>
            <p>{dialog.message}</p>
            <div className="modal-actions">
              {dialog.mode === 'confirm' && <button onClick={onDialogConfirm}>Confirm</button>}
              <button onClick={closeDialog}>{dialog.mode === 'confirm' ? 'Cancel' : 'OK'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
