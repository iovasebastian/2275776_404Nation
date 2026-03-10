import React, { useEffect, useMemo, useState } from 'react';

const API_BASE = (import.meta.env.VITE_API_BASE || 'http://localhost:8003').replace(/\/$/, '');
const RULES_KEY = 'mars-rules-local';

const SENSORS = [
  { id: 'greenhouse_temperature', label: 'greenhouse_temperature', unit: 'C' },
  { id: 'entrance_humidity', label: 'entrance_humidity', unit: '%' },
  { id: 'co2_hall', label: 'co2_hall', unit: 'ppm' },
  { id: 'hydroponic_ph', label: 'hydroponic_ph', unit: 'pH' },
  { id: 'water_tank_level', label: 'water_tank_level', unit: '%' },
  { id: 'corridor_pressure', label: 'corridor_pressure', unit: 'kPa' },
  { id: 'air_quality_pm25', label: 'air_quality_pm25', unit: 'ug/m3' },
  { id: 'air_quality_voc', label: 'air_quality_voc', unit: 'ppb' }
];

const ACTUATORS = ['cooling_fan', 'entrance_humidifier', 'hall_ventilation', 'habitat_heater'];
const OPERATORS = ['<', '<=', '=', '>', '>='];

const EMPTY_FORM = {
  id: '',
  sensor_name: 'greenhouse_temperature',
  operator: '>',
  threshold_value: '28',
  actuator_name: 'cooling_fan',
  action_state: 'ON',
  enabled: true
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

  if (!res.ok) {
    throw new Error(`${method} ${path} -> ${res.status}`);
  }

  const contentType = res.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return res.json();
  }
  return null;
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

const ruleBaseSig = (r) =>
  [r.sensor_name, r.operator, Number(r.threshold_value), r.actuator_name].join('|');

const ruleSig = (r) =>
  `${ruleBaseSig(r)}|${r.action_state}`;

const uniqueRules = (rules) =>
  Array.from(new Map(rules.map((r) => [String(r.id), r])).values());

const getEventNumericValue = (event) => {
  if (!event) return null;

  if (typeof event.value === 'number') return event.value;
  if (typeof event.level_pct === 'number') return event.level_pct;
  if (typeof event.pm25_ug_m3 === 'number') return event.pm25_ug_m3;
  if (typeof event.temperature_c === 'number') return event.temperature_c;
  if (typeof event.cycles_per_hour === 'number') return event.cycles_per_hour;
  if (typeof event.power_kw === 'number') return event.power_kw;

  if (Array.isArray(event.measurements) && event.measurements.length > 0) {
    const firstNumeric = event.measurements.find((m) => typeof m.value === 'number');
    return firstNumeric?.value ?? null;
  }

  return null;
};

const sensorText = (event) => {
  if (!event) return '-';

  if (typeof event.value === 'number') {
    return `${event.value} ${event.unit || ''}`.trim();
  }

  if (typeof event.level_pct === 'number') {
    return `${event.level_pct}%`;
  }

  if (typeof event.pm25_ug_m3 === 'number') {
    return `${event.pm25_ug_m3} ug/m3`;
  }

  if (typeof event.temperature_c === 'number') {
    return `${event.temperature_c} C`;
  }

  if (typeof event.cycles_per_hour === 'number') {
    return `${event.cycles_per_hour} cycles/h`;
  }

  if (typeof event.power_kw === 'number') {
    return `${event.power_kw} kW`;
  }

  if (Array.isArray(event.measurements)) {
    return event.measurements
      .map((m) => `${m.metric}:${m.value}${m.unit ? ` ${m.unit}` : ''}`)
      .join(' | ');
  }

  return '-';
};

function MiniChart({ title, unit, points }) {
  if (!points.length) {
    return (
      <div className="chart-panel">
        <h4>{title}</h4>
        <div className="empty">Waiting for data...</div>
      </div>
    );
  }

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

  const d = pts
    .map((p, i) => `${i ? 'L' : 'M'}${p.x.toFixed(1)},${p.y.toFixed(1)}`)
    .join(' ');

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
            <text x={p.x} y={p.y - 8} textAnchor="middle" className="point-label">
              {p.value.toFixed(2)}
            </text>
          </g>
        ))}
        <text x="8" y="14" className="meta">
          min: {min.toFixed(2)} {unit}
        </text>
        <text x="8" y={h - 8} className="meta">
          max: {max.toFixed(2)} {unit}
        </text>
      </svg>
    </div>
  );
}

export default function App() {
  const [page, setPage] = useState('operations');
  const [latestEvents, setLatestEvents] = useState({});
  const [series, setSeries] = useState(
    Object.fromEntries(SENSORS.map((s) => [s.id, []]))
  );
  const [actuators, setActuators] = useState(
    Object.fromEntries(ACTUATORS.map((a) => [a, 'OFF']))
  );
  const [rules, setRules] = useState([]);
  const [form, setForm] = useState(EMPTY_FORM);
  const [dialog, setDialog] = useState({
    open: false,
    mode: 'info',
    message: '',
    ruleId: ''
  });

  const canEdit = Boolean(form.id);

  const sensorMap = useMemo(
    () => Object.fromEntries(SENSORS.map((s) => [s.id, s])),
    []
  );

  const closeDialog = () =>
    setDialog({ open: false, mode: 'info', message: '', ruleId: '' });

  const showInfo = (message) =>
    setDialog({ open: true, mode: 'info', message, ruleId: '' });

  const confirmDelete = (ruleId) =>
    setDialog({
      open: true,
      mode: 'confirm',
      message: 'Are you sure you want to delete this rule?',
      ruleId
    });

  useEffect(() => {
    const loadInitialLatest = async () => {
      try {
        const rows = await request('/latest');
        const next = {};

        for (const row of rows || []) {
          const sensorId = row.sensor_id;
          if (sensorId) next[sensorId] = row;
        }

        setLatestEvents(next);

        setSeries((prev) => {
          const copy = { ...prev };
          for (const sensor of SENSORS) {
            const event = next[sensor.id];
            const value = getEventNumericValue(event);
            if (typeof value === 'number') {
              copy[sensor.id] = [{ ts: Date.now(), value }];
            }
          }
          return copy;
        });
      } catch {
        showInfo('Failed to load latest sensor values.');
      }
    };

    loadInitialLatest();
  }, []);

  useEffect(() => {
    const loadRules = async () => {
      try {
        const data = await request('/rules');

        if (data?.rules) {
          const normalized = data.rules.map((r) => ({
            ...r,
            enabled: true
          }));

          setRules(uniqueRules(normalized));
          localRules.save(normalized);
        }
      } catch (err) {
        // fallback to local storage if backend fails
        const stored = localRules.load();
        setRules(uniqueRules(stored));
      }
    };

    loadRules();
  }, []);

  // ---- Actuator polling: fetch on mount + every second ----
  useEffect(() => {
    const ACTUATOR_POLL_INTERVAL = 1000;

    const fetchActuators = async () => {
      try {
        const data = await request('/actuators');
        // Expects { actuators: { cooling_fan: "ON", ... } }
        const map = data?.actuators;
        if (map && typeof map === 'object') {
          setActuators((prev) => {
            const next = { ...prev };
            for (const name of ACTUATORS) {
              if (map[name] !== undefined) {
                next[name] = map[name];
              }
            }
            return next;
          });
        }
      } catch {
        // Silently ignore – the UI will keep its last known state
      }
    };

    fetchActuators();                                       // initial fetch
    const id = setInterval(fetchActuators, ACTUATOR_POLL_INTERVAL); // periodic poll
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const ws = new WebSocket(wsUrl('/ws'));

    ws.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data);

        if (payload?.type === 'snapshot' && Array.isArray(payload.data)) {
          setLatestEvents((prev) => {
            const next = { ...prev };
            for (const row of payload.data) {
              if (row?.sensor_id) next[row.sensor_id] = row;
            }
            return next;
          });

          setSeries((prev) => {
            const next = { ...prev };
            for (const row of payload.data) {
              if (!row?.sensor_id) continue;
              const value = getEventNumericValue(row);
              if (typeof value !== 'number') continue;
              next[row.sensor_id] = [...(next[row.sensor_id] || []), { ts: Date.now(), value }].slice(-20);
            }
            return next;
          });

          return;
        }

        const sensorId = payload?.sensor_id;
        if (!sensorId) return;

        setLatestEvents((prev) => ({ ...prev, [sensorId]: payload }));

        const value = getEventNumericValue(payload);
        if (typeof value === 'number') {
          setSeries((prev) => ({
            ...prev,
            [sensorId]: [...(prev[sensorId] || []), { ts: Date.now(), value }].slice(-20)
          }));
        }
      } catch {
        // ignore malformed websocket messages
      }
    };

    ws.onerror = () => {
      showInfo('WebSocket connection failed.');
    };

    return () => ws.close();
  }, []);

  const persistRulesLocally = (next) => {
    setRules(next);
    localRules.save(next);
  };

  const setActuatorState = async (name, state) => {
    try {
      await request(`/change-actuator/${encodeURIComponent(name)}/${encodeURIComponent(state)}`, {
        method: 'POST'
      });

      setActuators((prev) => ({ ...prev, [name]: state }));
    } catch {
      showInfo('Failed to switch actuator state.');
    }
  };

  const submitRule = async (e) => {
  e.preventDefault();

  const candidate = {
    ...form,
    threshold_value: Number(form.threshold_value),
    enabled: form.enabled !== false
  };

  const peerRules = rules.filter((r) =>
    canEdit ? String(r.id) !== String(form.id) : true
  );

  if (peerRules.some((r) => ruleSig(r) === ruleSig(candidate))) {
    showInfo('The rule already exists.');
    return;
  }

  if (
    peerRules.some(
      (r) =>
        ruleBaseSig(r) === ruleBaseSig(candidate) &&
        r.action_state !== candidate.action_state
    )
  ) {
    showInfo('This rule conflicts with the existing regulations.');
    return;
  }

  try {
    if (canEdit) {
      const updatedRule = {
        ...candidate,
        id: form.id
      };

      await request('/update-rule', {
        method: 'PUT',
        body: {
          id: updatedRule.id,
          sensor_name: updatedRule.sensor_name,
          operator: updatedRule.operator,
          threshold_value: updatedRule.threshold_value,
          actuator_name: updatedRule.actuator_name,
          action_state: updatedRule.action_state
        }
      });

      const next = uniqueRules(
        rules.map((r) => (String(r.id) === String(updatedRule.id) ? updatedRule : r))
      );
      persistRulesLocally(next);
    } else {
      const response = await request('/new-rule', {
        method: 'POST',
        body: {
          sensor_name: candidate.sensor_name,
          operator: candidate.operator,
          threshold_value: candidate.threshold_value,
          actuator_name: candidate.actuator_name,
          action_state: candidate.action_state
        }
      });

      const createdRule = {
        ...candidate,
        id: response.id, // or response.rule.id
      };

      const next = uniqueRules([...rules, createdRule]);
      persistRulesLocally(next);
    }

    setForm(EMPTY_FORM);
  } catch {
    showInfo(canEdit ? 'Failed to update rule.' : 'Failed to create rule.');
  }
};

  const editRule = (r) =>
    setForm({
      ...r,
      threshold_value: String(r.threshold_value)
    });

  const toggleRule = (r) => {
    const nextRule = { ...r, enabled: !(r.enabled !== false) };
    const next = rules.map((x) => (String(x.id) === String(r.id) ? nextRule : x));
    persistRulesLocally(next);
  };

  const removeRule = async (id) => {
    try {
      await request(`/delete-rule/${id}`, { method: 'DELETE' });
      persistRulesLocally(rules.filter((r) => String(r.id) !== String(id)));
    } catch {
      showInfo('Failed to delete rule.');
    }
  };

  const onDialogConfirm = () => {
    if (dialog.mode === 'confirm' && dialog.ruleId) {
      removeRule(dialog.ruleId);
    }
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
            <h2>Latest Sensors</h2>
            <div className="sensor-grid">
              {SENSORS.map((s) => {
                const event = latestEvents[s.id];
                return (
                  <div className="sensor-card" key={s.id}>
                    <strong>{s.label}</strong>
                    <div className="sensor-value">{sensorText(event)}</div>
                    <small>Status: {event ? 'online' : 'waiting'}</small>
                  </div>
                );
              })}
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
                        <button
                          className={state === 'ON' ? 'selected' : ''}
                          onClick={() => state !== 'ON' && setActuatorState(name, 'ON')}
                        >
                          ON
                        </button>
                        <button
                          className={state === 'OFF' ? 'selected' : ''}
                          onClick={() => state !== 'OFF' && setActuatorState(name, 'OFF')}
                        >
                          OFF
                        </button>
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
              <label>
                Sensor Name
                <select
                  value={form.sensor_name}
                  onChange={(e) =>
                    setForm((x) => ({ ...x, sensor_name: e.target.value }))
                  }
                >
                  {SENSORS.map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.label}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                Operator
                <select
                  value={form.operator}
                  onChange={(e) => setForm((x) => ({ ...x, operator: e.target.value }))}
                >
                  {OPERATORS.map((o) => (
                    <option key={o} value={o}>
                      {o}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                Threshold Value
                <input
                  type="number"
                  value={form.threshold_value}
                  onChange={(e) =>
                    setForm((x) => ({ ...x, threshold_value: e.target.value }))
                  }
                  required
                />
              </label>

              <label>
                Actuator Name
                <select
                  value={form.actuator_name}
                  onChange={(e) =>
                    setForm((x) => ({ ...x, actuator_name: e.target.value }))
                  }
                >
                  {ACTUATORS.map((a) => (
                    <option key={a} value={a}>
                      {a}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                Action State
                <select
                  value={form.action_state}
                  onChange={(e) =>
                    setForm((x) => ({ ...x, action_state: e.target.value }))
                  }
                >
                  <option value="ON">ON</option>
                  <option value="OFF">OFF</option>
                </select>
              </label>

              <div className="rule-actions">
                <button className="rule-action-btn" type="submit">
                  {canEdit ? 'Update Rule' : 'Create Rule'}
                </button>
                {canEdit && (
                  <button
                    className="rule-action-btn"
                    type="button"
                    onClick={() => setForm(EMPTY_FORM)}
                  >
                    Cancel
                  </button>
                )}
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
                      <td>{`IF ${r.sensor_name} ${r.operator} ${r.threshold_value}`}</td>
                      <td>{`THEN set ${r.actuator_name} to ${r.action_state}`}</td>
                      <td>{r.enabled === false ? 'Disabled' : 'Enabled'}</td>
                      <td>
                        <button onClick={() => editRule(r)}>Edit</button>
                        <button onClick={() => toggleRule(r)}>
                          {r.enabled === false ? 'Enable' : 'Disable'}
                        </button>
                        <button onClick={() => confirmDelete(r.id)}>Delete</button>
                      </td>
                    </tr>
                  ))}
                  {!rules.length && (
                    <tr>
                      <td colSpan="5">No rules available.</td>
                    </tr>
                  )}
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
              {SENSORS.map((sensor) => (
                <MiniChart
                  key={sensor.id}
                  title={sensor.label}
                  unit={sensorMap[sensor.id]?.unit || ''}
                  points={series[sensor.id] || []}
                />
              ))}
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
              {dialog.mode === 'confirm' && (
                <button onClick={onDialogConfirm}>Confirm</button>
              )}
              <button onClick={closeDialog}>
                {dialog.mode === 'confirm' ? 'Cancel' : 'OK'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}