import { useMemo, useState } from "react";
import { useCurve, PHASE_LABELS } from "../hooks/useCurve";
import { formatBps, formatRls, parseRls } from "../lib/format";
import { CurveChart } from "./CurveChart";

export function CurvePlayground({ address, label }: { address: `0x${string}` | undefined; label: string }) {
  const [rlsStakedInput, setRlsStakedInput] = useState("500000000");
  const [yieldAmountInput, setYieldAmountInput] = useState("10000");
  const [baseInput, setBaseInput] = useState("500000");
  const [revenueInput, setRevenueInput] = useState("50000");
  const [phaseChoice, setPhaseChoice] = useState(0);

  const rlsStaked = useMemo(() => parseRls(rlsStakedInput), [rlsStakedInput]);
  const yieldAmount = useMemo(() => parseRls(yieldAmountInput), [yieldAmountInput]);

  const { data, loading, error, txLog, setBaseMonthlyEmission, recordRevenue, resetMonthlyRevenue, setPhase } =
    useCurve(address, rlsStaked, yieldAmount);

  if (!address) {
    return (
      <div className="playground">
        <p className="error">No address configured for "{label}" — run `npm run deploy` first, then restart the dev server.</p>
      </div>
    );
  }

  return (
    <div className="playground">
      <header className="playground-header">
        <h2>{label}</h2>
        <code className="address">{address}</code>
      </header>

      {error && <p className="error">{error}</p>}

      <section className="stats">
        <div className="stat">
          <span className="stat-label">Current APY</span>
          <span className="stat-value big">{data ? formatBps(data.apyBps) : "…"}</span>
          {data && (
            <span className="stat-sub">
              base {formatBps(data.baseApyBps)} + revenue {formatBps(data.variableApyBps)}
            </span>
          )}
        </div>
        <div className="stat">
          <span className="stat-label">Annual emission</span>
          <span className="stat-value">{data ? `${formatRls(data.annualEmission)} RLS` : "…"}</span>
          {data && (
            <span className="stat-sub">
              base {formatRls(data.baseMonthly)}/mo + revenue {formatRls(data.variableMonthly)}/mo
            </span>
          )}
        </div>
        <div className="stat">
          <span className="stat-label">Phase</span>
          <span className="stat-value">{data ? PHASE_LABELS[data.currentPhase] : "…"}</span>
        </div>
      </section>

      <section className="controls">
        <div className="control-row">
          <label>
            Total RLS staked (network)
            <input value={rlsStakedInput} onChange={(e) => setRlsStakedInput(e.target.value)} inputMode="decimal" />
          </label>
          <label>
            My stake amount
            <input value={yieldAmountInput} onChange={(e) => setYieldAmountInput(e.target.value)} inputMode="decimal" />
          </label>
          <div className="yield-result">
            <span className="stat-label">Estimated annual yield</span>
            <span className="stat-value">{data ? `${formatRls(data.estimatedYield)} RLS` : "…"}</span>
          </div>
        </div>

        <div className="control-row">
          <label>
            Set base monthly emission
            <input value={baseInput} onChange={(e) => setBaseInput(e.target.value)} inputMode="decimal" />
          </label>
          <button onClick={() => setBaseMonthlyEmission(parseRls(baseInput))} disabled={loading}>
            Set
          </button>
        </div>

        <div className="control-row">
          <label>
            Record revenue
            <input value={revenueInput} onChange={(e) => setRevenueInput(e.target.value)} inputMode="decimal" />
          </label>
          <button onClick={() => recordRevenue(parseRls(revenueInput))} disabled={loading}>
            Record Revenue
          </button>
          <button onClick={() => resetMonthlyRevenue()} disabled={loading} className="secondary">
            Reset Monthly Revenue
          </button>
        </div>

        <div className="control-row">
          <label>
            Phase
            <select value={phaseChoice} onChange={(e) => setPhaseChoice(Number(e.target.value))}>
              {PHASE_LABELS.map((name, i) => (
                <option key={name} value={i}>
                  {name}
                </option>
              ))}
            </select>
          </label>
          <button onClick={() => setPhase(phaseChoice)} disabled={loading}>
            Set Phase
          </button>
        </div>
      </section>

      <section className="chart">
        <h3>APY vs. total RLS staked</h3>
        {data && <CurveChart points={data.curvePoints} highlightStake={rlsStaked} />}
      </section>

      <section className="txlog">
        <h3>Recent transactions</h3>
        {txLog.length === 0 && <p className="muted">No transactions yet.</p>}
        <ul>
          {txLog.map((entry) => (
            <li key={entry.hash} className={`tx-${entry.status}`}>
              <span>{entry.label}</span>
              <code>{entry.hash.slice(0, 10)}…</code>
              <span className="tx-status">{entry.status}</span>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
