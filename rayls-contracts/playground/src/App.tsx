import { useState } from "react";
import { CurvePlayground } from "./components/CurvePlayground";
import { PRIORITY_CURVE_ADDRESS, OPEN_TIER_CURVE_ADDRESS } from "./lib/chain";

const TABS = [
  { key: "priority", label: "Track A · Priority", address: PRIORITY_CURVE_ADDRESS },
  { key: "openTier", label: "Track B · Open Tier", address: OPEN_TIER_CURVE_ADDRESS },
] as const;

export function App() {
  const [activeTab, setActiveTab] = useState<(typeof TABS)[number]["key"]>("priority");
  const tab = TABS.find((t) => t.key === activeTab)!;

  return (
    <div className="app">
      <h1>RewardCurve Playground</h1>
      <p className="muted">Local dApp for axyl issue #103 — a revenue-based reward curve for staking.</p>

      <nav className="tabs">
        {TABS.map((t) => (
          <button
            key={t.key}
            className={t.key === activeTab ? "tab active" : "tab"}
            onClick={() => setActiveTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </nav>

      <CurvePlayground address={tab.address as `0x${string}` | undefined} label={tab.label} />
    </div>
  );
}
