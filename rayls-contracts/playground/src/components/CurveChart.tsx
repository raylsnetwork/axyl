import { useState } from "react";
import { formatBps, formatRls } from "../lib/format";

type Point = { stake: bigint; apyBps: bigint };

const WIDTH = 560;
const HEIGHT = 220;
const PAD_LEFT = 44;
const PAD_RIGHT = 12;
const PAD_TOP = 12;
const PAD_BOTTOM = 28;

const TOOLTIP_W = 108;
const TOOLTIP_H = 38;

export function CurveChart({ points, highlightStake }: { points: Point[]; highlightStake?: bigint }) {
  const [hovered, setHovered] = useState<number | null>(null);

  if (points.length === 0) return null;

  const logStakes = points.map((p) => Math.log10(Number(p.stake) / 1e18));
  const apys = points.map((p) => Number(p.apyBps));
  const minLog = Math.min(...logStakes);
  const maxLog = Math.max(...logStakes);
  const maxApy = Math.max(...apys, 1);

  const plotW = WIDTH - PAD_LEFT - PAD_RIGHT;
  const plotH = HEIGHT - PAD_TOP - PAD_BOTTOM;

  const x = (logStake: number) => PAD_LEFT + ((logStake - minLog) / (maxLog - minLog || 1)) * plotW;
  const y = (apy: number) => PAD_TOP + plotH - (apy / maxApy) * plotH;

  const pathD = points
    .map((_p, i) => `${i === 0 ? "M" : "L"} ${x(logStakes[i]).toFixed(1)} ${y(apys[i]).toFixed(1)}`)
    .join(" ");

  const highlightLog = highlightStake && highlightStake > 0n ? Math.log10(Number(highlightStake) / 1e18) : undefined;
  const highlightVisible = highlightLog !== undefined && highlightLog >= minLog && highlightLog <= maxLog;

  const tooltip = hovered !== null ? { i: hovered, px: x(logStakes[hovered]), py: y(apys[hovered]) } : undefined;
  const tooltipLeft = tooltip
    ? Math.min(Math.max(tooltip.px - TOOLTIP_W / 2, PAD_LEFT), WIDTH - PAD_RIGHT - TOOLTIP_W)
    : 0;
  const tooltipAbove = tooltip ? tooltip.py - TOOLTIP_H - 10 >= 0 : true;
  const tooltipTop = tooltip ? (tooltipAbove ? tooltip.py - TOOLTIP_H - 10 : tooltip.py + 10) : 0;

  return (
    <svg
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      width="100%"
      height={HEIGHT}
      role="img"
      aria-label="APY vs. total RLS staked"
      onMouseLeave={() => setHovered(null)}
    >
      {/* y-axis gridlines + labels */}
      {[0, 0.5, 1].map((frac) => {
        const apyVal = maxApy * frac;
        return (
          <g key={frac}>
            <line
              x1={PAD_LEFT}
              x2={WIDTH - PAD_RIGHT}
              y1={y(apyVal)}
              y2={y(apyVal)}
              stroke="currentColor"
              strokeOpacity={0.15}
            />
            <text x={PAD_LEFT - 6} y={y(apyVal)} textAnchor="end" dominantBaseline="middle" fontSize={10} fill="currentColor">
              {formatBps(BigInt(Math.round(apyVal)))}
            </text>
          </g>
        );
      })}

      {/* x-axis labels */}
      {points.map((p, i) => (
        <text
          key={p.stake.toString()}
          x={x(logStakes[i])}
          y={HEIGHT - 8}
          textAnchor="middle"
          fontSize={9}
          fill="currentColor"
          opacity={0.7}
        >
          {i % 2 === 0 ? formatRls(p.stake) : ""}
        </text>
      ))}

      {highlightVisible && (
        <line x1={x(highlightLog!)} x2={x(highlightLog!)} y1={PAD_TOP} y2={PAD_TOP + plotH} stroke="currentColor" strokeOpacity={0.35} strokeDasharray="4 3" />
      )}

      {tooltip && (
        <line
          x1={tooltip.px}
          x2={tooltip.px}
          y1={PAD_TOP}
          y2={PAD_TOP + plotH}
          stroke="currentColor"
          strokeOpacity={0.25}
        />
      )}

      <path d={pathD} fill="none" stroke="var(--accent)" strokeWidth={2} />

      {/* visible points */}
      {points.map((p, i) => (
        <circle
          key={p.stake.toString()}
          cx={x(logStakes[i])}
          cy={y(apys[i])}
          r={hovered === i ? 5 : 3}
          fill="var(--accent)"
        />
      ))}

      {/* larger invisible hit targets, so hovering near a point (not just exactly on it) works */}
      {points.map((p, i) => (
        <circle
          key={`hit-${p.stake.toString()}`}
          cx={x(logStakes[i])}
          cy={y(apys[i])}
          r={12}
          fill="transparent"
          onMouseEnter={() => setHovered(i)}
        />
      ))}

      {tooltip && (
        <g pointerEvents="none">
          <rect
            x={tooltipLeft}
            y={tooltipTop}
            width={TOOLTIP_W}
            height={TOOLTIP_H}
            rx={5}
            fill="var(--panel)"
            stroke="var(--border)"
          />
          <text x={tooltipLeft + 8} y={tooltipTop + 15} fontSize={10} fill="currentColor">
            Stake: {formatRls(points[tooltip.i].stake)}
          </text>
          <text x={tooltipLeft + 8} y={tooltipTop + 29} fontSize={11} fontWeight={600} fill="var(--accent)">
            APY: {formatBps(points[tooltip.i].apyBps)}
          </text>
        </g>
      )}
    </svg>
  );
}
