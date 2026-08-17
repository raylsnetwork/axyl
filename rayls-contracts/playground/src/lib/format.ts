const WEI = 10n ** 18n;

/** bigint RLS amount (1e18 scale) -> a human-friendly abbreviated string, e.g. "1.5B". */
export function formatRls(amount: bigint): string {
  const whole = amount / WEI;
  const units: [bigint, string][] = [
    [1_000_000_000n, "B"],
    [1_000_000n, "M"],
    [1_000n, "K"],
  ];
  for (const [threshold, suffix] of units) {
    if (whole >= threshold) {
      const scaled = Number(whole) / Number(threshold);
      return `${scaled.toFixed(scaled >= 100 ? 0 : 1)}${suffix}`;
    }
  }
  return whole.toString();
}

/** bps bigint -> a percentage string, e.g. 3600n -> "36.00%". */
export function formatBps(bps: bigint): string {
  return `${(Number(bps) / 100).toFixed(2)}%`;
}

/** Parse a plain decimal-RLS string (e.g. "500000") from a form input into 1e18-scale bigint. */
export function parseRls(input: string): bigint {
  const trimmed = input.trim();
  if (trimmed === "" || Number.isNaN(Number(trimmed))) return 0n;
  const [wholePart, fracPart = ""] = trimmed.split(".");
  const fracPadded = (fracPart + "0".repeat(18)).slice(0, 18);
  const sign = wholePart.startsWith("-") ? -1n : 1n;
  const wholeDigits = wholePart.replace("-", "") || "0";
  return sign * (BigInt(wholeDigits) * WEI + BigInt(fracPadded || "0"));
}
