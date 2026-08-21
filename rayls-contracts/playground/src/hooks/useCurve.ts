import { useCallback, useEffect, useState } from "react";
import { curveAbi, publicClient, walletClient, account } from "../lib/chain";

// Log-spaced stake levels for the curve chart: ~10M -> ~100B RLS (1e18 scale).
export const CHART_STAKE_LEVELS: bigint[] = [
  10_000_000n, 30_000_000n, 100_000_000n, 300_000_000n, 1_000_000_000n, 3_000_000_000n,
  10_000_000_000n, 30_000_000_000n, 100_000_000_000n,
].map((v) => v * 10n ** 18n);

export const PHASE_LABELS = ["FoundationHeavy", "Mixed", "RevenueOnly"] as const;

export type TxLogEntry = {
  hash: `0x${string}`;
  label: string;
  status: "pending" | "confirmed" | "failed";
};

export type CurveSnapshot = {
  baseMonthly: bigint;
  variableMonthly: bigint;
  annualEmission: bigint;
  currentPhase: number;
  apyBps: bigint;
  baseApyBps: bigint;
  variableApyBps: bigint;
  estimatedYield: bigint;
  curvePoints: { stake: bigint; apyBps: bigint }[];
};

async function fetchSnapshot(
  address: `0x${string}`,
  rlsStaked: bigint,
  yieldAmount: bigint,
): Promise<CurveSnapshot> {
  const [breakdown, currentPhase, apyBps, apyBreakdown, estimatedYield, curveApys] =
    await Promise.all([
      publicClient.readContract({
        address,
        abi: curveAbi,
        functionName: "getEmissionBreakdown",
      }) as Promise<[bigint, bigint, bigint]>,
      publicClient.readContract({ address, abi: curveAbi, functionName: "currentPhase" }) as Promise<number>,
      publicClient.readContract({
        address,
        abi: curveAbi,
        functionName: "getCurrentApyBps",
        args: [rlsStaked],
      }) as Promise<bigint>,
      publicClient.readContract({
        address,
        abi: curveAbi,
        functionName: "getApyBreakdown",
        args: [rlsStaked],
      }) as Promise<[bigint, bigint]>,
      publicClient.readContract({
        address,
        abi: curveAbi,
        functionName: "estimateYield",
        args: [yieldAmount, rlsStaked],
      }) as Promise<bigint>,
      publicClient.readContract({
        address,
        abi: curveAbi,
        functionName: "previewCurve",
        args: [CHART_STAKE_LEVELS],
      }) as Promise<bigint[]>,
    ]);

  const [baseMonthly, variableMonthly, annualEmission] = breakdown;
  const [baseApyBps, variableApyBps] = apyBreakdown;

  return {
    baseMonthly,
    variableMonthly,
    annualEmission,
    currentPhase,
    apyBps,
    baseApyBps,
    variableApyBps,
    estimatedYield,
    curvePoints: CHART_STAKE_LEVELS.map((stake, i) => ({ stake, apyBps: curveApys[i] })),
  };
}

/**
 * @param rlsStaked Hypothetical TOTAL network stake — this is what positions the curve and
 *                  determines the live APY, mirroring RewardCurve's `rlsStaked` parameter.
 * @param yieldAmount A smaller, separate "if I personally staked this much" amount, evaluated
 *                     against `rlsStaked` via `estimateYield`.
 */
export function useCurve(address: `0x${string}` | undefined, rlsStaked: bigint, yieldAmount: bigint) {
  const [data, setData] = useState<CurveSnapshot | undefined>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | undefined>();
  const [txLog, setTxLog] = useState<TxLogEntry[]>([]);

  const refetch = useCallback(async () => {
    if (!address) return;
    try {
      const snapshot = await fetchSnapshot(address, rlsStaked, yieldAmount);
      setData(snapshot);
      setError(undefined);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [address, rlsStaked, yieldAmount]);

  useEffect(() => {
    setLoading(true);
    refetch();
  }, [refetch]);

  const send = useCallback(
    async (label: string, functionName: string, args: readonly unknown[]) => {
      if (!address || !walletClient || !account) return;
      let hash: `0x${string}`;
      try {
        hash = await walletClient.writeContract({
          address,
          abi: curveAbi,
          functionName,
          args,
          account,
          chain: walletClient.chain,
        });
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
        return;
      }
      setTxLog((log) => [{ hash, label, status: "pending" as const }, ...log].slice(0, 20));
      try {
        const receipt = await publicClient.waitForTransactionReceipt({ hash });
        const finalStatus: TxLogEntry["status"] = receipt.status === "success" ? "confirmed" : "failed";
        setTxLog((log) => log.map((entry) => (entry.hash === hash ? { ...entry, status: finalStatus } : entry)));
        await refetch();
      } catch (err) {
        setTxLog((log) => log.map((entry) => (entry.hash === hash ? { ...entry, status: "failed" } : entry)));
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [address, refetch],
  );

  return {
    data,
    loading,
    error,
    txLog,
    setBaseMonthlyEmission: (amount: bigint) => send("Set base emission", "setBaseMonthlyEmission", [amount]),
    recordRevenue: (amount: bigint) => send("Record revenue", "recordRevenue", [amount]),
    resetMonthlyRevenue: () => send("Reset monthly revenue", "resetMonthlyRevenue", []),
    setPhase: (phase: number) => send(`Set phase: ${PHASE_LABELS[phase]}`, "setPhase", [phase]),
  };
}
