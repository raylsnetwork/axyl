import { createPublicClient, createWalletClient, http, type Abi } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import curveArtifact from "../artifacts/RewardCurve.json";

export const curveAbi = curveArtifact.abi as Abi;

const rpcUrl = import.meta.env.VITE_RPC_URL ?? "http://127.0.0.1:8546";

export const anvilChain = {
  id: 31337,
  name: "Anvil",
  nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
  rpcUrls: { default: { http: [rpcUrl] } },
} as const;

export const publicClient = createPublicClient({
  chain: anvilChain,
  transport: http(),
});

// Local-anvil-only: this is Foundry's well-known default account #0 key, printed in Foundry's
// own docs. Never reuse this pattern with a real key on a real network — see README.
const adminPrivateKey = import.meta.env.VITE_ADMIN_PRIVATE_KEY as `0x${string}` | undefined;

export const account = adminPrivateKey ? privateKeyToAccount(adminPrivateKey) : undefined;

export const walletClient = account
  ? createWalletClient({ account, chain: anvilChain, transport: http() })
  : undefined;

export const PRIORITY_CURVE_ADDRESS = import.meta.env.VITE_PRIORITY_CURVE_ADDRESS as
  | `0x${string}`
  | undefined;
export const OPEN_TIER_CURVE_ADDRESS = import.meta.env.VITE_OPEN_TIER_CURVE_ADDRESS as
  | `0x${string}`
  | undefined;
