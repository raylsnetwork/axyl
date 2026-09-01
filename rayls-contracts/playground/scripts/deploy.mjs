// Deploys two RewardCurve proxies (Track A "priority" + Track B "open tier") to a running local
// anvil chain, seeds them with the same base-heavy / revenue-leaning emission mix used in axyl's
// RewardDistributorExtendedTest.t.sol integration test, and writes the resulting addresses (plus
// the local signer) into .env.local for the Vite app to pick up.
//
// Signs with Foundry's well-known anvil default account #0 key — see README's "Local-only key"
// warning. Never use this key anywhere but a throwaway local anvil chain.
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createWalletClient, createPublicClient, http, encodeFunctionData, parseEther } from "viem";
import { privateKeyToAccount } from "viem/accounts";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..");

const ANVIL_DEFAULT_KEY_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const RPC_URL = process.env.RPC_URL ?? "http://127.0.0.1:8546";

const anvilChain = {
  id: 31337,
  name: "Anvil",
  nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
  rpcUrls: { default: { http: [RPC_URL] } },
};

function loadArtifact(name) {
  const path = resolve(repoRoot, "src/artifacts", name);
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    console.error(`Could not read ${path} — run "npm run sync-artifacts" first.`);
    throw err;
  }
}

async function main() {
  const account = privateKeyToAccount(ANVIL_DEFAULT_KEY_0);
  const publicClient = createPublicClient({ chain: anvilChain, transport: http() });
  const walletClient = createWalletClient({ account, chain: anvilChain, transport: http() });

  // Fail fast with a clear message if anvil isn't running, instead of a generic fetch error.
  try {
    await publicClient.getChainId();
  } catch {
    console.error(`Could not reach a chain at ${RPC_URL}. Start it first: anvil --host 0.0.0.0`);
    process.exit(1);
  }

  const curveArtifact = loadArtifact("RewardCurve.json");
  const proxyArtifact = loadArtifact("ERC1967Proxy.json");
  const curveAbi = curveArtifact.abi;
  const proxyAbi = proxyArtifact.abi;

  async function deployCurve(label, baseMonthly, revenueToRecord) {
    const implHash = await walletClient.deployContract({
      abi: curveAbi,
      bytecode: curveArtifact.bytecode.object,
      args: [],
    });
    const implReceipt = await publicClient.waitForTransactionReceipt({ hash: implHash });
    const implAddress = implReceipt.contractAddress;

    const initData = encodeFunctionData({
      abi: curveAbi,
      functionName: "initialize",
      args: [account.address],
    });
    const proxyHash = await walletClient.deployContract({
      abi: proxyAbi,
      bytecode: proxyArtifact.bytecode.object,
      args: [implAddress, initData],
    });
    const proxyReceipt = await publicClient.waitForTransactionReceipt({ hash: proxyHash });
    const proxyAddress = proxyReceipt.contractAddress;

    // No separate reporter-role grant needed: initialize() grants REVENUE_REPORTER_ROLE to
    // admin_ (== account) directly. Additional reporters can be added later via grantRole.
    const setBaseHash = await walletClient.writeContract({
      address: proxyAddress,
      abi: curveAbi,
      functionName: "setBaseMonthlyEmission",
      args: [baseMonthly],
    });
    await publicClient.waitForTransactionReceipt({ hash: setBaseHash });

    const recordRevenueHash = await walletClient.writeContract({
      address: proxyAddress,
      abi: curveAbi,
      functionName: "recordRevenue",
      args: [revenueToRecord],
    });
    await publicClient.waitForTransactionReceipt({ hash: recordRevenueHash });

    console.log(`${label}: implementation ${implAddress}, proxy ${proxyAddress}`);
    return proxyAddress;
  }

  const priorityAddress = await deployCurve("Track A · Priority", parseEther("500000"), parseEther("100000"));
  const openTierAddress = await deployCurve("Track B · Open Tier", parseEther("100000"), parseEther("500000"));

  const envContents = [
    `VITE_RPC_URL=${RPC_URL}`,
    `VITE_ADMIN_ADDRESS=${account.address}`,
    `VITE_ADMIN_PRIVATE_KEY=${ANVIL_DEFAULT_KEY_0}`,
    `VITE_PRIORITY_CURVE_ADDRESS=${priorityAddress}`,
    `VITE_OPEN_TIER_CURVE_ADDRESS=${openTierAddress}`,
    "",
  ].join("\n");
  writeFileSync(resolve(repoRoot, ".env.local"), envContents);
  console.log("\nWrote .env.local — run `npm run dev` (restart it if already running).");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
