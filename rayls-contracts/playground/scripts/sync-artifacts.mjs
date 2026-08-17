// Copies the compiled RewardCurve + ERC1967Proxy artifacts (ABI + bytecode) out of
// rayls-contracts' own forge build output (this playground lives at rayls-contracts/playground/,
// one directory below), so the playground never needs its own Solidity toolchain.
// Mirrors the pattern already used by axyl-automation-testing/scripts/sync-abi.sh.
import { existsSync, mkdirSync, copyFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..");
// Default: rayls-contracts is the playground's parent directory. Override AXYL_CONTRACTS_PATH
// if you've copied this playground elsewhere and it's no longer nested under rayls-contracts.
const contractsPath = resolve(repoRoot, process.env.AXYL_CONTRACTS_PATH ?? "..");

const artifacts = [
  { src: "out/RewardCurve.sol/RewardCurve.json", dest: "RewardCurve.json" },
  { src: "out/ERC1967Proxy.sol/ERC1967Proxy.json", dest: "ERC1967Proxy.json" },
];

if (!existsSync(contractsPath)) {
  console.error(
    `Could not find rayls-contracts at "${contractsPath}".\n` +
      `Set AXYL_CONTRACTS_PATH to the rayls-contracts directory, e.g.:\n` +
      `  AXYL_CONTRACTS_PATH="C:/Users/you/axyl/rayls-contracts" npm run sync-artifacts`,
  );
  process.exit(1);
}

const destDir = resolve(repoRoot, "src/artifacts");
mkdirSync(destDir, { recursive: true });

for (const { src, dest } of artifacts) {
  const srcPath = resolve(contractsPath, src);
  if (!existsSync(srcPath)) {
    console.error(
      `Missing "${srcPath}".\n` +
        `Run "forge build" in ${contractsPath} first (the artifact only exists after a build).`,
    );
    process.exit(1);
  }
  copyFileSync(srcPath, resolve(destDir, dest));
  console.log(`synced ${dest}`);
}
