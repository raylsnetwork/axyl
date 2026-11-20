// Records real MetaMask (official build, fresh throwaway wallet) on the local RLS-flavour chain:
// USDr network -> Send USDr (gas in USDr) -> switch to the RLS chain id -> Send RLS (gas in RLS).
// Only the "wallet" page is kept as the video; the dapp page just issues wallet requests.
import { chromium } from "playwright";
import { execSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { writeFileSync, mkdirSync, rmSync } from "node:fs";

const here = new URL(".", import.meta.url).pathname;
const ext = `${here}../mm-ext/mm13560`;
const profile = `${here}profile-video`;
const videoDir = `${here}video`;
rmSync(profile, { recursive: true, force: true });
rmSync(videoDir, { recursive: true, force: true });
mkdirSync(`${here}shots-video`, { recursive: true });

const NODE = "http://127.0.0.1:8543";
const DEV_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"; // Anvil #0, USDr
const RLS_KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"; // Anvil #1, RLS
const TO = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC";
const sh = (c) => execSync(c, { encoding: "utf8" }).trim();
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.log(new Date().toISOString().slice(11, 19), m);

const ctx = await chromium.launchPersistentContext(profile, {
  channel: "chromium",
  headless: true,
  viewport: { width: 420, height: 900 },
  args: [`--disable-extensions-except=${ext}`, `--load-extension=${ext}`],
});
const sw = ctx.serviceWorkers()[0] ?? (await ctx.waitForEvent("serviceworker"));
const extId = sw.url().split("/")[2];
const EXT = `chrome-extension://${extId}`;
await pause(4000);

// --- onboarding on a throwaway page (its video is discarded) ---
const setup = ctx.pages()[0];
await setup.goto(`${EXT}/home.html`);
await setup.waitForTimeout(4000);
const clickText = async (p, t, wait = 2500) => { await p.getByText(t, { exact: true }).first().click({ timeout: 20000 }); await p.waitForTimeout(wait); };
await clickText(setup, "Create a new wallet");
await clickText(setup, "Use Secret Recovery Phrase", 3000);
const pw = randomBytes(12).toString("base64url");
writeFileSync(`${here}.mm-video-password`, pw);
await setup.locator("input[type=password]").nth(0).fill(pw);
await setup.locator("input[type=password]").nth(1).fill(pw);
await setup.locator("input[type=checkbox]").first().check({ force: true });
await setup.getByRole("button", { name: "Create password" }).click();
await setup.waitForTimeout(4000);
await clickText(setup, "Maybe later", 3000);
await clickText(setup, "Remind me later", 3000);
await setup.getByText("Gather basic usage data").click(); // opt out
await setup.waitForTimeout(800);
await clickText(setup, "Continue", 4000);
await clickText(setup, "Open wallet", 5000);
const unlockIfNeeded = async (p) => {
  if (!p.url().includes("unlock")) return;
  await p.locator("input[type=password]").first().fill(pw);
  await p.keyboard.press("Enter");
  await p.waitForTimeout(4000);
};
await setup.goto(`${EXT}/home.html#/`); await setup.waitForTimeout(4000); await unlockIfNeeded(setup);
log(`wallet created (${setup.url().split("#")[1]})`);

// --- dapp page (not shown) and the recorded wallet page ---
const dapp = await ctx.newPage();
await dapp.goto("http://127.0.0.1:5199/");
await dapp.waitForTimeout(1500);
const wallet = await ctx.newPage();
// Frame recorder: screenshots whichever page is active, with the current caption, ~4 fps.
mkdirSync(`${videoDir}/frames`, { recursive: true });
let active = wallet, caption = "", frame = 0, recording = true;
const captions = [];
const recorder = (async () => {
  while (recording) {
    try {
      await active.screenshot({ path: `${videoDir}/frames/${String(frame).padStart(5, "0")}.png` }); // NOSONAR: UI steps run one after another on purpose
      captions.push(caption); frame++;
    } catch {}
    await pause(250); // NOSONAR: UI steps run one after another on purpose
  }
})();
const say = async (text, hold = 1500) => { caption = text; await pause(hold); };
const shotN = { n: 0 };
const shot = async (name) => active.screenshot({ path: `${here}shots-video/${String(++shotN.n).padStart(2, "0")}-${name}.png` });
const request = async (name, method, params) => { await dapp.evaluate(`run(${JSON.stringify(name)}, ${JSON.stringify(method)}, ${params})`); await pause(2500); };
const result = (name) => dapp.evaluate(`R[${JSON.stringify(name)}]`);
// MetaMask closes its confirmation page when the queue empties, so each confirmation gets its own tab.
let confirmPage = null;
const openConfirm = async () => {
  // MetaMask may open its own notification window; use it, else open one.
  for (let i = 0; i < 6 && !confirmPage; i++) {
    confirmPage = ctx.pages().find((p) => p.url().includes("/notification.html")) ?? null;
    if (!confirmPage) await pause(500); // NOSONAR: UI steps run one after another on purpose
  }
  if (!confirmPage) {
    confirmPage = await ctx.newPage();
    await confirmPage.goto(`${EXT}/notification.html`).catch(() => {});
  }
  log(`confirm page ${confirmPage.url()} (pages: ${ctx.pages().map((p) => p.url().split("/").pop()).join(", ")})`);
  await pause(3000);
  active = confirmPage; await pause(1500);
};
const press = async (label, wait = 3500) => {
  const page = active;
  await page.getByRole("button", { name: label, exact: true }).first().click({ timeout: 20000 });
  await pause(wait);
  if (page === confirmPage) { active = wallet; await confirmPage.close().catch(() => {}); confirmPage = null; }
};
// Close MetaMask's backup reminder and news cards so the balance row is visible.
const tidy = async () => {
  // close buttons without accessible names: the news card's X and the backup toast's X
  if (await wallet.getByText("Back up now").count()) { await wallet.mouse.click(360, 716); await wallet.waitForTimeout(600); }
  for (let i = 0; i < 3 && (await wallet.getByText("Add").count()) && (await wallet.locator("text=/IPO|Competition|Join the/").count()); i++) { // NOSONAR: UI steps run one after another on purpose
    await wallet.mouse.click(376, 388); await wallet.waitForTimeout(800); // NOSONAR: UI steps run one after another on purpose
  }
  for (let i = 0; i < 4; i++) {
    const close = wallet.getByRole("button", { name: /close|dismiss/i });
    if (!(await close.count())) break;
    await close.first().click({ timeout: 3000 }).catch(() => {}); // NOSONAR: UI steps run one after another on purpose
    await wallet.waitForTimeout(600); // NOSONAR: UI steps run one after another on purpose
  }
};
const home = async () => { await wallet.goto(`${EXT}/home.html#/`); await wallet.waitForTimeout(4000); await tidy(); await wallet.waitForTimeout(800); };

// 1. connect the dapp, fund the new account with USDr and RLS
await wallet.goto(`${EXT}/home.html#/`); await wallet.waitForTimeout(3000); await unlockIfNeeded(wallet);
await wallet.waitForTimeout(3000);
await say("A fresh MetaMask wallet (official 13.56.0 build)", 2500);
await say("A dapp asks to connect");
await request("connect", "eth_requestAccounts", "[]");
await openConfirm(); await shot("connect"); await press("Connect");
const account = (await result("connect")).result[0];
log(`account ${account}`);
sh(`cast send --rpc-url ${NODE} --private-key ${DEV_KEY} ${account} --value 2ether`);
sh(`cast send --rpc-url http://127.0.0.1:8645 --private-key ${RLS_KEY} ${account} --value 100ether`);
log("funded 2 USDr + 100 RLS");

// 2. add the USDr network (chain 487) and pay gas in USDr
await say("Funded on the local chain: 2 USDr and 100 RLS", 2000);
await say("Add Rayls with chain id 487: gas in USDr");
await request("addUsdr", "wallet_addEthereumChain", "[USDR_NET]");
await openConfirm(); await shot("add-usdr-network"); await press("Confirm", 4000);
await home(); await say("Chain 487: the native coin is USDr", 3000); await shot("home-usdr");
const send = async (asset, amount) => {
  await wallet.getByText("Send", { exact: true }).first().click(); await wallet.waitForTimeout(3500);
  await wallet.getByText(new RegExp(String.raw`^[\d.,]+ ${asset}$`)).last().click(); await wallet.waitForTimeout(3500);
  await wallet.getByPlaceholder("Enter or paste an address or name").fill(TO); await wallet.waitForTimeout(1500);
  await wallet.getByPlaceholder("0", { exact: true }).pressSequentially(amount, { delay: 150 }); await wallet.waitForTimeout(2000);
  await shot(`send-${asset}-form`);
  await press("Continue", 3000); await say(`Main Send button: network fee paid in ${asset}`, 4000); await shot(`send-${asset}-confirm`);
  await press("Confirm", 9000);
};
await say("Send 0.1 USDr with the main Send button");
await send("USDr", "0.1");
await wallet.goto(`${EXT}/home.html#/activity`); await say("Sent: gas paid in USDr", 3500); await shot("activity-usdr");
log("sent 0.1 USDr");

// 3. switch to the RLS chain id (4871) and pay gas in RLS
await say("Switch to Rayls chain id 4871: same chain, gas in RLS", 2500);
await request("addRls", "wallet_addEthereumChain", "[RLS_NET]");
await openConfirm(); await shot("add-rls-network"); await press("Confirm", 4000);
await home(); await say("Chain 4871: the native coin is RLS, no USDr needed", 3500); await shot("home-rls");
await say("Send 1 RLS with the main Send button");
await send("RLS", "1");
await wallet.goto(`${EXT}/home.html#/activity`); await say("Sent: gas paid in RLS", 3500); await shot("activity-rls");
await home(); await say("Done: USDr gas on 487, RLS gas on 4871, one account, one chain", 4000); await shot("home-rls-after");
log("sent 1 RLS");

const balances = {
  usdr: sh(`cast balance ${account} --rpc-url ${NODE}`),
  rls: sh(`cast call 0x07e17e17e17e17e17e17e17e17e17e17e17e17ea 'balanceOf(address)(uint256)' ${account} --rpc-url ${NODE}`),
  nonce: sh(`cast nonce ${account} --rpc-url ${NODE}`),
};
log(JSON.stringify(balances));
recording = false; await recorder;
writeFileSync(`${videoDir}/captions.json`, JSON.stringify(captions));
await ctx.close();
console.log("FRAMES", frame);
