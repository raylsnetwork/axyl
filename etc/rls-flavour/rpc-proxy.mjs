#!/usr/bin/env node
// JSON-RPC endpoint for the RLS flavour of a Rayls chain (spike).
//
// A wallet pointed here sees the RLS chain id, RLS as the native balance and gas prices in RLS.
// Signed transactions pass through unchanged: the node itself accepts the RLS chain id and
// charges gas and value in RLS. Everything else is forwarded to the node as it is.
//
//   UPSTREAM=http://127.0.0.1:8545 PORT=8645 node rpc-proxy.mjs

import http from "node:http";

const UPSTREAM = process.env.UPSTREAM ?? "http://127.0.0.1:8545";
const PORT = Number(process.env.PORT ?? 8645);
// Demo tooling: listen on this machine only, and forward only the wallet namespaces, so a webpage or
// a LAN peer can't reach the node's admin_/debug_/txpool_ methods through this proxy.
const HOST = process.env.HOST ?? "127.0.0.1";
const FORWARDED = /^(eth|net|web3)_/;
const MAX_BODY_BYTES = 1 << 20;
const MAX_DRAIN_BYTES = 8 << 20;
// Must match crates/execution/evm/src/rls_flavour.rs.
const RLS_TOKEN = "0x07e17e17e17e17e17e17e17e17e17e17e17e17ea";
const RLS_PER_USDR = 552n;
// RLS chain id per Rayls network (RLS_CHAIN_IDS in rls_flavour.rs). Explicit, not derived: each
// must be unused by every other chain, or transactions signed there would replay here.
const RLS_CHAIN_IDS = new Map([
  [72957n, 729571n], // mainnet
  [7295799n, 72957991n], // testnet
  [503n, 50301n], // devnet
  [487n, 4871n], // local
]);

const hex = (n) => "0x" + n.toString(16);
let nextId = 1;

async function upstream(method, params) {
  // The node sometimes closes a kept-alive connection under load; retry those, not RPC errors.
  let res;
  for (let attempt = 1; ; attempt++) {
    try {
      res = await fetch(UPSTREAM, {
        method: "POST",
        headers: { "content-type": "application/json", connection: "close" },
        body: JSON.stringify({ jsonrpc: "2.0", id: nextId++, method, params }),
      });
      break;
    } catch (err) {
      if (attempt === 3) throw err;
      await new Promise((r) => setTimeout(r, 200 * attempt)); // NOSONAR: retries are sequential on purpose
    }
  }
  const body = await res.json();
  if (body.error) throw Object.assign(new Error(body.error.message), { rpc: body.error });
  return body.result;
}

const nodeChainId = BigInt(await upstream("eth_chainId", []));
const RLS_CHAIN_ID = RLS_CHAIN_IDS.get(nodeChainId);
if (RLS_CHAIN_ID === undefined) throw new Error(`chain ${nodeChainId} is not a Rayls network with an RLS flavour`);

const toRls = (usdrPerGas) => (usdrPerGas == null ? usdrPerGas : hex(BigInt(usdrPerGas) * RLS_PER_USDR));
const withRlsBaseFee = (block) => block && { ...block, baseFeePerGas: toRls(block.baseFeePerGas) };

// eth_call / eth_estimateGas run as the RLS flavour on the node: the RLS chain id selects it, so
// CHAINID, value and balance checks match what the signed transaction will do. Gas prices are
// dropped; the node simulates without a base fee.
function asRlsCall(call) {
  const { gasPrice, maxFeePerGas, maxPriorityFeePerGas, ...rest } = call ?? {};
  return { ...rest, chainId: hex(RLS_CHAIN_ID) };
}

const handlers = {
  eth_chainId: () => hex(RLS_CHAIN_ID),
  net_version: () => RLS_CHAIN_ID.toString(),
  async eth_getBalance([address, tag = "latest"]) {
    const data = "0x70a08231" + address.slice(2).toLowerCase().padStart(64, "0");
    return hex(BigInt(await upstream("eth_call", [{ to: RLS_TOKEN, data }, tag])));
  },
  // The node's gas price is base fee + oracle tip, and the oracle tip already holds RLS-flavour
  // txs' tips in RLS units: scale only the base fee, or the tip compounds 552x per round.
  async eth_gasPrice() {
    const [block, tip] = await Promise.all([
      upstream("eth_getBlockByNumber", ["latest", false]),
      upstream("eth_maxPriorityFeePerGas", []),
    ]);
    // The pool checks the fee cap against the *next* block's base fee, which can be up to 1/8 above
    // the latest (BaseFeeParams::ethereum(), as RaylsChainSpec uses). A legacy sender pays the whole
    // gas price, so price for the worst case, rounding up, like reth's own oracle margin.
    const nextBase = (BigInt(block.baseFeePerGas) * 9n + 7n) / 8n;
    return hex(nextBase * RLS_PER_USDR + BigInt(tip));
  },
  // Tips (eth_maxPriorityFeePerGas, feeHistory.reward) pass through unconverted: the node's
  // percentiles already hold RLS-flavour txs' tips in RLS units, so scaling them by 552 would
  // compound every round once RLS txs fill blocks. USDr txs' tips in them stay in USDr units (open
  // item: compute tips per flavour in the node RPC).
  async eth_feeHistory(params) {
    const history = await upstream("eth_feeHistory", params);
    return { ...history, baseFeePerGas: history.baseFeePerGas.map(toRls) };
  },
  async eth_getBlockByNumber(params) {
    return withRlsBaseFee(await upstream("eth_getBlockByNumber", params));
  },
  async eth_getBlockByHash(params) {
    return withRlsBaseFee(await upstream("eth_getBlockByHash", params));
  },
  async eth_estimateGas([call, ...rest]) {
    return upstream("eth_estimateGas", [asRlsCall(call), ...rest]);
  },
  async eth_call([call, ...rest]) {
    return upstream("eth_call", [asRlsCall(call), ...rest]);
  },
  // The node prices RLS-flavour gas at the RLS base fee; report that price, not the USDr one.
  async eth_getTransactionReceipt([hash]) {
    const receipt = await upstream("eth_getTransactionReceipt", [hash]);
    if (!receipt) return receipt;
    const tx = await upstream("eth_getTransactionByHash", [hash]);
    if (BigInt(tx.chainId ?? 0) !== RLS_CHAIN_ID) return receipt;
    const block = await upstream("eth_getBlockByHash", [receipt.blockHash, false]);
    const baseFee = BigInt(block.baseFeePerGas) * RLS_PER_USDR;
    const maxFee = BigInt(tx.maxFeePerGas ?? tx.gasPrice);
    const tip = BigInt(tx.maxPriorityFeePerGas ?? tx.gasPrice);
    // EIP-1559: min(max fee, base fee + tip); legacy: the gas price
    let price = maxFee;
    if (tx.maxFeePerGas && baseFee + tip < maxFee) price = baseFee + tip;
    return { ...receipt, effectiveGasPrice: hex(price) };
  },
};

async function handle(request) {
  // `null` (or a non-object batch entry) must not throw: any open webpage can POST one
  // JSON-RPC 2.0: an id that can't be determined is replied to as null
  const { id = null, method, params = [] } = request ?? {};
  if (!Object.hasOwn(handlers, method) && !FORWARDED.test(String(method))) {
    return { jsonrpc: "2.0", id, error: { code: -32601, message: "Method not found" } };
  }
  try {
    const result = Object.hasOwn(handlers, method) ? await handlers[method](params) : await upstream(method, params);
    return { jsonrpc: "2.0", id, result };
  } catch (err) {
    return { jsonrpc: "2.0", id, error: err.rpc ?? { code: -32603, message: String(err.message) } };
  }
}

http
  .createServer(async (req, res) => {
    // One try around everything, body read included: an aborted upload rejects the read, and an
    // unhandled rejection would exit Node.
    try {
      res.setHeader("access-control-allow-origin", "*");
      res.setHeader("access-control-allow-headers", "content-type");
      if (req.method === "OPTIONS") return res.end();
      // collect bytes, then decode once: the cap counts bytes, and a UTF-8 character split
      // across chunks stays intact
      const chunks = [];
      let size = 0;
      let tooLarge = false;
      for await (const chunk of req) {
        size += chunk.length;
        if (tooLarge) {
          // keep draining so the 413 reaches the client (leaving the loop would reset the socket
          // first), up to a hard limit
          if (size > MAX_DRAIN_BYTES) return req.destroy();
          continue;
        }
        if (size > MAX_BODY_BYTES) {
          tooLarge = true;
          res.statusCode = 413;
          res.setHeader("connection", "close");
          res.end();
          continue;
        }
        chunks.push(chunk);
      }
      if (tooLarge) return;
      let payload;
      try {
        payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      } catch {
        res.statusCode = 400;
        return res.end();
      }
      const reply = Array.isArray(payload) ? await Promise.all(payload.map(handle)) : await handle(payload);
      if (res.destroyed) return;
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(reply));
    } catch (err) {
      console.error("request failed:", err.message ?? err);
      if (!res.destroyed && !res.headersSent) {
        res.statusCode = 500;
        res.end();
      }
    }
  })
  .listen(PORT, HOST, () => console.log(`RLS-flavour RPC on ${HOST}:${PORT} (chain ${RLS_CHAIN_ID}) -> ${UPSTREAM}`));
