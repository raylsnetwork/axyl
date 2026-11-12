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
// Must match crates/execution/evm/src/rls_flavour.rs.
const RLS_TOKEN = "0x07e17e17e17e17e17e17e17e17e17e17e17e17ea";
const RLS_PER_USDR = 552n;
const rlsChainId = (chainId) => chainId * 10n + 1n;

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
const RLS_CHAIN_ID = rlsChainId(nodeChainId);

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
  async eth_gasPrice() {
    return toRls(await upstream("eth_gasPrice", []));
  },
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
  const { id, method, params = [] } = request;
  try {
    const result = Object.hasOwn(handlers, method) ? await handlers[method](params) : await upstream(method, params);
    return { jsonrpc: "2.0", id, result };
  } catch (err) {
    return { jsonrpc: "2.0", id, error: err.rpc ?? { code: -32603, message: String(err.message) } };
  }
}

http
  .createServer(async (req, res) => {
    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("access-control-allow-headers", "content-type");
    if (req.method === "OPTIONS") return res.end();
    let body = "";
    for await (const chunk of req) body += chunk;
    let payload;
    try {
      payload = JSON.parse(body);
    } catch {
      res.statusCode = 400;
      return res.end();
    }
    const reply = Array.isArray(payload) ? await Promise.all(payload.map(handle)) : await handle(payload);
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(reply));
  })
  .listen(PORT, () => console.log(`RLS-flavour RPC on :${PORT} (chain ${RLS_CHAIN_ID}) -> ${UPSTREAM}`));
