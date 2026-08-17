/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_RPC_URL?: string;
  readonly VITE_ADMIN_ADDRESS?: string;
  readonly VITE_ADMIN_PRIVATE_KEY?: string;
  readonly VITE_PRIORITY_CURVE_ADDRESS?: string;
  readonly VITE_OPEN_TIER_CURVE_ADDRESS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
