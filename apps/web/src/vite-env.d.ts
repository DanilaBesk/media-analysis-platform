/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_WS_URL?: string;
}

interface Window {
  __WEB_UI_RUNTIME__?: {
    channelAccountId?: string;
  };
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
