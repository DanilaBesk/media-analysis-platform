import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./tests/setup.ts",
    coverage: {
      provider: "v8",
      reporter: ["text-summary", "json-summary"],
      all: true,
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/vite-env.d.ts"],
    },
  },
});
