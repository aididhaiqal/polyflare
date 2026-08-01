import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Served embedded by the polyflare binary under `/dashboard/` (rust-embed), so all asset URLs
// must be prefixed with that base. A single JS + CSS chunk keeps the embedded payload small.
export default defineConfig({
  base: "/dashboard/",
  plugins: [react()],
  // Dev-only: forward API calls to a locally running polyflare instance so `bun run dev` renders
  // live data instead of request errors. No effect on the embedded production build.
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8080",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    chunkSizeWarningLimit: 1200,
  },
});
