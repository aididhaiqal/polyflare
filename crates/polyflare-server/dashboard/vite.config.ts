import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Served embedded by the polyflare binary under `/dashboard/` (rust-embed), so all asset URLs
// must be prefixed with that base. A single JS + CSS chunk keeps the embedded payload small.
export default defineConfig({
  base: "/dashboard/",
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    chunkSizeWarningLimit: 1200,
  },
});
