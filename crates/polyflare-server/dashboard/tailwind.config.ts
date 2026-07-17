import type { Config } from "tailwindcss";

// ccflare-parity token theme: all colors are indirections onto CSS variables (defined in
// src/index.css as bare HSL components) so both the dark default and the `data-theme="light"`
// override apply without any Tailwind-side duplication.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: ["class", '[data-theme="dark"]'],
  theme: {
    extend: {
      colors: {
        bg: "hsl(var(--bg))",
        card: "hsl(var(--card))",
        muted: "hsl(var(--muted))",
        border: "hsl(var(--border))",
        fg: "hsl(var(--fg))",
        accent: "hsl(var(--accent))",
        codex: "hsl(var(--codex))",
        claude: "hsl(var(--claude))",
        success: "hsl(var(--success))",
        warn: "hsl(var(--warn))",
        error: "hsl(var(--error))",
      },
      borderRadius: {
        DEFAULT: "0.375rem",
      },
    },
  },
  plugins: [],
} satisfies Config;
