import { useEffect, useState } from "react";
import clsx from "clsx";

import { Moon, Sun } from "../ui/icons";

type Theme = "dark" | "light";

// Same localStorage key `index.html`'s inline pre-paint script reads/writes (see that file's
// flash-of-wrong-theme-prevention snippet) — reusing it here means a toggle click and a page
// reload never disagree about which theme is "saved".
const STORAGE_KEY = "pf-theme";

function readInitialTheme(): Theme {
  return document.documentElement.dataset.theme === "light" ? "light" : "dark";
}

/** Sun/moon button that flips `<html data-theme>` between `dark`/`light` and persists the choice.
 * Reads the *current* `data-theme` on mount (already resolved before first paint by
 * `index.html`'s inline script) rather than re-deciding from scratch, so toggle state always
 * matches what's on screen. */
export function ThemeToggle({ className }: { className?: string }) {
  const [theme, setTheme] = useState<Theme>(readInitialTheme);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  return (
    <button
      type="button"
      onClick={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
      aria-label={theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}
      className={clsx(
        "inline-flex h-7 w-7 items-center justify-center rounded border border-border bg-card text-fg opacity-70 transition-colors hover:border-accent hover:opacity-100",
        className,
      )}
    >
      {theme === "dark" ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
    </button>
  );
}
