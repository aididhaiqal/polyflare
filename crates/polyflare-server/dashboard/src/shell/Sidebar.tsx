import { NavLink } from "react-router-dom";
import clsx from "clsx";

import { useAuth } from "../auth/AuthProvider";
import { useCapabilityFlags } from "../capabilities/CapabilitiesProvider";
import { useScreenShield } from "../privacy/ScreenShield";
import {
  Activity,
  BarChart3,
  Eye,
  EyeOff,
  KeyRound,
  Layers,
  LayoutGrid,
  Link2,
  List,
  Route,
  Settings,
  Users,
  type LucideIcon,
} from "../ui/icons";
import { ThemeToggle } from "./ThemeToggle";

interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  /** Only true for "/" — react-router-dom's `NavLink` otherwise treats every route as a prefix
   * match, which would keep Overview highlighted on every other page. */
  end?: boolean;
  /** Gated on `CapabilitiesView.live_logs` (see `CapabilitiesProvider`) — hidden, not disabled,
   * when the server was started without that feature. */
  requiresLiveLogs?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { to: "/", label: "Overview", icon: LayoutGrid, end: true },
  { to: "/accounts", label: "Accounts", icon: Users },
  { to: "/pools", label: "Pools", icon: Layers },
  { to: "/providers", label: "Providers", icon: Route },
  { to: "/requests", label: "Requests", icon: List },
  { to: "/sessions", label: "Sessions", icon: Link2 },
  { to: "/logs", label: "Live Logs", icon: Activity, requiresLiveLogs: true },
  { to: "/reports", label: "Analytics", icon: BarChart3 },
  { to: "/settings", label: "Settings", icon: Settings },
  { to: "/keys", label: "API Keys", icon: KeyRound },
];

const SOON_ITEMS: Array<{ label: string; icon: LucideIcon }> = [];

function ScreenShieldToggle() {
  const { active, toggle } = useScreenShield();
  const Icon = active ? EyeOff : Eye;
  const label = active ? "Show account identities" : "Shield account identities";
  return (
    <button
      type="button"
      onClick={toggle}
      aria-label={label}
      aria-pressed={active}
      title={label}
      className={clsx(
        "flex h-8 w-8 items-center justify-center rounded-lg border text-fg transition-colors",
        active
          ? "border-signal/35 bg-signal/[0.09] text-signal"
          : "border-border bg-card opacity-60 hover:border-signal hover:text-signal hover:opacity-100",
      )}
    >
      <Icon className="h-3.5 w-3.5" strokeWidth={1.8} />
    </button>
  );
}

/** Left nav: brand, primary nav (active state = accent-tinted background, per the mockups' `.pf-nav
 * a.on`), an optional divider + permanently-disabled "soon" items (hidden entirely, divider
 * included, whenever `SOON_ITEMS` is empty — as it is now that "Settings" has shipped), and a
 * footer with the theme toggle + sign-out. Matches `overview-ccflare-v2.html` /
 * `requests-page-v2.html`'s `.pf-side`. */
export function Sidebar() {
  const { liveLogs } = useCapabilityFlags();
  const { localAccess, signOut } = useAuth();

  return (
    <aside className="hidden h-screen w-56 shrink-0 flex-col border-r border-border/80 bg-card/55 px-3 py-5 shadow-[12px_0_40px_hsl(var(--surface-shadow)/0.14)] backdrop-blur-xl md:flex">
      <BrandMark />

      <div className="mb-2 mt-7 px-3 text-[8px] font-bold uppercase tracking-[0.22em] text-fg opacity-35">
        Operations
      </div>

      <nav className="flex flex-col gap-1">
        {NAV_ITEMS.filter((item) => !item.requiresLiveLogs || liveLogs).map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end ?? false}
            className={({ isActive }) =>
              clsx(
                "group relative flex items-center gap-3 rounded-lg px-3 py-2 text-[12px] no-underline transition-colors",
                isActive
                  ? "bg-accent/[0.1] font-semibold text-fg"
                  : "text-fg opacity-55 hover:bg-muted/55 hover:opacity-100",
              )
            }
          >
            {({ isActive }) => (
              <>
                {isActive && (
                  <span className="absolute -left-3 h-5 w-[3px] rounded-r-full bg-accent shadow-[0_0_14px_hsl(var(--accent)/0.85)]" />
                )}
                <item.icon
                  className={clsx("h-4 w-4", isActive ? "text-accent" : "text-signal")}
                  strokeWidth={1.8}
                />
                {item.label}
                {isActive && <span className="ml-auto h-1.5 w-1.5 rounded-full bg-accent" />}
              </>
            )}
          </NavLink>
        ))}

        {SOON_ITEMS.length > 0 && (
          <>
            <div className="my-2 h-px bg-border" />
            {SOON_ITEMS.map((item) => (
              <div
                key={item.label}
                className="flex items-center gap-2.5 rounded px-2.5 py-1.5 text-[11.5px] text-fg opacity-30"
              >
                <item.icon className="h-3.5 w-3.5" strokeWidth={1.8} />
                {item.label}
              </div>
            ))}
          </>
        )}
      </nav>

      <div className="mt-auto flex items-center justify-between border-t border-border/80 px-1 pt-4">
        <div className="flex items-center gap-2">
          <ThemeToggle />
          <ScreenShieldToggle />
        </div>
        {localAccess ? (
          <span className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-[0.12em] text-success opacity-75">
            <span className="h-1.5 w-1.5 rounded-full bg-success" />
            Local
          </span>
        ) : (
          <button
            type="button"
            onClick={signOut}
            className="text-[11px] text-fg opacity-60 hover:text-accent hover:opacity-100"
          >
            Log out
          </button>
        )}
      </div>
    </aside>
  );
}

export function BrandMark({ compact = false }: { compact?: boolean }) {
  return (
    <div className={clsx("flex items-center", compact ? "gap-2" : "gap-3 px-2")}>
      <span className="relative flex h-8 w-8 shrink-0 items-center justify-center rounded-xl border border-accent/35 bg-accent/[0.09] shadow-[0_0_22px_hsl(var(--accent)/0.13)]">
        <span className="absolute h-3.5 w-3.5 rounded-full bg-accent/25 blur-[3px]" />
        <span className="relative h-2 w-2 rounded-full bg-accent shadow-[0_0_11px_hsl(var(--accent)/0.95)]" />
        <span className="absolute h-px w-5 rotate-[-34deg] bg-gradient-to-r from-transparent via-signal to-transparent opacity-80" />
      </span>
      <span>
        <span className="pf-wordmark block text-[16px] font-bold leading-none text-fg">
          Poly<span className="text-accent">Flare</span>
        </span>
        {!compact && (
          <span className="mt-1 block text-[7px] font-semibold uppercase tracking-[0.23em] text-signal opacity-65">
            Routing control
          </span>
        )}
      </span>
    </div>
  );
}

/** Compact mobile shell with a horizontally scrollable route rail; all destinations remain
 * reachable without turning the dashboard into a modal-drawer interaction. */
export function MobileNavigation() {
  const { liveLogs } = useCapabilityFlags();
  const { localAccess, signOut } = useAuth();

  return (
    <header className="sticky top-0 z-30 border-b border-border/80 bg-card/90 backdrop-blur-xl md:hidden">
      <div className="flex h-14 items-center justify-between px-4">
        <BrandMark compact />
        <div className="flex items-center gap-2">
          <ThemeToggle />
          <ScreenShieldToggle />
          {localAccess ? (
            <span className="rounded-lg border border-success/25 bg-success/[0.07] px-2.5 py-1.5 text-[9px] font-bold uppercase tracking-[0.1em] text-success">
              Local
            </span>
          ) : (
            <button
              type="button"
              onClick={signOut}
              className="rounded-lg border border-border px-2.5 py-1.5 text-[10px] font-semibold text-fg opacity-65"
            >
              Log out
            </button>
          )}
        </div>
      </div>
      <nav className="flex gap-1 overflow-x-auto px-3 pb-2" aria-label="Dashboard pages">
        {NAV_ITEMS.filter((item) => !item.requiresLiveLogs || liveLogs).map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end ?? false}
            className={({ isActive }) =>
              clsx(
                "flex shrink-0 items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-[10.5px] font-medium no-underline",
                isActive
                  ? "bg-accent/[0.12] text-accent"
                  : "bg-muted/45 text-fg opacity-60",
              )
            }
          >
            <item.icon className="h-3.5 w-3.5" strokeWidth={1.8} />
            {item.label}
          </NavLink>
        ))}
      </nav>
    </header>
  );
}
