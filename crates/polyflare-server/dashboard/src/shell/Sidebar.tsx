import { NavLink } from "react-router-dom";
import clsx from "clsx";

import { useAuth } from "../auth/AuthProvider";
import { useCapabilityFlags } from "../capabilities/CapabilitiesProvider";
import {
  Activity,
  BarChart3,
  KeyRound,
  Layers,
  LayoutGrid,
  Link2,
  List,
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
  { to: "/requests", label: "Requests", icon: List },
  { to: "/sessions", label: "Sessions", icon: Link2 },
  { to: "/logs", label: "Live Logs", icon: Activity, requiresLiveLogs: true },
  { to: "/reports", label: "Analytics", icon: BarChart3 },
  { to: "/settings", label: "Settings", icon: Settings },
  { to: "/keys", label: "API Keys", icon: KeyRound },
];

const SOON_ITEMS: Array<{ label: string; icon: LucideIcon }> = [];

/** Left nav: brand, primary nav (active state = accent-tinted background, per the mockups' `.pf-nav
 * a.on`), an optional divider + permanently-disabled "soon" items (hidden entirely, divider
 * included, whenever `SOON_ITEMS` is empty — as it is now that "Settings" has shipped), and a
 * footer with the theme toggle + sign-out. Matches `overview-ccflare-v2.html` /
 * `requests-page-v2.html`'s `.pf-side`. */
export function Sidebar() {
  const { liveLogs } = useCapabilityFlags();
  const { signOut } = useAuth();

  return (
    <aside className="flex h-screen w-44 shrink-0 flex-col border-r border-border bg-bg px-2 py-3.5">
      <div className="px-2 pb-4 text-sm font-bold text-fg">
        Poly<span className="text-accent">Flare</span>
      </div>

      <nav className="flex flex-col gap-0.5">
        {NAV_ITEMS.filter((item) => !item.requiresLiveLogs || liveLogs).map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end ?? false}
            className={({ isActive }) =>
              clsx(
                "flex items-center gap-2.5 rounded px-2.5 py-1.5 text-[11.5px] no-underline",
                isActive
                  ? "bg-accent/[0.12] font-medium text-accent"
                  : "text-fg opacity-60 hover:opacity-100",
              )
            }
          >
            <item.icon className="h-3.5 w-3.5" strokeWidth={1.8} />
            {item.label}
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

      <div className="mt-auto flex items-center justify-between border-t border-border pt-3">
        <ThemeToggle />
        <button
          type="button"
          onClick={signOut}
          className="text-[11px] text-fg opacity-60 hover:text-accent hover:opacity-100"
        >
          Log out
        </button>
      </div>
    </aside>
  );
}
