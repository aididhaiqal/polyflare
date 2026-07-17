// Central re-export of every lucide-react icon used across the dashboard (nav + the UI atoms in
// this directory, plus the page-level actions Tasks 5-10 build on top of them: pause/delete an
// account, edit an alias, retry/reset, the security/routing-policy/token badges, search, and the
// requests-table pager). Import icons from here, not directly from "lucide-react", so the set in
// active use stays auditable in one place — and, per the binding "no emoji" rule, every icon-like
// glyph in the app is guaranteed to be a real lucide icon, never a Unicode symbol or emoji.
//
// Sun/Moon are additions beyond the brief's enumerated list — the ThemeToggle atom (Task 4) needs
// *some* rendered icon for its dark/light affordance, and lucide is the only allowed icon source,
// so these two were the obvious, minimal choice. See task-4-report.md.
//
// AlertTriangle/CheckCircle2/Clock/Coins are Task 5 additions for the Overview page's KPI cards
// (success rate / avg latency / tokens) and the recent-errors strip — same reasoning: the mockup's
// glyphs (✓ ◷ ◆ ⚠) aren't real icons, and "no emoji" means every one of them needs a genuine lucide
// replacement rather than a Unicode symbol. See task-5-report.md.
export {
  Activity,
  AlertTriangle,
  ArrowDown,
  BarChart3,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Clock,
  Coins,
  Layers,
  List,
  LayoutGrid,
  Lock,
  Moon,
  Pause,
  Pencil,
  RotateCcw,
  Route,
  Search,
  Settings,
  ShieldCheck,
  Sun,
  Trash2,
  Users,
  type LucideIcon,
} from "lucide-react";
