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
//
// Key/ChevronDown are Task 6 additions for the Accounts page: `Key` replaces the mockup's 🔑
// token-health glyph (`accounts-page.html`'s `.afoot`), `ChevronDown` is the Radix Select trigger's
// caret (the pool filter). Same no-emoji rationale as above. See task-6-report.md.
//
// LogIn/Download/Zap/Flame are Task 7 additions for the Account detail page's disabled Phase-3
// Actions panel (`accounts-detail-v2.html`'s reworked Operations/Configuration columns): `LogIn`
// replaces the mockup's plain "Re-authenticate" label glyph, `Download` is "Export auth", `Zap` is
// "Force probe", `Flame` is "Limit warm-up" (thematically apt for a PolyFlare warm-up affordance).
// All four are rendered only inside disabled/non-functional controls — see task-7-report.md.
//
// Link2 is the TA6(c) addition for the Sessions page's sidebar nav entry — the page shows which
// account each conversation session is stuck/linked to (session→account affinity), so a link glyph
// is the apt lucide choice; same no-emoji rationale as every other icon here. See ta6c report.
//
// Play/ArrowDownToLine/EyeOff are Task 10 additions for the Live Logs console
// (`live-logs.html`): `Play` is the Resume affordance (paired with the existing `Pause`), the
// mockup's own "auto-scroll" glyph isn't a real icon so `ArrowDownToLine` (scroll-to-bottom) is
// its lucide replacement, and `EyeOff` fronts the flag-off disabled notice. See task-10-report.md.
//
// Check/X/CircleAlert are the mutation-foundation Toast additions (ui/Toast.tsx): a success toast
// gets `Check`, an error toast gets `CircleAlert`, and `X` is the toast's manual-dismiss control.
// Same no-emoji rationale — a real lucide glyph, not a Unicode checkmark/cross.
//
// MoreVertical is the Task 6 addition for the shared `ActionMenu` primitive (ui/ActionMenu.tsx):
// it's the kebab (⋯) trigger icon fronting the account-row action menu (Tasks 7/8 consume it).
// `Check` (already exported above for Toast) is reused by `ActionMenu.CheckItem`'s checkmark.
//
// KeyRound/Copy/Plus are the dashboard API-Keys subsystem's (Outcome 2) additions: `KeyRound`
// fronts the `/keys` sidebar nav entry and the empty-state note (distinct from the plain `Key`
// glyph already used for account token-health, so the two read as different concepts at a
// glance); `Copy` is the show-once create-key modal's copy-to-clipboard affordance; `Plus` fronts
// the page's "Create key" action.
export {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowDownToLine,
  BarChart3,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  Clock,
  Coins,
  Copy,
  Download,
  Eye,
  EyeOff,
  Flame,
  Key,
  KeyRound,
  Layers,
  Link2,
  List,
  LayoutGrid,
  Lock,
  LogIn,
  Moon,
  MoreVertical,
  Pause,
  Pencil,
  Play,
  Plus,
  RotateCcw,
  Route,
  Search,
  Settings,
  ShieldCheck,
  Sun,
  Trash2,
  Users,
  X,
  Zap,
  type LucideIcon,
} from "lucide-react";
