import { Outlet } from "react-router-dom";

import { Sidebar } from "./Sidebar";

/** App shell: fixed-width sidebar + a scrollable main region rendering the active route via
 * `<Outlet/>`. Page padding lives here so every page (Tasks 5-10) starts from a consistent inset
 * rather than each re-declaring it. */
export function Shell() {
  return (
    <div className="flex h-screen overflow-hidden bg-bg">
      <Sidebar />
      <main className="flex-1 overflow-y-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
