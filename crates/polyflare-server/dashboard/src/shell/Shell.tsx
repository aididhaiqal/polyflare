import { Outlet } from "react-router-dom";

import { MobileNavigation, Sidebar } from "./Sidebar";

/** App shell: fixed-width sidebar + a scrollable main region rendering the active route via
 * `<Outlet/>`. Page padding lives here so every page (Tasks 5-10) starts from a consistent inset
 * rather than each re-declaring it. */
export function Shell() {
  return (
    <div className="flex min-h-screen flex-col overflow-hidden bg-bg/80 md:h-screen md:flex-row">
      <Sidebar />
      <MobileNavigation />
      <main className="min-w-0 flex-1 overflow-y-auto px-4 py-5 sm:px-5 md:px-7 md:py-7 xl:px-9 xl:py-8">
        <div className="mx-auto w-full max-w-[1500px]">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
