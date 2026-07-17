// QueryClientProvider → BrowserRouter → AuthProvider → Routes. This wires the app shell for good
// (provider stack + route tree); the per-page placeholders below are the only remaining TEMPORARY
// pieces — each later page task replaces its own placeholder route element.
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import { AuthProvider, RequireAuth } from "./auth/AuthProvider";
import { CapabilitiesProvider } from "./capabilities/CapabilitiesProvider";
import { Accounts } from "./pages/Accounts";
import { Login } from "./pages/Login";
import { Overview } from "./pages/Overview";
import { Shell } from "./shell/Shell";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      refetchOnWindowFocus: true,
    },
  },
});

// TEMPORARY — each later page task supplies the real route element.
function RoutePlaceholder({ label }: { label: string }) {
  return <div className="text-fg">{label} (todo)</div>;
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter basename="/dashboard">
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route
              path="/"
              element={
                <RequireAuth>
                  <CapabilitiesProvider>
                    <Shell />
                  </CapabilitiesProvider>
                </RequireAuth>
              }
            >
              <Route index element={<Overview />} />
              <Route path="accounts" element={<Accounts />} />
              <Route path="accounts/:id" element={<RoutePlaceholder label="Account detail" />} />
              <Route path="pools" element={<RoutePlaceholder label="Pools" />} />
              <Route path="requests" element={<RoutePlaceholder label="Requests" />} />
              <Route path="logs" element={<RoutePlaceholder label="Logs" />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
