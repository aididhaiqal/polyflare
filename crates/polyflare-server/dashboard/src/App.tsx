// QueryClientProvider → BrowserRouter → AuthProvider → Routes. This wires the app shell (provider
// stack + route tree); every route now points at a real page (Task 10 — Live Logs — was the last
// placeholder route, see progress.md).
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import { AuthProvider, RequireAuth } from "./auth/AuthProvider";
import { CapabilitiesProvider } from "./capabilities/CapabilitiesProvider";
import { AccountDetail } from "./pages/AccountDetail";
import { Accounts } from "./pages/Accounts";
import { LiveLogs } from "./pages/LiveLogs";
import { Login } from "./pages/Login";
import { Overview } from "./pages/Overview";
import { Pools } from "./pages/Pools";
import { Reports } from "./pages/Reports";
import { Requests } from "./pages/Requests";
import { Sessions } from "./pages/Sessions";
import { Shell } from "./shell/Shell";
import { ToastProvider } from "./ui/Toast";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      refetchOnWindowFocus: true,
    },
  },
});

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
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
                <Route path="accounts/:id" element={<AccountDetail />} />
                <Route path="pools" element={<Pools />} />
                <Route path="requests" element={<Requests />} />
                <Route path="sessions" element={<Sessions />} />
                <Route path="reports" element={<Reports />} />
                <Route path="logs" element={<LiveLogs />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Route>
            </Routes>
          </AuthProvider>
        </BrowserRouter>
      </ToastProvider>
    </QueryClientProvider>
  );
}
