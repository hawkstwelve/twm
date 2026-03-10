import { Suspense, lazy } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import MarketingLayout from "./layouts/MarketingLayout";
import AppLayout from "./layouts/AppLayout";
import AdminLayout from "./layouts/AdminLayout";

const Home = lazy(() => import("./pages/home"));
const Models = lazy(() => import("./pages/models"));
const Variables = lazy(() => import("./pages/variables"));
const Status = lazy(() => import("./pages/status"));
const Login = lazy(() => import("./pages/login"));
const AdminPerformance = lazy(() => import("./pages/admin/performance"));
const AdminUsage = lazy(() => import("./pages/admin/usage"));
const Viewer = lazy(() => import("./pages/viewer"));

function withSuspense(node: React.ReactNode) {
  return <Suspense fallback={null}>{node}</Suspense>;
}

export default function RouterApp() {
  return (
    <Routes>
      <Route element={<MarketingLayout />}>
        <Route path="/" element={withSuspense(<Home />)} />
        <Route path="/models" element={withSuspense(<Models />)} />
        <Route path="/variables" element={withSuspense(<Variables />)} />
        <Route path="/status" element={withSuspense(<Status />)} />
        <Route path="/login" element={withSuspense(<Login />)} />
      </Route>

      <Route element={<AppLayout />}>
        <Route path="/viewer" element={withSuspense(<Viewer />)} />
        <Route path="/admin" element={<AdminLayout />}>
          <Route index element={<Navigate to="/admin/performance" replace />} />
          <Route path="performance" element={withSuspense(<AdminPerformance />)} />
          <Route path="usage" element={withSuspense(<AdminUsage />)} />
        </Route>
      </Route>

      <Route path="/app" element={<Navigate to="/viewer" replace />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
