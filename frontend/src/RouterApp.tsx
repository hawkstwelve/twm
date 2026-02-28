import { Navigate, Route, Routes } from "react-router-dom";
import MarketingLayout from "./layouts/MarketingLayout";
import AppLayout from "./layouts/AppLayout";

import Home from "./pages/home";
import Models from "./pages/models";
import Variables from "./pages/variables";
import Changelog from "./pages/changelog";
import Status from "./pages/status";
import Login from "./pages/login";

import Viewer from "./pages/viewer";

export default function RouterApp() {
  return (
    <Routes>
      <Route element={<MarketingLayout />}>
        <Route path="/" element={<Home />} />
        <Route path="/models" element={<Models />} />
        <Route path="/variables" element={<Variables />} />
        <Route path="/changelog" element={<Changelog />} />
        <Route path="/status" element={<Status />} />
        <Route path="/login" element={<Login />} />
      </Route>

      <Route element={<AppLayout />}>
        <Route path="/viewer" element={<Viewer />} />
      </Route>

      <Route path="/app" element={<Navigate to="/viewer" replace />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}