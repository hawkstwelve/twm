import { Outlet } from "react-router-dom";
import SiteHeader from "../components/SiteHeader";

export default function AppLayout() {
  return (
    <div className="min-h-svh flex flex-col bg-background text-foreground">
      <SiteHeader variant="app" />
      <div className="flex-1 min-h-0">
        <Outlet />
      </div>
    </div>
  );
}
