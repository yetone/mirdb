import { Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';
import Dashboard from '../pages/Dashboard';
import NotFound from '../pages/NotFound';
import ProtectedLayout from '../components/ProtectedLayout';

/**
 * Application routing.
 * Owner: Scenario 10 - Homepage Routing Integration.
 *
 * The "/" route renders <Home /> as a public route and is NOT wrapped by
 * <ProtectedLayout />. Authenticated routes (/dashboard, /stats/:shortCode,
 * /settings) are wrapped by <ProtectedLayout /> which redirects to /login when
 * the visitor is unauthenticated. Unknown routes fall back to <NotFound />.
 */
export default function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route
        path="/login"
        element={<div data-testid="login-page">Login Page</div>}
      />
      <Route
        path="/register"
        element={<div data-testid="register-page">Register Page</div>}
      />

      <Route element={<ProtectedLayout />}>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route
          path="/stats/:shortCode"
          element={<div data-testid="stats-page">Stats Page</div>}
        />
        <Route
          path="/settings"
          element={<div data-testid="settings-page">Settings Page</div>}
        />
      </Route>

      <Route path="*" element={<NotFound />} />
    </Routes>
  );
}
