import { Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';

/**
 * Application routing.
 * Owner: Scenario 10 - Homepage Routing Integration.
 */
export default function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
      <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
    </Routes>
  );
}
