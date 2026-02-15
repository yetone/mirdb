/**
 * Navbar Component
 * Added for Scenario 3 - Navigation and Routing
 * Updated for Scenario 4 - Theme Switching
 *
 * Navigation bar with brand logo, auth links, and theme toggle.
 */

import { Link } from 'react-router-dom';
import { Link2 } from 'lucide-react';
import ThemeToggle from './ThemeToggle';

export default function Navbar() {
  return (
    <nav className="navbar bg-base-100 shadow-lg px-4 lg:px-8" data-testid="navbar">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold gap-2" data-testid="brand-logo">
          <Link2 className="w-6 h-6 text-primary" />
          LinkSnip
        </Link>
      </div>

      <div className="navbar-end flex items-center gap-2 lg:gap-4">
        <ThemeToggle />
        <Link to="/login" className="btn btn-ghost" data-testid="login-link">
          Login
        </Link>
        <Link to="/register" className="btn btn-primary" data-testid="register-link">
          Register
        </Link>
      </div>
    </nav>
  );
}
