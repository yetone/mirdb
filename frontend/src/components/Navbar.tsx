/**
 * Navbar Component
 * Owner: Scenario 3 - Navigation Menu
 *
 * The main navigation bar for the landing page containing:
 * - Logo/Brand
 * - Login link
 * - Register link
 * - ThemeToggle component
 *
 * Features:
 * - Sticky positioning (remains visible on scroll)
 * - Responsive design
 * - React Router navigation
 */

import { Link } from 'react-router-dom';
import { ThemeToggle } from './ThemeToggle';

export function Navbar() {
  return (
    <nav
      className="navbar bg-base-100/80 backdrop-blur-sm sticky top-0 z-50 border-b border-base-200"
      role="navigation"
      aria-label="Main navigation"
      data-testid="navbar"
    >
      <div className="navbar-start">
        <Link
          to="/"
          className="btn btn-ghost text-xl font-bold"
          data-testid="navbar-logo"
        >
          URL Shortener
        </Link>
      </div>

      <div className="navbar-end gap-2">
        <Link
          to="/login"
          className="btn btn-ghost"
          data-testid="navbar-login"
        >
          Login
        </Link>
        <Link
          to="/register"
          className="btn btn-primary"
          data-testid="navbar-register"
        >
          Register
        </Link>
        <ThemeToggle />
      </div>
    </nav>
  );
}

export default Navbar;
