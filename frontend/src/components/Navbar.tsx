/**
 * Navbar Component
 * Added for Scenario 3 - Navigation and Routing
 *
 * Navigation bar with brand logo and auth links.
 */

import { Link } from 'react-router-dom';
import { Link2 } from 'lucide-react';

export default function Navbar() {
  return (
    <nav className="navbar bg-base-100 shadow-lg px-4 lg:px-8" data-testid="navbar">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold gap-2" data-testid="brand-logo">
          <Link2 className="w-6 h-6 text-primary" />
          LinkSnip
        </Link>
      </div>

      <div className="navbar-end flex items-center gap-4">
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
