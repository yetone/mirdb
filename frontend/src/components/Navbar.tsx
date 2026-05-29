import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import ThemeToggle from './ThemeToggle';

export default function Navbar() {
  const { isAuthenticated, logout } = useAuth();
  const location = useLocation();

  const isActive = (path: string) => location.pathname === path;

  return (
    <nav data-testid="navbar" aria-label="Main navigation">
      <div className="navbar-brand">
        <Link to="/" data-testid="nav-home-link" aria-label="Home">
          URL Shortener
        </Link>
      </div>

      <ul className="navbar-links" role="menubar">
        <li role="none">
          <Link
            to="/"
            data-testid="nav-home"
            role="menuitem"
            aria-current={isActive('/') ? 'page' : undefined}
          >
            Home
          </Link>
        </li>

        {isAuthenticated ? (
          <>
            <li role="none">
              <Link
                to="/dashboard"
                data-testid="nav-dashboard"
                role="menuitem"
                aria-current={isActive('/dashboard') ? 'page' : undefined}
              >
                Dashboard
              </Link>
            </li>
            <li role="none">
              <button
                onClick={logout}
                data-testid="nav-logout"
                role="menuitem"
              >
                Logout
              </button>
            </li>
          </>
        ) : (
          <>
            <li role="none">
              <Link
                to="/login"
                data-testid="nav-login"
                role="menuitem"
                aria-current={isActive('/login') ? 'page' : undefined}
              >
                Login
              </Link>
            </li>
            <li role="none">
              <Link
                to="/register"
                data-testid="nav-register"
                role="menuitem"
                aria-current={isActive('/register') ? 'page' : undefined}
              >
                Register
              </Link>
            </li>
          </>
        )}
      </ul>

      <div className="navbar-actions">
        <ThemeToggle />
      </div>
    </nav>
  );
}
