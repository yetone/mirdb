/**
 * Homepage Navigation Bar Component.
 * Owner: Scenario 3 - Navigation Bar Functionality
 *
 * Renders the navigation bar with:
 * - Logo/brand name (left-aligned)
 * - Theme toggle (center)
 * - Login and Register links (right-aligned) when unauthenticated
 * - Dashboard link and user menu (right-aligned) when authenticated
 * - 60px height
 * - Responsive: hamburger menu on mobile (< 768px)
 *
 * Requirements: REQ-8, REQ-9
 * Auth Integration: Scenario 19
 */
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { Link2, Menu, User, LogOut, AlertCircle } from 'lucide-react';
import { ThemeToggle } from '../ThemeToggle';
import { MobileMenu } from './MobileMenu';
import { useAuth } from '../../contexts/AuthContext';

interface HomeNavbarProps {
  className?: string;
}

export function HomeNavbar({ className = '' }: HomeNavbarProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const { isAuthenticated, isLoading, user, logout, isTokenExpired } = useAuth();

  const handleLogout = () => {
    logout();
  };

  return (
    <>
      <nav
        className={`navbar bg-base-100 shadow-sm h-[60px] min-h-[60px] max-h-[60px] ${className}`}
        data-testid="home-navbar"
        role="navigation"
        aria-label="Main navigation"
      >
        {/* Logo - Left aligned */}
        <div className="navbar-start">
          <Link
            to="/"
            className="btn btn-ghost text-xl normal-case flex items-center gap-2"
            aria-label="URL Shortener Home"
            data-testid="navbar-logo"
          >
            <Link2 className="h-6 w-6" aria-hidden="true" />
            <span className="font-bold hidden sm:inline">URL Shortener</span>
            <span className="font-bold sm:hidden">URL</span>
          </Link>
        </div>

        {/* Theme Toggle - Center (hidden on mobile) */}
        <div className="navbar-center hidden md:flex">
          <ThemeToggle />
        </div>

        {/* Auth Links - Right aligned (hidden on mobile) */}
        <div className="navbar-end hidden md:flex">
          {isLoading ? (
            <span className="loading loading-spinner loading-sm" data-testid="auth-loading" />
          ) : isTokenExpired ? (
            /* Token expired state - show warning and login options */
            <div className="flex items-center gap-2">
              <div
                className="tooltip tooltip-bottom"
                data-tip="Your session has expired. Please log in again."
              >
                <AlertCircle
                  className="h-5 w-5 text-warning"
                  aria-label="Session expired"
                  data-testid="token-expired-indicator"
                />
              </div>
              <Link
                to="/login"
                className="btn btn-ghost"
                data-testid="navbar-login-link"
              >
                Login
              </Link>
              <Link
                to="/register"
                className="btn btn-primary"
                data-testid="navbar-register-link"
              >
                Register
              </Link>
            </div>
          ) : isAuthenticated ? (
            /* Authenticated state - show dashboard and user menu */
            <div className="flex items-center gap-2">
              <Link
                to="/dashboard"
                className="btn btn-ghost"
                data-testid="navbar-dashboard-link"
              >
                Dashboard
              </Link>
              <div className="dropdown dropdown-end" data-testid="user-menu">
                <label
                  tabIndex={0}
                  className="btn btn-ghost btn-circle avatar placeholder"
                  aria-label="User menu"
                  data-testid="user-menu-trigger"
                >
                  <div className="bg-primary text-primary-content rounded-full w-10">
                    <User className="h-5 w-5" aria-hidden="true" />
                  </div>
                </label>
                <ul
                  tabIndex={0}
                  className="mt-3 z-[1] p-2 shadow menu menu-sm dropdown-content bg-base-100 rounded-box w-52"
                  data-testid="user-menu-dropdown"
                >
                  <li className="menu-title">
                    <span data-testid="user-email">{user?.email || 'User'}</span>
                  </li>
                  <li>
                    <Link to="/dashboard" data-testid="menu-dashboard-link">
                      Dashboard
                    </Link>
                  </li>
                  <li>
                    <button
                      onClick={handleLogout}
                      className="flex items-center gap-2"
                      data-testid="logout-button"
                    >
                      <LogOut className="h-4 w-4" aria-hidden="true" />
                      Logout
                    </button>
                  </li>
                </ul>
              </div>
            </div>
          ) : (
            /* Unauthenticated state - show login/register */
            <>
              <Link
                to="/login"
                className="btn btn-ghost"
                data-testid="navbar-login-link"
              >
                Login
              </Link>
              <Link
                to="/register"
                className="btn btn-primary"
                data-testid="navbar-register-link"
              >
                Register
              </Link>
            </>
          )}
        </div>

        {/* Hamburger Menu Button - Visible only on mobile */}
        <div className="navbar-end md:hidden">
          <button
            className="btn btn-ghost btn-circle"
            onClick={() => setIsMobileMenuOpen(true)}
            aria-label="Open menu"
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            data-testid="hamburger-button"
          >
            <Menu className="h-6 w-6" aria-hidden="true" />
          </button>
        </div>
      </nav>

      {/* Mobile Menu */}
      <MobileMenu
        isOpen={isMobileMenuOpen}
        onClose={() => setIsMobileMenuOpen(false)}
      />
    </>
  );
}
