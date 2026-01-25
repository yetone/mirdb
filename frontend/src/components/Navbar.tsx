import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { ThemeToggle } from './ThemeToggle';

export function Navbar() {
  const { isAuthenticated, logout } = useAuth();
  const navigate = useNavigate();
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const handleLogout = () => {
    logout();
    navigate('/');
    setIsMobileMenuOpen(false);
  };

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false);
  };

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen);
  };

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 z-50 shadow-sm">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold" onClick={closeMobileMenu}>
          ShortURL
        </Link>
      </div>

      {/* Desktop navigation */}
      <div className="navbar-end gap-2 hidden md:flex">
        <ThemeToggle />

        {isAuthenticated ? (
          <>
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
            <button onClick={handleLogout} className="btn btn-ghost">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost" data-testid="nav-login">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary" data-testid="nav-register">
              Register
            </Link>
          </>
        )}
      </div>

      {/* Mobile hamburger menu button */}
      <div className="navbar-end md:hidden">
        <button
          className="btn btn-ghost min-w-[44px] min-h-[44px] p-2"
          onClick={toggleMobileMenu}
          aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={isMobileMenuOpen}
          data-testid="hamburger-menu"
        >
          <svg
            className="w-6 h-6"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            xmlns="http://www.w3.org/2000/svg"
            aria-hidden="true"
          >
            {isMobileMenuOpen ? (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            ) : (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 6h16M4 12h16M4 18h16"
              />
            )}
          </svg>
        </button>
      </div>

      {/* Mobile menu dropdown */}
      {isMobileMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md shadow-lg md:hidden"
          data-testid="mobile-menu"
        >
          <div className="flex flex-col p-4 gap-2">
            <div className="flex justify-center mb-2">
              <ThemeToggle />
            </div>

            {isAuthenticated ? (
              <>
                <Link
                  to="/dashboard"
                  className="btn btn-ghost justify-start min-h-[44px]"
                  onClick={closeMobileMenu}
                >
                  Dashboard
                </Link>
                <button
                  onClick={handleLogout}
                  className="btn btn-ghost justify-start min-h-[44px]"
                >
                  Logout
                </button>
              </>
            ) : (
              <>
                <Link
                  to="/login"
                  className="btn btn-ghost justify-start min-h-[44px]"
                  data-testid="mobile-nav-login"
                  onClick={closeMobileMenu}
                >
                  Login
                </Link>
                <Link
                  to="/register"
                  className="btn btn-primary justify-start min-h-[44px]"
                  data-testid="mobile-nav-register"
                  onClick={closeMobileMenu}
                >
                  Register
                </Link>
              </>
            )}
          </div>
        </div>
      )}
    </nav>
  );
}
