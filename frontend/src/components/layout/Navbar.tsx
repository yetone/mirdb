import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useTheme } from '../../contexts/ThemeContext';
import { Theme } from '../../types';

const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

export function Navbar() {
  const { theme, setTheme } = useTheme();
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen);
  };

  return (
    <header className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-content/10" data-testid="navbar">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl" data-testid="navbar-logo">
          URLShort
        </Link>
      </div>

      {/* Mobile hamburger menu button */}
      <div className="flex-none md:hidden">
        <button
          className="btn btn-ghost btn-square"
          onClick={toggleMobileMenu}
          aria-label="Toggle mobile menu"
          aria-expanded={isMobileMenuOpen}
          data-testid="navbar-hamburger"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-6 w-6"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            {isMobileMenuOpen ? (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            )}
          </svg>
        </button>
      </div>

      {/* Desktop navigation */}
      <div className="flex-none gap-2 hidden md:flex" data-testid="navbar-desktop-nav">
        <div className="dropdown dropdown-end">
          <label tabIndex={0} className="btn btn-ghost btn-sm" data-testid="theme-toggle">
            Theme
          </label>
          <ul tabIndex={0} className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-52">
            {themes.map((t) => (
              <li key={t}>
                <button
                  onClick={() => setTheme(t)}
                  className={theme === t ? 'active' : ''}
                  data-testid={`theme-option-${t}`}
                >
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              </li>
            ))}
          </ul>
        </div>
        <Link to="/login" className="btn btn-ghost btn-sm" data-testid="navbar-login">
          Login
        </Link>
        <Link to="/register" className="btn btn-primary btn-sm" data-testid="navbar-register">
          Register
        </Link>
      </div>

      {/* Mobile dropdown menu */}
      {isMobileMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md border-b border-base-content/10 md:hidden"
          data-testid="navbar-mobile-menu"
        >
          <div className="flex flex-col p-4 gap-2">
            <div className="dropdown">
              <label tabIndex={0} className="btn btn-ghost w-full justify-start" data-testid="theme-toggle-mobile">
                Theme: {theme.charAt(0).toUpperCase() + theme.slice(1)}
              </label>
              <ul tabIndex={0} className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-full">
                {themes.map((t) => (
                  <li key={t}>
                    <button
                      onClick={() => {
                        setTheme(t);
                        setIsMobileMenuOpen(false);
                      }}
                      className={theme === t ? 'active' : ''}
                      data-testid={`theme-option-mobile-${t}`}
                    >
                      {t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                  </li>
                ))}
              </ul>
            </div>
            <Link
              to="/login"
              className="btn btn-ghost w-full justify-start"
              data-testid="navbar-login-mobile"
              onClick={() => setIsMobileMenuOpen(false)}
            >
              Login
            </Link>
            <Link
              to="/register"
              className="btn btn-primary w-full"
              data-testid="navbar-register-mobile"
              onClick={() => setIsMobileMenuOpen(false)}
            >
              Register
            </Link>
          </div>
        </div>
      )}
    </header>
  );
}
