/**
 * Reused navbar with responsive hamburger menu.
 * Stub created by first builder; responsive behavior added by Scenario 5.
 */
import React, { useState } from 'react';
import { ThemeToggle } from './ThemeToggle';

export function Navbar() {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <nav className="navbar" data-testid="navbar">
      <span className="navbar-brand" data-testid="navbar-brand">URL Shortener</span>
      <button
        type="button"
        className="navbar-toggle tap-target"
        aria-label="Toggle navigation menu"
        aria-expanded={menuOpen}
        onClick={() => setMenuOpen(!menuOpen)}
        data-testid="navbar-toggle"
      >
        <svg
          width="24"
          height="24"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          aria-hidden="true"
        >
          {menuOpen ? (
            <>
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </>
          ) : (
            <>
              <line x1="3" y1="6" x2="21" y2="6" />
              <line x1="3" y1="12" x2="21" y2="12" />
              <line x1="3" y1="18" x2="21" y2="18" />
            </>
          )}
        </svg>
      </button>
      <ul
        className={`navbar-menu${menuOpen ? ' is-open' : ''}`}
        data-testid="navbar-menu"
      >
        <li>
          <a href="/" className="tap-target" data-testid="nav-link-home">Home</a>
        </li>
        <li>
          <a href="/login" className="tap-target" data-testid="nav-link-login">Login</a>
        </li>
        <li>
          <a href="/register" className="tap-target" data-testid="nav-link-register">Register</a>
        </li>
      </ul>
      <ThemeToggle />
    </nav>
  );
}
