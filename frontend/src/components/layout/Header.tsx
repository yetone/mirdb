/**
 * Header component with navigation
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Features:
 * - MirDB logo
 * - Navigation links (Dashboard, Browser, Config, Docs)
 * - Theme toggle button placeholder
 * - Responsive hamburger menu on mobile
 */

import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import './Header.css';

interface NavLink {
  label: string;
  path: string;
}

const navLinks: NavLink[] = [
  { label: 'Dashboard', path: '/dashboard' },
  { label: 'Browser', path: '/browser' },
  { label: 'Config', path: '/config' },
  { label: 'Docs', path: '/docs' },
];

export default function Header() {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const location = useLocation();

  const toggleMenu = () => setIsMenuOpen(!isMenuOpen);
  const closeMenu = () => setIsMenuOpen(false);

  return (
    <header className="header" role="banner">
      <div className="header-container">
        <Link to="/" className="header-logo" aria-label="MirDB Home">
          <svg
            className="header-logo-icon"
            width="32"
            height="32"
            viewBox="0 0 32 32"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            aria-hidden="true"
          >
            <rect width="32" height="32" rx="8" fill="currentColor" />
            <path
              d="M8 12h16M8 16h12M8 20h8"
              stroke="white"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
          <span className="header-logo-text">MirDB</span>
        </Link>

        <button
          className="header-menu-toggle"
          onClick={toggleMenu}
          aria-expanded={isMenuOpen}
          aria-controls="main-nav"
          aria-label={isMenuOpen ? 'Close menu' : 'Open menu'}
        >
          <span className="header-menu-icon" aria-hidden="true">
            {isMenuOpen ? '✕' : '☰'}
          </span>
        </button>

        <nav
          id="main-nav"
          className={`header-nav ${isMenuOpen ? 'header-nav--open' : ''}`}
          role="navigation"
          aria-label="Main navigation"
        >
          <ul className="header-nav-list">
            {navLinks.map((link) => (
              <li key={link.path}>
                <Link
                  to={link.path}
                  className={`header-nav-link ${
                    location.pathname === link.path ? 'header-nav-link--active' : ''
                  }`}
                  onClick={closeMenu}
                >
                  {link.label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </header>
  );
}
