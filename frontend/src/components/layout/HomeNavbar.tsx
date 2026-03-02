/**
 * Homepage Navigation Bar Component.
 * Owner: Scenario 3 - Navigation Bar Functionality
 *
 * Renders the navigation bar with:
 * - Logo/brand name (left-aligned)
 * - Theme toggle (center)
 * - Login and Register links (right-aligned)
 * - 60px height
 * - Responsive: hamburger menu on mobile (< 768px)
 *
 * Requirements: REQ-8, REQ-9
 */
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { Link2, Menu } from 'lucide-react';
import { ThemeToggle } from '../ThemeToggle';
import { MobileMenu } from './MobileMenu';

interface HomeNavbarProps {
  className?: string;
}

export function HomeNavbar({ className = '' }: HomeNavbarProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

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
