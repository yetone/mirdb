/**
 * Footer Component
 * Owner: Scenario 5 - Footer Section
 *
 * Landing page footer containing:
 * - Copyright notice with current year
 * - Links to Login and Register pages
 * - ThemeToggle component for theme switching
 *
 * Uses:
 * - ThemeToggle component from existing codebase
 * - React Router Link for navigation
 * - Tailwind CSS for styling
 */
import { Link } from 'react-router-dom';
import ThemeToggle from '../ThemeToggle';

export default function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="footer footer-center p-6 bg-base-200 text-base-content border-t border-base-300"
      role="contentinfo"
    >
      <div className="container mx-auto flex flex-col md:flex-row items-center justify-between gap-4">
        {/* Copyright Notice */}
        <p className="text-sm">
          &copy; {currentYear} URL Shortener. All rights reserved.
        </p>

        {/* Navigation Links */}
        <nav aria-label="Footer navigation" className="flex items-center gap-4">
          <Link
            to="/login"
            className="link link-hover text-sm"
          >
            Login
          </Link>
          <Link
            to="/register"
            className="link link-hover text-sm"
          >
            Register
          </Link>
        </nav>

        {/* Theme Toggle */}
        <div className="flex items-center">
          <ThemeToggle />
        </div>
      </div>
    </footer>
  );
}
