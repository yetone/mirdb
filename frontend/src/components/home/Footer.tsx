/**
 * Footer Component
 * Owner: Scenario 4 - Footer Navigation
 *
 * Requirements covered:
 * - REQ-4: Provide footer with navigation links and copyright information
 * - REQ-10: Link CTAs to registration (/register) and login (/login) routes
 *
 * Expected exports:
 * - Footer: React.FC - Navigation footer component
 *
 * Features:
 * - Navigation links: Home, Login, Register
 * - Dynamic copyright year
 * - Centered layout
 * - Consistent styling with app
 */

import { Link } from 'react-router-dom';

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="w-full py-8 px-4 border-t border-base-300 bg-base-200/50"
      role="contentinfo"
    >
      <div className="max-w-4xl mx-auto flex flex-col items-center space-y-4">
        {/* Navigation Links */}
        <nav aria-label="Footer navigation">
          <ul className="flex flex-wrap justify-center gap-6">
            <li>
              <Link
                to="/"
                className="text-base-content/70 hover:text-primary transition-colors"
              >
                Home
              </Link>
            </li>
            <li>
              <Link
                to="/login"
                className="text-base-content/70 hover:text-primary transition-colors"
              >
                Login
              </Link>
            </li>
            <li>
              <Link
                to="/register"
                className="text-base-content/70 hover:text-primary transition-colors"
              >
                Register
              </Link>
            </li>
          </ul>
        </nav>

        {/* Copyright */}
        <p className="text-sm text-base-content/60">
          &copy; {currentYear} URL Shortener. All rights reserved.
        </p>
      </div>
    </footer>
  );
}
