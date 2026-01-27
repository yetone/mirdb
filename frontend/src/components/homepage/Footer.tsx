/**
 * Footer Component
 * Owner: Scenario 10 - Footer Section Display
 *
 * Requirements:
 * - Application branding/logo
 * - Navigation links (Login, Register)
 * - Copyright notice with current year
 * - Theme-aware styling
 */
import { Link } from 'react-router-dom';

export default function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="py-8 border-t border-base-content/10 bg-base-200/50">
      <div className="container mx-auto px-4">
        <div className="flex flex-col md:flex-row items-center justify-between gap-4">
          {/* Branding */}
          <div className="flex items-center gap-2">
            <span className="text-xl font-bold text-primary">🔗</span>
            <span className="text-lg font-semibold text-base-content">URL Shortener</span>
          </div>

          {/* Navigation Links */}
          <div className="flex items-center gap-6">
            <Link
              to="/login"
              className="text-base-content/70 hover:text-primary transition-colors"
            >
              Login
            </Link>
            <Link
              to="/register"
              className="text-base-content/70 hover:text-primary transition-colors"
            >
              Register
            </Link>
          </div>

          {/* Copyright */}
          <p className="text-base-content/70 text-sm">
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
