/**
 * Homepage Footer Component.
 * Owner: Scenario 8 - Footer Section
 *
 * Footer section with:
 * - Logo/brand name
 * - Copyright notice with current year
 * - Navigation links: Login, Register, About (optional)
 * - Social media link placeholders
 *
 * Requirements: REQ-8
 * Min height: 150px
 */
import { Link } from 'react-router-dom';
import { Link2 } from 'lucide-react';

interface HomeFooterProps {
  className?: string;
}

export function HomeFooter({ className = '' }: HomeFooterProps) {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className={`footer footer-center bg-base-200 text-base-content p-10 min-h-[150px] ${className}`}
      data-testid="home-footer"
      role="contentinfo"
      aria-label="Site footer"
    >
      {/* Logo/Brand */}
      <div className="flex items-center gap-2">
        <Link2 className="h-8 w-8" aria-hidden="true" />
        <span className="font-bold text-xl" data-testid="footer-brand">
          URL Shortener
        </span>
      </div>

      {/* Navigation Links */}
      <nav className="grid grid-flow-col gap-4" aria-label="Footer navigation">
        <Link
          to="/login"
          className="link link-hover"
          data-testid="footer-login-link"
        >
          Login
        </Link>
        <Link
          to="/register"
          className="link link-hover"
          data-testid="footer-register-link"
        >
          Register
        </Link>
      </nav>

      {/* Copyright Notice */}
      <aside>
        <p data-testid="footer-copyright">
          Copyright &copy; {currentYear} URL Shortener. All rights reserved.
        </p>
      </aside>
    </footer>
  );
}
