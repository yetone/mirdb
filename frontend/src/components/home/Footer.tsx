/**
 * Footer Component
 * Owner: Scenario 1 - Homepage Structure and Layout
 *
 * Page footer with:
 * - Brand/logo
 * - Privacy Policy link
 * - Terms of Service link
 * - Copyright notice
 */

import { Link } from 'react-router-dom';
import { Link2 } from 'lucide-react';
import type { FooterProps } from '../../types/home';

export default function Footer({ brandName = 'LinkSnip' }: FooterProps) {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="footer footer-center p-10 bg-base-200 text-base-content" data-testid="footer">
      <aside>
        <div className="flex items-center gap-2 mb-4">
          <Link2 className="w-8 h-8 text-primary" />
          <span className="text-xl font-bold">{brandName}</span>
        </div>
        <p className="text-base-content/70">
          Shorten URLs, track clicks, and analyze your link performance.
        </p>
      </aside>
      <nav>
        <div className="grid grid-flow-col gap-4">
          <Link to="/privacy" className="link link-hover" data-testid="privacy-policy-link">
            Privacy Policy
          </Link>
          <Link to="/terms" className="link link-hover" data-testid="terms-of-service-link">
            Terms of Service
          </Link>
        </div>
      </nav>
      <aside>
        <p className="text-sm text-base-content/70" data-testid="copyright">
          Copyright &copy; {currentYear} {brandName}. All rights reserved.
        </p>
      </aside>
    </footer>
  );
}
