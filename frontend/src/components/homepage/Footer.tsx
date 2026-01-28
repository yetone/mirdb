/**
 * Footer Component
 * Owner: Scenario 5 - Footer Section
 *
 * Homepage footer with:
 * - Copyright notice with current year
 * - Links to Terms of Service
 * - Links to Privacy Policy
 *
 * Expected props:
 * - links: FooterLink[] (optional, defaults to Terms and Privacy links)
 * - copyright: string (optional, defaults to company copyright)
 *
 * Requirements: REQ-9
 */

import React from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import type { FooterLink } from '../../types/homepage';

export interface FooterProps {
  links?: FooterLink[];
  copyright?: string;
}

const defaultLinks: FooterLink[] = [
  { label: 'Terms of Service', href: '/terms' },
  { label: 'Privacy Policy', href: '/privacy' },
];

const currentYear = new Date().getFullYear();
const defaultCopyright = `© ${currentYear} URL Shortener. All rights reserved.`;

/**
 * Footer - Homepage footer component
 * Displays copyright notice and relevant links (Terms, Privacy).
 */
export const Footer: React.FC<FooterProps> = ({
  links = defaultLinks,
  copyright = defaultCopyright,
}) => {
  return (
    <motion.footer
      data-testid="footer-section"
      role="contentinfo"
      className="py-8 border-t border-gray-700/50 bg-base-200/30"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.5, delay: 0.2 }}
    >
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          {/* Copyright Notice */}
          <p
            data-testid="footer-copyright"
            className="text-gray-400 text-sm text-center md:text-left"
          >
            {copyright}
          </p>

          {/* Footer Links */}
          <nav
            aria-label="Footer navigation"
            className="flex flex-wrap justify-center gap-6"
          >
            {links.map((link) => (
              <Link
                key={link.href}
                to={link.href}
                className="text-gray-400 text-sm hover:text-primary transition-colors duration-200"
              >
                {link.label}
              </Link>
            ))}
          </nav>
        </div>
      </div>
    </motion.footer>
  );
};

export default Footer;
