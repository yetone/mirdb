/**
 * Footer component.
 * Owner: Scenario 7 - Footer Section
 *
 * Features:
 * - MIT License information with link
 * - GitHub links (repo, issues, discussions)
 * - Copyright notice with current year
 *
 * Requirements: REQ-14
 */

import React from 'react';
import { GITHUB_URL } from '@/lib/constants';

interface FooterLink {
  label: string;
  href: string;
  external?: boolean;
}

const FOOTER_LINKS: FooterLink[] = [
  { label: 'Repository', href: GITHUB_URL, external: true },
  { label: 'Issues', href: `${GITHUB_URL}/issues`, external: true },
  { label: 'Discussions', href: `${GITHUB_URL}/discussions`, external: true },
];

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="bg-gray-100 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-800">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-6">
          {/* License Information */}
          <div className="text-center md:text-left">
            <p className="text-gray-600 dark:text-gray-400">
              Released under the{' '}
              <a
                href={`${GITHUB_URL}/blob/master/LICENSE`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary-600 dark:text-primary-400 hover:underline font-medium"
                aria-label="View MIT License"
              >
                MIT License
              </a>
            </p>
          </div>

          {/* GitHub Links */}
          <nav aria-label="Footer navigation">
            <ul className="flex flex-wrap justify-center md:justify-end gap-4 md:gap-6">
              {FOOTER_LINKS.map((link) => (
                <li key={link.label}>
                  <a
                    href={link.href}
                    target={link.external ? '_blank' : undefined}
                    rel={link.external ? 'noopener noreferrer' : undefined}
                    className="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition-colors"
                  >
                    {link.label}
                  </a>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* Copyright Notice */}
        <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-800">
          <p className="text-center text-sm text-gray-600 dark:text-gray-400">
            &copy; {currentYear} MirDB. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
