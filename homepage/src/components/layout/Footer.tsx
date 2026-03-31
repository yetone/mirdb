/**
 * Footer Component
 * Owner: Scenario 9 - Resource Links and Footer
 *
 * Site footer with:
 * - GitHub link
 * - License information
 * - CircleCI build link
 * - Community links
 *
 * Requirements: REQ-4
 */

import { Github, Heart } from 'lucide-react';
import { GITHUB_URL, CIRCLECI_URL, LICENSE, COPYRIGHT_YEAR, SITE_TITLE } from '../../constants/content';

export function Footer() {
  return (
    <footer
      className="bg-gray-900 dark:bg-gray-950 text-gray-300 py-12 px-4 sm:px-6 lg:px-8"
      data-testid="footer"
      role="contentinfo"
    >
      <div className="max-w-6xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          {/* Brand & Description */}
          <div>
            <h3 className="text-xl font-bold text-white mb-4" data-testid="footer-brand">
              {SITE_TITLE}
            </h3>
            <p className="text-gray-400">
              A persistent key-value store with Memcached protocol support.
            </p>
          </div>

          {/* Links */}
          <div>
            <h4 className="text-lg font-semibold text-white mb-4">Links</h4>
            <ul className="space-y-2">
              <li>
                <a
                  href={GITHUB_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:text-white transition-colors flex items-center"
                  data-testid="footer-github-link"
                >
                  <Github className="w-4 h-4 mr-2" aria-hidden="true" />
                  GitHub Repository
                </a>
              </li>
              <li>
                <a
                  href={`${GITHUB_URL}#readme`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:text-white transition-colors"
                  data-testid="footer-docs-link"
                >
                  Documentation
                </a>
              </li>
              <li>
                <a
                  href={`${GITHUB_URL}/issues`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:text-white transition-colors"
                  data-testid="footer-issues-link"
                >
                  Report Issues
                </a>
              </li>
            </ul>
          </div>

          {/* Build Status */}
          <div>
            <h4 className="text-lg font-semibold text-white mb-4">Build Status</h4>
            <a
              href={CIRCLECI_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center px-3 py-2 bg-gray-800 hover:bg-gray-700 rounded-md transition-colors"
              data-testid="footer-circleci-link"
            >
              <svg
                className="w-5 h-5 mr-2"
                viewBox="0 0 103 105"
                fill="currentColor"
                aria-hidden="true"
              >
                <path d="M38.6 52.5c0-6.9 5.6-12.5 12.5-12.5s12.5 5.6 12.5 12.5-5.6 12.5-12.5 12.5-12.5-5.6-12.5-12.5zm12.5-52.5C23.3 0 .5 21.1 .5 52.5c0 15.8 6.4 30.1 16.8 40.4 4.5 4.5 11.8 4.5 16.3 0l.4-.4c4.2-4.2 4.3-10.9.4-15.3-6.7-7.6-10.8-17.5-10.8-28.5 0-23.9 19.4-43.3 43.3-43.3s43.3 19.4 43.3 43.3c0 11-4.1 21-10.8 28.5-3.9 4.4-3.8 11.1.4 15.3l.4.4c4.5 4.5 11.8 4.5 16.3 0 10.4-10.4 16.8-24.6 16.8-40.4C102.5 21.1 79.7 0 51.1 0z" />
              </svg>
              CircleCI Build
            </a>
          </div>
        </div>

        {/* Divider */}
        <div className="border-t border-gray-800 pt-8">
          <div className="flex flex-col md:flex-row justify-between items-center">
            {/* License */}
            <p className="text-sm text-gray-400 mb-4 md:mb-0" data-testid="footer-license">
              Released under the {LICENSE}
            </p>

            {/* Copyright & Credits */}
            <p className="text-sm text-gray-400 flex items-center">
              Made with <Heart className="w-4 h-4 mx-1 text-red-500" aria-hidden="true" /> &copy; {COPYRIGHT_YEAR} {SITE_TITLE}
            </p>
          </div>
        </div>
      </div>
    </footer>
  );
}
