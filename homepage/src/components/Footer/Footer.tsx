/**
 * Footer component for MirDB homepage.
 * Owner: Scenario 17 - Footer Content and Links
 *
 * Requirements:
 * - Copyright notice
 * - GitHub link
 * - Additional resource links
 */

import { links, GITHUB_URL } from '../../data/links'

/**
 * Footer component displaying copyright notice and essential links.
 */
export const Footer: React.FC = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="bg-gray-900 text-gray-300 py-8 border-t border-gray-800"
      role="contentinfo"
    >
      <div className="container mx-auto px-4">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          {/* Copyright Section */}
          <div className="text-center md:text-left">
            <p className="text-sm">
              &copy; {currentYear} MirDB. All rights reserved.
            </p>
            <p className="text-xs text-gray-500 mt-1">
              A persistent key-value store with Memcached protocol support
            </p>
          </div>

          {/* Navigation Links */}
          <nav aria-label="Footer navigation">
            <ul className="flex flex-wrap justify-center gap-6">
              {links.map((link) => (
                <li key={link.url}>
                  <a
                    href={link.url}
                    className="text-sm text-gray-400 hover:text-white transition-colors duration-200"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {link.label}
                  </a>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* GitHub CTA */}
        <div className="mt-6 pt-6 border-t border-gray-800 text-center">
          <a
            href={GITHUB_URL}
            className="inline-flex items-center gap-2 text-sm text-gray-400 hover:text-white transition-colors duration-200"
            target="_blank"
            rel="noopener noreferrer"
            aria-label="View MirDB on GitHub"
          >
            <svg
              className="w-5 h-5"
              fill="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                fillRule="evenodd"
                d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                clipRule="evenodd"
              />
            </svg>
            <span>Star us on GitHub</span>
          </a>
        </div>
      </div>
    </footer>
  )
}
