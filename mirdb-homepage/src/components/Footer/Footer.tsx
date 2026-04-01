/**
 * Footer Component.
 * Owner: Scenario 6 - Navigation and External Links
 *
 * Displays footer with:
 * - GitHub repository link with icon
 * - License information
 * - External links (docs, contributions)
 *
 * All external links have:
 * - target="_blank"
 * - rel="noopener noreferrer"
 */

import { GITHUB_REPO_URL } from '../../utils/constants'

interface FooterLink {
  href: string
  label: string
  ariaLabel?: string
}

const externalLinks: FooterLink[] = [
  {
    href: GITHUB_REPO_URL,
    label: 'GitHub',
    ariaLabel: 'View MirDB source code on GitHub',
  },
  {
    href: `${GITHUB_REPO_URL}#readme`,
    label: 'Documentation',
    ariaLabel: 'View MirDB documentation',
  },
  {
    href: `${GITHUB_REPO_URL}/issues`,
    label: 'Issues',
    ariaLabel: 'View or report issues',
  },
  {
    href: `${GITHUB_REPO_URL}/blob/master/CONTRIBUTING.md`,
    label: 'Contributing',
    ariaLabel: 'Learn how to contribute to MirDB',
  },
]

export function Footer() {
  return (
    <footer className="bg-gray-100 dark:bg-gray-800 py-8 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          {/* Links Section */}
          <nav aria-label="Footer navigation" className="flex flex-wrap justify-center gap-4 md:gap-6">
            {externalLinks.map((link) => (
              <a
                key={link.href}
                href={link.href}
                target="_blank"
                rel="noopener noreferrer"
                aria-label={link.ariaLabel}
                className="text-gray-600 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
              >
                {link.label === 'GitHub' ? (
                  <span className="flex items-center gap-2">
                    <GitHubIcon />
                    {link.label}
                  </span>
                ) : (
                  link.label
                )}
              </a>
            ))}
          </nav>

          {/* License Section */}
          <div className="text-gray-600 dark:text-gray-400 text-sm text-center md:text-right">
            <p data-testid="license-info">
              Released under the{' '}
              <a
                href={`${GITHUB_REPO_URL}/blob/master/LICENSE`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 dark:text-blue-400 underline hover:no-underline"
              >
                MIT License
              </a>
            </p>
            <p className="mt-1">© {new Date().getFullYear()} MirDB</p>
          </div>
        </div>
      </div>
    </footer>
  )
}

function GitHubIcon() {
  return (
    <svg
      className="w-5 h-5"
      viewBox="0 0 24 24"
      fill="currentColor"
      aria-hidden="true"
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.17 6.839 9.49.5.092.682-.217.682-.482 0-.237-.008-.866-.013-1.7-2.782.604-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.025 2.747-1.025.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.167 22 16.418 22 12c0-5.523-4.477-10-10-10z"
      />
    </svg>
  )
}

export default Footer
