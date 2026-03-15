/**
 * Header Navigation Component.
 * Owner: Scenario 5 - Navigation Header and Footer
 *
 * Responsibilities:
 * - Sticky/fixed header on scroll
 * - Logo display
 * - Navigation links (Features, Quick Start, Architecture, Community)
 * - Theme toggle integration point
 * - Mobile menu toggle (hamburger)
 *
 * Requirements:
 * - REQ-9: Navigation header with relevant links
 */

import { PRODUCT_NAME, SECTIONS, GITHUB_URL } from '@/utils/constants'
import { useSmoothScroll } from '@/hooks/useSmoothScroll'

export interface HeaderProps {
  className?: string
}

export function Header({ className = '' }: HeaderProps) {
  const { scrollTo } = useSmoothScroll()

  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    if (href.startsWith('#')) {
      e.preventDefault()
      const sectionId = href.slice(1)
      scrollTo(sectionId)
    }
  }

  return (
    <header
      className={`sticky top-0 z-50 w-full border-b border-gray-200 bg-white/95 backdrop-blur supports-[backdrop-filter]:bg-white/60 dark:border-gray-800 dark:bg-gray-900/95 dark:supports-[backdrop-filter]:bg-gray-900/60 ${className}`}
      role="banner"
    >
      <div className="container mx-auto flex h-16 items-center justify-between px-4">
        {/* Logo */}
        <a
          href="/"
          className="flex items-center gap-2 text-xl font-bold text-gray-900 dark:text-white"
          aria-label={`${PRODUCT_NAME} homepage`}
        >
          <img
            src="/logo.gif"
            alt={`${PRODUCT_NAME} logo`}
            className="h-8 w-8"
          />
          <span>{PRODUCT_NAME}</span>
        </a>

        {/* Navigation */}
        <nav aria-label="Main navigation" role="navigation">
          <ul className="hidden md:flex items-center gap-6">
            {SECTIONS.map((section) => (
              <li key={section.href}>
                <a
                  href={section.href}
                  onClick={(e) => handleNavClick(e, section.href)}
                  className="text-sm font-medium text-gray-600 transition-colors hover:text-gray-900 dark:text-gray-300 dark:hover:text-white"
                  aria-label={`Navigate to ${section.label} section`}
                >
                  {section.label}
                </a>
              </li>
            ))}
            <li>
              <a
                href={GITHUB_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm font-medium text-gray-600 transition-colors hover:text-gray-900 dark:text-gray-300 dark:hover:text-white"
                aria-label="View GitHub repository (opens in new tab)"
              >
                GitHub
              </a>
            </li>
          </ul>
        </nav>

        {/* Mobile menu button placeholder - to be implemented by Scenario 8 */}
        <button
          className="md:hidden p-2 text-gray-600 hover:text-gray-900 dark:text-gray-300 dark:hover:text-white"
          aria-label="Open mobile menu"
          aria-expanded="false"
          type="button"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <line x1="3" y1="12" x2="21" y2="12" />
            <line x1="3" y1="6" x2="21" y2="6" />
            <line x1="3" y1="18" x2="21" y2="18" />
          </svg>
        </button>
      </div>
    </header>
  )
}

export default Header
