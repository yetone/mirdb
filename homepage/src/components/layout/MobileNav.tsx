/**
 * Mobile Navigation Component.
 * Owner: Scenario 8 - Responsive Design
 *
 * Features:
 * - Hamburger menu toggle
 * - Full-screen navigation overlay
 * - Touch-friendly targets (44px minimum)
 * - Navigation links for all sections
 *
 * Requirements:
 * - REQ-6: Responsive display on mobile
 * - US-5: Mobile accessibility
 */

import { useState, useCallback, useEffect } from 'react'
import { Menu, X } from 'lucide-react'
import { SECTIONS, GITHUB_URL, PRODUCT_NAME } from '@/utils/constants'
import { useSmoothScroll } from '@/hooks/useSmoothScroll'

export interface MobileNavProps {
  className?: string
}

export function MobileNav({ className = '' }: MobileNavProps) {
  const [isOpen, setIsOpen] = useState(false)
  const { scrollTo } = useSmoothScroll()

  const toggleMenu = useCallback(() => {
    setIsOpen((prev) => !prev)
  }, [])

  const closeMenu = useCallback(() => {
    setIsOpen(false)
  }, [])

  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    if (href.startsWith('#')) {
      e.preventDefault()
      const sectionId = href.slice(1)
      closeMenu()
      // Small delay to allow menu close animation
      setTimeout(() => {
        scrollTo(sectionId)
      }, 100)
    } else {
      closeMenu()
    }
  }

  // Lock body scroll when menu is open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden'
    } else {
      document.body.style.overflow = ''
    }
    return () => {
      document.body.style.overflow = ''
    }
  }, [isOpen])

  // Close menu on escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        closeMenu()
      }
    }
    document.addEventListener('keydown', handleEscape)
    return () => {
      document.removeEventListener('keydown', handleEscape)
    }
  }, [isOpen, closeMenu])

  return (
    <div className={`md:hidden ${className}`}>
      {/* Hamburger Menu Button - 44x44 minimum touch target */}
      <button
        onClick={toggleMenu}
        className="mobile-menu-button min-h-[44px] min-w-[44px] p-2 flex items-center justify-center text-gray-600 hover:text-gray-900 dark:text-gray-300 dark:hover:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 dark:focus:ring-offset-gray-900"
        aria-label={isOpen ? 'Close navigation menu' : 'Open navigation menu'}
        aria-expanded={isOpen}
        aria-controls="mobile-nav-menu"
        type="button"
        data-testid="mobile-menu-button"
      >
        {isOpen ? (
          <X className="w-6 h-6" aria-hidden="true" />
        ) : (
          <Menu className="w-6 h-6" aria-hidden="true" />
        )}
      </button>

      {/* Mobile Navigation Overlay */}
      {isOpen && (
        <>
          {/* Backdrop */}
          <div
            className="fixed inset-0 bg-black/50 z-40"
            onClick={closeMenu}
            aria-hidden="true"
          />

          {/* Navigation Panel */}
          <nav
            id="mobile-nav-menu"
            className="mobile-nav-menu fixed inset-y-0 right-0 z-50 w-full max-w-sm bg-white dark:bg-gray-900 shadow-xl"
            aria-label="Mobile navigation"
            role="navigation"
          >
            <div className="flex flex-col h-full">
              {/* Header with Close Button */}
              <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
                <span className="text-lg font-semibold text-gray-900 dark:text-white">
                  {PRODUCT_NAME}
                </span>
                <button
                  onClick={closeMenu}
                  className="mobile-close-button min-h-[44px] min-w-[44px] p-2 flex items-center justify-center text-gray-600 hover:text-gray-900 dark:text-gray-300 dark:hover:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
                  aria-label="Close navigation menu"
                  type="button"
                  data-testid="mobile-menu-close"
                >
                  <X className="w-6 h-6" aria-hidden="true" />
                </button>
              </div>

              {/* Navigation Links */}
              <div className="flex-1 overflow-y-auto py-4">
                <ul className="space-y-1 px-4">
                  {SECTIONS.map((section) => (
                    <li key={section.href}>
                      <a
                        href={section.href}
                        onClick={(e) => handleNavClick(e, section.href)}
                        className="mobile-nav-link min-h-[44px] flex items-center px-4 py-3 text-base font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 dark:text-gray-300 dark:hover:text-white dark:hover:bg-gray-800 rounded-lg transition-colors"
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
                      onClick={() => closeMenu()}
                      className="mobile-nav-link min-h-[44px] flex items-center px-4 py-3 text-base font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 dark:text-gray-300 dark:hover:text-white dark:hover:bg-gray-800 rounded-lg transition-colors"
                      aria-label="View GitHub repository (opens in new tab)"
                    >
                      GitHub
                      <svg
                        className="ml-2 w-4 h-4"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                        aria-hidden="true"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                        />
                      </svg>
                    </a>
                  </li>
                </ul>
              </div>

              {/* Footer */}
              <div className="p-4 border-t border-gray-200 dark:border-gray-700">
                <p className="text-sm text-gray-500 dark:text-gray-400 text-center">
                  A Persistent Key-Value Store
                </p>
              </div>
            </div>
          </nav>
        </>
      )}
    </div>
  )
}

export default MobileNav
