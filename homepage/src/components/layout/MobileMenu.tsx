/**
 * Mobile navigation menu component.
 * Owner: Scenario 1 - Header Section Implementation
 *
 * Requirements:
 * - Hamburger icon trigger
 * - Slide-in or overlay menu
 * - Contains all navigation links
 * - Close on link click or outside click
 * - Keyboard accessible (Escape to close)
 */

import { useEffect, useCallback } from 'react'
import type { NavLink, CTAButton } from '../../types'

export interface MobileMenuProps {
  isOpen: boolean
  onClose: () => void
  navLinks: NavLink[]
  ctaButton: CTAButton
}

export function MobileMenu({
  isOpen,
  onClose,
  navLinks,
  ctaButton,
}: MobileMenuProps) {
  // Handle escape key press to close menu
  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      if (event.key === 'Escape' && isOpen) {
        onClose()
      }
    },
    [isOpen, onClose]
  )

  // Add keyboard event listener
  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [handleKeyDown])

  // Prevent body scroll when menu is open
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

  // Handle link click - close menu after navigation
  const handleLinkClick = () => {
    onClose()
  }

  if (!isOpen) {
    return null
  }

  return (
    <>
      {/* Backdrop overlay */}
      <div
        className="fixed inset-0 bg-black/50 z-40 md:hidden"
        onClick={onClose}
        aria-hidden="true"
        data-testid="mobile-menu-backdrop"
      />

      {/* Mobile menu panel */}
      <nav
        id="mobile-menu"
        className="fixed top-16 left-0 right-0 bottom-0 z-50 bg-white md:hidden overflow-y-auto"
        role="navigation"
        aria-label="Mobile navigation"
        data-testid="mobile-menu"
      >
        <div className="px-4 py-6 space-y-4">
          <ul className="space-y-2">
            {navLinks.map((link) => (
              <li key={link.href}>
                <a
                  href={link.href}
                  onClick={handleLinkClick}
                  className="block px-4 py-3 text-lg font-medium text-secondary-700 hover:text-primary-600 hover:bg-secondary-50 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 transition-colors"
                  {...(link.isExternal && {
                    target: '_blank',
                    rel: 'noopener noreferrer',
                  })}
                  data-testid={`mobile-nav-link-${link.label.toLowerCase()}`}
                >
                  {link.label}
                </a>
              </li>
            ))}
          </ul>

          {/* CTA Button */}
          <div className="pt-4 border-t border-secondary-200">
            <a
              href={ctaButton.href}
              onClick={handleLinkClick}
              className={`
                block w-full text-center font-medium rounded-lg
                focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2
                transition-all
                ${ctaButton.size === 'sm' ? 'px-3 py-2 text-sm' : ''}
                ${ctaButton.size === 'md' ? 'px-4 py-3 text-base' : ''}
                ${ctaButton.size === 'lg' ? 'px-6 py-4 text-lg' : ''}
                ${
                  ctaButton.variant === 'primary'
                    ? 'bg-primary-600 text-white hover:bg-primary-700'
                    : ''
                }
                ${
                  ctaButton.variant === 'secondary'
                    ? 'bg-secondary-100 text-secondary-700 hover:bg-secondary-200'
                    : ''
                }
                ${
                  ctaButton.variant === 'outline'
                    ? 'border-2 border-primary-600 text-primary-600 hover:bg-primary-50'
                    : ''
                }
              `}
              data-testid="mobile-menu-cta"
            >
              {ctaButton.label}
            </a>
          </div>
        </div>
      </nav>
    </>
  )
}
