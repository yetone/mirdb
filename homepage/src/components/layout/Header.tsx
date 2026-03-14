/**
 * Header component with sticky navigation.
 * Owner: Scenario 1 - Header Section Implementation
 *
 * Requirements:
 * - Fixed/sticky positioning at top of viewport (REQ-1)
 * - Logo on left, navigation links and CTA on right
 * - Mobile hamburger menu for compact viewports
 * - Keyboard accessible navigation (REQ-8)
 */

import { useState } from 'react'
import type { NavLink, CTAButton } from '../../types'
import { MobileMenu } from './MobileMenu'

export interface HeaderProps {
  logo?: string
  productName: string
  tagline?: string
  navLinks: NavLink[]
  ctaButton: CTAButton
}

export function Header({
  logo,
  productName,
  tagline,
  navLinks,
  ctaButton,
}: HeaderProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  return (
    <header
      className="fixed top-0 left-0 right-0 z-50 bg-white border-b border-secondary-200 shadow-sm"
      role="banner"
    >
      <div className="container-main">
        <div className="flex items-center justify-between h-16 md:h-20">
          {/* Logo and Branding - Left Side */}
          <div className="flex items-center space-x-3">
            {logo && (
              <img
                src={logo}
                alt={`${productName} logo`}
                className="h-8 w-auto md:h-10"
                data-testid="header-logo"
              />
            )}
            <div className="flex flex-col">
              <span className="text-lg md:text-xl font-bold text-secondary-900">
                {productName}
              </span>
              {tagline && (
                <span className="hidden sm:block text-xs text-secondary-500">
                  {tagline}
                </span>
              )}
            </div>
          </div>

          {/* Desktop Navigation - Right Side */}
          <nav
            className="hidden md:flex items-center space-x-8"
            role="navigation"
            aria-label="Main navigation"
          >
            <ul className="flex items-center space-x-6">
              {navLinks.map((link) => (
                <li key={link.href}>
                  <a
                    href={link.href}
                    className="text-secondary-600 hover:text-primary-600 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 rounded-md px-2 py-1 transition-colors"
                    {...(link.isExternal && {
                      target: '_blank',
                      rel: 'noopener noreferrer',
                    })}
                  >
                    {link.label}
                  </a>
                </li>
              ))}
            </ul>
            <a
              href={ctaButton.href}
              className={`
                inline-flex items-center justify-center font-medium rounded-lg
                focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2
                transition-all
                ${ctaButton.size === 'sm' ? 'px-3 py-1.5 text-sm' : ''}
                ${ctaButton.size === 'md' ? 'px-4 py-2 text-base' : ''}
                ${ctaButton.size === 'lg' ? 'px-6 py-3 text-lg' : ''}
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
              data-testid="header-cta"
            >
              {ctaButton.label}
            </a>
          </nav>

          {/* Mobile Hamburger Menu Button */}
          <button
            type="button"
            className="md:hidden inline-flex items-center justify-center p-2 rounded-md text-secondary-600 hover:text-primary-600 hover:bg-secondary-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
            onClick={toggleMobileMenu}
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            data-testid="hamburger-menu-button"
          >
            <span className="sr-only">
              {isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            </span>
            {/* Hamburger Icon */}
            {!isMobileMenuOpen ? (
              <svg
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                strokeWidth={1.5}
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5"
                />
              </svg>
            ) : (
              <svg
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                strokeWidth={1.5}
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
            )}
          </button>
        </div>
      </div>

      {/* Mobile Menu */}
      <MobileMenu
        isOpen={isMobileMenuOpen}
        onClose={closeMobileMenu}
        navLinks={navLinks}
        ctaButton={ctaButton}
      />
    </header>
  )
}
