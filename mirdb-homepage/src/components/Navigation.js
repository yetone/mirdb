/**
 * Navigation Component
 * Owner: Scenario 8 - Navigation Component
 *
 * Renders a sticky navigation bar with responsive mobile menu,
 * smooth scrolling navigation links, and proper accessibility attributes.
 */

import { handleNavLinkClick } from '../utils/scroll.js'

/**
 * MirDB Logo SVG for the navigation bar
 */
const navLogoSvg = `
  <svg
    class="w-8 h-8"
    viewBox="0 0 200 200"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <defs>
      <linearGradient id="navLogoGradient" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" style="stop-color:#3b82f6;stop-opacity:1" />
        <stop offset="100%" style="stop-color:#1d4ed8;stop-opacity:1" />
      </linearGradient>
    </defs>
    <circle cx="100" cy="100" r="90" fill="url(#navLogoGradient)"/>
    <path d="M60 70 L100 50 L140 70 L140 130 L100 150 L60 130 Z" fill="white" opacity="0.9"/>
    <path d="M60 70 L100 90 L140 70" stroke="white" stroke-width="2" fill="none"/>
    <path d="M100 90 L100 150" stroke="white" stroke-width="2" fill="none"/>
    <text x="100" y="115" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="#1d4ed8" text-anchor="middle">DB</text>
  </svg>
`

/**
 * Navigation links configuration
 */
const navLinks = [
  { href: '#features', label: 'Features', isExternal: false },
  { href: '#quickstart', label: 'Documentation', isExternal: false },
  { href: 'https://github.com/transybao93/mirdb', label: 'GitHub', isExternal: true }
]

/**
 * Renders a single navigation link
 * @param {Object} link - Link configuration
 * @param {string} link.href - The link URL
 * @param {string} link.label - The link text
 * @param {boolean} link.isExternal - Whether this is an external link
 * @returns {string} HTML string for the navigation link
 */
export function NavLink({ href, label, isExternal }) {
  const externalAttrs = isExternal
    ? 'target="_blank" rel="noopener noreferrer"'
    : ''

  const dataTestId = `nav-link-${label.toLowerCase().replace(/\s+/g, '-')}`

  return `
    <a
      href="${href}"
      class="nav-link text-gray-700 dark:text-gray-200 hover:text-blue-600 dark:hover:text-blue-400 transition-colors duration-200 font-medium px-3 py-2"
      data-testid="${dataTestId}"
      ${externalAttrs}
    >
      ${label}
      ${isExternal ? `
        <svg class="inline-block w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
        </svg>
      ` : ''}
    </a>
  `
}

/**
 * Renders the mobile menu overlay
 * @returns {string} HTML string for the mobile menu
 */
export function MobileMenu() {
  const mobileLinks = navLinks.map(link => {
    const externalAttrs = link.isExternal
      ? 'target="_blank" rel="noopener noreferrer"'
      : ''
    const dataTestId = `mobile-nav-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`

    return `
      <a
        href="${link.href}"
        class="mobile-nav-link block px-4 py-3 text-lg text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors duration-200"
        data-testid="${dataTestId}"
        ${externalAttrs}
      >
        ${link.label}
        ${link.isExternal ? `
          <svg class="inline-block w-5 h-5 ml-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
          </svg>
        ` : ''}
      </a>
    `
  }).join('')

  return `
    <div
      id="mobile-menu"
      class="mobile-menu hidden fixed inset-0 z-[60] bg-white dark:bg-gray-900 transform transition-transform duration-300"
      data-testid="mobile-menu"
      role="dialog"
      aria-modal="true"
      aria-label="Navigation menu"
    >
      <div class="flex flex-col h-full">
        <!-- Mobile Menu Header -->
        <div class="flex items-center justify-between px-4 py-4 border-b border-gray-200 dark:border-gray-700">
          <a href="#" class="flex items-center gap-2" data-testid="mobile-nav-logo">
            ${navLogoSvg}
            <span class="text-xl font-bold text-gray-900 dark:text-white">MirDB</span>
          </a>
          <button
            type="button"
            class="mobile-menu-close p-2 text-gray-700 dark:text-gray-200 hover:text-gray-900 dark:hover:text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            data-testid="mobile-menu-close"
            aria-label="Close navigation menu"
          >
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
            </svg>
          </button>
        </div>

        <!-- Mobile Menu Links -->
        <nav class="flex-1 px-2 py-4" aria-label="Mobile navigation">
          ${mobileLinks}
        </nav>
      </div>
    </div>
  `
}

/**
 * Renders the complete navigation bar
 * @returns {string} HTML string for the navigation component
 */
export function Navigation() {
  const desktopLinks = navLinks.map(link => NavLink(link)).join('')

  return `
    <nav
      class="navigation fixed top-0 left-0 right-0 z-50 bg-white/95 dark:bg-gray-900/95 backdrop-blur-sm border-b border-gray-200 dark:border-gray-700 shadow-sm"
      data-testid="navigation-bar"
      role="navigation"
      aria-label="Main navigation"
    >
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex items-center justify-between h-16">
          <!-- Logo and Brand -->
          <a
            href="#"
            class="flex items-center gap-2"
            data-testid="nav-logo"
            aria-label="MirDB Home"
          >
            ${navLogoSvg}
            <span class="text-xl font-bold text-gray-900 dark:text-white">MirDB</span>
          </a>

          <!-- Desktop Navigation Links -->
          <div class="hidden md:flex items-center gap-1" data-testid="desktop-nav-links">
            ${desktopLinks}
          </div>

          <!-- Mobile Menu Button -->
          <button
            type="button"
            class="mobile-menu-toggle md:hidden p-2 text-gray-700 dark:text-gray-200 hover:text-gray-900 dark:hover:text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            data-testid="mobile-menu-toggle"
            aria-expanded="false"
            aria-controls="mobile-menu"
            aria-label="Open navigation menu"
          >
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
            </svg>
          </button>
        </div>
      </div>
    </nav>

    <!-- Mobile Menu Overlay -->
    ${MobileMenu()}
  `
}

/**
 * Initializes navigation event handlers
 * Call this after the navigation is rendered to the DOM
 */
export function initNavigation() {
  // Mobile menu toggle
  const mobileMenuToggle = document.querySelector('[data-testid="mobile-menu-toggle"]')
  const mobileMenuClose = document.querySelector('[data-testid="mobile-menu-close"]')
  const mobileMenu = document.querySelector('[data-testid="mobile-menu"]')

  if (mobileMenuToggle && mobileMenu) {
    mobileMenuToggle.addEventListener('click', () => {
      mobileMenu.classList.remove('hidden')
      mobileMenuToggle.setAttribute('aria-expanded', 'true')
      // Focus the close button for accessibility
      const closeBtn = mobileMenu.querySelector('[data-testid="mobile-menu-close"]')
      if (closeBtn) {
        closeBtn.focus()
      }
    })
  }

  if (mobileMenuClose && mobileMenu) {
    mobileMenuClose.addEventListener('click', () => {
      mobileMenu.classList.add('hidden')
      if (mobileMenuToggle) {
        mobileMenuToggle.setAttribute('aria-expanded', 'false')
        mobileMenuToggle.focus()
      }
    })
  }

  // Close mobile menu when clicking a link
  const mobileNavLinks = document.querySelectorAll('.mobile-nav-link')
  mobileNavLinks.forEach(link => {
    link.addEventListener('click', (event) => {
      const href = link.getAttribute('href')
      if (href && href.startsWith('#') && mobileMenu) {
        mobileMenu.classList.add('hidden')
        if (mobileMenuToggle) {
          mobileMenuToggle.setAttribute('aria-expanded', 'false')
        }
      }
      // Handle smooth scrolling for internal links
      if (href && href.startsWith('#')) {
        handleNavLinkClick(event)
      }
    })
  })

  // Handle desktop nav link clicks for smooth scrolling
  const desktopNavLinks = document.querySelectorAll('.nav-link')
  desktopNavLinks.forEach(link => {
    link.addEventListener('click', (event) => {
      const href = link.getAttribute('href')
      if (href && href.startsWith('#')) {
        handleNavLinkClick(event)
      }
    })
  })

  // Handle keyboard navigation for mobile menu
  if (mobileMenu) {
    mobileMenu.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        mobileMenu.classList.add('hidden')
        if (mobileMenuToggle) {
          mobileMenuToggle.setAttribute('aria-expanded', 'false')
          mobileMenuToggle.focus()
        }
      }
    })
  }

  // Allow toggling mobile menu with Enter/Space on the toggle button
  if (mobileMenuToggle) {
    mobileMenuToggle.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault()
        mobileMenuToggle.click()
      }
    })
  }
}

export default Navigation
