/**
 * Navigation Component
 * Owner: Scenario 8 - Navigation Component
 *
 * Renders a sticky navigation bar with smooth scrolling links,
 * mobile hamburger menu, and accessibility support.
 */

import { initSmoothScroll } from '../utils/scroll.js'

/**
 * MirDB Logo SVG for navigation bar
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
 * Hamburger menu icon SVG
 */
const hamburgerIcon = `
  <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
  </svg>
`

/**
 * Close icon SVG
 */
const closeIcon = `
  <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
  </svg>
`

/**
 * Navigation link data
 */
const navLinks = [
  { href: '#features', label: 'Features', isExternal: false },
  { href: '#demo', label: 'Demo', isExternal: false },
  { href: '#quickstart', label: 'Quick Start', isExternal: false },
  { href: '#protocol', label: 'Protocol', isExternal: false },
  { href: 'https://github.com/transybao93/mirdb', label: 'GitHub', isExternal: true }
]

/**
 * Renders a single navigation link
 * @param {Object} link - Link configuration object
 * @param {string} link.href - Link URL
 * @param {string} link.label - Link text
 * @param {boolean} link.isExternal - Whether link opens in new tab
 * @param {string} [extraClasses=''] - Additional CSS classes
 * @returns {string} HTML string for the nav link
 */
export function NavLink({ href, label, isExternal }, extraClasses = '') {
  const externalAttrs = isExternal
    ? 'target="_blank" rel="noopener noreferrer"'
    : ''
  const linkClasses = `nav-link text-gray-700 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 px-3 py-2 text-sm font-medium transition-colors duration-200 ${extraClasses}`

  return `
    <a
      href="${href}"
      class="${linkClasses}"
      data-testid="nav-link-${label.toLowerCase().replace(/\s+/g, '-')}"
      ${externalAttrs}
    >
      ${label}
      ${isExternal ? `
        <svg class="inline-block w-3 h-3 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
        </svg>
      ` : ''}
    </a>
  `
}

/**
 * Renders the mobile menu overlay
 * @returns {string} HTML string for mobile menu
 */
export function MobileMenu() {
  const mobileLinks = navLinks.map(link =>
    NavLink(link, 'block py-3 text-lg')
  ).join('')

  return `
    <div
      id="mobile-menu"
      class="mobile-menu fixed inset-0 z-[60] bg-white dark:bg-gray-900 transform translate-x-full transition-transform duration-300 ease-in-out md:hidden"
      data-testid="mobile-menu"
      role="dialog"
      aria-modal="true"
      aria-label="Navigation menu"
      aria-hidden="true"
    >
      <div class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
        <span class="text-lg font-semibold text-gray-900 dark:text-white">Menu</span>
        <button
          type="button"
          class="mobile-menu-close p-2 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
          data-testid="mobile-menu-close"
          aria-label="Close menu"
        >
          ${closeIcon}
        </button>
      </div>
      <nav class="px-4 py-6" aria-label="Mobile navigation">
        ${mobileLinks}
      </nav>
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
      class="navigation fixed top-0 left-0 right-0 z-50 bg-white dark:bg-gray-900 shadow-md transition-shadow duration-300"
      data-testid="navigation"
      aria-label="Main navigation"
    >
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex items-center justify-between h-16">
          <!-- Logo and Brand -->
          <div class="flex items-center">
            <a
              href="#"
              class="flex items-center space-x-2"
              data-testid="nav-logo"
              aria-label="MirDB Home"
            >
              ${navLogoSvg}
              <span class="text-xl font-bold text-gray-900 dark:text-white">MirDB</span>
            </a>
          </div>

          <!-- Desktop Navigation Links -->
          <div class="hidden md:flex md:items-center md:space-x-1" data-testid="nav-desktop-links">
            ${desktopLinks}
          </div>

          <!-- Mobile Menu Button -->
          <div class="flex md:hidden">
            <button
              type="button"
              class="mobile-menu-toggle p-2 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
              data-testid="mobile-menu-toggle"
              aria-label="Open menu"
              aria-expanded="false"
              aria-controls="mobile-menu"
            >
              ${hamburgerIcon}
            </button>
          </div>
        </div>
      </div>
    </nav>
    ${MobileMenu()}
    <div class="h-16" aria-hidden="true"></div>
  `
}

/**
 * Initialize navigation functionality
 * Sets up mobile menu toggle, smooth scrolling, and scroll-based styling
 */
export function initNavigation() {
  // Initialize smooth scrolling for anchor links
  initSmoothScroll()

  // Mobile menu toggle functionality
  const setupMobileMenu = () => {
    const toggleBtn = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const closeBtn = document.querySelector('[data-testid="mobile-menu-close"]')
    const mobileMenu = document.getElementById('mobile-menu')

    if (!toggleBtn || !mobileMenu) return

    const openMenu = () => {
      mobileMenu.classList.remove('translate-x-full')
      mobileMenu.classList.add('translate-x-0')
      mobileMenu.setAttribute('aria-hidden', 'false')
      toggleBtn.setAttribute('aria-expanded', 'true')
      document.body.style.overflow = 'hidden'

      // Focus the close button when menu opens
      if (closeBtn) {
        closeBtn.focus()
      }
    }

    const closeMenu = () => {
      mobileMenu.classList.remove('translate-x-0')
      mobileMenu.classList.add('translate-x-full')
      mobileMenu.setAttribute('aria-hidden', 'true')
      toggleBtn.setAttribute('aria-expanded', 'false')
      document.body.style.overflow = ''

      // Return focus to toggle button
      toggleBtn.focus()
    }

    toggleBtn.addEventListener('click', openMenu)

    if (closeBtn) {
      closeBtn.addEventListener('click', closeMenu)
    }

    // Close menu when clicking a nav link
    const navLinks = mobileMenu.querySelectorAll('a')
    navLinks.forEach(link => {
      link.addEventListener('click', () => {
        closeMenu()
      })
    })

    // Close menu on escape key
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && mobileMenu.getAttribute('aria-hidden') === 'false') {
        closeMenu()
      }
    })

    // Close menu when clicking outside
    mobileMenu.addEventListener('click', (e) => {
      if (e.target === mobileMenu) {
        closeMenu()
      }
    })
  }

  // Add shadow on scroll
  const setupScrollShadow = () => {
    const nav = document.querySelector('[data-testid="navigation"]')
    if (!nav) return

    window.addEventListener('scroll', () => {
      if (window.scrollY > 10) {
        nav.classList.add('shadow-lg')
      } else {
        nav.classList.remove('shadow-lg')
      }
    })
  }

  setupMobileMenu()
  setupScrollShadow()
}

export default Navigation
