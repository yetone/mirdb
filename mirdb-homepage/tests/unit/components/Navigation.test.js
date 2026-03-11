/**
 * Navigation Component Unit Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - Sticky navigation bar with logo and menu links
 * - Desktop and mobile navigation links
 * - Mobile hamburger menu functionality
 * - Accessibility attributes (ARIA)
 * - External link configuration
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Navigation, NavLink, MobileMenu, initNavigation } from '../../../src/components/Navigation.js'

describe('Navigation', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = Navigation()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render Navigation component', () => {
    it('should render sticky navigation bar', () => {
      const nav = container.querySelector('[data-testid="navigation"]')
      expect(nav).toBeTruthy()
      expect(nav.classList.contains('fixed')).toBe(true)
    })

    it('should display logo', () => {
      const logo = container.querySelector('[data-testid="nav-logo"]')
      expect(logo).toBeTruthy()
      expect(logo.textContent).toContain('MirDB')
    })

    it('should have proper navigation role', () => {
      const nav = container.querySelector('[data-testid="navigation"]')
      expect(nav.tagName.toLowerCase()).toBe('nav')
      expect(nav.getAttribute('aria-label')).toBe('Main navigation')
    })

    it('should render desktop navigation links', () => {
      const desktopLinks = container.querySelector('[data-testid="nav-desktop-links"]')
      expect(desktopLinks).toBeTruthy()

      const links = desktopLinks.querySelectorAll('a')
      expect(links.length).toBeGreaterThan(0)
    })

    it('should have mobile menu toggle button', () => {
      const toggleBtn = container.querySelector('[data-testid="mobile-menu-toggle"]')
      expect(toggleBtn).toBeTruthy()
      expect(toggleBtn.getAttribute('aria-label')).toBe('Open menu')
      expect(toggleBtn.getAttribute('aria-expanded')).toBe('false')
    })
  })

  describe('Test Case 2: Features link', () => {
    it('should have Features link that navigates to features section', () => {
      const featuresLink = container.querySelector('[data-testid="nav-link-features"]')
      expect(featuresLink).toBeTruthy()
      expect(featuresLink.getAttribute('href')).toBe('#features')
      expect(featuresLink.textContent).toContain('Features')
    })
  })

  describe('Test Case 3: Documentation/Demo links', () => {
    it('should have navigation links for key sections', () => {
      const demoLink = container.querySelector('[data-testid="nav-link-demo"]')
      const quickStartLink = container.querySelector('[data-testid="nav-link-quick-start"]')
      const protocolLink = container.querySelector('[data-testid="nav-link-protocol"]')

      expect(demoLink).toBeTruthy()
      expect(quickStartLink).toBeTruthy()
      expect(protocolLink).toBeTruthy()
    })
  })

  describe('Test Case 4: GitHub Repository link', () => {
    it('should have GitHub link that opens in new tab', () => {
      const githubLink = container.querySelector('[data-testid="nav-link-github"]')
      expect(githubLink).toBeTruthy()
      expect(githubLink.getAttribute('href')).toContain('github.com')
      expect(githubLink.getAttribute('target')).toBe('_blank')
      expect(githubLink.getAttribute('rel')).toContain('noopener')
    })

    it('should have external link indicator for GitHub', () => {
      const githubLink = container.querySelector('[data-testid="nav-link-github"]')
      const externalIcon = githubLink.querySelector('svg')
      expect(externalIcon).toBeTruthy()
    })
  })

  describe('Test Case 7: Mobile menu', () => {
    it('should have mobile menu overlay', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      expect(mobileMenu).toBeTruthy()
    })

    it('should have mobile menu hidden by default', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      expect(mobileMenu.getAttribute('aria-hidden')).toBe('true')
      expect(mobileMenu.classList.contains('translate-x-full')).toBe(true)
    })

    it('should have close button in mobile menu', () => {
      const closeBtn = container.querySelector('[data-testid="mobile-menu-close"]')
      expect(closeBtn).toBeTruthy()
      expect(closeBtn.getAttribute('aria-label')).toBe('Close menu')
    })

    it('should have navigation links in mobile menu', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      const links = mobileMenu.querySelectorAll('a')
      expect(links.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 8: Mobile menu accessibility', () => {
    it('should have proper ARIA attributes on mobile menu', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      expect(mobileMenu.getAttribute('role')).toBe('dialog')
      expect(mobileMenu.getAttribute('aria-modal')).toBe('true')
      expect(mobileMenu.getAttribute('aria-label')).toBe('Navigation menu')
    })

    it('should have toggle button with aria-controls', () => {
      const toggleBtn = container.querySelector('[data-testid="mobile-menu-toggle"]')
      expect(toggleBtn.getAttribute('aria-controls')).toBe('mobile-menu')
    })

    it('should have accessible navigation label in mobile menu', () => {
      const mobileMenuNav = container.querySelector('[data-testid="mobile-menu"] nav')
      expect(mobileMenuNav).toBeTruthy()
      expect(mobileMenuNav.getAttribute('aria-label')).toBe('Mobile navigation')
    })
  })

  describe('Navigation structure', () => {
    it('should have spacer div for fixed nav offset', () => {
      const spacer = container.querySelector('[aria-hidden="true"].h-16')
      expect(spacer).toBeTruthy()
    })

    it('should have shadow styling class', () => {
      const nav = container.querySelector('[data-testid="navigation"]')
      expect(nav.classList.contains('shadow-md')).toBe(true)
    })
  })
})

describe('NavLink', () => {
  it('should render internal link correctly', () => {
    const html = NavLink({ href: '#features', label: 'Features', isExternal: false })
    const container = document.createElement('div')
    container.innerHTML = html

    const link = container.querySelector('a')
    expect(link.getAttribute('href')).toBe('#features')
    expect(link.textContent).toContain('Features')
    expect(link.getAttribute('target')).toBeNull()
  })

  it('should render external link with target blank', () => {
    const html = NavLink({ href: 'https://github.com', label: 'GitHub', isExternal: true })
    const container = document.createElement('div')
    container.innerHTML = html

    const link = container.querySelector('a')
    expect(link.getAttribute('target')).toBe('_blank')
    expect(link.getAttribute('rel')).toContain('noopener')
  })

  it('should include extra classes when provided', () => {
    const html = NavLink({ href: '#test', label: 'Test', isExternal: false }, 'extra-class')
    const container = document.createElement('div')
    container.innerHTML = html

    const link = container.querySelector('a')
    expect(link.classList.contains('extra-class')).toBe(true)
  })
})

describe('MobileMenu', () => {
  it('should render mobile menu with all navigation links', () => {
    const html = MobileMenu()
    const container = document.createElement('div')
    container.innerHTML = html

    const menu = container.querySelector('[data-testid="mobile-menu"]')
    expect(menu).toBeTruthy()

    const links = menu.querySelectorAll('a')
    expect(links.length).toBe(5) // Features, Demo, Quick Start, Protocol, GitHub
  })

  it('should have proper role and aria attributes', () => {
    const html = MobileMenu()
    const container = document.createElement('div')
    container.innerHTML = html

    const menu = container.querySelector('[data-testid="mobile-menu"]')
    expect(menu.getAttribute('role')).toBe('dialog')
    expect(menu.getAttribute('aria-modal')).toBe('true')
  })
})

describe('initNavigation', () => {
  beforeEach(() => {
    document.body.innerHTML = Navigation()
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  it('should initialize without errors', () => {
    expect(() => initNavigation()).not.toThrow()
  })

  it('should set up mobile menu toggle', () => {
    initNavigation()

    const toggleBtn = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = document.getElementById('mobile-menu')

    // Simulate click
    toggleBtn.click()

    // Menu should open
    expect(mobileMenu.classList.contains('translate-x-0')).toBe(true)
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('false')
  })

  it('should close mobile menu with close button', () => {
    initNavigation()

    const toggleBtn = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const closeBtn = document.querySelector('[data-testid="mobile-menu-close"]')
    const mobileMenu = document.getElementById('mobile-menu')

    // Open menu
    toggleBtn.click()
    expect(mobileMenu.classList.contains('translate-x-0')).toBe(true)

    // Close menu
    closeBtn.click()
    expect(mobileMenu.classList.contains('translate-x-full')).toBe(true)
    expect(mobileMenu.getAttribute('aria-hidden')).toBe('true')
  })

  it('should close mobile menu on escape key', () => {
    initNavigation()

    const toggleBtn = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = document.getElementById('mobile-menu')

    // Open menu
    toggleBtn.click()
    expect(mobileMenu.classList.contains('translate-x-0')).toBe(true)

    // Press escape
    const escapeEvent = new KeyboardEvent('keydown', { key: 'Escape' })
    document.dispatchEvent(escapeEvent)

    expect(mobileMenu.classList.contains('translate-x-full')).toBe(true)
  })

  it('should close mobile menu when clicking nav link', () => {
    initNavigation()

    const toggleBtn = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = document.getElementById('mobile-menu')
    const navLink = mobileMenu.querySelector('a')

    // Open menu
    toggleBtn.click()
    expect(mobileMenu.classList.contains('translate-x-0')).toBe(true)

    // Click nav link
    navLink.click()

    expect(mobileMenu.classList.contains('translate-x-full')).toBe(true)
  })
})
