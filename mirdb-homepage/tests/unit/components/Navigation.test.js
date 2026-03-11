/**
 * Navigation Component Unit Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - Sticky navigation bar with logo and menu links displayed
 * - Desktop navigation links presence
 * - Mobile menu button visibility
 * - Proper ARIA attributes for accessibility
 * - GitHub link opens in new tab
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
      const navBar = container.querySelector('[data-testid="navigation-bar"]')
      expect(navBar).toBeTruthy()

      // Should have fixed positioning class
      const navClasses = navBar.className
      expect(navClasses).toContain('fixed')
    })

    it('should display logo', () => {
      const logo = container.querySelector('[data-testid="nav-logo"]')
      expect(logo).toBeTruthy()

      // Should contain SVG logo
      const svg = logo.querySelector('svg')
      expect(svg).toBeTruthy()

      // Should have brand name text
      const brandText = logo.textContent
      expect(brandText).toContain('MirDB')
    })

    it('should display desktop navigation links', () => {
      const desktopLinks = container.querySelector('[data-testid="desktop-nav-links"]')
      expect(desktopLinks).toBeTruthy()

      // Should have Features link
      const featuresLink = container.querySelector('[data-testid="nav-link-features"]')
      expect(featuresLink).toBeTruthy()
      expect(featuresLink.textContent.trim()).toContain('Features')

      // Should have Documentation link
      const docsLink = container.querySelector('[data-testid="nav-link-documentation"]')
      expect(docsLink).toBeTruthy()
      expect(docsLink.textContent.trim()).toContain('Documentation')

      // Should have GitHub link
      const githubLink = container.querySelector('[data-testid="nav-link-github"]')
      expect(githubLink).toBeTruthy()
      expect(githubLink.textContent.trim()).toContain('GitHub')
    })

    it('should have mobile menu toggle button', () => {
      const mobileToggle = container.querySelector('[data-testid="mobile-menu-toggle"]')
      expect(mobileToggle).toBeTruthy()

      // Should have hamburger icon (SVG with three lines)
      const icon = mobileToggle.querySelector('svg')
      expect(icon).toBeTruthy()
    })

    it('should have proper navigation role and aria-label', () => {
      const navBar = container.querySelector('[data-testid="navigation-bar"]')
      expect(navBar).toBeTruthy()

      expect(navBar.getAttribute('role')).toBe('navigation')
      expect(navBar.getAttribute('aria-label')).toBeTruthy()
    })
  })

  describe('Test Case 2: Features link', () => {
    it('should have Features link with correct href', () => {
      const featuresLink = container.querySelector('[data-testid="nav-link-features"]')
      expect(featuresLink).toBeTruthy()
      expect(featuresLink.getAttribute('href')).toBe('#features')
    })
  })

  describe('Test Case 3: Documentation link', () => {
    it('should have Documentation link navigating to documentation section', () => {
      const docsLink = container.querySelector('[data-testid="nav-link-documentation"]')
      expect(docsLink).toBeTruthy()

      const href = docsLink.getAttribute('href')
      // Should link to quickstart or an internal section
      expect(href).toMatch(/^#|^https?:\/\//)
    })
  })

  describe('Test Case 4: GitHub Repository link', () => {
    it('should have GitHub link that opens in new tab', () => {
      const githubLink = container.querySelector('[data-testid="nav-link-github"]')
      expect(githubLink).toBeTruthy()

      expect(githubLink.getAttribute('target')).toBe('_blank')
      expect(githubLink.getAttribute('rel')).toContain('noopener')
    })

    it('should have GitHub link pointing to repository', () => {
      const githubLink = container.querySelector('[data-testid="nav-link-github"]')
      expect(githubLink).toBeTruthy()

      const href = githubLink.getAttribute('href')
      expect(href).toContain('github.com')
    })
  })

  describe('Mobile Menu', () => {
    it('should render mobile menu (hidden by default)', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      expect(mobileMenu).toBeTruthy()

      // Should be hidden by default
      expect(mobileMenu.classList.contains('hidden')).toBe(true)
    })

    it('should have mobile menu close button', () => {
      const closeBtn = container.querySelector('[data-testid="mobile-menu-close"]')
      expect(closeBtn).toBeTruthy()
      expect(closeBtn.getAttribute('aria-label')).toBeTruthy()
    })

    it('should have mobile navigation links', () => {
      // Check for mobile versions of navigation links
      const mobileFeaturesLink = container.querySelector('[data-testid="mobile-nav-link-features"]')
      const mobileDocsLink = container.querySelector('[data-testid="mobile-nav-link-documentation"]')
      const mobileGithubLink = container.querySelector('[data-testid="mobile-nav-link-github"]')

      expect(mobileFeaturesLink).toBeTruthy()
      expect(mobileDocsLink).toBeTruthy()
      expect(mobileGithubLink).toBeTruthy()
    })

    it('should have proper ARIA attributes for mobile menu dialog', () => {
      const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
      expect(mobileMenu).toBeTruthy()

      expect(mobileMenu.getAttribute('role')).toBe('dialog')
      expect(mobileMenu.getAttribute('aria-modal')).toBe('true')
      expect(mobileMenu.getAttribute('aria-label')).toBeTruthy()
    })
  })

  describe('Mobile Menu Toggle Accessibility', () => {
    it('should have aria-expanded attribute on toggle button', () => {
      const mobileToggle = container.querySelector('[data-testid="mobile-menu-toggle"]')
      expect(mobileToggle).toBeTruthy()

      expect(mobileToggle.getAttribute('aria-expanded')).toBe('false')
      expect(mobileToggle.getAttribute('aria-controls')).toBe('mobile-menu')
      expect(mobileToggle.getAttribute('aria-label')).toBeTruthy()
    })
  })
})

describe('NavLink', () => {
  it('should render internal link correctly', () => {
    const html = NavLink({ href: '#features', label: 'Features', isExternal: false })
    const container = document.createElement('div')
    container.innerHTML = html

    const link = container.querySelector('a')
    expect(link).toBeTruthy()
    expect(link.getAttribute('href')).toBe('#features')
    expect(link.textContent.trim()).toContain('Features')
    expect(link.getAttribute('target')).toBeNull()
  })

  it('should render external link with target _blank', () => {
    const html = NavLink({ href: 'https://github.com/test', label: 'GitHub', isExternal: true })
    const container = document.createElement('div')
    container.innerHTML = html

    const link = container.querySelector('a')
    expect(link).toBeTruthy()
    expect(link.getAttribute('target')).toBe('_blank')
    expect(link.getAttribute('rel')).toContain('noopener')
  })
})

describe('MobileMenu', () => {
  it('should render mobile menu with all navigation links', () => {
    const html = MobileMenu()
    const container = document.createElement('div')
    container.innerHTML = html

    const mobileMenu = container.querySelector('[data-testid="mobile-menu"]')
    expect(mobileMenu).toBeTruthy()

    // Check for mobile navigation links
    const links = mobileMenu.querySelectorAll('.mobile-nav-link')
    expect(links.length).toBeGreaterThan(0)
  })

  it('should have close button', () => {
    const html = MobileMenu()
    const container = document.createElement('div')
    container.innerHTML = html

    const closeBtn = container.querySelector('[data-testid="mobile-menu-close"]')
    expect(closeBtn).toBeTruthy()
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

  it('should set up mobile menu toggle functionality', () => {
    initNavigation()

    const mobileToggle = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = document.querySelector('[data-testid="mobile-menu"]')

    // Initially hidden
    expect(mobileMenu.classList.contains('hidden')).toBe(true)

    // Click toggle to open
    mobileToggle.click()
    expect(mobileMenu.classList.contains('hidden')).toBe(false)
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('true')
  })

  it('should close mobile menu when close button is clicked', () => {
    initNavigation()

    const mobileToggle = document.querySelector('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = document.querySelector('[data-testid="mobile-menu"]')
    const closeBtn = document.querySelector('[data-testid="mobile-menu-close"]')

    // Open menu first
    mobileToggle.click()
    expect(mobileMenu.classList.contains('hidden')).toBe(false)

    // Close menu
    closeBtn.click()
    expect(mobileMenu.classList.contains('hidden')).toBe(true)
    expect(mobileToggle.getAttribute('aria-expanded')).toBe('false')
  })
})
