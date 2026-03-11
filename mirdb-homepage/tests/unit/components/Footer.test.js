/**
 * Footer Component Unit Tests
 * Owner: Scenario 9 - Footer Component
 *
 * Test coverage:
 * - Renders footer with links, license, and copyright
 * - GitHub link is present and properly configured
 * - Documentation link is present and properly configured
 * - License type displayed (MIT)
 * - Copyright notice with current year displayed
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { Footer, getCurrentYear } from '../../../src/components/Footer.js'

describe('Footer', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = Footer()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Render Footer component', () => {
    it('should render the footer section', () => {
      const footer = container.querySelector('[data-testid="footer-section"]')
      expect(footer).toBeTruthy()
    })

    it('should display all main sections (Resources, About, Legal)', () => {
      const headings = container.querySelectorAll('h3')
      expect(headings.length).toBeGreaterThanOrEqual(3)

      const headingTexts = Array.from(headings).map(h => h.textContent.toLowerCase())
      expect(headingTexts.some(text => text.includes('resources'))).toBe(true)
      expect(headingTexts.some(text => text.includes('about'))).toBe(true)
      expect(headingTexts.some(text => text.includes('legal'))).toBe(true)
    })

    it('should have proper semantic structure', () => {
      const footer = container.querySelector('[data-testid="footer-section"]')
      expect(footer).toBeTruthy()

      // Should contain links
      const links = footer.querySelectorAll('a')
      expect(links.length).toBeGreaterThan(0)
    })

    it('should have accessible design with proper contrast classes', () => {
      const footer = container.querySelector('[data-testid="footer-section"]')
      expect(footer).toBeTruthy()

      // Should have background and text classes for light/dark mode
      const footerClasses = footer.className
      expect(footerClasses).toContain('bg-')
      expect(footerClasses).toContain('dark:')
    })
  })

  describe('Test Case 2: Check GitHub link in footer', () => {
    it('should display GitHub link', () => {
      const githubLink = container.querySelector('[data-testid="footer-github-link"]')
      expect(githubLink).toBeTruthy()
    })

    it('should have correct GitHub repository URL', () => {
      const githubLink = container.querySelector('[data-testid="footer-github-link"]')
      expect(githubLink).toBeTruthy()

      const href = githubLink.getAttribute('href')
      expect(href).toContain('github.com')
      expect(href).toContain('mirdb')
    })

    it('should open GitHub link in new tab', () => {
      const githubLink = container.querySelector('[data-testid="footer-github-link"]')
      expect(githubLink).toBeTruthy()
      expect(githubLink.getAttribute('target')).toBe('_blank')
      expect(githubLink.getAttribute('rel')).toContain('noopener')
    })

    it('should display GitHub text label', () => {
      const githubLink = container.querySelector('[data-testid="footer-github-link"]')
      expect(githubLink).toBeTruthy()

      const text = githubLink.textContent.toLowerCase()
      expect(text).toContain('github')
    })

    it('should include GitHub icon', () => {
      const githubLink = container.querySelector('[data-testid="footer-github-link"]')
      expect(githubLink).toBeTruthy()

      const svg = githubLink.querySelector('svg')
      expect(svg).toBeTruthy()
    })
  })

  describe('Test Case 3: Check documentation link in footer', () => {
    it('should display documentation link', () => {
      const docsLink = container.querySelector('[data-testid="footer-documentation-link"]')
      expect(docsLink).toBeTruthy()
    })

    it('should have correct documentation URL', () => {
      const docsLink = container.querySelector('[data-testid="footer-documentation-link"]')
      expect(docsLink).toBeTruthy()

      const href = docsLink.getAttribute('href')
      expect(href).toContain('github.com')
      expect(href).toContain('mirdb')
      expect(href).toContain('readme')
    })

    it('should open documentation link in new tab', () => {
      const docsLink = container.querySelector('[data-testid="footer-documentation-link"]')
      expect(docsLink).toBeTruthy()
      expect(docsLink.getAttribute('target')).toBe('_blank')
      expect(docsLink.getAttribute('rel')).toContain('noopener')
    })

    it('should display documentation text label', () => {
      const docsLink = container.querySelector('[data-testid="footer-documentation-link"]')
      expect(docsLink).toBeTruthy()

      const text = docsLink.textContent.toLowerCase()
      expect(text).toContain('documentation')
    })

    it('should include documentation icon', () => {
      const docsLink = container.querySelector('[data-testid="footer-documentation-link"]')
      expect(docsLink).toBeTruthy()

      const svg = docsLink.querySelector('svg')
      expect(svg).toBeTruthy()
    })
  })

  describe('Test Case 4: Check license information', () => {
    it('should display license information section', () => {
      const license = container.querySelector('[data-testid="footer-license"]')
      expect(license).toBeTruthy()
    })

    it('should show MIT license type', () => {
      const license = container.querySelector('[data-testid="footer-license"]')
      expect(license).toBeTruthy()

      const text = license.textContent.toLowerCase()
      expect(text).toContain('mit')
    })

    it('should include link to full license', () => {
      const licenseLink = container.querySelector('[data-testid="footer-license-link"]')
      expect(licenseLink).toBeTruthy()

      const href = licenseLink.getAttribute('href').toLowerCase()
      expect(href).toContain('license')
    })

    it('should display license label clearly', () => {
      const license = container.querySelector('[data-testid="footer-license"]')
      expect(license).toBeTruthy()

      const text = license.textContent.toLowerCase()
      expect(text).toContain('license')
    })
  })

  describe('Test Case 5: Check copyright notice', () => {
    it('should display copyright notice', () => {
      const copyright = container.querySelector('[data-testid="footer-copyright"]')
      expect(copyright).toBeTruthy()
    })

    it('should include current year in copyright', () => {
      const copyright = container.querySelector('[data-testid="footer-copyright"]')
      expect(copyright).toBeTruthy()

      const currentYear = new Date().getFullYear().toString()
      const text = copyright.textContent
      expect(text).toContain(currentYear)
    })

    it('should include copyright symbol', () => {
      const copyright = container.querySelector('[data-testid="footer-copyright"]')
      expect(copyright).toBeTruthy()

      const text = copyright.textContent
      expect(text).toContain('©')
    })

    it('should include project name in copyright', () => {
      const copyright = container.querySelector('[data-testid="footer-copyright"]')
      expect(copyright).toBeTruthy()

      const text = copyright.textContent
      expect(text).toContain('MirDB')
    })

    it('should include all rights reserved text', () => {
      const copyright = container.querySelector('[data-testid="footer-copyright"]')
      expect(copyright).toBeTruthy()

      const text = copyright.textContent.toLowerCase()
      expect(text).toContain('all rights reserved')
    })
  })

  describe('Additional Footer Features', () => {
    it('should include community link', () => {
      const communityLink = container.querySelector('[data-testid="footer-community-link"]')
      expect(communityLink).toBeTruthy()

      const href = communityLink.getAttribute('href')
      expect(href).toContain('discussions')
    })

    it('should have hover styles for links', () => {
      const links = container.querySelectorAll('[data-testid="footer-section"] a')
      expect(links.length).toBeGreaterThan(0)

      links.forEach(link => {
        const classes = link.className
        expect(classes).toContain('hover:')
      })
    })

    it('should support dark mode', () => {
      const footer = container.querySelector('[data-testid="footer-section"]')
      expect(footer).toBeTruthy()

      const classes = footer.className
      expect(classes).toContain('dark:')
    })

    it('should have responsive layout classes', () => {
      const footer = container.querySelector('[data-testid="footer-section"]')
      expect(footer).toBeTruthy()

      // Check for grid or flex with responsive breakpoints
      const footerHtml = footer.innerHTML
      expect(footerHtml).toMatch(/md:|lg:|sm:/)
    })
  })
})

describe('getCurrentYear', () => {
  it('should return the current year', () => {
    const year = getCurrentYear()
    const expectedYear = new Date().getFullYear()
    expect(year).toBe(expectedYear)
  })

  it('should return a number', () => {
    const year = getCurrentYear()
    expect(typeof year).toBe('number')
  })

  it('should return a valid 4-digit year', () => {
    const year = getCurrentYear()
    expect(year).toBeGreaterThanOrEqual(2020)
    expect(year).toBeLessThanOrEqual(2100)
  })
})
