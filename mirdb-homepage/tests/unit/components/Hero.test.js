/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test coverage:
 * - Renders product name 'MirDB' prominently
 * - Displays tagline about persistent key-value store with Memcached protocol
 * - Shows value proposition text
 * - CTA buttons are present and correctly configured
 * - Logo/branding element is visible and properly sized
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Hero, renderCTAButtons } from '../../../src/components/Hero.js'

describe('Hero', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = Hero()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render Hero component', () => {
    it('should display product name MirDB prominently', () => {
      const productName = container.querySelector('[data-testid="hero-product-name"]')
      expect(productName).toBeTruthy()
      expect(productName.textContent.trim()).toBe('MirDB')

      // Verify it's a prominent heading (h1)
      expect(productName.tagName.toLowerCase()).toBe('h1')
    })

    it('should display tagline describing persistent key-value store', () => {
      const tagline = container.querySelector('[data-testid="hero-tagline"]')
      expect(tagline).toBeTruthy()

      const taglineText = tagline.textContent.toLowerCase()
      expect(taglineText).toContain('persistent')
      expect(taglineText).toContain('key-value')
    })

    it('should mention Memcached protocol in tagline or description', () => {
      const heroSection = container.querySelector('[data-testid="hero-section"]')
      expect(heroSection).toBeTruthy()

      const heroText = heroSection.textContent.toLowerCase()
      expect(heroText).toContain('memcached')
      expect(heroText).toContain('protocol')
    })

    it('should have proper semantic structure', () => {
      const heroSection = container.querySelector('[data-testid="hero-section"]')
      expect(heroSection).toBeTruthy()

      // Should contain h1 for main heading
      const h1 = heroSection.querySelector('h1')
      expect(h1).toBeTruthy()

      // Should have proper ARIA attributes
      expect(heroSection.getAttribute('aria-label') || heroSection.getAttribute('aria-labelledby')).toBeTruthy()
    })
  })

  describe('Test Case 2: Verify value proposition text', () => {
    it('should display value proposition communicating persistence benefit', () => {
      const valueProposition = container.querySelector('[data-testid="hero-value-proposition"]')
      expect(valueProposition).toBeTruthy()

      const text = valueProposition.textContent.toLowerCase()
      expect(text).toContain('persist')
    })

    it('should communicate memcached compatibility in value proposition', () => {
      const valueProposition = container.querySelector('[data-testid="hero-value-proposition"]')
      expect(valueProposition).toBeTruthy()

      const text = valueProposition.textContent.toLowerCase()
      expect(text).toContain('memcached')
    })

    it('should clearly state the primary benefit', () => {
      const heroSection = container.querySelector('[data-testid="hero-section"]')
      const text = heroSection.textContent.toLowerCase()

      // Should communicate the main benefit: persistence + memcached compatibility
      expect(text).toContain('persistent')
      expect(text).toContain('memcached')
    })
  })

  describe('Test Case 5: Check logo/branding element', () => {
    it('should display product logo or branding element', () => {
      const logo = container.querySelector('[data-testid="hero-logo"]')
      expect(logo).toBeTruthy()
    })

    it('should have logo properly sized', () => {
      const logo = container.querySelector('[data-testid="hero-logo"]')
      expect(logo).toBeTruthy()

      // Check that sizing classes are applied to the SVG element
      const logoSvg = logo.querySelector('svg')
      expect(logoSvg).toBeTruthy()

      const logoClasses = logoSvg.getAttribute('class') || ''
      expect(logoClasses).toMatch(/w-\d+|h-\d+|size-\d+/)
    })

    it('should have accessible alt text for logo image', () => {
      const logoImg = container.querySelector('[data-testid="hero-logo"] img, [data-testid="hero-logo"] svg')
      expect(logoImg).toBeTruthy()

      if (logoImg.tagName.toLowerCase() === 'img') {
        expect(logoImg.getAttribute('alt')).toBeTruthy()
      } else if (logoImg.tagName.toLowerCase() === 'svg') {
        // SVG should have aria-label or title
        const hasAccessibleName = logoImg.getAttribute('aria-label') ||
                                   logoImg.querySelector('title') ||
                                   logoImg.getAttribute('role') === 'img'
        expect(hasAccessibleName).toBeTruthy()
      }
    })
  })

  describe('CTA Buttons', () => {
    it('should display Get Started CTA button', () => {
      const getStartedBtn = container.querySelector('[data-testid="hero-cta-get-started"]')
      expect(getStartedBtn).toBeTruthy()
      expect(getStartedBtn.textContent.toLowerCase()).toContain('get started')
    })

    it('should display GitHub Repo CTA button', () => {
      const githubBtn = container.querySelector('[data-testid="hero-cta-github"]')
      expect(githubBtn).toBeTruthy()

      const text = githubBtn.textContent.toLowerCase()
      expect(text).toMatch(/github|view.*source|repository/)
    })

    it('should have Get Started button linking to quickstart section', () => {
      const getStartedBtn = container.querySelector('[data-testid="hero-cta-get-started"]')
      expect(getStartedBtn).toBeTruthy()

      const href = getStartedBtn.getAttribute('href')
      expect(href).toBe('#quickstart')
    })

    it('should have GitHub button opening in new tab', () => {
      const githubBtn = container.querySelector('[data-testid="hero-cta-github"]')
      expect(githubBtn).toBeTruthy()
      expect(githubBtn.getAttribute('target')).toBe('_blank')
      expect(githubBtn.getAttribute('rel')).toContain('noopener')
    })

    it('should have prominent styling for CTA buttons', () => {
      const getStartedBtn = container.querySelector('[data-testid="hero-cta-get-started"]')
      const githubBtn = container.querySelector('[data-testid="hero-cta-github"]')

      // Both buttons should have proper styling classes
      expect(getStartedBtn.className).toMatch(/bg-|btn-/)
      expect(githubBtn.className).toMatch(/bg-|btn-|border/)
    })
  })
})

describe('renderCTAButtons', () => {
  it('should render CTA buttons container', () => {
    const ctaHtml = renderCTAButtons()
    const container = document.createElement('div')
    container.innerHTML = ctaHtml

    const ctaContainer = container.querySelector('[data-testid="hero-cta-container"]')
    expect(ctaContainer).toBeTruthy()
  })

  it('should include both Get Started and GitHub buttons', () => {
    const ctaHtml = renderCTAButtons()
    const container = document.createElement('div')
    container.innerHTML = ctaHtml

    const getStartedBtn = container.querySelector('[data-testid="hero-cta-get-started"]')
    const githubBtn = container.querySelector('[data-testid="hero-cta-github"]')

    expect(getStartedBtn).toBeTruthy()
    expect(githubBtn).toBeTruthy()
  })
})
