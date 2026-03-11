/**
 * Scroll Utilities Unit Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - smoothScrollTo function
 * - getScrollPosition function
 * - isElementInView function
 * - getNavbarHeight function
 * - Reduced motion preference handling
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  smoothScrollTo,
  getScrollPosition,
  isElementInView,
  getNavbarHeight,
  initSmoothScroll
} from '../../../src/utils/scroll.js'

describe('scroll utilities', () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <nav data-testid="navigation" style="height: 64px; position: fixed;"></nav>
      <section id="hero" style="height: 100vh;">Hero</section>
      <section id="features" style="height: 100vh;">Features</section>
      <section id="quickstart" style="height: 100vh;">Quickstart</section>
    `
    // Mock window.scrollTo
    window.scrollTo = vi.fn()
    // Reset scroll position
    Object.defineProperty(window, 'scrollY', { value: 0, writable: true })
  })

  afterEach(() => {
    document.body.innerHTML = ''
    vi.clearAllMocks()
  })

  describe('getNavbarHeight', () => {
    it('should return the height of the navigation element', () => {
      // Mock offsetHeight since happy-dom doesn't compute actual layout heights
      const navbar = document.querySelector('[data-testid="navigation"]')
      Object.defineProperty(navbar, 'offsetHeight', { value: 64, configurable: true })

      const height = getNavbarHeight()
      expect(height).toBe(64)
    })

    it('should return 0 if navigation element is not found', () => {
      document.body.innerHTML = '<div>No nav</div>'
      const height = getNavbarHeight()
      expect(height).toBe(0)
    })
  })

  describe('smoothScrollTo', () => {
    it('should scroll to element by ID', () => {
      const result = smoothScrollTo('features')
      expect(result).toBe(true)
      expect(window.scrollTo).toHaveBeenCalledWith(
        expect.objectContaining({
          top: expect.any(Number),
          behavior: 'smooth'
        })
      )
    })

    it('should handle IDs with or without hash prefix', () => {
      const result1 = smoothScrollTo('features')
      const result2 = smoothScrollTo('#features')
      expect(result1).toBe(true)
      expect(result2).toBe(true)
    })

    it('should return false for non-existent element', () => {
      const result = smoothScrollTo('non-existent-section')
      expect(result).toBe(false)
      expect(window.scrollTo).not.toHaveBeenCalled()
    })

    it('should use auto behavior when reduced motion is preferred', () => {
      // Mock matchMedia to return reduced motion preference
      const mockMatchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      }))
      window.matchMedia = mockMatchMedia

      smoothScrollTo('features')
      expect(window.scrollTo).toHaveBeenCalledWith(
        expect.objectContaining({
          behavior: 'auto'
        })
      )
    })

    it('should use smooth behavior when no motion preference', () => {
      // Mock matchMedia to return no reduced motion preference
      const mockMatchMedia = vi.fn().mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn()
      }))
      window.matchMedia = mockMatchMedia

      smoothScrollTo('features')
      expect(window.scrollTo).toHaveBeenCalledWith(
        expect.objectContaining({
          behavior: 'smooth'
        })
      )
    })
  })

  describe('getScrollPosition', () => {
    it('should return current scroll position', () => {
      Object.defineProperty(window, 'scrollY', { value: 100, writable: true })
      const position = getScrollPosition()
      expect(position).toBe(100)
    })

    it('should return 0 when not scrolled', () => {
      Object.defineProperty(window, 'scrollY', { value: 0, writable: true })
      const position = getScrollPosition()
      expect(position).toBe(0)
    })
  })

  describe('isElementInView', () => {
    it('should return true when element is in viewport', () => {
      // Mock getBoundingClientRect for the features section
      const featuresSection = document.getElementById('features')
      vi.spyOn(featuresSection, 'getBoundingClientRect').mockReturnValue({
        top: 100,
        bottom: 500,
        left: 0,
        right: 500,
        width: 500,
        height: 400
      })

      // Mock window inner height
      Object.defineProperty(window, 'innerHeight', { value: 768, writable: true })

      const result = isElementInView('features')
      expect(result).toBe(true)
    })

    it('should return false when element is above viewport', () => {
      const featuresSection = document.getElementById('features')
      vi.spyOn(featuresSection, 'getBoundingClientRect').mockReturnValue({
        top: -500,
        bottom: -100,
        left: 0,
        right: 500,
        width: 500,
        height: 400
      })

      Object.defineProperty(window, 'innerHeight', { value: 768, writable: true })

      const result = isElementInView('features')
      expect(result).toBe(false)
    })

    it('should return false when element is below viewport', () => {
      const featuresSection = document.getElementById('features')
      vi.spyOn(featuresSection, 'getBoundingClientRect').mockReturnValue({
        top: 1000,
        bottom: 1400,
        left: 0,
        right: 500,
        width: 500,
        height: 400
      })

      Object.defineProperty(window, 'innerHeight', { value: 768, writable: true })

      const result = isElementInView('features')
      expect(result).toBe(false)
    })

    it('should return false for non-existent element', () => {
      const result = isElementInView('non-existent')
      expect(result).toBe(false)
    })

    it('should handle hash prefix in elementId', () => {
      const featuresSection = document.getElementById('features')
      vi.spyOn(featuresSection, 'getBoundingClientRect').mockReturnValue({
        top: 100,
        bottom: 500,
        left: 0,
        right: 500,
        width: 500,
        height: 400
      })

      Object.defineProperty(window, 'innerHeight', { value: 768, writable: true })

      const result = isElementInView('#features')
      expect(result).toBe(true)
    })
  })

  describe('initSmoothScroll', () => {
    it('should attach click handler to document', () => {
      const addEventListenerSpy = vi.spyOn(document, 'addEventListener')
      initSmoothScroll()
      expect(addEventListenerSpy).toHaveBeenCalledWith('click', expect.any(Function))
    })

    it('should handle anchor link clicks', () => {
      initSmoothScroll()

      // Create and append an anchor link
      const anchor = document.createElement('a')
      anchor.href = '#features'
      anchor.textContent = 'Features'
      document.body.appendChild(anchor)

      // Trigger click
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true
      })

      anchor.dispatchEvent(clickEvent)

      expect(window.scrollTo).toHaveBeenCalled()
    })
  })
})
