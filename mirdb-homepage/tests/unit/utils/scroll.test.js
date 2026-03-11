/**
 * Scroll Utilities Unit Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - smoothScrollTo function
 * - getScrollPosition function
 * - isElementInView function
 * - getHeaderOffset function
 * - prefersReducedMotion function
 * - handleNavLinkClick function
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  smoothScrollTo,
  getScrollPosition,
  isElementInView,
  getHeaderOffset,
  prefersReducedMotion,
  handleNavLinkClick
} from '../../../src/utils/scroll.js'

describe('Scroll Utilities', () => {
  beforeEach(() => {
    document.body.innerHTML = ''
    // Reset scroll position
    window.scrollTo(0, 0)
  })

  afterEach(() => {
    document.body.innerHTML = ''
    vi.restoreAllMocks()
  })

  describe('getScrollPosition', () => {
    it('should return current scroll position', () => {
      const position = getScrollPosition()
      expect(typeof position).toBe('number')
      expect(position).toBeGreaterThanOrEqual(0)
    })

    it('should return 0 when page is at top', () => {
      window.scrollTo(0, 0)
      const position = getScrollPosition()
      expect(position).toBe(0)
    })
  })

  describe('getHeaderOffset', () => {
    it('should return 0 when no navigation bar exists', () => {
      const offset = getHeaderOffset()
      expect(offset).toBe(0)
    })

    it('should return navigation bar height when it exists', () => {
      // Create a mock navigation bar
      const nav = document.createElement('nav')
      nav.setAttribute('data-testid', 'navigation-bar')
      // Mock offsetHeight (not directly settable, so we check the function runs)
      Object.defineProperty(nav, 'offsetHeight', { value: 64, writable: false })
      document.body.appendChild(nav)

      const offset = getHeaderOffset()
      expect(offset).toBe(64)
    })
  })

  describe('prefersReducedMotion', () => {
    it('should return a boolean', () => {
      const result = prefersReducedMotion()
      expect(typeof result).toBe('boolean')
    })

    it('should respect reduced motion preference', () => {
      // Mock matchMedia to return true for reduced motion
      const originalMatchMedia = window.matchMedia
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const result = prefersReducedMotion()
      expect(result).toBe(true)

      window.matchMedia = originalMatchMedia
    })

    it('should return false when no reduced motion preference', () => {
      // Mock matchMedia to return false for reduced motion
      const originalMatchMedia = window.matchMedia
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const result = prefersReducedMotion()
      expect(result).toBe(false)

      window.matchMedia = originalMatchMedia
    })
  })

  describe('smoothScrollTo', () => {
    it('should not throw when element does not exist', () => {
      expect(() => smoothScrollTo('nonexistent')).not.toThrow()
    })

    it('should call window.scrollTo when element exists', () => {
      // Create target element
      const target = document.createElement('div')
      target.id = 'target-section'
      document.body.appendChild(target)

      // Mock scrollTo
      const scrollToSpy = vi.spyOn(window, 'scrollTo').mockImplementation(() => {})

      smoothScrollTo('target-section')

      expect(scrollToSpy).toHaveBeenCalled()
    })

    it('should use smooth behavior when no reduced motion preference', () => {
      // Create target element
      const target = document.createElement('div')
      target.id = 'target-section'
      document.body.appendChild(target)

      // Mock no reduced motion preference
      const originalMatchMedia = window.matchMedia
      window.matchMedia = vi.fn().mockImplementation(() => ({
        matches: false,
        media: '',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const scrollToSpy = vi.spyOn(window, 'scrollTo').mockImplementation(() => {})

      smoothScrollTo('target-section')

      expect(scrollToSpy).toHaveBeenCalledWith(
        expect.objectContaining({ behavior: 'smooth' })
      )

      window.matchMedia = originalMatchMedia
    })

    it('should use auto behavior when reduced motion is preferred', () => {
      // Create target element
      const target = document.createElement('div')
      target.id = 'target-section'
      document.body.appendChild(target)

      // Mock reduced motion preference
      const originalMatchMedia = window.matchMedia
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const scrollToSpy = vi.spyOn(window, 'scrollTo').mockImplementation(() => {})

      smoothScrollTo('target-section')

      expect(scrollToSpy).toHaveBeenCalledWith(
        expect.objectContaining({ behavior: 'auto' })
      )

      window.matchMedia = originalMatchMedia
    })
  })

  describe('isElementInView', () => {
    it('should return false when element does not exist', () => {
      const result = isElementInView('nonexistent')
      expect(result).toBe(false)
    })

    it('should return true when element is in viewport', () => {
      // Create an element that would be visible
      const element = document.createElement('div')
      element.id = 'visible-element'
      document.body.appendChild(element)

      // Mock getBoundingClientRect
      element.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 100,
        bottom: 200,
        left: 0,
        right: 100,
        width: 100,
        height: 100
      })

      // Mock window dimensions
      Object.defineProperty(window, 'innerHeight', { value: 800, writable: true })

      const result = isElementInView('visible-element')
      expect(result).toBe(true)
    })

    it('should return false when element is above viewport', () => {
      const element = document.createElement('div')
      element.id = 'above-element'
      document.body.appendChild(element)

      // Element is completely above viewport
      element.getBoundingClientRect = vi.fn().mockReturnValue({
        top: -200,
        bottom: -100,
        left: 0,
        right: 100,
        width: 100,
        height: 100
      })

      Object.defineProperty(window, 'innerHeight', { value: 800, writable: true })

      const result = isElementInView('above-element')
      expect(result).toBe(false)
    })

    it('should return false when element is below viewport', () => {
      const element = document.createElement('div')
      element.id = 'below-element'
      document.body.appendChild(element)

      // Element is completely below viewport
      element.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 1000,
        bottom: 1100,
        left: 0,
        right: 100,
        width: 100,
        height: 100
      })

      Object.defineProperty(window, 'innerHeight', { value: 800, writable: true })

      const result = isElementInView('below-element')
      expect(result).toBe(false)
    })
  })

  describe('handleNavLinkClick', () => {
    it('should prevent default for internal anchor links', () => {
      const link = document.createElement('a')
      link.href = '#section'
      document.body.appendChild(link)

      // Create target section
      const section = document.createElement('div')
      section.id = 'section'
      document.body.appendChild(section)

      const event = {
        currentTarget: link,
        preventDefault: vi.fn()
      }

      // Mock scrollTo
      vi.spyOn(window, 'scrollTo').mockImplementation(() => {})
      // Mock history.pushState
      vi.spyOn(history, 'pushState').mockImplementation(() => {})

      handleNavLinkClick(event)

      expect(event.preventDefault).toHaveBeenCalled()
    })

    it('should not prevent default for external links', () => {
      const link = document.createElement('a')
      link.href = 'https://example.com'
      document.body.appendChild(link)

      const event = {
        currentTarget: link,
        preventDefault: vi.fn()
      }

      handleNavLinkClick(event)

      expect(event.preventDefault).not.toHaveBeenCalled()
    })

    it('should update URL hash after scrolling', () => {
      const link = document.createElement('a')
      link.href = '#features'
      document.body.appendChild(link)

      // Create target section
      const section = document.createElement('div')
      section.id = 'features'
      document.body.appendChild(section)

      const event = {
        currentTarget: link,
        preventDefault: vi.fn()
      }

      vi.spyOn(window, 'scrollTo').mockImplementation(() => {})
      const pushStateSpy = vi.spyOn(history, 'pushState').mockImplementation(() => {})

      handleNavLinkClick(event)

      expect(pushStateSpy).toHaveBeenCalledWith(null, '', '#features')
    })
  })
})
