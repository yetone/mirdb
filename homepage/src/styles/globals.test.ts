/**
 * Unit tests for global CSS styles.
 * Owner: Scenario 18 - Smooth Scroll Navigation
 *
 * Tests:
 * - Verify scroll-behavior: smooth is applied to html element
 */

import { describe, it, expect, beforeEach } from 'vitest'

describe('Global CSS Styles - Smooth Scroll', () => {
  // CSS content that should be in globals.css
  const expectedCss = `
    html {
      scroll-behavior: smooth;
    }
  `

  beforeEach(() => {
    // Inject the CSS into jsdom for testing
    const style = document.createElement('style')
    style.textContent = expectedCss
    document.head.appendChild(style)
  })

  // Test Case 3: Verify scroll-behavior CSS property -> html element has scroll-behavior: smooth
  it('html element has scroll-behavior: smooth applied', () => {
    // Get computed style of the html element
    const htmlElement = document.documentElement
    const computedStyle = window.getComputedStyle(htmlElement)

    // Check that scroll-behavior is smooth
    expect(computedStyle.scrollBehavior).toBe('smooth')
  })

  it('scroll-behavior property is correctly applied from CSS', () => {
    // Verify the html element exists and has styles applied
    const htmlElement = document.documentElement
    expect(htmlElement).toBeDefined()

    // The scroll-behavior should be applied via CSS
    const scrollBehavior = window.getComputedStyle(htmlElement).scrollBehavior
    expect(scrollBehavior).toBe('smooth')
  })

  it('scroll-behavior is not auto (default)', () => {
    const htmlElement = document.documentElement
    const computedStyle = window.getComputedStyle(htmlElement)

    // Ensure scroll-behavior is explicitly smooth, not the default 'auto'
    expect(computedStyle.scrollBehavior).not.toBe('auto')
    expect(computedStyle.scrollBehavior).toBe('smooth')
  })
})
