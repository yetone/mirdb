/**
 * Unit Tests for Semantic HTML Structure
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 4: Page uses header, main, nav, section, article, footer elements appropriately
 */

import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import App from '../../../src/App'

// Mock CSS imports
vi.mock('../../../src/styles/globals.css', () => ({}))
vi.mock('../../../src/components/layout/Header.css', () => ({}))
vi.mock('../../../src/components/layout/Footer.css', () => ({}))
vi.mock('../../../src/components/layout/Navigation.css', () => ({}))
vi.mock('../../../src/components/layout/MobileMenu.css', () => ({}))
vi.mock('../../../src/components/sections/Hero.css', () => ({}))
vi.mock('../../../src/components/sections/Demo.css', () => ({}))
vi.mock('../../../src/components/sections/Features.css', () => ({}))
vi.mock('../../../src/components/sections/QuickStart.css', () => ({}))
vi.mock('../../../src/components/ui/Button.css', () => ({}))
vi.mock('../../../src/components/ui/Badge.css', () => ({}))
vi.mock('../../../src/components/ui/FeatureCard.css', () => ({}))
vi.mock('../../../src/components/ui/CodeBlock.css', () => ({}))
vi.mock('../../../src/components/ui/ThemeToggle.css', () => ({}))

describe('Test Case 4: Semantic HTML Structure', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('page has a header element', () => {
    const header = document.querySelector('header')
    expect(header).not.toBeNull()
  })

  it('page has a main element', () => {
    const main = document.querySelector('main')
    expect(main).not.toBeNull()
  })

  it('page has nav elements', () => {
    const navElements = document.querySelectorAll('nav')
    expect(navElements.length).toBeGreaterThan(0)
  })

  it('nav elements have appropriate aria-label or are within labeled container', () => {
    const navElements = document.querySelectorAll('nav')

    navElements.forEach((nav) => {
      const ariaLabel = nav.getAttribute('aria-label')
      const ariaLabelledBy = nav.getAttribute('aria-labelledby')

      // Check if nav is inside a labeled dialog or container
      const labeledParent = nav.closest('[aria-label], [aria-labelledby]')
      const isInsideLabeledContainer = labeledParent !== null && labeledParent !== nav

      const hasAccessibleLabel =
        (ariaLabel !== null && ariaLabel !== '') ||
        ariaLabelledBy !== null ||
        isInsideLabeledContainer

      expect(
        hasAccessibleLabel,
        'Nav element should have aria-label, aria-labelledby, or be inside a labeled container'
      ).toBe(true)
    })
  })

  it('page has section elements', () => {
    const sections = document.querySelectorAll('section')
    expect(sections.length).toBeGreaterThan(0)
  })

  it('page has a footer element', () => {
    const footer = document.querySelector('footer')
    expect(footer).not.toBeNull()
  })

  it('semantic structure follows logical order (header > main > footer)', () => {
    const elements = Array.from(document.querySelectorAll('header, main, footer'))

    const headerIndex = elements.findIndex((el) => el.tagName === 'HEADER')
    const mainIndex = elements.findIndex((el) => el.tagName === 'MAIN')
    const footerIndex = elements.findIndex((el) => el.tagName === 'FOOTER')

    expect(headerIndex).toBeLessThan(mainIndex)
    expect(mainIndex).toBeLessThan(footerIndex)
  })

  it('there is exactly one main element', () => {
    const mainElements = document.querySelectorAll('main')
    expect(mainElements.length).toBe(1)
  })
})
