/**
 * Unit Tests for Heading Hierarchy
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 6: Headings follow logical order (h1 > h2 > h3), single h1 per page
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

describe('Test Case 6: Heading Hierarchy', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('page has exactly one h1 element', () => {
    const h1Elements = document.querySelectorAll('h1')
    expect(h1Elements.length).toBe(1)
  })

  it('h1 is the first heading on the page', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
    expect(headings.length).toBeGreaterThan(0)
    expect(headings[0].tagName.toLowerCase()).toBe('h1')
  })

  it('heading hierarchy is logical (no skipping levels)', () => {
    const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'))
    const headingLevels = headings.map((h) => ({
      level: parseInt(h.tagName.replace('H', '')),
      text: h.textContent?.trim().substring(0, 50) || '',
    }))

    // Check that we don't skip heading levels
    let previousLevel = 0

    for (const heading of headingLevels) {
      if (previousLevel === 0) {
        // First heading should be h1
        expect(heading.level).toBe(1)
      } else {
        // Going deeper shouldn't skip more than one level
        const isValidProgression =
          heading.level <= previousLevel + 1 || // Going same or one level deeper
          heading.level <= previousLevel // Going up or staying same

        expect(
          isValidProgression,
          `Invalid heading progression: h${previousLevel} to h${heading.level} ("${heading.text}")`
        ).toBe(true)
      }

      previousLevel = heading.level
    }
  })

  it('all headings have meaningful content', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')

    headings.forEach((heading) => {
      const text = heading.textContent?.trim() || ''
      expect(text.length, 'Heading should not be empty').toBeGreaterThan(0)
    })
  })

  it('h2 elements are used for main section headings', () => {
    // Verify sections have h2 headings
    const sections = document.querySelectorAll('section')
    let count = 0

    sections.forEach((section) => {
      if (section.querySelector('h2')) {
        count++
      }
    })

    // At least some sections should have h2 headings
    expect(count).toBeGreaterThan(0)
  })
})
