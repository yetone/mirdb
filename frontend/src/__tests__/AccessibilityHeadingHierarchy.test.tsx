import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'

/**
 * Accessibility - Heading Hierarchy Tests
 *
 * These tests verify proper heading structure for screen readers (NFR-2)
 * ensuring the homepage follows accessibility best practices.
 *
 * Requirements tested: NFR-2 (90+ Lighthouse accessibility score)
 * Related PRD sections: Accessibility Considerations (proper heading hierarchy h1, h2, h3)
 */

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Accessibility - Heading Hierarchy', () => {
  describe('Test Case 1: Exactly one h1 element exists on the page', () => {
    it('should have exactly one h1 element on the homepage', () => {
      renderWithRouter(<Home />)

      const h1Elements = screen.getAllByRole('heading', { level: 1 })

      expect(h1Elements).toHaveLength(1)
    })

    it('should have the h1 in the hero section', () => {
      renderWithRouter(<Home />)

      const h1 = screen.getByRole('heading', { level: 1 })
      const heroSection = screen.getByTestId('hero-section')

      expect(heroSection).toContainElement(h1)
    })

    it('should have meaningful content in the h1', () => {
      renderWithRouter(<Home />)

      const h1 = screen.getByRole('heading', { level: 1 })

      // The h1 should contain text related to the product's primary purpose
      expect(h1.textContent).toBeTruthy()
      expect(h1.textContent?.length).toBeGreaterThan(5)
    })
  })

  describe('Test Case 2: No heading levels are skipped', () => {
    it('should not skip heading levels (e.g., h1 followed by h3 without h2)', () => {
      renderWithRouter(<Home />)

      // Get all heading elements
      const allHeadings = screen.getAllByRole('heading')

      // Extract heading levels from the DOM
      const headingLevels: number[] = []
      allHeadings.forEach((heading) => {
        const tagName = heading.tagName.toLowerCase()
        const level = parseInt(tagName.charAt(1), 10)
        headingLevels.push(level)
      })

      // Verify no levels are skipped
      // Track which levels have been used
      const usedLevels = new Set(headingLevels)

      // Check that we don't skip from h1 to h3 (i.e., if h3 exists, h2 must exist)
      if (usedLevels.has(3)) {
        expect(usedLevels.has(2)).toBe(true)
      }

      // Check that we don't skip from h2 to h4 (i.e., if h4 exists, h3 must exist)
      if (usedLevels.has(4)) {
        expect(usedLevels.has(3)).toBe(true)
      }

      // Check that we don't skip from h3 to h5 (i.e., if h5 exists, h4 must exist)
      if (usedLevels.has(5)) {
        expect(usedLevels.has(4)).toBe(true)
      }

      // Check that we don't skip from h4 to h6 (i.e., if h6 exists, h5 must exist)
      if (usedLevels.has(6)) {
        expect(usedLevels.has(5)).toBe(true)
      }
    })

    it('should start with h1 as the first heading level', () => {
      renderWithRouter(<Home />)

      const allHeadings = screen.getAllByRole('heading')
      const headingLevels: number[] = []

      allHeadings.forEach((heading) => {
        const tagName = heading.tagName.toLowerCase()
        const level = parseInt(tagName.charAt(1), 10)
        headingLevels.push(level)
      })

      // The minimum level should be 1 (h1 must exist)
      const minLevel = Math.min(...headingLevels)
      expect(minLevel).toBe(1)
    })

    it('should have a proper descending hierarchy without gaps', () => {
      renderWithRouter(<Home />)

      const allHeadings = screen.getAllByRole('heading')
      const headingLevels: number[] = []

      allHeadings.forEach((heading) => {
        const tagName = heading.tagName.toLowerCase()
        const level = parseInt(tagName.charAt(1), 10)
        headingLevels.push(level)
      })

      // Get unique levels and sort them
      const uniqueLevels = [...new Set(headingLevels)].sort((a, b) => a - b)

      // Check that levels form a consecutive sequence from the minimum
      for (let i = 1; i < uniqueLevels.length; i++) {
        const gap = uniqueLevels[i] - uniqueLevels[i - 1]
        // Gap should be 1 (consecutive levels)
        expect(gap).toBe(1)
      }
    })
  })

  describe('Test Case 3: Each section uses h2 for section titles', () => {
    it('should use h2 for the Features section title', () => {
      renderWithRouter(<Home />)

      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })

      expect(featuresHeading.tagName.toLowerCase()).toBe('h2')
    })

    it('should use h2 for the Demo section title', () => {
      renderWithRouter(<Home />)

      const demoHeading = screen.getByRole('heading', { name: /try it now/i })

      expect(demoHeading.tagName.toLowerCase()).toBe('h2')
    })

    it('should have h2 headings with proper aria-labelledby connections', () => {
      renderWithRouter(<Home />)

      // Features section should be linked to its heading
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-heading')

      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading.id).toBe('features-heading')
    })

    it('should use h3 for feature card titles (subsections)', () => {
      renderWithRouter(<Home />)

      // Feature cards use h3 which is proper nesting under h2
      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const cardHeading = card.querySelector('h3')
        expect(cardHeading).toBeInTheDocument()
      })
    })

    it('should have the correct heading count per level', () => {
      renderWithRouter(<Home />)

      const h1Count = screen.getAllByRole('heading', { level: 1 }).length
      const h2Count = screen.getAllByRole('heading', { level: 2 }).length
      const h3Count = screen.getAllByRole('heading', { level: 3 }).length

      // Exactly one h1
      expect(h1Count).toBe(1)

      // At least 2 h2s (Features and Demo sections)
      expect(h2Count).toBeGreaterThanOrEqual(2)

      // At least 3 h3s (feature cards)
      expect(h3Count).toBeGreaterThanOrEqual(3)
    })
  })

  describe('Additional Accessibility Tests', () => {
    it('should have all headings accessible via screen readers', () => {
      renderWithRouter(<Home />)

      const allHeadings = screen.getAllByRole('heading')

      // Each heading should have text content
      allHeadings.forEach((heading) => {
        expect(heading.textContent?.trim().length).toBeGreaterThan(0)
      })
    })

    it('should maintain heading hierarchy even in nested components', () => {
      renderWithRouter(<Home />)

      // Get headings in document order
      const mainElement = screen.getByRole('main')
      const headings = mainElement.querySelectorAll('h1, h2, h3, h4, h5, h6')

      let previousLevel = 0
      headings.forEach((heading) => {
        const currentLevel = parseInt(heading.tagName.charAt(1), 10)

        // First heading should be h1
        if (previousLevel === 0) {
          expect(currentLevel).toBe(1)
        } else {
          // Each subsequent heading should not jump more than one level down
          // (i.e., can go from h1 to h2 or h2 to h3, but not h1 to h3 directly for new sections)
          // Note: It's valid to go back up (e.g., h3 to h2 when starting a new section)
          const levelIncrease = currentLevel - previousLevel
          // When going deeper, should not skip levels
          if (levelIncrease > 0) {
            expect(levelIncrease).toBeLessThanOrEqual(1)
          }
        }

        previousLevel = currentLevel
      })
    })
  })
})
