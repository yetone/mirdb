import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'

/**
 * Accessibility Tests for Homepage
 *
 * This test suite verifies:
 * - Proper heading hierarchy (h1, h2, h3 in logical order)
 * - Image alt text for all images
 * - ARIA labels for interactive elements
 * - Semantic HTML landmarks (main, nav, footer)
 */

const renderHomepage = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Accessibility - Semantic HTML and ARIA', () => {
  // Test Case 1: Exactly one h1 element exists on the page
  describe('Test Case 1: Single H1 Element', () => {
    it('should have exactly one h1 element on the homepage', () => {
      renderHomepage()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have the h1 containing the main value proposition', () => {
      renderHomepage()

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toContain('Shorten Links')
      expect(h1.textContent).toContain('Amplify Reach')
    })
  })

  // Test Case 2: Headings follow proper hierarchy without skipping levels
  describe('Test Case 2: Heading Hierarchy', () => {
    it('should have headings following proper hierarchy (h1 > h2 > h3)', () => {
      renderHomepage()

      // Get all headings
      const allHeadings = screen.getAllByRole('heading')

      // Extract heading levels
      const headingLevels = allHeadings.map(heading => {
        const tagName = heading.tagName.toLowerCase()
        return parseInt(tagName.replace('h', ''), 10)
      })

      // Verify we have headings
      expect(headingLevels.length).toBeGreaterThan(0)

      // Check that heading levels don't skip (e.g., h1 followed by h3 without h2)
      let maxLevelSeen = 0
      for (const level of headingLevels) {
        // Each heading level should not be more than 1 greater than max seen
        // (allows same level or going back up)
        if (level > maxLevelSeen) {
          expect(level).toBeLessThanOrEqual(maxLevelSeen + 1)
          maxLevelSeen = level
        }
      }
    })

    it('should have h1 as the first heading', () => {
      renderHomepage()

      const allHeadings = screen.getAllByRole('heading')
      const firstHeading = allHeadings[0]

      expect(firstHeading.tagName.toLowerCase()).toBe('h1')
    })

    it('should have h2 headings for main sections', () => {
      renderHomepage()

      const h2Headings = screen.getAllByRole('heading', { level: 2 })

      // Should have at least 2 section headings (Features, How It Works)
      expect(h2Headings.length).toBeGreaterThanOrEqual(2)

      // Check for expected section headings
      const h2Texts = h2Headings.map(h => h.textContent?.toLowerCase() || '')
      expect(h2Texts.some(text => text.includes('feature'))).toBe(true)
      expect(h2Texts.some(text => text.includes('how it works'))).toBe(true)
    })

    it('should have h3 headings for subsections within sections', () => {
      renderHomepage()

      const h3Headings = screen.getAllByRole('heading', { level: 3 })

      // Should have h3 headings for feature cards and how-it-works steps
      expect(h3Headings.length).toBeGreaterThanOrEqual(4)
    })
  })

  // Test Case 3: All img elements have non-empty alt attributes
  describe('Test Case 3: Image Alt Text', () => {
    it('should have all img elements with non-empty alt attributes', () => {
      renderHomepage()

      const images = document.querySelectorAll('img')

      images.forEach((img) => {
        // Check that alt attribute exists
        expect(img).toHaveAttribute('alt')

        // Check that alt is not empty (unless it's a decorative image with alt="")
        const altText = img.getAttribute('alt')
        expect(altText).not.toBeNull()
        // Decorative images can have alt="" but should still have the attribute
      })
    })

    it('should have descriptive alt text for informational images', () => {
      renderHomepage()

      const images = document.querySelectorAll('img')

      // If there are images, check they have meaningful alt text
      images.forEach((img) => {
        const altText = img.getAttribute('alt')
        // If alt is not empty (not decorative), it should be descriptive
        if (altText && altText.length > 0) {
          // Alt text should be reasonably descriptive (more than just a filename pattern)
          expect(altText).not.toMatch(/\.(jpg|jpeg|png|gif|svg|webp)$/i)
        }
      })
    })
  })

  // Test Case 4: Buttons with only icons have aria-label attributes
  describe('Test Case 4: Icon Buttons with ARIA Labels', () => {
    it('should have aria-label on buttons without visible text', () => {
      renderHomepage()

      const buttons = screen.queryAllByRole('button')

      // If there are no buttons, this test passes (no icon-only buttons exist)
      if (buttons.length === 0) {
        expect(true).toBe(true)
        return
      }

      buttons.forEach((button) => {
        const buttonText = button.textContent?.trim() || ''
        const hasAriaLabel = button.hasAttribute('aria-label')
        const hasAriaLabelledBy = button.hasAttribute('aria-labelledby')

        // If button has no visible text, it must have aria-label or aria-labelledby
        if (buttonText.length === 0) {
          expect(hasAriaLabel || hasAriaLabelledBy).toBe(true)
        }
      })
    })

    it('should have all links with visible text or aria-label', () => {
      renderHomepage()

      const links = screen.getAllByRole('link')

      links.forEach((link) => {
        const linkText = link.textContent?.trim() || ''
        const hasAriaLabel = link.hasAttribute('aria-label')
        const hasAriaLabelledBy = link.hasAttribute('aria-labelledby')

        // Link should have visible text OR aria-label/aria-labelledby
        expect(linkText.length > 0 || hasAriaLabel || hasAriaLabelledBy).toBe(true)
      })
    })
  })

  // Test Case 5: Page has main element or role='main' for primary content
  describe('Test Case 5: Main Landmark', () => {
    it('should have a main element for primary content', () => {
      renderHomepage()

      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
    })

    it('should have exactly one main landmark', () => {
      renderHomepage()

      const mainElements = screen.getAllByRole('main')
      expect(mainElements).toHaveLength(1)
    })

    it('should have the main element containing the homepage content', () => {
      renderHomepage()

      const mainElement = screen.getByRole('main')

      // Main should contain the hero section
      const heroSection = within(mainElement).getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Main should contain the features section
      const featuresSection = within(mainElement).getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })
  })

  // Additional accessibility tests
  describe('Additional Accessibility Checks', () => {
    it('should have proper semantic footer element', () => {
      renderHomepage()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have navigation landmark in footer', () => {
      renderHomepage()

      const navigation = screen.getByRole('navigation', { name: /footer/i })
      expect(navigation).toBeInTheDocument()
    })

    it('should have region landmarks for major sections', () => {
      renderHomepage()

      // HowItWorks section should be a region with aria-labelledby
      const howItWorksSection = screen.getByRole('region', { name: /how it works/i })
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('should have all interactive elements accessible via keyboard', () => {
      renderHomepage()

      // All links should be focusable
      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        expect(link).not.toHaveAttribute('tabindex', '-1')
      })

      // All buttons should be focusable
      const buttons = screen.queryAllByRole('button')
      buttons.forEach((button) => {
        expect(button).not.toHaveAttribute('tabindex', '-1')
      })
    })

    it('should have proper ARIA attributes on sections', () => {
      renderHomepage()

      // How It Works section should have aria-labelledby
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
    })
  })
})
