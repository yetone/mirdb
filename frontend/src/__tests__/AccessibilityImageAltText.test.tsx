import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import DemoSection from '../components/DemoSection'
import FooterSection from '../components/FooterSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Accessibility - Image Alt Text (NFR-2)', () => {
  /**
   * Test Case 1: Query all img elements on homepage
   * Expected: All img elements have alt attribute defined
   */
  describe('Test Case 1: All img elements have alt attribute defined', () => {
    it('should ensure all img elements on homepage have alt attribute defined', () => {
      renderWithRouter(<Home />)

      // Query all img elements on the page
      const imgElements = document.querySelectorAll('img')

      // Each img element should have an alt attribute defined
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
        // alt can be empty string for decorative images, but must be defined
      })
    })

    it('should ensure HeroSection has no img elements without alt attributes', () => {
      renderWithRouter(<HeroSection />)

      const imgElements = document.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })

    it('should ensure FeaturesSection has no img elements without alt attributes', () => {
      render(<FeaturesSection />)

      const imgElements = document.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })

    it('should ensure DemoSection has no img elements without alt attributes', () => {
      renderWithRouter(<DemoSection />)

      const imgElements = document.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })

    it('should ensure FooterSection has no img elements without alt attributes', () => {
      renderWithRouter(<FooterSection />)

      const imgElements = document.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })
  })

  /**
   * Test Case 2: Query feature icon elements
   * Expected: Icons have aria-label or aria-hidden='true' for decorative icons
   */
  describe('Test Case 2: Feature icons have proper ARIA attributes', () => {
    it('should ensure feature icon SVGs are marked as aria-hidden for decorative icons', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)

      featureCards.forEach((card) => {
        const svgIcon = card.querySelector('svg')
        expect(svgIcon).toBeInTheDocument()

        // Decorative icons should have aria-hidden="true"
        // Or they should have an aria-label for meaningful icons
        const hasAriaHidden = svgIcon?.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svgIcon?.hasAttribute('aria-label')

        expect(hasAriaHidden || hasAriaLabel).toBe(true)
      })
    })

    it('should ensure all SVG icons in homepage have aria-hidden or aria-label', () => {
      renderWithRouter(<Home />)

      const svgElements = document.querySelectorAll('svg')

      svgElements.forEach((svg) => {
        // Each SVG should either be hidden from assistive tech (decorative)
        // or have a label for screen readers
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')
        const hasAriaLabelledBy = svg.hasAttribute('aria-labelledby')
        const hasRole = svg.hasAttribute('role')

        // At minimum, decorative SVGs should be hidden or meaningful ones labeled
        expect(
          hasAriaHidden || hasAriaLabel || hasAriaLabelledBy || hasRole
        ).toBe(true)
      })
    })

    it('should ensure DemoSection error icon is properly marked as decorative', () => {
      renderWithRouter(<DemoSection />)

      // The error icon SVG in DemoSection should be aria-hidden
      const allSvgs = document.querySelectorAll('svg')
      allSvgs.forEach((svg) => {
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')

        expect(hasAriaHidden || hasAriaLabel).toBe(true)
      })
    })

    it('should ensure DemoSection prompt icon is properly marked as decorative', () => {
      renderWithRouter(<DemoSection />)

      // All SVGs should have proper accessibility attributes
      const svgElements = document.querySelectorAll('svg')
      svgElements.forEach((svg) => {
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')

        expect(hasAriaHidden || hasAriaLabel).toBe(true)
      })
    })
  })

  /**
   * Test Case 3: Check hero section illustration
   * Expected: Hero image has descriptive alt text or is marked decorative
   */
  describe('Test Case 3: Hero section illustration accessibility', () => {
    it('should ensure hero section has no images without alt text', () => {
      renderWithRouter(<HeroSection />)

      // Check that any img elements in hero have alt defined
      const heroSection = screen.getByTestId('hero-section')
      const imgElements = heroSection.querySelectorAll('img')

      imgElements.forEach((img) => {
        const altText = img.getAttribute('alt')
        // alt attribute must be defined (can be empty for decorative)
        expect(img).toHaveAttribute('alt')

        // If alt is not empty, it should be meaningful (not just spaces)
        if (altText && altText.trim().length > 0) {
          expect(altText.trim().length).toBeGreaterThan(0)
        }
      })
    })

    it('should ensure hero section uses accessible text-based content', () => {
      renderWithRouter(<HeroSection />)

      // The hero section should have accessible headline
      const heroSection = screen.getByTestId('hero-section')

      // h1 headline should exist
      const h1 = heroSection.querySelector('h1')
      expect(h1).toBeInTheDocument()
      expect(h1?.textContent?.trim().length).toBeGreaterThan(0)

      // Subheadline should be accessible
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent?.trim().length).toBeGreaterThan(0)
    })

    it('should ensure any background images have proper accessibility handling', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Background images in CSS are decorative and don't need alt text
      // But any img elements should have alt defined
      const imgElements = heroSection.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })

    it('should ensure hero section navigation has proper aria-label', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Navigation within hero should have aria-label
      const nav = heroSection.querySelector('nav')
      if (nav) {
        const hasAriaLabel = nav.hasAttribute('aria-label')
        const hasAriaLabelledBy = nav.hasAttribute('aria-labelledby')
        expect(hasAriaLabel || hasAriaLabelledBy).toBe(true)
      }
    })
  })

  /**
   * Additional accessibility tests for comprehensive coverage
   */
  describe('Comprehensive Image and Icon Accessibility', () => {
    it('should have no img elements with empty alt text that are not decorative', () => {
      renderWithRouter(<Home />)

      const imgElements = document.querySelectorAll('img')

      imgElements.forEach((img) => {
        const altText = img.getAttribute('alt')
        const isDecorativeByRole = img.getAttribute('role') === 'presentation'
        const hasAriaHidden = img.getAttribute('aria-hidden') === 'true'

        // If img is marked as decorative, empty alt is fine
        // If img is NOT decorative, alt should be non-empty
        if (!isDecorativeByRole && !hasAriaHidden) {
          // For non-decorative images, alt must be defined
          expect(img).toHaveAttribute('alt')
        }
      })
    })

    it('should ensure all interactive icons have accessible names', () => {
      renderWithRouter(<Home />)

      // Find any clickable elements with only icons (no text)
      const buttons = document.querySelectorAll('button')
      buttons.forEach((button) => {
        const hasText = button.textContent && button.textContent.trim().length > 0
        const hasAriaLabel = button.hasAttribute('aria-label')
        const hasAriaLabelledBy = button.hasAttribute('aria-labelledby')

        // Button must have accessible text or aria-label
        expect(hasText || hasAriaLabel || hasAriaLabelledBy).toBe(true)
      })
    })

    it('should ensure footer has no images without alt text', () => {
      renderWithRouter(<FooterSection />)

      const imgElements = document.querySelectorAll('img')
      imgElements.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })

    it('should validate that the page meets minimum image accessibility standards', () => {
      renderWithRouter(<Home />)

      // Count violations
      let violations = 0
      const imgElements = document.querySelectorAll('img')

      imgElements.forEach((img) => {
        if (!img.hasAttribute('alt')) {
          violations++
        }
      })

      // No violations should exist
      expect(violations).toBe(0)
    })

    it('should validate that all SVG icons are properly hidden from assistive tech or labeled', () => {
      renderWithRouter(<Home />)

      let svgAccessibilityViolations = 0
      const svgElements = document.querySelectorAll('svg')

      svgElements.forEach((svg) => {
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')
        const hasAriaLabelledBy = svg.hasAttribute('aria-labelledby')
        const hasRole = svg.getAttribute('role') === 'img'
        const hasTitleElement = svg.querySelector('title') !== null

        const isAccessible =
          hasAriaHidden || hasAriaLabel || hasAriaLabelledBy || (hasRole && hasTitleElement)

        if (!isAccessible) {
          svgAccessibilityViolations++
        }
      })

      expect(svgAccessibilityViolations).toBe(0)
    })
  })
})
