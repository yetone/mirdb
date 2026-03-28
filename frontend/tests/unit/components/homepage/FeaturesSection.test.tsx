/**
 * Unit and Integration tests for FeaturesSection component.
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * Test Cases:
 * 1. Features section is present with appropriate section heading
 * 2. At least 3 feature cards are rendered
 * 3. No more than 5 feature cards are rendered
 * 4. Each feature card contains an icon element
 * 5. Each feature card contains a title (heading element)
 * 6. Each feature card contains a description (2-3 sentences)
 * 7. Feature cards are readable with proper contrast in dark theme
 * 8. Feature cards are readable with proper contrast in light theme
 */
import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../../setup'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  // Test Case 1: Features section is present with appropriate section heading
  describe('Test Case 1: Section presence and heading', () => {
    it('renders the features section with appropriate section heading', () => {
      render(<FeaturesSection />)

      // Check section is present
      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      // Check heading is present with appropriate text
      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading.tagName).toBe('H2')
      expect(heading).toHaveTextContent(/features/i)
    })

    it('has proper ARIA labeling for accessibility', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })
  })

  // Test Case 2: At least 3 feature cards are rendered
  describe('Test Case 2: Minimum feature cards', () => {
    it('renders at least 3 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })
  })

  // Test Case 3: No more than 5 feature cards are rendered
  describe('Test Case 3: Maximum feature cards', () => {
    it('renders no more than 5 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeLessThanOrEqual(5)
    })
  })

  // Test Case 4: Each feature card contains an icon element
  describe('Test Case 4: Icon elements', () => {
    it('each feature card contains an icon element', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach((card) => {
        const icon = within(card).getByTestId('feature-icon')
        expect(icon).toBeInTheDocument()
        // Icon should have content (SVG or other element)
        expect(icon.children.length).toBeGreaterThan(0)
      })
    })

    it('icons are hidden from screen readers', () => {
      render(<FeaturesSection />)

      const icons = screen.getAllByTestId('feature-icon')
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  // Test Case 5: Each feature card contains a title (heading element)
  describe('Test Case 5: Title elements', () => {
    it('each feature card contains a title heading element', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach((card) => {
        const title = within(card).getByTestId('feature-title')
        expect(title).toBeInTheDocument()
        expect(title.tagName).toBe('H3')
        expect(title.textContent).toBeTruthy()
        expect(title.textContent!.length).toBeGreaterThan(0)
      })
    })

    it('all feature titles are unique', () => {
      render(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      const titleTexts = titles.map((t) => t.textContent)
      const uniqueTitles = new Set(titleTexts)
      expect(uniqueTitles.size).toBe(titleTexts.length)
    })
  })

  // Test Case 6: Each feature card contains a description (2-3 sentences)
  describe('Test Case 6: Description elements', () => {
    it('each feature card contains a description element', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach((card) => {
        const description = within(card).getByTestId('feature-description')
        expect(description).toBeInTheDocument()
        expect(description.textContent).toBeTruthy()
      })
    })

    it('descriptions have meaningful length (at least 50 characters)', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      descriptions.forEach((description) => {
        expect(description.textContent!.length).toBeGreaterThanOrEqual(50)
      })
    })

    it('descriptions contain multiple words forming sentences', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      descriptions.forEach((description) => {
        const words = description.textContent!.trim().split(/\s+/)
        // A 2-3 sentence description should have at least 10 words
        expect(words.length).toBeGreaterThanOrEqual(10)
      })
    })
  })

  // Test Case 7: Dark theme integration - verifies theme-aware classes
  describe('Test Case 7: Dark theme integration', () => {
    it('feature cards have theme-aware background classes', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      // Cards should have DaisyUI theme-aware classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })
    })

    it('feature section has dark theme compatible background', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('bg-base-200')
    })

    it('titles have theme-aware text color classes', () => {
      render(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      titles.forEach((title) => {
        expect(title).toHaveClass('text-base-content')
      })
    })

    it('descriptions have theme-aware text color classes', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      descriptions.forEach((desc) => {
        // Using text-base-content/70 for opacity
        expect(desc.className).toContain('text-base-content')
      })
    })
  })

  // Test Case 8: Light theme integration - same checks apply
  describe('Test Case 8: Light theme integration', () => {
    it('feature cards render correctly with theme-aware styling', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      // Cards use DaisyUI theme variables that work in both themes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-md')
      })
    })

    it('all text elements are present and readable', () => {
      render(<FeaturesSection />)

      // Verify heading is present
      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading.textContent!.length).toBeGreaterThan(0)

      // Verify all titles and descriptions are present
      const titles = screen.getAllByTestId('feature-title')
      const descriptions = screen.getAllByTestId('feature-description')

      titles.forEach((title) => {
        expect(title).toBeInTheDocument()
        expect(title.textContent!.length).toBeGreaterThan(0)
      })

      descriptions.forEach((desc) => {
        expect(desc).toBeInTheDocument()
        expect(desc.textContent!.length).toBeGreaterThan(0)
      })
    })

    it('section has proper background for light theme', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      // bg-base-200 works in both light and dark themes
      expect(section).toHaveClass('bg-base-200')
    })
  })

  // Additional structural tests
  describe('Grid layout', () => {
    it('features are displayed in a grid layout', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })
  })

  describe('Expected features content', () => {
    it('includes URL Shortening feature', () => {
      render(<FeaturesSection />)
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    })

    it('includes Click Analytics feature', () => {
      render(<FeaturesSection />)
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    })

    it('includes Dashboard feature', () => {
      render(<FeaturesSection />)
      expect(screen.getByText('Dashboard')).toBeInTheDocument()
    })

    it('includes Real-time Tracking feature', () => {
      render(<FeaturesSection />)
      expect(screen.getByText('Real-time Tracking')).toBeInTheDocument()
    })

    it('includes QR Codes feature', () => {
      render(<FeaturesSection />)
      expect(screen.getByText('QR Codes')).toBeInTheDocument()
    })
  })
})
