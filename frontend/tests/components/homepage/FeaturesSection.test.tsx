import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { FeaturesSection, Feature } from '../../../src/components/homepage/FeaturesSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({
      children,
      whileHover,
      whileTap,
      whileInView,
      initial,
      animate,
      transition,
      variants,
      viewport,
      ...props
    }: React.PropsWithChildren<Record<string, unknown>>) => <div {...props}>{children}</div>,
  },
}))

const renderFeaturesSection = (props: { features?: Feature[] } = {}) => {
  return render(<FeaturesSection {...props} />)
}

describe('FeaturesSection', () => {
  // Test Case 1: Component renders without errors
  describe('Rendering', () => {
    it('should render the component without errors', () => {
      renderFeaturesSection()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('should render the features section with correct structure', () => {
      renderFeaturesSection()
      const section = screen.getByTestId('features-section')
      expect(section.tagName).toBe('SECTION')
      expect(section).toHaveAttribute('id', 'features')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('should render a section heading', () => {
      renderFeaturesSection()
      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading.tagName).toBe('H2')
      expect(heading.textContent).toContain('Features')
    })
  })

  // Test Case 2: Count rendered feature cards - at least 3
  describe('Feature Cards Count', () => {
    it('should render at least 3 feature cards', () => {
      renderFeaturesSection()
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('should render all default features (4 cards)', () => {
      renderFeaturesSection()
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(4)
    })

    it('should render custom features when provided', () => {
      const customFeatures: Feature[] = [
        {
          id: 'custom-1',
          icon: <svg data-testid="custom-icon-1" />,
          title: 'Custom Feature 1',
          description: 'Description 1',
        },
        {
          id: 'custom-2',
          icon: <svg data-testid="custom-icon-2" />,
          title: 'Custom Feature 2',
          description: 'Description 2',
        },
        {
          id: 'custom-3',
          icon: <svg data-testid="custom-icon-3" />,
          title: 'Custom Feature 3',
          description: 'Description 3',
        },
      ]
      renderFeaturesSection({ features: customFeatures })
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
    })
  })

  // Test Case 3: Check for URL Shortening feature card
  describe('URL Shortening Feature', () => {
    it('should have a card with title containing URL or Shorten', () => {
      renderFeaturesSection()
      const titles = screen.getAllByTestId('feature-title')
      const urlShorteningTitle = titles.find(
        (title) =>
          title.textContent?.toLowerCase().includes('url') ||
          title.textContent?.toLowerCase().includes('shorten')
      )
      expect(urlShorteningTitle).toBeTruthy()
    })

    it('should have URL Shortening as one of the default features', () => {
      renderFeaturesSection()
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    })
  })

  // Test Case 4: Check for Analytics feature card
  describe('Analytics Feature', () => {
    it('should have a card with title containing Analytics or Track', () => {
      renderFeaturesSection()
      const titles = screen.getAllByTestId('feature-title')
      const analyticsTitle = titles.find(
        (title) =>
          title.textContent?.toLowerCase().includes('analytics') ||
          title.textContent?.toLowerCase().includes('track')
      )
      expect(analyticsTitle).toBeTruthy()
    })

    it('should have Click Analytics as one of the default features', () => {
      renderFeaturesSection()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    })
  })

  // Test Case 5: Check for GeoIP/Location feature card
  describe('GeoIP/Location Feature', () => {
    it('should have a card mentioning location tracking or GeoIP', () => {
      renderFeaturesSection()
      const titles = screen.getAllByTestId('feature-title')
      const descriptions = screen.getAllByTestId('feature-description')

      const hasLocationInTitle = titles.some(
        (title) =>
          title.textContent?.toLowerCase().includes('location') ||
          title.textContent?.toLowerCase().includes('geoip') ||
          title.textContent?.toLowerCase().includes('geo')
      )

      const hasLocationInDescription = descriptions.some(
        (desc) =>
          desc.textContent?.toLowerCase().includes('location') ||
          desc.textContent?.toLowerCase().includes('geographic') ||
          desc.textContent?.toLowerCase().includes('geoip')
      )

      expect(hasLocationInTitle || hasLocationInDescription).toBe(true)
    })

    it('should have GeoIP Location Tracking as one of the default features', () => {
      renderFeaturesSection()
      expect(screen.getByText('GeoIP Location Tracking')).toBeInTheDocument()
    })
  })

  // Test Case 6: Verify each card has icon
  describe('Feature Card Icons', () => {
    it('should have an icon element (svg or img) in each feature card', () => {
      renderFeaturesSection()
      const iconContainers = screen.getAllByTestId('feature-icon')
      expect(iconContainers.length).toBeGreaterThanOrEqual(3)

      iconContainers.forEach((container) => {
        const svg = container.querySelector('svg')
        const img = container.querySelector('img')
        expect(svg || img).toBeTruthy()
      })
    })

    it('should render all icons as SVG elements by default', () => {
      renderFeaturesSection()
      const iconContainers = screen.getAllByTestId('feature-icon')
      iconContainers.forEach((container) => {
        const svg = container.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })
  })

  // Test Case 7: Verify each card has title (heading)
  describe('Feature Card Titles', () => {
    it('should have a heading (h2, h3, or h4) in each feature card', () => {
      renderFeaturesSection()
      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const cardElement = within(card)
        const title = cardElement.getByTestId('feature-title')
        expect(title).toBeInTheDocument()
        // card-title in DaisyUI can be h3 or div with card-title class
        // Our implementation uses h3
        expect(title.tagName).toBe('H3')
      })
    })

    it('should have non-empty title text in each card', () => {
      renderFeaturesSection()
      const titles = screen.getAllByTestId('feature-title')
      titles.forEach((title) => {
        expect(title.textContent?.trim()).not.toBe('')
      })
    })
  })

  // Test Case 8: Verify each card has description
  describe('Feature Card Descriptions', () => {
    it('should have a descriptive text paragraph in each feature card', () => {
      renderFeaturesSection()
      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const cardElement = within(card)
        const description = cardElement.getByTestId('feature-description')
        expect(description).toBeInTheDocument()
        expect(description.tagName).toBe('P')
      })
    })

    it('should have non-empty description text in each card', () => {
      renderFeaturesSection()
      const descriptions = screen.getAllByTestId('feature-description')
      descriptions.forEach((desc) => {
        expect(desc.textContent?.trim()).not.toBe('')
        // Descriptions should be meaningful (more than just a few words)
        expect(desc.textContent!.length).toBeGreaterThan(20)
      })
    })
  })

  // Test Case 9: Check features section accessibility
  describe('Accessibility', () => {
    it('should have proper heading hierarchy with h2 for section title', () => {
      renderFeaturesSection()
      const sectionHeading = screen.getByTestId('features-heading')
      expect(sectionHeading.tagName).toBe('H2')
    })

    it('should have h3 headings for feature card titles', () => {
      renderFeaturesSection()
      const featureCards = screen.getAllByTestId('feature-card')
      featureCards.forEach((card) => {
        const cardElement = within(card)
        const title = cardElement.getByTestId('feature-title')
        expect(title.tagName).toBe('H3')
      })
    })

    it('should have aria-hidden on icons since they are decorative', () => {
      renderFeaturesSection()
      const iconContainers = screen.getAllByTestId('feature-icon')
      iconContainers.forEach((container) => {
        // The icon container or the SVG inside should have aria-hidden
        const svg = container.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('should have aria-labelledby pointing to the section heading', () => {
      renderFeaturesSection()
      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('should have proper text contrast classes for readability', () => {
      renderFeaturesSection()
      const titles = screen.getAllByTestId('feature-title')
      const descriptions = screen.getAllByTestId('feature-description')

      // Titles should use base-content for full contrast
      titles.forEach((title) => {
        expect(title).toHaveClass('text-base-content')
      })

      // Descriptions use slightly reduced contrast but still readable
      descriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })

    it('should have a section heading with id for aria-labelledby', () => {
      renderFeaturesSection()
      const heading = screen.getByTestId('features-heading')
      expect(heading).toHaveAttribute('id', 'features-heading')
    })
  })

  // Additional tests for grid layout
  describe('Layout', () => {
    it('should render features in a grid container', () => {
      renderFeaturesSection()
      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })

    it('should have responsive grid classes', () => {
      renderFeaturesSection()
      const grid = screen.getByTestId('features-grid')
      // Check for responsive grid columns
      expect(grid).toHaveClass('md:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-4')
    })
  })
})
