/**
 * Unit tests for Features section.
 * Owner: Scenario 3 - Features Section Implementation
 *
 * Test cases:
 * - Features section displays 3-5 feature cards in grid layout
 * - Each card contains icon, title (h3/h4), and description paragraph
 * - Each feature displays an appropriate icon (SVG or icon font)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Features, FeatureCard, FeaturesProps, FeatureCardProps } from '../../../src/components/sections/Features'
import type { Feature } from '../../../src/types'

const mockFeatures: Feature[] = [
  {
    id: 'feature-1',
    icon: 'bolt',
    title: 'Lightning Fast Performance',
    description: 'Experience blazing fast load times with our optimized platform.',
  },
  {
    id: 'feature-2',
    icon: 'chart',
    title: 'Advanced Analytics Dashboard',
    description: 'Get comprehensive insights with real-time data visualization.',
  },
  {
    id: 'feature-3',
    icon: 'shield',
    title: 'Enterprise Security Standards',
    description: 'Your data is protected with industry-leading encryption.',
  },
]

const fiveMockFeatures: Feature[] = [
  ...mockFeatures,
  {
    id: 'feature-4',
    icon: 'cog',
    title: 'Customizable Integrations',
    description: 'Connect with hundreds of third-party tools and services.',
  },
  {
    id: 'feature-5',
    icon: 'users',
    title: 'Team Collaboration Tools',
    description: 'Work together seamlessly with real-time collaboration features.',
  },
]

describe('Features Section', () => {
  // Test Case 1: Features section displays 3-5 feature cards in grid layout
  describe('Test Case 1: Render Features section', () => {
    it('renders features section with grid layout', () => {
      render(<Features features={mockFeatures} />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()
      // Check grid classes for responsive layout
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
      expect(featuresGrid).toHaveClass('lg:grid-cols-3')
    })

    it('renders exactly 3 feature cards when given 3 features', () => {
      render(<Features features={mockFeatures} />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
    })

    it('renders exactly 5 feature cards when given 5 features', () => {
      render(<Features features={fiveMockFeatures} />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(5)
    })

    it('renders default features when no features prop provided', () => {
      render(<Features />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
      expect(featureCards.length).toBeLessThanOrEqual(5)
    })

    it('renders section with proper heading', () => {
      render(<Features sectionTitle="Our Amazing Features" />)

      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Our Amazing Features')
      expect(heading.tagName).toBe('H2')
    })
  })

  // Test Case 2: Each card contains icon, title (h3/h4), and description paragraph
  describe('Test Case 2: Check each feature card structure', () => {
    it('each card contains icon, title, and description', () => {
      render(<Features features={mockFeatures} />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const icon = within(card).getByTestId('feature-icon')
        const title = within(card).getByTestId('feature-title')
        const description = within(card).getByTestId('feature-description')

        expect(icon).toBeInTheDocument()
        expect(title).toBeInTheDocument()
        expect(description).toBeInTheDocument()
      })
    })

    it('title is rendered as h3 element', () => {
      render(<Features features={mockFeatures} />)

      const titles = screen.getAllByTestId('feature-title')

      titles.forEach((title) => {
        expect(title.tagName).toBe('H3')
      })
    })

    it('title has bold styling', () => {
      render(<Features features={mockFeatures} />)

      const titles = screen.getAllByTestId('feature-title')

      titles.forEach((title) => {
        expect(title).toHaveClass('font-semibold')
      })
    })

    it('description is rendered as paragraph element', () => {
      render(<Features features={mockFeatures} />)

      const descriptions = screen.getAllByTestId('feature-description')

      descriptions.forEach((desc) => {
        expect(desc.tagName).toBe('P')
      })
    })

    it('card displays the correct title and description', () => {
      render(<Features features={[mockFeatures[0]]} />)

      const title = screen.getByTestId('feature-title')
      const description = screen.getByTestId('feature-description')

      expect(title).toHaveTextContent(mockFeatures[0].title)
      expect(description).toHaveTextContent(mockFeatures[0].description)
    })
  })

  // Test Case 3: Each feature displays an appropriate icon (SVG or icon font)
  describe('Test Case 3: Verify feature icons render', () => {
    it('each feature displays an icon container', () => {
      render(<Features features={mockFeatures} />)

      const iconContainers = screen.getAllByTestId('feature-icon')
      expect(iconContainers).toHaveLength(mockFeatures.length)
    })

    it('icon container contains an SVG element', () => {
      render(<Features features={mockFeatures} />)

      const iconContainers = screen.getAllByTestId('feature-icon')

      iconContainers.forEach((container) => {
        const svg = container.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('icon container has proper styling', () => {
      render(<Features features={mockFeatures} />)

      const iconContainers = screen.getAllByTestId('feature-icon')

      iconContainers.forEach((container) => {
        expect(container).toHaveClass('flex')
        expect(container).toHaveClass('items-center')
        expect(container).toHaveClass('justify-center')
      })
    })

    it('icon is decorative with aria-hidden', () => {
      render(<Features features={mockFeatures} />)

      const iconContainers = screen.getAllByTestId('feature-icon')

      iconContainers.forEach((container) => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  // Additional tests for FeatureCard component
  describe('FeatureCard Component', () => {
    const singleFeature: Feature = {
      id: 'test-feature',
      icon: 'bolt',
      title: 'Test Feature Title',
      description: 'This is a test feature description for verification.',
    }

    it('renders feature card with all elements', () => {
      render(<FeatureCard feature={singleFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card).toBeInTheDocument()
      expect(card).toHaveAttribute('aria-labelledby', `feature-title-${singleFeature.id}`)
    })

    it('feature card is keyboard focusable', () => {
      render(<FeatureCard feature={singleFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card).toHaveAttribute('tabIndex', '0')
    })

    it('feature card has hover transition classes', () => {
      render(<FeatureCard feature={singleFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card).toHaveClass('transition-all')
      expect(card).toHaveClass('duration-300')
    })

    it('title has proper id for accessibility', () => {
      render(<FeatureCard feature={singleFeature} />)

      const title = screen.getByTestId('feature-title')
      expect(title).toHaveAttribute('id', `feature-title-${singleFeature.id}`)
    })
  })

  // Features section accessibility
  describe('Features section accessibility', () => {
    it('section has proper aria-labelledby attribute', () => {
      render(<Features features={mockFeatures} />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('section has proper id for navigation', () => {
      render(<Features features={mockFeatures} />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('id', 'features')
    })

    it('features grid has role="list" for screen readers', () => {
      render(<Features features={mockFeatures} />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveAttribute('role', 'list')
    })

    it('each feature card wrapper has role="listitem"', () => {
      render(<Features features={mockFeatures} />)

      const listItems = screen.getAllByRole('listitem')
      expect(listItems).toHaveLength(mockFeatures.length)
    })
  })

  // Grid layout tests
  describe('Grid layout classes', () => {
    it('has proper responsive grid classes', () => {
      render(<Features features={mockFeatures} />)

      const grid = screen.getByTestId('features-grid')

      // Mobile: single column
      expect(grid).toHaveClass('grid-cols-1')
      // Tablet: 2 columns
      expect(grid).toHaveClass('md:grid-cols-2')
      // Desktop: 3 columns
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('has proper gap spacing', () => {
      render(<Features features={mockFeatures} />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('gap-6')
      expect(grid).toHaveClass('md:gap-8')
    })
  })

  // Custom props tests
  describe('Custom props', () => {
    it('renders custom section title', () => {
      render(<Features sectionTitle="Custom Title" />)

      const heading = screen.getByTestId('features-heading')
      expect(heading).toHaveTextContent('Custom Title')
    })

    it('renders custom section description', () => {
      render(<Features sectionDescription="Custom description text" />)

      const description = screen.getByTestId('features-description')
      expect(description).toHaveTextContent('Custom description text')
    })

    it('renders custom features array', () => {
      const customFeatures: Feature[] = [
        {
          id: 'custom-1',
          icon: 'bolt',
          title: 'Custom Feature',
          description: 'Custom description.',
        },
      ]

      render(<Features features={customFeatures} />)

      const cards = screen.getAllByTestId('feature-card')
      expect(cards).toHaveLength(1)

      const title = screen.getByTestId('feature-title')
      expect(title).toHaveTextContent('Custom Feature')
    })
  })
})
