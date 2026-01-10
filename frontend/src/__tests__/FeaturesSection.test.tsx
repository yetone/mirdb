import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeaturesSection from '../components/FeaturesSection'

describe('FeaturesSection', () => {
  // Test Case 1: Component renders with exactly 3 feature cards
  describe('Feature Cards Rendering', () => {
    it('should render exactly 3 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)
    })

    it('should render the features section container', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()
    })

    it('should render a "Powerful Features" heading', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { name: /powerful features/i })
      expect(heading).toBeInTheDocument()
    })
  })

  // Test Case 2: URL Shortening feature card
  describe('URL Shortening Feature Card', () => {
    it('should display URL Shortening title', () => {
      render(<FeaturesSection />)

      const title = screen.getByText('URL Shortening')
      expect(title).toBeInTheDocument()
    })

    it('should display URL Shortening icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('url-shortening-icon')
      expect(icon).toBeInTheDocument()
    })

    it('should display description about creating short links', () => {
      render(<FeaturesSection />)

      const description = screen.getByText(/create memorable, short links instantly/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 3: Analytics Dashboard feature card
  describe('Analytics Dashboard Feature Card', () => {
    it('should display Analytics Dashboard title', () => {
      render(<FeaturesSection />)

      const title = screen.getByText('Analytics Dashboard')
      expect(title).toBeInTheDocument()
    })

    it('should display analytics icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('analytics-icon')
      expect(icon).toBeInTheDocument()
    })

    it('should display description about tracking clicks', () => {
      render(<FeaturesSection />)

      const description = screen.getByText(/track clicks, locations, and referrers/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 4: Link Management feature card
  describe('Link Management Feature Card', () => {
    it('should display Link Management title', () => {
      render(<FeaturesSection />)

      const title = screen.getByText('Link Management')
      expect(title).toBeInTheDocument()
    })

    it('should display link management icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('link-management-icon')
      expect(icon).toBeInTheDocument()
    })

    it('should display description about organizing links', () => {
      render(<FeaturesSection />)

      const description = screen.getByText(/organize and manage all your links/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 5: Three-column grid layout on desktop
  describe('Grid Layout', () => {
    it('should have grid layout container', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })

    it('should have responsive grid classes for three columns on desktop', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // Check for lg:grid-cols-3 which creates three columns on desktop (1024px+)
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('should have single column layout on mobile (default)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid-cols-1')
    })

    it('should have two columns on medium screens', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('md:grid-cols-2')
    })

    it('should have appropriate gap between cards', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('gap-8')
    })
  })

  // Test Case 6: GlassMorphismCard usage
  describe('GlassMorphismCard Styling', () => {
    it('should render each feature card with GlassMorphismCard component', () => {
      render(<FeaturesSection />)

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')
      expect(glassmorphismCards).toHaveLength(3)
    })

    it('should have consistent glassmorphism styling on all cards', () => {
      render(<FeaturesSection />)

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')

      glassmorphismCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('rounded-2xl')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should have semi-transparent background on cards', () => {
      render(<FeaturesSection />)

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')

      glassmorphismCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100/30')
      })
    })

    it('should have border on cards', () => {
      render(<FeaturesSection />)

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')

      glassmorphismCards.forEach((card) => {
        expect(card).toHaveClass('border')
        expect(card).toHaveClass('border-base-content/10')
      })
    })
  })

  // Additional tests for visual structure
  describe('Visual Structure', () => {
    it('should have padding on feature cards', () => {
      render(<FeaturesSection />)

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')

      glassmorphismCards.forEach((card) => {
        expect(card).toHaveClass('p-6')
      })
    })

    it('should center content within each card', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/feature-card-\d/)

      featureCards.forEach((card) => {
        expect(card).toHaveClass('text-center')
        expect(card).toHaveClass('items-center')
      })
    })

    it('should have proper icon styling', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        expect(icon).toHaveClass('w-12')
        expect(icon).toHaveClass('h-12')
      })
    })
  })
})
