import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeaturesSection } from '../FeaturesSection'

describe('FeaturesSection', () => {
  // Test Case 1: Features section is present with appropriate styling
  describe('Test Case 1: Features section container', () => {
    it('should render the features section with appropriate styling', () => {
      render(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveAttribute('id', 'features')
      expect(featuresSection.tagName).toBe('SECTION')
    })

    it('should have the section heading', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { level: 2, name: /powerful features/i })
      expect(heading).toBeInTheDocument()
    })

    it('should have a description paragraph', () => {
      render(<FeaturesSection />)

      const description = screen.getByText(/everything you need to manage and track your links/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 2: At least 3 feature cards are rendered
  describe('Test Case 2: Feature cards count', () => {
    it('should render at least 3 feature cards (URL shortening, analytics, link management)', () => {
      render(<FeaturesSection />)

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const analyticsCard = screen.getByTestId('feature-card-analytics')
      const linkManagementCard = screen.getByTestId('feature-card-link-management')

      expect(urlShorteningCard).toBeInTheDocument()
      expect(analyticsCard).toBeInTheDocument()
      expect(linkManagementCard).toBeInTheDocument()
    })

    it('should render exactly 4 feature cards', () => {
      render(<FeaturesSection />)

      const featureCardsContainer = screen.getByTestId('feature-cards-container')
      const cards = featureCardsContainer.querySelectorAll('[data-testid^="feature-card-"]')

      expect(cards.length).toBeGreaterThanOrEqual(3)
      expect(cards.length).toBe(4) // 4 cards as specified in PRD
    })
  })

  // Test Case 3: Each feature card has title, description, and visual element
  describe('Test Case 3: Feature card content', () => {
    it('should have URL shortening card with title, description, and icon', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-url-shortening')
      const description = screen.getByTestId('feature-description-url-shortening')
      const icon = screen.getByTestId('feature-icon-url-shortening')

      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent('URL Shortening')
      expect(description).toBeInTheDocument()
      expect(description.textContent?.length).toBeGreaterThan(0)
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('should have analytics card with title, description, and icon', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-analytics')
      const description = screen.getByTestId('feature-description-analytics')
      const icon = screen.getByTestId('feature-icon-analytics')

      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent('Analytics Dashboard')
      expect(description).toBeInTheDocument()
      expect(description.textContent?.length).toBeGreaterThan(0)
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('should have link management card with title, description, and icon', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-link-management')
      const description = screen.getByTestId('feature-description-link-management')
      const icon = screen.getByTestId('feature-icon-link-management')

      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent('Link Management')
      expect(description).toBeInTheDocument()
      expect(description.textContent?.length).toBeGreaterThan(0)
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('should have themes card with title, description, and icon', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-themes')
      const description = screen.getByTestId('feature-description-themes')
      const icon = screen.getByTestId('feature-icon-themes')

      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent('Multiple Themes')
      expect(description).toBeInTheDocument()
      expect(description.textContent?.length).toBeGreaterThan(0)
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('all feature cards should have proper heading structure', () => {
      render(<FeaturesSection />)

      const featureHeadings = screen.getAllByRole('heading', { level: 3 })
      expect(featureHeadings.length).toBe(4)

      const expectedTitles = ['URL Shortening', 'Analytics Dashboard', 'Link Management', 'Multiple Themes']
      featureHeadings.forEach((heading, index) => {
        expect(heading).toHaveTextContent(expectedTitles[index])
      })
    })
  })

  // Test Case 4: Feature cards use GlassMorphismCard component pattern
  describe('Test Case 4: GlassMorphismCard component usage', () => {
    it('should use GlassMorphismCard styling pattern for URL shortening card', () => {
      render(<FeaturesSection />)

      const card = screen.getByTestId('feature-card-url-shortening')
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('backdrop-blur-md')
    })

    it('should use GlassMorphismCard styling pattern for analytics card', () => {
      render(<FeaturesSection />)

      const card = screen.getByTestId('feature-card-analytics')
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('backdrop-blur-md')
    })

    it('should use GlassMorphismCard styling pattern for link management card', () => {
      render(<FeaturesSection />)

      const card = screen.getByTestId('feature-card-link-management')
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('backdrop-blur-md')
    })

    it('should use GlassMorphismCard styling pattern for themes card', () => {
      render(<FeaturesSection />)

      const card = screen.getByTestId('feature-card-themes')
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('backdrop-blur-md')
    })

    it('all feature cards should have consistent GlassMorphismCard styling', () => {
      render(<FeaturesSection />)

      const featureCardsContainer = screen.getByTestId('feature-cards-container')
      const cards = featureCardsContainer.querySelectorAll('[data-testid^="feature-card-"]')

      cards.forEach((card) => {
        expect(card).toHaveClass('glassmorphism-card')
        expect(card).toHaveClass('rounded-2xl')
        expect(card).toHaveClass('shadow-xl')
      })
    })
  })
})
