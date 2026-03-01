import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../utils/render'
import { Home } from '../../../src/pages/Home'
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  describe('Test Case 1: URL shortening feature card', () => {
    it('should display URL shortening feature card with icon and description', () => {
      render(<Home />)

      // Verify the feature section exists
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the URL shortening text
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(
        screen.getByText(/Transform long URLs into short, memorable links/i)
      ).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Analytics and click tracking feature card', () => {
    it('should display Analytics and click tracking feature card', () => {
      render(<Home />)

      // Find analytics feature
      expect(screen.getByText('Analytics & Click Tracking')).toBeInTheDocument()
      expect(
        screen.getByText(/Monitor your link performance with detailed analytics/i)
      ).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Secure authentication feature card', () => {
    it('should display Secure authentication feature card', () => {
      render(<Home />)

      // Find authentication feature
      expect(screen.getByText('Secure Authentication')).toBeInTheDocument()
      expect(
        screen.getByText(/Keep your links safe with robust authentication/i)
      ).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Dashboard management feature card', () => {
    it('should display Dashboard management feature card', () => {
      render(<Home />)

      // Find dashboard feature
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument()
      expect(
        screen.getByText(/Manage all your shortened URLs from a centralized/i)
      ).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Grid layout with 4 cards', () => {
    it('should arrange feature cards in a grid layout with 4 cards', () => {
      render(<Home />)

      // Get the features grid container
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify grid has correct CSS classes for responsive grid layout
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('lg:grid-cols-4')

      // Count the glass cards (feature cards)
      const glassCards = within(featuresGrid).getAllByTestId('glass-card')
      expect(glassCards).toHaveLength(4)
    })
  })

  describe('Test Case 6: Each feature card contains an icon', () => {
    it('should display an icon element in each feature card', () => {
      render(<Home />)

      // Get all feature icons
      const featureIcons = screen.getAllByTestId('feature-icon')

      // Should have 4 icons (one for each feature)
      expect(featureIcons).toHaveLength(4)

      // Each icon should be present in the document
      featureIcons.forEach((icon) => {
        expect(icon).toBeInTheDocument()
        // Icon should contain an SVG element (from lucide-react)
        const svg = icon.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })
  })

  describe('Additional integration tests', () => {
    it('should render the features section as a standalone component', () => {
      render(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Verify section has proper heading
      expect(screen.getByText('Core Features')).toBeInTheDocument()
    })

    it('should have accessible heading structure', () => {
      render(<FeaturesSection />)

      // Check for proper heading
      const heading = screen.getByRole('heading', { level: 2, name: /core features/i })
      expect(heading).toBeInTheDocument()

      // Check for feature titles as h3
      const featureTitles = screen.getAllByTestId('feature-title')
      expect(featureTitles).toHaveLength(4)
    })

    it('should render feature descriptions', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions).toHaveLength(4)

      // Each description should have content
      descriptions.forEach((desc) => {
        expect(desc.textContent?.length).toBeGreaterThan(0)
      })
    })
  })
})
