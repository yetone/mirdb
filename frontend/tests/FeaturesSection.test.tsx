import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import FeaturesSection from '../src/components/FeaturesSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  )
}

describe('Features Section Display', () => {
  // Test Case 1: Features section container exists with proper section heading
  describe('Test Case 1: Features Section Container', () => {
    it('should render the features section with proper heading', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const heading = screen.getByRole('heading', { name: /powerful features/i })
      expect(heading).toBeInTheDocument()
    })
  })

  // Test Case 2: At least 3 feature card elements are rendered
  describe('Test Case 2: Feature Cards Count', () => {
    it('should render at least 3 feature cards', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })
  })

  // Test Case 3: URL shortening feature card exists
  describe('Test Case 3: URL Shortening Feature Card', () => {
    it('should display a feature card for URL shortening with icon and description', () => {
      renderWithRouter(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      const urlShorteningTitle = titles.find(
        (title) =>
          title.textContent?.toLowerCase().includes('url') ||
          title.textContent?.toLowerCase().includes('short') ||
          title.textContent?.toLowerCase().includes('link')
      )
      expect(urlShorteningTitle).toBeTruthy()

      // Verify the card has a description
      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions.length).toBeGreaterThan(0)

      // Verify icons are present (cards have icon containers)
      const featureCards = screen.getAllByTestId('feature-card')
      featureCards.forEach((card) => {
        const iconContainer = card.querySelector('.rounded-full')
        expect(iconContainer).toBeInTheDocument()
      })
    })
  })

  // Test Case 4: Analytics feature card exists
  describe('Test Case 4: Analytics Feature Card', () => {
    it('should display a feature card for analytics/click tracking with icon and description', () => {
      renderWithRouter(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      const analyticsTitle = titles.find(
        (title) =>
          title.textContent?.toLowerCase().includes('analytics') ||
          title.textContent?.toLowerCase().includes('click') ||
          title.textContent?.toLowerCase().includes('track')
      )
      expect(analyticsTitle).toBeTruthy()

      // Verify there are descriptions for all feature cards
      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions.length).toBeGreaterThanOrEqual(3)
    })
  })

  // Test Case 5: Link management feature card exists
  describe('Test Case 5: Link Management Feature Card', () => {
    it('should display a feature card for link management/dashboard with icon and description', () => {
      renderWithRouter(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      const linkManagementTitle = titles.find(
        (title) =>
          title.textContent?.toLowerCase().includes('management') ||
          title.textContent?.toLowerCase().includes('dashboard') ||
          title.textContent?.toLowerCase().includes('manage')
      )
      expect(linkManagementTitle).toBeTruthy()

      // Verify the feature card has both title and description
      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions.length).toEqual(titles.length)
    })
  })

  // Additional tests for completeness
  describe('Additional Feature Section Tests', () => {
    it('should render all feature cards with icons, titles, and descriptions', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      const titles = screen.getAllByTestId('feature-title')
      const descriptions = screen.getAllByTestId('feature-description')

      // Each card should have a title and description
      expect(titles.length).toBe(featureCards.length)
      expect(descriptions.length).toBe(featureCards.length)

      // Verify each title is not empty
      titles.forEach((title) => {
        expect(title.textContent).not.toBe('')
      })

      // Verify each description is not empty
      descriptions.forEach((description) => {
        expect(description.textContent).not.toBe('')
      })
    })

    it('should have exactly 4 feature cards as per PRD', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)
    })
  })
})
