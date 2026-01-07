import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import SocialProofSection from '../src/components/SocialProofSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  )
}

describe('Social Proof Section Display', () => {
  // Test Case 1: Query for statistics or social proof section
  describe('Test Case 1: Social Proof Section Container', () => {
    it('should render the social proof section with statistics or testimonials', () => {
      renderWithRouter(<Home />)

      const socialProofSection = screen.getByTestId('social-proof-section')
      expect(socialProofSection).toBeInTheDocument()

      // Check for statistics content (e.g., 'X links shortened') or testimonials
      const statsOrTestimonials = screen.queryAllByTestId(/stat-/i)
      expect(statsOrTestimonials.length).toBeGreaterThan(0)
    })

    it('should contain statistics like links shortened or clicks tracked', () => {
      renderWithRouter(<SocialProofSection />)

      // Check for statistics content
      const socialProofSection = screen.getByTestId('social-proof-section')
      expect(socialProofSection).toBeInTheDocument()

      // Look for statistics values
      const statValues = screen.getAllByTestId('stat-value')
      expect(statValues.length).toBeGreaterThan(0)

      // Check that statistics have labels
      const statLabels = screen.getAllByTestId('stat-label')
      expect(statLabels.length).toBeGreaterThan(0)
      expect(statLabels.length).toEqual(statValues.length)
    })
  })

  // Test Case 2: Check statistics values format
  describe('Test Case 2: Statistics Values Format', () => {
    it('should display formatted numbers or meaningful placeholder values', () => {
      renderWithRouter(<SocialProofSection />)

      const statValues = screen.getAllByTestId('stat-value')

      // Each stat value should have formatted content
      statValues.forEach((statValue) => {
        const textContent = statValue.textContent || ''
        expect(textContent).not.toBe('')
        // Check that value contains either numbers, formatted numbers with symbols (+, K, M), or percentage
        expect(textContent).toMatch(/[\d,+KMB%]+/)
      })
    })

    it('should have descriptive labels for each statistic', () => {
      renderWithRouter(<SocialProofSection />)

      const statLabels = screen.getAllByTestId('stat-label')

      // Each label should have non-empty descriptive text
      statLabels.forEach((label) => {
        const textContent = label.textContent || ''
        expect(textContent).not.toBe('')
        expect(textContent.length).toBeGreaterThan(0)
      })
    })

    it('should display at least 3 statistics', () => {
      renderWithRouter(<SocialProofSection />)

      const statItems = screen.getAllByTestId(/stat-item/)
      expect(statItems.length).toBeGreaterThanOrEqual(3)
    })
  })

  // Additional tests for social proof content quality
  describe('Additional Social Proof Tests', () => {
    it('should have a section heading for context', () => {
      renderWithRouter(<SocialProofSection />)

      // Should have a heading element (h2)
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading.textContent).not.toBe('')
    })

    it('should display statistics related to URL shortening service', () => {
      renderWithRouter(<SocialProofSection />)

      const socialProofSection = screen.getByTestId('social-proof-section')

      // Check for relevant keywords in the section
      const sectionText = socialProofSection.textContent?.toLowerCase() || ''
      const hasRelevantContent =
        sectionText.includes('links') ||
        sectionText.includes('clicks') ||
        sectionText.includes('users') ||
        sectionText.includes('shortened') ||
        sectionText.includes('tracked')

      expect(hasRelevantContent).toBe(true)
    })

    it('should render statistics items with proper structure', () => {
      renderWithRouter(<SocialProofSection />)

      const statItems = screen.getAllByTestId(/stat-item/)

      statItems.forEach((item) => {
        // Each item should contain both a value and label
        const value = item.querySelector('[data-testid="stat-value"]')
        const label = item.querySelector('[data-testid="stat-label"]')

        expect(value).toBeInTheDocument()
        expect(label).toBeInTheDocument()
      })
    })
  })
})
