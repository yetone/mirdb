import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeaturesSection from '../FeaturesSection'

describe('FeaturesSection', () => {
  describe('Test Case 1: Feature cards display', () => {
    it('should display at least 3 feature cards (URL shortening, analytics, sharing)', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)

      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Icon elements', () => {
    it('should have each feature card contain an icon element (svg)', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const svgIcon = card.querySelector('svg')
        expect(svgIcon).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 3: Title and description', () => {
    it('should have each feature card contain a title (h3) and description text', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const title = card.querySelector('h3')
        expect(title).toBeInTheDocument()
        expect(title?.textContent).toBeTruthy()

        const paragraphs = card.querySelectorAll('p')
        expect(paragraphs.length).toBeGreaterThan(0)
        const hasDescription = Array.from(paragraphs).some(
          (p) => p.textContent && p.textContent.length > 0
        )
        expect(hasDescription).toBe(true)
      })
    })

    it('should have meaningful titles for each feature', () => {
      render(<FeaturesSection />)

      const titles = screen.getAllByRole('heading', { level: 3 })
      expect(titles.length).toBeGreaterThanOrEqual(3)

      const titleTexts = titles.map((t) => t.textContent)
      expect(titleTexts).toContain('URL Shortening')
      expect(titleTexts).toContain('Analytics Dashboard')
      expect(titleTexts).toContain('Share Statistics')
    })

    it('should have descriptions for each feature', () => {
      render(<FeaturesSection />)

      expect(
        screen.getByText(/Create memorable, short links in seconds/)
      ).toBeInTheDocument()
      expect(
        screen.getByText(/Track clicks, referrers, and geographic data/)
      ).toBeInTheDocument()
      expect(
        screen.getByText(/Share your link analytics publicly/)
      ).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Responsive layout', () => {
    it('should use grid layout for responsive card arrangement', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: /features/i })
      expect(section).toBeInTheDocument()

      const gridContainer = section.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      expect(gridContainer?.classList.contains('grid')).toBe(true)
      expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
      expect(gridContainer?.classList.contains('md:grid-cols-2')).toBe(true)
      expect(gridContainer?.classList.contains('lg:grid-cols-3')).toBe(true)
    })

    it('should have proper gap spacing between cards', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: /features/i })
      const gridContainer = section.querySelector('.grid')

      expect(gridContainer?.classList.contains('gap-8')).toBe(true)
    })
  })

  describe('Accessibility', () => {
    it('should have a proper section landmark with aria-labelledby', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: /features/i })
      expect(section).toBeInTheDocument()
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('should have a main heading for the section', () => {
      render(<FeaturesSection />)

      const mainHeading = screen.getByRole('heading', { level: 2 })
      expect(mainHeading).toBeInTheDocument()
      expect(mainHeading.textContent).toContain('Powerful Features')
    })

    it('should have icons marked as aria-hidden', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const svgIcon = card.querySelector('svg')
        expect(svgIcon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })
})
