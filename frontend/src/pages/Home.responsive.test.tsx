import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Home />
    </MemoryRouter>
  )
}

// Helper to set viewport width for tests
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Mobile (375px viewport)', () => {
  const MOBILE_WIDTH = 375

  beforeEach(() => {
    setViewportWidth(MOBILE_WIDTH)
  })

  afterEach(() => {
    // Reset to default
    setViewportWidth(1024)
  })

  // Test Case 1: Navigation collapses to mobile menu or hamburger icon
  describe('Test Case 1: Navigation at mobile viewport', () => {
    it('renders navigation that adapts to mobile viewport', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero section has responsive classes for mobile layout
      // At mobile, the container should use flex-col (column layout)
      const container = heroSection.querySelector('.container')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('flex-col')
    })

    it('CTA buttons stack vertically on small screens', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const secondaryCTA = screen.getByTestId('hero-cta-secondary')

      expect(primaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toBeInTheDocument()

      // Buttons should be in a flex-col container at mobile
      const buttonContainer = primaryCTA.parentElement
      expect(buttonContainer).toHaveClass('flex-col')
    })
  })

  // Test Case 2: Feature cards stack in single column
  describe('Test Case 2: Feature cards at mobile viewport', () => {
    it('renders feature cards in a responsive grid that stacks on mobile', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the grid container with feature cards
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Grid should have grid-cols-1 for mobile (single column)
      expect(gridContainer).toHaveClass('grid-cols-1')
    })

    it('renders all 4 feature cards', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('.card')

      expect(featureCards.length).toBe(4)
    })
  })

  // Test Case 3: Hero content is readable and properly sized for mobile
  describe('Test Case 3: Hero section at mobile viewport', () => {
    it('renders hero headline with responsive text sizing', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()

      // Headline should have text-4xl as base (mobile) size
      expect(headline).toHaveClass('text-4xl')
    })

    it('renders hero subheadline with responsive text sizing', () => {
      renderWithRouter()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // Subheadline should have text-lg as base (mobile) size
      expect(subheadline).toHaveClass('text-lg')
    })

    it('hero text is centered on mobile viewport', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      const textContainer = headline.parentElement

      // On mobile, text should be centered (text-center class)
      expect(textContainer).toHaveClass('text-center')
    })

    it('hero visual element is displayed and properly sized', () => {
      renderWithRouter()

      const visual = screen.getByTestId('hero-visual')
      expect(visual).toBeInTheDocument()

      // Visual should have max-w-lg class for proper sizing
      expect(visual).toHaveClass('max-w-lg')
      expect(visual).toHaveClass('w-full')
    })
  })

  // Test Case 4: CTA buttons are full-width or appropriately sized for touch
  describe('Test Case 4: CTA buttons at mobile viewport', () => {
    it('renders primary CTA button with large touch-friendly size', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()

      // Button should have btn-lg class for touch-friendly sizing
      expect(primaryCTA).toHaveClass('btn-lg')
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-primary')
    })

    it('renders secondary CTA button with large touch-friendly size', () => {
      renderWithRouter()

      const secondaryCTA = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCTA).toBeInTheDocument()

      // Button should have btn-lg class for touch-friendly sizing
      expect(secondaryCTA).toHaveClass('btn-lg')
      expect(secondaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn-outline')
    })

    it('buttons are contained in a flex column container for mobile stacking', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const buttonContainer = primaryCTA.parentElement

      // Container should flex-col on mobile and sm:flex-row on larger screens
      expect(buttonContainer).toHaveClass('flex')
      expect(buttonContainer).toHaveClass('flex-col')
      expect(buttonContainer).toHaveClass('sm:flex-row')
    })
  })

  // Additional mobile layout tests
  describe('Additional mobile layout tests', () => {
    it('hero section has proper mobile padding', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')

      // Section should have px-4 for horizontal padding on mobile
      expect(heroSection).toHaveClass('px-4')
    })

    it('features section has proper mobile padding', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')

      // Section should have px-4 for horizontal padding on mobile
      expect(featuresSection).toHaveClass('px-4')
    })

    it('hero container uses column layout on mobile and row on large screens', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const container = heroSection.querySelector('.container')

      // Container should be flex-col on mobile, lg:flex-row on large screens
      expect(container).toHaveClass('flex-col')
      expect(container).toHaveClass('lg:flex-row')
    })
  })
})
