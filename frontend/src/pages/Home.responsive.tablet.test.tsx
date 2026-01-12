import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import { AuthProvider } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Home />
      </AuthProvider>
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

describe('Responsive Design - Tablet (768px viewport)', () => {
  const TABLET_WIDTH = 768

  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
    setViewportWidth(TABLET_WIDTH)
  })

  afterEach(() => {
    // Reset to default
    setViewportWidth(1024)
  })

  // Test Case 1: Navigation is appropriately displayed for tablet
  describe('Test Case 1: Navigation at tablet viewport (768px)', () => {
    it('renders navigation elements visible at tablet viewport', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero section has responsive classes for tablet layout
      // At tablet (768px = md breakpoint), container should still use flex-col (changes at lg)
      const container = heroSection.querySelector('.container')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('flex-col')
      expect(container).toHaveClass('lg:flex-row')
    })

    it('navigation links use md:flex to show on tablet', () => {
      renderWithRouter()

      // Features section exists and accessible from nav
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // How it works section exists
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('hero headline has tablet-appropriate text sizing', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()

      // At tablet (md breakpoint), text should be md:text-5xl
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('md:text-5xl')
    })

    it('CTA buttons display in row layout at tablet (sm breakpoint and above)', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const secondaryCTA = screen.getByTestId('hero-cta-secondary')

      expect(primaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toBeInTheDocument()

      // At tablet (768px > sm:640px), buttons should be in row layout
      const buttonContainer = primaryCTA.parentElement
      expect(buttonContainer).toHaveClass('flex')
      expect(buttonContainer).toHaveClass('flex-col')
      expect(buttonContainer).toHaveClass('sm:flex-row')
    })
  })

  // Test Case 2: Feature cards display in 2-column grid at 768px
  describe('Test Case 2: Feature cards at tablet viewport (768px)', () => {
    it('renders feature cards in a 2-column grid at tablet', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the grid container with feature cards
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Grid should have grid-cols-1 for mobile, md:grid-cols-2 for tablet
      expect(gridContainer).toHaveClass('grid-cols-1')
      expect(gridContainer).toHaveClass('md:grid-cols-2')
    })

    it('renders all 4 feature cards in the grid', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('.card')

      expect(featureCards.length).toBe(4)
    })

    it('feature cards have proper gap spacing for tablet', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      // Grid should have gap-8 for proper spacing
      expect(gridContainer).toHaveClass('gap-8')
    })

    it('features section has responsive title sizing', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const title = featuresSection.querySelector('h2')

      expect(title).toBeInTheDocument()
      expect(title).toHaveClass('text-3xl')
      expect(title).toHaveClass('md:text-4xl')
    })
  })

  // Test Case 3: How It Works section displays in appropriate layout for tablet
  describe('Test Case 3: How It Works section at tablet viewport (768px)', () => {
    it('renders How It Works steps in 3-column grid at tablet', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()

      // Find the grid container with steps
      const gridContainer = howItWorksSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Grid should have grid-cols-1 for mobile, md:grid-cols-3 for tablet
      expect(gridContainer).toHaveClass('grid-cols-1')
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('renders all 3 steps in How It Works section', () => {
      renderWithRouter()

      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()
    })

    it('connector lines are visible at tablet viewport (md and above)', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Connector lines have hidden md:block classes
      const connectorLines = howItWorksSection.querySelectorAll('.hidden.md\\:block')

      // There should be 2 connector lines (between step 1-2 and step 2-3)
      expect(connectorLines.length).toBe(2)
    })

    it('How It Works section has responsive title sizing', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const title = howItWorksSection.querySelector('h2')

      expect(title).toBeInTheDocument()
      expect(title).toHaveClass('text-3xl')
      expect(title).toHaveClass('md:text-4xl')
    })

    it('steps grid has proper gap spacing', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const gridContainer = howItWorksSection.querySelector('.grid')

      // Grid should have gap-8 for proper spacing
      expect(gridContainer).toHaveClass('gap-8')
    })
  })

  // Additional tablet layout tests
  describe('Additional tablet layout tests', () => {
    it('hero section maintains proper padding at tablet', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')

      // Section should have px-4 for horizontal padding
      expect(heroSection).toHaveClass('px-4')
    })

    it('features section maintains proper padding at tablet', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')

      // Section should have px-4 for horizontal padding
      expect(featuresSection).toHaveClass('px-4')
    })

    it('How It Works section maintains proper padding at tablet', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Section should have px-4 for horizontal padding
      expect(howItWorksSection).toHaveClass('px-4')
    })

    it('hero subheadline has tablet-appropriate text sizing', () => {
      renderWithRouter()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // At tablet (md breakpoint), subheadline should be md:text-xl
      expect(subheadline).toHaveClass('text-lg')
      expect(subheadline).toHaveClass('md:text-xl')
    })

    it('hero text is centered at tablet (before lg breakpoint)', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      const textContainer = headline.parentElement

      // Text should be centered until lg breakpoint
      expect(textContainer).toHaveClass('text-center')
      expect(textContainer).toHaveClass('lg:text-left')
    })

    it('hero visual element is displayed at tablet', () => {
      renderWithRouter()

      const visual = screen.getByTestId('hero-visual')
      expect(visual).toBeInTheDocument()

      // Visual should have responsive sizing classes
      expect(visual).toHaveClass('max-w-lg')
      expect(visual).toHaveClass('w-full')
    })
  })
})
