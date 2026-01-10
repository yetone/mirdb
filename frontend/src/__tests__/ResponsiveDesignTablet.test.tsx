import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import FeaturesSection from '../components/FeaturesSection'
import HeroSection from '../components/HeroSection'
import HowItWorksSection from '../components/HowItWorksSection'
import FooterSection from '../components/FooterSection'

/**
 * Responsive Design - Tablet Viewport Tests (NFR-2)
 *
 * These tests verify that the homepage displays correctly at tablet viewports (768px width).
 * Tailwind CSS uses `md:` prefix for medium breakpoints (768px+).
 *
 * Test cases:
 * 1. Homepage content displays appropriately without overflow or cramping at 768px viewport
 * 2. FeaturesSection displays feature cards in appropriate grid (2 columns at md breakpoint)
 */

describe('Responsive Design - Tablet Viewport (768px)', () => {
  // Test Case 1: Homepage displays correctly at tablet viewport
  describe('Homepage at Tablet Viewport', () => {
    it('should render all main sections of the homepage', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify all main sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should have HeroSection with responsive text classes for tablet viewport', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heading = screen.getByRole('heading', { level: 1 })
      // Check for md:text-6xl which applies at tablet (768px+) viewport
      expect(heading).toHaveClass('md:text-6xl')
    })

    it('should have HeroSection with responsive padding for tablet', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroContent = screen.getByTestId('hero-section').querySelector('.relative.z-10')
      expect(heroContent).toHaveClass('sm:px-6')
      expect(heroContent).toHaveClass('lg:px-8')
    })

    it('should have HeroSection subheadline with responsive text size', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const subheadline = screen.getByTestId('hero-subheadline')
      // Check for md:text-2xl which applies at tablet viewport
      expect(subheadline).toHaveClass('md:text-2xl')
    })

    it('should have HeroSection CTAs with responsive flex layout', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // The CTA container uses flex-col on mobile and sm:flex-row on small screens and up
      const ctaContainer = screen.getByRole('link', { name: /get started/i }).parentElement
      expect(ctaContainer).toHaveClass('flex')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  // Test Case 2: FeaturesSection at tablet viewport
  describe('FeaturesSection at Tablet Viewport', () => {
    it('should have grid layout with 2 columns at medium (tablet) breakpoint', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // md:grid-cols-2 means 2 columns at 768px+ viewport (tablet)
      expect(grid).toHaveClass('md:grid-cols-2')
    })

    it('should have single column layout on mobile (base case)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // grid-cols-1 is the base/mobile layout
      expect(grid).toHaveClass('grid-cols-1')
    })

    it('should have 3 columns only at large breakpoint (desktop)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // lg:grid-cols-3 only applies at 1024px+ (desktop), not tablet
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('should render all 3 feature cards for tablet display', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)
    })

    it('should have appropriate gap spacing for tablet viewport', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // gap-8 provides consistent spacing at all viewports
      expect(grid).toHaveClass('gap-8')
    })

    it('should have container with max-width constraint for tablet', () => {
      render(<FeaturesSection />)

      const container = screen.getByTestId('features-section').querySelector('.container')
      expect(container).toHaveClass('max-w-6xl')
      expect(container).toHaveClass('mx-auto')
    })
  })

  // Additional responsive tests for HowItWorksSection
  describe('HowItWorksSection at Tablet Viewport', () => {
    it('should have responsive grid with 3 columns at medium breakpoint', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('steps-container')
      // md:grid-cols-3 applies at 768px+ viewport
      expect(stepsContainer).toHaveClass('md:grid-cols-3')
    })

    it('should have single column layout on mobile (base case)', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('grid-cols-1')
    })

    it('should have responsive heading text size', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toHaveClass('md:text-4xl')
    })
  })

  // Additional responsive tests for FooterSection
  describe('FooterSection at Tablet Viewport', () => {
    it('should have responsive grid layout with 3 columns at tablet', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const footerGrid = screen.getByTestId('footer-section').querySelector('.grid')
      // md:grid-cols-3 applies at 768px+ viewport
      expect(footerGrid).toHaveClass('md:grid-cols-3')
    })

    it('should have single column layout on mobile (base case)', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const footerGrid = screen.getByTestId('footer-section').querySelector('.grid')
      expect(footerGrid).toHaveClass('grid-cols-1')
    })
  })

  // Integration test: Full homepage responsive structure
  describe('Full Homepage Tablet Responsive Integration', () => {
    it('should have proper responsive structure for tablet display', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Hero section responsive check
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading).toHaveClass('md:text-6xl')

      // Features section responsive check
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')

      // How It Works section responsive check
      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('md:grid-cols-3')

      // Footer section responsive check
      const footerGrid = screen.getByTestId('footer-section').querySelector('.grid')
      expect(footerGrid).toHaveClass('md:grid-cols-3')
    })

    it('should not have horizontal overflow issues (content contained)', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Check that main content areas use container and max-width constraints
      const featuresContainer = screen.getByTestId('features-section').querySelector('.container')
      expect(featuresContainer).toHaveClass('max-w-6xl')

      const howItWorksContainer = screen.getByTestId('how-it-works-section').querySelector('.container')
      expect(howItWorksContainer).toHaveClass('max-w-6xl')

      const footerContainer = screen.getByTestId('footer-section').querySelector('.container')
      expect(footerContainer).toHaveClass('max-w-6xl')
    })

    it('should have consistent padding across all sections', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Features section has padding
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('px-4')
      expect(featuresSection).toHaveClass('py-16')

      // How It Works section has padding
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('px-4')
      expect(howItWorksSection).toHaveClass('py-16')

      // Footer section has padding
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveClass('px-4')
    })
  })
})
