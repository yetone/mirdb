import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from './HeroSection'
import { ThemeProvider } from '../contexts/ThemeContext'

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('HeroSection', () => {
  // Test Case 2: Unit test - Component renders without errors and contains h1 headline element
  describe('Component Rendering', () => {
    it('renders without errors', () => {
      renderWithProviders(<HeroSection />)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('contains h1 headline element with compelling headline', () => {
      renderWithProviders(<HeroSection />)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten. Share. Analyze.')
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<HeroSection data-testid="custom-hero" />)
      expect(screen.getByTestId('custom-hero')).toBeInTheDocument()
    })
  })

  // Test Case 3: Integration test - BackgroundEffect component is used
  describe('Background Integration', () => {
    it('uses BackgroundEffect component for gradient/animated background', () => {
      renderWithProviders(<HeroSection />)
      expect(screen.getByTestId('hero-background')).toBeInTheDocument()
    })

    it('BackgroundEffect is positioned behind the content', () => {
      renderWithProviders(<HeroSection />)
      const background = screen.getByTestId('hero-background')
      expect(background).toHaveClass('-z-10')
    })
  })

  // Test Case 4: Unit test - CTA button styles
  describe('CTA Button Styles', () => {
    it('renders primary CTA button with "Get Started Free" text', () => {
      renderWithProviders(<HeroSection />)
      const primaryCta = screen.getByTestId('cta-get-started')
      expect(primaryCta).toBeInTheDocument()
      expect(primaryCta).toHaveTextContent('Get Started Free')
    })

    it('renders secondary CTA button with "Login" text', () => {
      renderWithProviders(<HeroSection />)
      const secondaryCta = screen.getByTestId('cta-login')
      expect(secondaryCta).toBeInTheDocument()
      expect(secondaryCta).toHaveTextContent('Login')
    })

    it('primary CTA has distinct primary styling', () => {
      renderWithProviders(<HeroSection />)
      const primaryCta = screen.getByTestId('cta-get-started')
      expect(primaryCta).toHaveClass('bg-primary')
    })

    it('secondary CTA has distinct secondary styling', () => {
      renderWithProviders(<HeroSection />)
      const secondaryCta = screen.getByTestId('cta-login')
      expect(secondaryCta).toHaveClass('bg-secondary')
    })

    it('CTA buttons link to correct routes', () => {
      renderWithProviders(<HeroSection />)
      const primaryCta = screen.getByTestId('cta-get-started')
      const secondaryCta = screen.getByTestId('cta-login')

      expect(primaryCta).toHaveAttribute('href', '/register')
      expect(secondaryCta).toHaveAttribute('href', '/login')
    })
  })

  describe('Subheadline', () => {
    it('displays supporting subheadline explaining the service', () => {
      renderWithProviders(<HeroSection />)
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toContain('Transform your long URLs')
    })
  })

  describe('Accessibility', () => {
    it('has proper heading hierarchy starting with h1', () => {
      renderWithProviders(<HeroSection />)
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()
    })

    it('navigation section has proper aria-label', () => {
      renderWithProviders(<HeroSection />)
      const nav = screen.getByRole('navigation', { name: 'Page sections' })
      expect(nav).toBeInTheDocument()
    })
  })

  describe('Layout and Visibility', () => {
    it('hero section has full-width layout', () => {
      renderWithProviders(<HeroSection />)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('min-h-screen')
    })

    it('renders two CTA buttons visible and distinguishable', () => {
      renderWithProviders(<HeroSection />)
      const buttons = screen.getAllByRole('link')
      const ctaButtons = buttons.filter(btn =>
        btn.getAttribute('data-testid') === 'cta-get-started' ||
        btn.getAttribute('data-testid') === 'cta-login'
      )
      expect(ctaButtons).toHaveLength(2)
    })
  })
})
