import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import ErrorBoundary from '../components/ErrorBoundary'

// Mock components that simulate homepage sections
function MockHeroSection({ shouldThrow = false }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('HeroSection render error')
  }
  return <section data-testid="hero-section">Hero Section Content</section>
}

function MockFeaturesSection({ shouldThrow = false }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('FeaturesSection render error')
  }
  return <section data-testid="features-section">Features Section Content</section>
}

function MockHowItWorksSection({ shouldThrow = false }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('HowItWorksSection render error')
  }
  return <section data-testid="how-it-works-section">How It Works Section Content</section>
}

function MockFooterSection({ shouldThrow = false }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('FooterSection render error')
  }
  return <footer data-testid="footer-section">Footer Section Content</footer>
}

// Simulated Home page with ErrorBoundary wrapping each section
function MockHomePage({
  heroThrows = false,
  featuresThrows = false,
  howItWorksThrows = false,
  footerThrows = false,
}: {
  heroThrows?: boolean
  featuresThrows?: boolean
  howItWorksThrows?: boolean
  footerThrows?: boolean
}) {
  return (
    <BrowserRouter>
      <main data-testid="home-page">
        <ErrorBoundary sectionName="Hero">
          <MockHeroSection shouldThrow={heroThrows} />
        </ErrorBoundary>
        <ErrorBoundary sectionName="Features">
          <MockFeaturesSection shouldThrow={featuresThrows} />
        </ErrorBoundary>
        <ErrorBoundary sectionName="How It Works">
          <MockHowItWorksSection shouldThrow={howItWorksThrows} />
        </ErrorBoundary>
        <ErrorBoundary sectionName="Footer">
          <MockFooterSection shouldThrow={footerThrows} />
        </ErrorBoundary>
      </main>
    </BrowserRouter>
  )
}

describe('ErrorBoundary Integration - Graceful Degradation', () => {
  // Suppress console.error during these tests since we expect errors
  const originalConsoleError = console.error
  beforeEach(() => {
    console.error = vi.fn()
  })
  afterEach(() => {
    console.error = originalConsoleError
  })

  describe('Test Case 2: Single section fails to render - Other sections continue to display normally', () => {
    it('should render all sections when none throw errors', () => {
      render(<MockHomePage />)

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should display other sections when HeroSection fails', () => {
      render(<MockHomePage heroThrows={true} />)

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Hero section should show error fallback
      expect(screen.queryByTestId('hero-section')).not.toBeInTheDocument()
      expect(screen.getByText('Unable to load Hero')).toBeInTheDocument()

      // Other sections should render normally
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should display other sections when FeaturesSection fails', () => {
      render(<MockHomePage featuresThrows={true} />)

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Features section should show error fallback
      expect(screen.queryByTestId('features-section')).not.toBeInTheDocument()
      expect(screen.getByText('Unable to load Features')).toBeInTheDocument()

      // Other sections should render normally
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should display other sections when HowItWorksSection fails', () => {
      render(<MockHomePage howItWorksThrows={true} />)

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // How It Works section should show error fallback
      expect(screen.queryByTestId('how-it-works-section')).not.toBeInTheDocument()
      expect(screen.getByText('Unable to load How It Works')).toBeInTheDocument()

      // Other sections should render normally
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should display other sections when FooterSection fails', () => {
      render(<MockHomePage footerThrows={true} />)

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Footer section should show error fallback
      expect(screen.queryByTestId('footer-section')).not.toBeInTheDocument()
      expect(screen.getByText('Unable to load Footer')).toBeInTheDocument()

      // Other sections should render normally
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should handle multiple sections failing simultaneously', () => {
      render(<MockHomePage heroThrows={true} footerThrows={true} />)

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Failed sections should show error fallbacks
      expect(screen.queryByTestId('hero-section')).not.toBeInTheDocument()
      expect(screen.queryByTestId('footer-section')).not.toBeInTheDocument()
      expect(screen.getByText('Unable to load Hero')).toBeInTheDocument()
      expect(screen.getByText('Unable to load Footer')).toBeInTheDocument()

      // Working sections should render normally
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should render page even if all sections fail', () => {
      render(
        <MockHomePage
          heroThrows={true}
          featuresThrows={true}
          howItWorksThrows={true}
          footerThrows={true}
        />
      )

      // Page container should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // All sections should show error fallbacks
      expect(screen.getByText('Unable to load Hero')).toBeInTheDocument()
      expect(screen.getByText('Unable to load Features')).toBeInTheDocument()
      expect(screen.getByText('Unable to load How It Works')).toBeInTheDocument()
      expect(screen.getByText('Unable to load Footer')).toBeInTheDocument()

      // Should have 4 fallback elements
      const fallbacks = screen.getAllByTestId('error-boundary-fallback')
      expect(fallbacks).toHaveLength(4)
    })
  })

  describe('Error isolation', () => {
    it('should isolate errors to individual boundaries', () => {
      render(<MockHomePage featuresThrows={true} howItWorksThrows={true} />)

      // Each error boundary should have its own fallback
      const fallbacks = screen.getAllByTestId('error-boundary-fallback')
      expect(fallbacks).toHaveLength(2)

      // Verify the correct fallback messages are displayed
      expect(screen.getByText('Unable to load Features')).toBeInTheDocument()
      expect(screen.getByText('Unable to load How It Works')).toBeInTheDocument()
    })

    it('should not propagate errors up to parent components', () => {
      // The main page element should always render even with child errors
      render(<MockHomePage heroThrows={true} />)

      const mainPage = screen.getByTestId('home-page')
      expect(mainPage).toBeInTheDocument()

      // Verify the main container has the expected structure
      expect(mainPage.tagName).toBe('MAIN')
    })
  })
})
