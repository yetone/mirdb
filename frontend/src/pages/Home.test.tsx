import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from './Home'

// Mock useNavigate
const mockNavigate = vi.fn()
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  }
})

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Home - Feature Cards Display', () => {
  describe('Test Case 1: At least 3 GlassMorphismCard components are rendered', () => {
    it('renders at least 3 feature cards using GlassMorphismCard components', () => {
      renderHome()

      // Get all feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/)

      // Verify at least 3 cards are rendered
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('renders the features section', () => {
      renderHome()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('renders the features grid', () => {
      renderHome()

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()
    })
  })

  describe('Test Case 2: URL Shortening feature card with appropriate icon and description', () => {
    it('renders URL Shortening feature card', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      expect(urlShorteningCard).toBeInTheDocument()
    })

    it('URL Shortening card has correct title', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const title = urlShorteningCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('URL Shortening')
    })

    it('URL Shortening card has description', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const description = urlShorteningCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('short links')
    })

    it('URL Shortening card has icon', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const icon = urlShorteningCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Analytics Dashboard feature card with chart/stats icon', () => {
    it('renders Analytics Dashboard feature card', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      expect(analyticsCard).toBeInTheDocument()
    })

    it('Analytics Dashboard card has correct title', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const title = analyticsCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('Analytics Dashboard')
    })

    it('Analytics Dashboard card has description about tracking', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const description = analyticsCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('Track')
    })

    it('Analytics Dashboard card has chart/stats icon', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const icon = analyticsCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
      // Icon should contain an SVG element (the BarChart3 icon)
      const svgElement = icon?.querySelector('svg')
      expect(svgElement).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Geographic Insights feature card with location/globe icon', () => {
    it('renders Geographic Insights feature card', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      expect(geoCard).toBeInTheDocument()
    })

    it('Geographic Insights card has correct title', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const title = geoCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('Geographic Insights')
    })

    it('Geographic Insights card has description about location/audience', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const description = geoCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('audience')
    })

    it('Geographic Insights card has globe/location icon', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const icon = geoCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
      // Icon should contain an SVG element (the Globe icon)
      const svgElement = icon?.querySelector('svg')
      expect(svgElement).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Each feature card has title, description, and visual icon element', () => {
    it('all feature cards have a title', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const title = card.querySelector('[data-testid="card-title"]')
        expect(title).toBeInTheDocument()
        expect(title?.textContent).not.toBe('')
      })
    })

    it('all feature cards have a description', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const description = card.querySelector('[data-testid="card-description"]')
        expect(description).toBeInTheDocument()
        expect(description?.textContent).not.toBe('')
      })
    })

    it('all feature cards have a visual icon element', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const icon = card.querySelector('[data-testid="card-icon"]')
        expect(icon).toBeInTheDocument()

        // Each icon should contain an SVG
        const svgElement = icon?.querySelector('svg')
        expect(svgElement).toBeInTheDocument()
      })
    })

    it('renders exactly 4 feature cards', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(4)
    })
  })
})

describe('Home - Hero Section Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // Test Case 1: Hero section contains h1 element with compelling headline text
  describe('Test Case 1: Hero section headline', () => {
    it('renders hero section with h1 headline', () => {
      renderHome()

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten, Share, Track')
    })

    it('renders hero section element', () => {
      renderHome()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })
  })

  // Test Case 2: Subheadline paragraph explaining value proposition is visible
  describe('Test Case 2: Subheadline value proposition', () => {
    it('renders subheadline paragraph with value proposition', () => {
      renderHome()

      const subheadline = screen.getByText(/Create short, memorable links and track their performance/i)
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.tagName.toLowerCase()).toBe('p')
    })
  })

  // Test Case 3: Primary CTA button with 'Get Started Free' text is rendered
  describe('Test Case 3: Primary CTA button', () => {
    it('renders primary CTA button with "Get Started Free" text', () => {
      renderHome()

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i })
      expect(primaryCTA).toBeInTheDocument()
    })

    it('renders FuturisticButton components for CTAs', () => {
      renderHome()

      const buttons = screen.getAllByRole('button')
      expect(buttons.length).toBeGreaterThanOrEqual(2)

      expect(screen.getByRole('button', { name: /Get Started Free/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /Learn More/i })).toBeInTheDocument()
    })
  })

  // Test Case 4: Click primary CTA navigates to /register route
  describe('Test Case 4: Primary CTA navigation', () => {
    it('navigates to /register when primary CTA is clicked', async () => {
      renderHome()

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i })
      fireEvent.click(primaryCTA)

      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/register')
      })
    })
  })

  // Test Case 5: Click 'Learn More' secondary CTA scrolls to features section
  describe('Test Case 5: Secondary CTA scroll behavior', () => {
    it('scrolls to features section when Learn More is clicked', async () => {
      const scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock

      renderHome()

      const secondaryCTA = screen.getByRole('button', { name: /Learn More/i })
      fireEvent.click(secondaryCTA)

      await waitFor(() => {
        expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })
      })
    })
  })
})
