import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from '../../src/pages/Home'
import { FEATURES } from '../../src/constants/features'

/**
 * Homepage Hero Section Tests
 * Scenario 1: Validates hero section, headline, subheadline, and features
 */

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Home Page', () => {
  describe('Hero Section', () => {
    /**
     * Test Case 2: Headline element exists with role='heading' and level 1
     */
    it('should render headline with h1 element containing URL or Shorten', () => {
      renderHome()

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      const headlineText = headline.textContent || ''
      const containsUrlOrShorten =
        headlineText.toLowerCase().includes('url') ||
        headlineText.toLowerCase().includes('shorten')
      expect(containsUrlOrShorten).toBe(true)
    })

    it('should render subheadline text', () => {
      renderHome()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toBeTruthy()
    })

    it('should have accessible hero section with proper labeling', () => {
      renderHome()

      const heroSection = document.querySelector('[aria-labelledby="hero-headline"]')
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Features Section', () => {
    /**
     * Test Case 3: Between 3 and 5 FeatureCard components are rendered
     */
    it('should render between 3 and 5 feature cards', () => {
      renderHome()

      const featureList = screen.getByRole('list', { name: /product features/i })
      expect(featureList).toBeInTheDocument()

      const featureItems = screen.getAllByRole('listitem')
      expect(featureItems.length).toBeGreaterThanOrEqual(3)
      expect(featureItems.length).toBeLessThanOrEqual(5)
    })

    it('should render features from FEATURES constant', () => {
      renderHome()

      FEATURES.forEach((feature) => {
        expect(screen.getByText(feature.title)).toBeInTheDocument()
        expect(screen.getByText(feature.description)).toBeInTheDocument()
      })
    })

    /**
     * Test Case 4: Each feature displays an icon element
     */
    it('should render icon for each feature', () => {
      renderHome()

      const icons = screen.getAllByTestId('feature-icon')
      expect(icons.length).toBe(FEATURES.length)

      icons.forEach((iconContainer) => {
        const svgElement = iconContainer.querySelector('svg')
        expect(svgElement).toBeInTheDocument()
      })
    })

    it('should have features section with proper heading', () => {
      renderHome()

      const featuresHeading = screen.getByRole('heading', {
        name: /why choose us/i,
      })
      expect(featuresHeading).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have skip to main content link', () => {
      renderHome()

      const skipLink = screen.getByText(/skip to main content/i)
      expect(skipLink).toBeInTheDocument()
      expect(skipLink).toHaveAttribute('href', '#main-content')
    })

    it('should have main content with id for skip link', () => {
      renderHome()

      const mainContent = document.getElementById('main-content')
      expect(mainContent).toBeInTheDocument()
    })
  })

  /**
   * Navigation and CTAs Tests
   * Scenario 4: Validates navigation links and call-to-action buttons
   * REQ-5: Homepage shall include navigation links to Login and Register pages
   * REQ-2: Homepage shall include a prominent CTA button leading to registration
   */
  describe('Navigation and CTAs (Scenario 4)', () => {
    /**
     * Test Case 1: Login link exists with correct href
     * Input: Render Home component and find Login link
     * Expected: Link element with text 'Login' or 'Sign In' exists with href='/login'
     */
    it('should render Login link with href="/login"', () => {
      renderHome()

      const loginLink = screen.getByRole('link', { name: /login|sign in/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    /**
     * Test Case 2: Click Login link navigates to /login
     * Input: Click Login link in rendered Home component
     * Expected: Router navigates to /login path
     */
    it('should navigate to /login when Login link is clicked', async () => {
      const user = userEvent.setup()
      let currentPath = '/'

      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route
              path="/login"
              element={
                <div data-testid="login-page">Login Page</div>
              }
            />
          </Routes>
        </MemoryRouter>
      )

      const loginLink = screen.getByRole('link', { name: /login|sign in/i })
      await user.click(loginLink)

      expect(screen.getByTestId('login-page')).toBeInTheDocument()
    })

    /**
     * Test Case 3: Get Started button exists
     * Input: Render Home component and find Get Started button
     * Expected: Button with text containing 'Get Started' exists
     */
    it('should render Get Started button', () => {
      renderHome()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()
    })

    /**
     * Test Case 4: Click Get Started button navigates to /register
     * Input: Click Get Started button
     * Expected: Router navigates to /register path
     */
    it('should navigate to /register when Get Started button is clicked', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route
              path="/register"
              element={
                <div data-testid="register-page">Register Page</div>
              }
            />
          </Routes>
        </MemoryRouter>
      )

      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      await user.click(getStartedButton)

      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })

    /**
     * Test Case 5: CTA button visibility - Get Started is prominently displayed
     * Input: Check CTA button visibility
     * Expected: Get Started CTA is prominently displayed (large, primary color)
     */
    it('should display Get Started CTA prominently with primary styling', () => {
      renderHome()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()
      // Check for btn-primary class (DaisyUI primary button style)
      expect(getStartedButton).toHaveClass('btn-primary')
      // Check for larger size (btn-lg or text styling)
      expect(getStartedButton).toHaveClass('btn-lg')
    })

    /**
     * Test Case 6: Both CTAs visible on page load
     * Input: Verify both CTAs are in viewport on page load
     * Expected: Login and Get Started buttons visible without scrolling on desktop
     */
    it('should have both Login and Get Started buttons visible', () => {
      renderHome()

      const loginLink = screen.getByRole('link', { name: /login|sign in/i })
      const getStartedButton = screen.getByRole('link', { name: /get started/i })

      expect(loginLink).toBeInTheDocument()
      expect(getStartedButton).toBeInTheDocument()
      // Both should be in the same CTA section for visibility
      expect(loginLink).toBeVisible()
      expect(getStartedButton).toBeVisible()
    })

    it('should have Get Started CTA with href="/register"', () => {
      renderHome()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })
  })
})
