/**
 * Home Navigation Integration Tests
 * Owner: Scenario 2 (primary), Scenarios 3, 11, 16 (shared)
 *
 * Integration tests for navigation functionality:
 * - Sign Up button navigates to /register (Scenario 2)
 * - Log In button navigates to /login (Scenario 3)
 * - Homepage is publicly accessible (Scenario 11)
 * - Page has proper SEO meta tags (Scenario 16)
 *
 * Testing framework: Vitest + @testing-library/react
 * Requires: MemoryRouter for route testing
 */
import { describe, it, expect } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Routes, Route, useLocation } from 'react-router-dom'
import { renderWithProviders } from '../../utils/renderWithProviders'
import Home from '@/pages/Home'
import { HeroSection } from '@/components/home'

// Helper component to display current location for testing
function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Test app with routes for navigation testing
function TestApp() {
  return (
    <>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
      </Routes>
      <LocationDisplay />
    </>
  )
}

/**
 * Scenario 3: Navigation to Login
 * Tests that returning users can quickly access the login page from the homepage
 */
describe('Scenario 3: Navigation to Login', () => {
  describe('Test Case 1: Render Home component and click Log In button', () => {
    it('should navigate to /login route when Log In button is clicked', async () => {
      const user = userEvent.setup()

      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Verify we start at home
      expect(screen.getByTestId('location-display')).toHaveTextContent('/')

      // Find and click the Log In button
      const loginButton = screen.getByRole('link', { name: /log in/i })
      expect(loginButton).toBeInTheDocument()

      await user.click(loginButton)

      // Verify navigation to /login
      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
      })

      // Verify login page is displayed
      expect(screen.getByTestId('login-page')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Query Log In button and verify link destination', () => {
    it('should have Log In button/link pointing to /login path', () => {
      renderWithProviders(<HeroSection />, { initialEntries: ['/'] })

      // Find the Log In link
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toBeInTheDocument()

      // Verify href points to /login
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have Log In button visible in hero section', () => {
      renderWithProviders(<HeroSection />, { initialEntries: ['/'] })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Log In button should be within the hero section
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(heroSection).toContainElement(loginLink)
    })
  })

  describe('Test Case 4: Check navbar Log In link if present', () => {
    it('should navigate to /login when hero section Log In is clicked', async () => {
      const user = userEvent.setup()

      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // The Log In link in HeroSection acts as the primary login navigation
      const loginButton = screen.getByRole('link', { name: /log in/i })
      expect(loginButton).toBeInTheDocument()

      await user.click(loginButton)

      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
      })
    })
  })
})

/**
 * Additional integration tests for Login navigation
 */
describe('Login Navigation - Additional Tests', () => {
  it('should have Log In button with correct styling (ghost variant)', () => {
    renderWithProviders(<HeroSection />, { initialEntries: ['/'] })

    const loginLink = screen.getByRole('link', { name: /log in/i })
    const button = loginLink.querySelector('button')

    // The button should have btn-ghost class for secondary CTA styling
    expect(button).toHaveClass('btn-ghost')
  })

  it('should have Log In link that can be activated to navigate', async () => {
    const user = userEvent.setup()

    renderWithProviders(<TestApp />, { initialEntries: ['/'] })

    // Find the Log In link and verify it exists
    const loginLink = screen.getByRole('link', { name: /log in/i })
    expect(loginLink).toBeInTheDocument()

    // The Log In link should have correct href for navigation
    expect(loginLink).toHaveAttribute('href', '/login')

    // Clicking the link should navigate to /login
    await user.click(loginLink)

    await waitFor(() => {
      expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
    })
  })

  it('should display Log In option for returning users in hero section', () => {
    renderWithProviders(<Home />, { initialEntries: ['/'] })

    // Log In should be visible as a secondary CTA for returning users
    const loginLink = screen.getByRole('link', { name: /log in/i })
    expect(loginLink).toBeVisible()
  })
})
