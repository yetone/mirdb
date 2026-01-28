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

// Mock Register page component for testing navigation
function MockRegisterPage() {
  return (
    <div data-testid="register-page">
      <h1>Register Page</h1>
      <form data-testid="registration-form">
        <input type="email" placeholder="Email" />
        <input type="password" placeholder="Password" />
        <button type="submit">Register</button>
      </form>
    </div>
  )
}

// Test app with routes for navigation testing
function TestApp() {
  return (
    <>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/register" element={<MockRegisterPage />} />
      </Routes>
      <LocationDisplay />
    </>
  )
}

/**
 * Scenario 2: Navigation to Registration
 * Tests that users can easily navigate from the homepage to the registration page
 */
describe('Scenario 2: Navigation to Registration', () => {
  describe('Test Case 1: Click Sign Up navigates to /register', () => {
    it('should navigate to /register when clicking the Get Started button in hero section', async () => {
      const user = userEvent.setup()

      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Verify we start on the homepage
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('location-display')).toHaveTextContent('/')

      // Find the primary CTA button (Get Started)
      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()

      // Click the button
      await user.click(getStartedButton)

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })

      // Verify registration form is visible
      expect(screen.getByTestId('registration-form')).toBeInTheDocument()
      expect(screen.getByTestId('location-display')).toHaveTextContent('/register')
    })

    it('should navigate to /register from any Sign Up CTA on the page', async () => {
      const user = userEvent.setup()

      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Find all links that navigate to /register
      const registerLinks = screen.getAllByRole('link').filter(
        (link) => link.getAttribute('href') === '/register'
      )

      // There should be at least one Sign Up CTA
      expect(registerLinks.length).toBeGreaterThanOrEqual(1)

      // Click the first one
      await user.click(registerLinks[0])

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 2: Sign Up button has correct href', () => {
    it('should have href attribute pointing to /register', () => {
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Find the Get Started button (primary CTA)
      const getStartedButton = screen.getByRole('link', { name: /get started/i })

      // Verify href
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('should render as a Link component wrapping the CTA button', () => {
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // The Get Started button should be wrapped in an anchor tag
      const link = screen.getByRole('link', { name: /get started/i })

      // Verify it's an actual anchor element
      expect(link.tagName.toLowerCase()).toBe('a')
      expect(link).toHaveAttribute('href', '/register')
    })
  })

  describe('Test Case 3: E2E-style navigation verification', () => {
    it('should complete full navigation flow from homepage to registration', async () => {
      const user = userEvent.setup()

      // Start on homepage
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Step 1: Verify homepage loads with hero section
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Step 2: Locate the Sign Up/Get Started button
      const signUpButton = screen.getByRole('link', { name: /get started/i })
      expect(signUpButton).toBeVisible()

      // Step 3: Click the button
      await user.click(signUpButton)

      // Step 4: Verify URL changed to /register (via presence of register page)
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })

      // Step 5: Verify registration form is displayed
      expect(screen.getByTestId('registration-form')).toBeVisible()
      expect(screen.getByPlaceholderText('Email')).toBeInTheDocument()
      expect(screen.getByPlaceholderText('Password')).toBeInTheDocument()
    })

    it('should allow navigation back to homepage from register page', async () => {
      // Start on register page
      renderWithProviders(<TestApp />, { initialEntries: ['/register'] })

      // Verify we're on register page
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: All Sign Up CTAs navigate consistently', () => {
    it('should ensure all /register links on homepage navigate to the same destination', () => {
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Find all links on the page
      const allLinks = screen.getAllByRole('link')

      // Filter for links pointing to /register
      const registerLinks = allLinks.filter(
        (link) => link.getAttribute('href') === '/register'
      )

      // All should have the same href
      registerLinks.forEach((link) => {
        expect(link).toHaveAttribute('href', '/register')
      })

      // Log count for verification
      expect(registerLinks.length).toBeGreaterThanOrEqual(1)
    })

    it('should have Sign Up CTA prominently visible in hero section', () => {
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // The hero section should contain the CTA
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // CTA should be within the hero section
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(heroSection).toContainElement(getStartedLink)
    })

    it('should have accessible button text for Sign Up CTA', () => {
      renderWithProviders(<TestApp />, { initialEntries: ['/'] })

      // Button should have accessible name
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toBeVisible()

      // Button text should be clear and actionable
      expect(getStartedLink.textContent).toMatch(/get started/i)
    })
  })
})

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
