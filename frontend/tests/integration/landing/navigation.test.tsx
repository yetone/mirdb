/**
 * Navigation Integration Tests
 * Owner: Scenario 3 - Navigation to Login and Register
 *
 * Tests navigation functionality from landing page to Login and Register pages.
 * Verifies REQ-3: Include clear navigation to Login and Register pages.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import { renderWithRouter, userEvent } from './test-utils'

describe('Navigation to Login and Register', () => {
  beforeEach(() => {
    // Clear localStorage before each test to ensure consistent state
    localStorage.clear()
  })

  describe('Test Case 1: Click Login link navigates to /login', () => {
    it('should navigate to /login when clicking Login link in navbar', async () => {
      // Render Home component with router
      renderWithRouter()
      const user = userEvent.setup()

      // Find and click the Login link in navbar
      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()

      await user.click(loginLink)

      // Verify navigation to /login route - Login page has "Sign In" as heading
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /sign in/i, level: 1 })).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 2: Click Register link navigates to /register', () => {
    it('should navigate to /register when clicking Register link in navbar', async () => {
      // Render Home component with router
      renderWithRouter()
      const user = userEvent.setup()

      // Find and click the Register link in navbar
      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()

      await user.click(registerLink)

      // Verify navigation to /register route - Register page has "Create Account" as heading
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /create account/i, level: 1 })).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 3: Click primary CTA "Get Started Free" navigates to /register', () => {
    it('should navigate to /register when clicking "Get Started Free" button', async () => {
      // Render Home component with router
      renderWithRouter()
      const user = userEvent.setup()

      // Find and click the primary CTA button in hero section
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent(/get started free/i)

      await user.click(primaryCTA)

      // Verify navigation to /register route for new user signup
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /create account/i, level: 1 })).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 4: Click secondary CTA "Sign In" navigates to /login', () => {
    it('should navigate to /login when clicking "Sign In" button in hero', async () => {
      // Render Home component with router
      renderWithRouter()
      const user = userEvent.setup()

      // Find and click the secondary CTA button in hero section
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toHaveTextContent(/sign in/i)

      await user.click(secondaryCTA)

      // Verify navigation to /login route
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /sign in/i, level: 1 })).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 5: Navbar contains both Login and Register links', () => {
    it('should have both Login and Register links in the navigation bar', () => {
      // Render Home component
      renderWithRouter()

      // Query navbar for Login link
      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')

      // Query navbar for Register link
      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')

      // Verify both are within the navigation element
      const navbar = screen.getByRole('navigation')
      expect(navbar).toContainElement(loginLink)
      expect(navbar).toContainElement(registerLink)
    })
  })
})
