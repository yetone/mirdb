/**
 * Navigation Integration Tests
 * Owner: Scenario 3 - Navigation and Header
 *
 * Tests for homepage navigation and header functionality.
 *
 * Test coverage:
 * - Site logo/branding is visible
 * - Login navigation link is present
 * - Register navigation link is present
 * - Theme toggle is present
 * - Navigation links route correctly
 */
import { describe, it, expect } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { render } from '../../utils/render'
import App from '../../../src/App'
import Home from '../../../src/pages/Home'

describe('Navigation and Header', () => {
  describe('Unit Tests - Header Elements', () => {
    it('should display site logo/branding in the header', () => {
      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      const logo = screen.getByTestId('site-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveTextContent('URL')
      expect(logo).toHaveTextContent('Shortener')
    })

    it('should display Login navigation link', () => {
      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      const loginLink = screen.getByTestId('nav-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Login')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should display Register navigation link', () => {
      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      const registerLink = screen.getByTestId('nav-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveTextContent('Register')
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should display theme toggle component in header', () => {
      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toHaveAttribute('aria-label')
    })
  })

  describe('Integration Tests - Navigation Routing', () => {
    it('should navigate to /login when Login link is clicked', async () => {
      const user = userEvent.setup()
      render(<App />, { useMemoryRouter: true, initialEntries: ['/'] })

      // Verify we're on the homepage
      expect(screen.getByTestId('site-logo')).toBeInTheDocument()

      // Click the Login link
      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      // Verify we navigated to the login page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /login/i })).toBeInTheDocument()
      })
    })

    it('should navigate to /register when Register link is clicked', async () => {
      const user = userEvent.setup()
      render(<App />, { useMemoryRouter: true, initialEntries: ['/'] })

      // Verify we're on the homepage
      expect(screen.getByTestId('site-logo')).toBeInTheDocument()

      // Click the Register link
      const registerLink = screen.getByTestId('nav-register')
      await user.click(registerLink)

      // Verify we navigated to the register page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })
    })
  })
})
