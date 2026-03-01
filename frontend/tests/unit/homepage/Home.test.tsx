/**
 * Home Page Unit Tests
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Tests for the main Home page component.
 *
 * Test coverage:
 * - Component renders without errors
 * - Hero section is present
 * - CTA buttons are rendered
 * - Navigation works correctly
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '../../utils/render'
import userEvent from '@testing-library/user-event'
import { Home } from '../../../src/pages/Home'

describe('Home Page', () => {
  describe('Test Case 1: Product name is visible in the hero section', () => {
    it('should render the Home page with product name visible', () => {
      render(<Home />)

      // Verify Home page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify hero section with product name is visible
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Product name should contain "URL" and "Shortener"
      const productName = screen.getByTestId('product-name')
      expect(productName).toBeInTheDocument()
      expect(productName).toHaveTextContent('URL')
      expect(productName).toHaveTextContent('Shortener')
    })
  })

  describe('Test Case 2: Tagline describing URL shortening service is visible', () => {
    it('should display a tagline about URL shortening', () => {
      render(<Home />)

      // Verify tagline is visible and describes the URL shortening service
      const tagline = screen.getByTestId('tagline')
      expect(tagline).toBeInTheDocument()

      // Tagline should describe URL shortening service capabilities
      expect(tagline.textContent).toMatch(/shorten/i)
      expect(tagline.textContent).toMatch(/track/i)
      expect(tagline.textContent).toMatch(/links/i)
    })
  })

  describe('Test Case 3: Sign Up button is present with correct text', () => {
    it('should display a Sign Up button', () => {
      render(<Home />)

      // Find Sign Up button
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toBeInTheDocument()
      expect(signUpButton).toHaveTextContent('Sign Up')
    })
  })

  describe('Test Case 4: Log In button is present with correct text', () => {
    it('should display a Log In button', () => {
      render(<Home />)

      // Find Log In button
      const logInButton = screen.getByRole('link', { name: /log in/i })
      expect(logInButton).toBeInTheDocument()
      expect(logInButton).toHaveTextContent('Log In')
    })
  })

  describe('Test Case 5: Click Sign Up button navigates to /register', () => {
    it('should navigate to /register when Sign Up button is clicked', async () => {
      const user = userEvent.setup()

      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      // Find and click Sign Up button
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toHaveAttribute('href', '/register')

      // Click the button
      await user.click(signUpButton)

      // The link should have the correct href attribute (navigation happens via router)
      expect(signUpButton).toHaveAttribute('href', '/register')
    })
  })

  describe('Test Case 6: Click Log In button navigates to /login', () => {
    it('should navigate to /login when Log In button is clicked', async () => {
      const user = userEvent.setup()

      render(<Home />, { useMemoryRouter: true, initialEntries: ['/'] })

      // Find Log In button
      const logInButton = screen.getByRole('link', { name: /log in/i })
      expect(logInButton).toHaveAttribute('href', '/login')

      // Click the button
      await user.click(logInButton)

      // The link should have the correct href attribute (navigation happens via router)
      expect(logInButton).toHaveAttribute('href', '/login')
    })
  })

  describe('Additional integration tests', () => {
    it('should render the home page without errors', () => {
      render(<Home />)

      // Page should render without throwing
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('should include all major sections', () => {
      render(<Home />)

      // Hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Features section
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // URL Preview section (stub)
      expect(screen.getByTestId('url-preview-section')).toBeInTheDocument()

      // Footer section (stub)
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should have both CTA buttons in the hero section', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Both buttons should be links with correct hrefs
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const logInButton = screen.getByRole('link', { name: /log in/i })

      expect(signUpButton).toHaveAttribute('href', '/register')
      expect(logInButton).toHaveAttribute('href', '/login')
    })
  })
})
