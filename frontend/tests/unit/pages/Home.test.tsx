/**
 * Home Page Hero Section Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * 1. Hero section displays with product name and tagline
 * 2. URL input field is present with appropriate placeholder text
 * 3. Primary CTA 'Shorten URL' button is visible
 * 4. Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '../../test-utils'
import Home from '../../../src/pages/Home'

describe('Home Page Hero Section', () => {
  /**
   * Test Case 1: Hero section displays with product name and tagline
   * Input: Render Home component
   * Expected: Hero section displays with product name 'URL Shortening Service'
   *           and tagline 'Shorten Links. Track Insights. Share Smarter.'
   */
  describe('Test Case 1: Hero section with product name and tagline', () => {
    it('displays the hero section', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('displays the product name', () => {
      render(<Home />)

      const productName = screen.getByTestId('product-name')
      expect(productName).toBeInTheDocument()
      expect(productName).toHaveTextContent('URL Shortening Service')
    })

    it('displays the tagline', () => {
      render(<Home />)

      const tagline = screen.getByTestId('tagline')
      expect(tagline).toBeInTheDocument()
      expect(tagline).toHaveTextContent('Shorten Links. Track Insights. Share Smarter.')
    })

    it('hero section has background gradient styling', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
    })
  })

  /**
   * Test Case 2: URL input field is present with appropriate placeholder text
   * Input: Render Home component
   * Expected: URL input field is present with appropriate placeholder text
   */
  describe('Test Case 2: URL input field with placeholder', () => {
    it('displays URL input field', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()
    })

    it('URL input has appropriate placeholder text', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('placeholder', 'Enter your long URL here...')
    })

    it('URL input has accessibility label', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('aria-label', 'URL input')
    })
  })

  /**
   * Test Case 3: Primary CTA 'Shorten URL' button is visible
   * Input: Render Home component
   * Expected: Primary CTA 'Shorten URL' button is visible
   */
  describe('Test Case 3: Primary CTA Shorten URL button', () => {
    it('displays Shorten URL button', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toBeInTheDocument()
      expect(shortenButton).toHaveTextContent('Shorten URL')
    })

    it('Shorten URL button is a submit button', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveAttribute('type', 'submit')
    })

    it('Shorten URL button is not disabled initially', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).not.toBeDisabled()
    })

    it('Shorten URL button has primary styling', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveClass('btn-primary')
    })
  })

  /**
   * Test Case 4: Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
   * Input: Render Home component
   * Expected: Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
   */
  describe('Test Case 4: Secondary CTA buttons', () => {
    it('displays Sign Up Free button', () => {
      render(<Home />)

      const signupButton = screen.getByTestId('signup-button')
      expect(signupButton).toBeInTheDocument()
      expect(signupButton).toHaveTextContent('Sign Up Free')
    })

    it('Sign Up Free button links to register page', () => {
      render(<Home />)

      const signupButton = screen.getByTestId('signup-button')
      expect(signupButton).toHaveAttribute('href', '/register')
    })

    it('displays Log In button', () => {
      render(<Home />)

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Log In')
    })

    it('Log In button links to login page', () => {
      render(<Home />)

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  /**
   * Additional: URL form structure test
   */
  describe('URL Form Structure', () => {
    it('displays the URL shortening form container', () => {
      render(<Home />)

      const formContainer = screen.getByTestId('url-form')
      expect(formContainer).toBeInTheDocument()
    })

    it('form container contains input and button', () => {
      render(<Home />)

      const formContainer = screen.getByTestId('url-form')
      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-url-button')

      expect(formContainer).toContainElement(urlInput)
      expect(formContainer).toContainElement(shortenButton)
    })
  })
})
