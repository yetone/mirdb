/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Tests for the HeroSection component.
 *
 * Test coverage:
 * - Product name is visible
 * - Tagline is visible
 * - Sign Up button is present
 * - Log In button is present
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '../../utils/render'
import { HeroSection } from '../../../src/components/homepage/HeroSection'

describe('HeroSection', () => {
  describe('Test Case 1: Product name is visible', () => {
    it('should display the product name "URL Shortener" in the hero section', () => {
      render(<HeroSection />)

      // Verify the hero section exists
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify product name elements are visible
      const productName = screen.getByTestId('product-name')
      expect(productName).toBeInTheDocument()
      expect(productName).toHaveTextContent('URL')
      expect(productName).toHaveTextContent('Shortener')
    })
  })

  describe('Test Case 2: Tagline is visible', () => {
    it('should display a tagline describing the URL shortening service', () => {
      render(<HeroSection />)

      // Verify tagline is visible
      const tagline = screen.getByTestId('tagline')
      expect(tagline).toBeInTheDocument()
      expect(tagline).toHaveTextContent(/shorten/i)
      expect(tagline).toHaveTextContent(/track/i)
      expect(tagline).toHaveTextContent(/links/i)
    })
  })

  describe('Test Case 3: Sign Up button is present', () => {
    it('should display a Sign Up button with correct text', () => {
      render(<HeroSection />)

      // Find Sign Up button by its text
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toBeInTheDocument()
      expect(signUpButton).toHaveTextContent('Sign Up')
    })
  })

  describe('Test Case 4: Log In button is present', () => {
    it('should display a Log In button with correct text', () => {
      render(<HeroSection />)

      // Find Log In button by its text
      const logInButton = screen.getByRole('link', { name: /log in/i })
      expect(logInButton).toBeInTheDocument()
      expect(logInButton).toHaveTextContent('Log In')
    })
  })

  describe('Additional tests', () => {
    it('should have proper semantic structure with h1 heading', () => {
      render(<HeroSection />)

      // Check for proper h1 heading
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('URL')
      expect(heading).toHaveTextContent('Shortener')
    })

    it('should have Sign Up button linking to /register', () => {
      render(<HeroSection />)

      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toHaveAttribute('href', '/register')
    })

    it('should have Log In button linking to /login', () => {
      render(<HeroSection />)

      const logInButton = screen.getByRole('link', { name: /log in/i })
      expect(logInButton).toHaveAttribute('href', '/login')
    })

    it('should apply FuturisticButton styling to CTA buttons', () => {
      render(<HeroSection />)

      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const logInButton = screen.getByRole('link', { name: /log in/i })

      // FuturisticButton adds 'btn' class
      expect(signUpButton).toHaveClass('btn')
      expect(logInButton).toHaveClass('btn')

      // Primary button has btn-primary class
      expect(signUpButton).toHaveClass('btn-primary')
      // Secondary button has btn-secondary class
      expect(logInButton).toHaveClass('btn-secondary')
    })

    it('should have proper aria-label for accessibility', () => {
      render(<HeroSection />)

      const heroSection = screen.getByRole('region', { name: /hero section/i })
      expect(heroSection).toBeInTheDocument()
    })
  })
})
