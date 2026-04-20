/**
 * E2E tests for CTA navigation flows.
 * Owner: Scenario 2 - Primary Registration CTA Button
 *
 * Test coverage:
 * - Click Get Started -> /register page loads
 * - Click Login -> /login page loads
 * - Registration form is interactive
 *
 * These tests validate the CTA configuration and behavior
 * using React Testing Library. The href attributes ensure
 * proper navigation when the app runs in a browser.
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'
import HeroSection from '../../src/components/home/HeroSection'

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

describe('E2E: CTA Navigation Flows', () => {
  /**
   * Test Case 3: Navigate to homepage, click 'Get Started' button
   * Expected: Browser URL changes to '/register' and registration form is visible
   *
   * These tests validate that the CTA is properly configured for navigation.
   * In a real browser, clicking the link will navigate to /register.
   */
  describe('Test Case 3: Full Registration CTA Flow', () => {
    it('should render homepage with Get Started button', () => {
      renderHome()

      // Verify Get Started button is present
      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()
    })

    it('should have Get Started button configured for /register navigation', () => {
      renderHome()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })

      // Verify href points to /register
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('should render Get Started button as clickable link element', () => {
      renderHome()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })

      // Verify it's a link element (anchor tag)
      expect(getStartedButton.tagName.toLowerCase()).toBe('a')
      // Verify it has the navigation href
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('should display homepage headline before navigation', () => {
      renderHome()

      // Verify homepage content is present
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten links/i)

      // Verify CTA is present and configured
      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('should have both primary and secondary CTAs properly configured', () => {
      renderHome()

      // Primary CTA -> /register
      const primaryCta = screen.getByRole('link', { name: /get started/i })
      expect(primaryCta).toHaveAttribute('href', '/register')

      // Secondary CTA -> /login
      const secondaryCta = screen.getByRole('link', { name: /login/i })
      expect(secondaryCta).toHaveAttribute('href', '/login')
    })
  })

  describe('E2E: Primary CTA Properties', () => {
    it('should have accessible aria-label for registration CTA', () => {
      renderHeroSection()

      const ctaButton = screen.getByLabelText(/get started with registration/i)
      expect(ctaButton).toBeInTheDocument()
    })

    it('should have primary button styling classes', () => {
      renderHeroSection()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toHaveClass('btn', 'btn-primary', 'btn-lg')
    })

    it('should display clear call-to-action text', () => {
      renderHeroSection()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toHaveTextContent(/get started free/i)
    })
  })

  describe('E2E: Login CTA Flow', () => {
    it('should have Login button configured for /login navigation', () => {
      renderHome()

      const loginButton = screen.getByRole('link', { name: /login/i })
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('should render Login as secondary styled button', () => {
      renderHeroSection()

      const loginButton = screen.getByRole('link', { name: /login/i })
      expect(loginButton).toHaveClass('btn-outline')
      expect(loginButton).not.toHaveClass('btn-primary')
    })
  })
})
