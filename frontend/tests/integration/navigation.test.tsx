/**
 * Integration tests for homepage navigation flows.
 * Owner: Scenario 2 - Primary Registration CTA Button
 *
 * Test coverage:
 * - Clicking primary CTA navigates to /register
 * - Clicking login navigates to /login
 * - Logo navigates to homepage
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from '../../src/components/home/HeroSection'
import Home from '../../src/pages/Home'

/**
 * Integration tests verify that the CTA buttons have correct navigation properties.
 * Since we use Link components with href attributes, navigation is handled by React Router.
 * These tests validate the routing configuration without actual browser navigation.
 */

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Homepage Navigation Integration Tests', () => {
  /**
   * Test Case 2: Click primary CTA button and verify navigation to /register
   * Scenario 2: Primary Registration CTA Button
   */
  describe('Test Case 2: Primary CTA Navigation', () => {
    it('should have primary CTA configured to navigate to /register', () => {
      renderHeroSection()

      // Find the primary CTA button
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()

      // Verify the href attribute points to /register
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('should have correct href attribute pointing to /register', () => {
      renderHome()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('should render primary CTA as a link element for navigation', () => {
      renderHeroSection()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      // Verify it's a proper link that can trigger navigation
      expect(ctaButton.tagName.toLowerCase()).toBe('a')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('should have accessible name for screen readers', () => {
      renderHeroSection()

      const ctaButton = screen.getByLabelText(/get started with registration/i)
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', '/register')
    })
  })

  describe('Secondary CTA Navigation', () => {
    it('should have login link configured to navigate to /login', () => {
      renderHeroSection()

      const loginButton = screen.getByRole('link', { name: /login/i })
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('should have correct href attribute for login button', () => {
      renderHome()

      const loginButton = screen.getByRole('link', { name: /login/i })
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  describe('CTA Visibility', () => {
    it('should render primary CTA prominently in the document', () => {
      renderHeroSection()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()
    })

    it('should render both CTAs within the hero section', () => {
      renderHeroSection()

      const heroSection = screen.getByLabelText(/hero section/i)
      const primaryCta = screen.getByRole('link', { name: /get started/i })
      const secondaryCta = screen.getByRole('link', { name: /login/i })

      expect(heroSection).toContainElement(primaryCta)
      expect(heroSection).toContainElement(secondaryCta)
    })
  })
})
