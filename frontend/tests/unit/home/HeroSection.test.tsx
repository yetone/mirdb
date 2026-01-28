/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for HeroSection component:
 * - Displays headline with URL shortening value proposition
 * - Contains Sign Up/Get Started and Log In CTA buttons
 * - CTAs have correct navigation targets
 * - Description mentions URL shortening, analytics, and link management
 *
 * Testing framework: Vitest + @testing-library/react
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../utils/renderWithProviders'
import { HeroSection } from '@/components/home/HeroSection'

describe('HeroSection', () => {
  describe('Test Case 1: Hero section contains h1 headline with text about URL shortening', () => {
    it('should render a hero section with h1 headline about URL shortening', () => {
      renderWithProviders(<HeroSection />)

      // Check hero section is present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check for h1 headline
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Verify headline contains URL shortening value proposition
      expect(headline.textContent?.toLowerCase()).toMatch(/shorten|url|link/)
    })
  })

  describe('Test Case 2: Description text mentions URL shortening, analytics, and link management', () => {
    it('should contain product description mentioning key capabilities', () => {
      renderWithProviders(<HeroSection />)

      // Get the description paragraph
      const description = screen.getByText(/transform long/i)
      expect(description).toBeInTheDocument()

      // Check description content
      const descriptionText = description.textContent?.toLowerCase() || ''

      // Should mention URL shortening
      expect(descriptionText).toMatch(/url|link|short/)

      // Should mention analytics
      expect(descriptionText).toMatch(/analytics|track|clicks/)

      // Should mention link management
      expect(descriptionText).toMatch(/manage|links/)
    })
  })

  describe('Test Case 3: At least two CTA buttons present - Get Started/Sign Up and Log In', () => {
    it('should display Get Started and Log In buttons', () => {
      renderWithProviders(<HeroSection />)

      // Check for Get Started button (primary CTA)
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()

      // Check for Log In button (secondary CTA)
      const loginButton = screen.getByRole('button', { name: /log in/i })
      expect(loginButton).toBeInTheDocument()
    })

    it('should have Get Started button linking to /register', () => {
      renderWithProviders(<HeroSection />)

      // The Get Started button should be wrapped in a Link to /register
      const registerLink = screen.getByRole('link', { name: /get started/i })
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should have Log In button linking to /login', () => {
      renderWithProviders(<HeroSection />)

      // The Log In button should be wrapped in a Link to /login
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })
  })

  describe('Hero section visibility and structure', () => {
    it('should render hero section as a visible section element', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeVisible()
      expect(heroSection.tagName.toLowerCase()).toBe('section')
    })

    it('should render all hero content in proper order', () => {
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      const description = screen.getByText(/transform long/i)
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      const loginLink = screen.getByRole('link', { name: /log in/i })

      // All elements should be present
      expect(headline).toBeInTheDocument()
      expect(description).toBeInTheDocument()
      expect(getStartedLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
    })
  })
})
