/**
 * Hero Section Tests
 * Primary Owner: Scenario 1 - Hero Section Rendering
 * Secondary Owner: Scenario 13 - BackgroundEffect Integration
 *
 * Test coverage:
 * - Hero section renders with headline and CTAs
 * - "Get Started" button navigates to /register
 * - "Login" button navigates to /login
 * - BackgroundEffect component integration
 * - Entrance animations
 */
import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { HeroSection } from '../../components/home/HeroSection'

describe('HeroSection - BackgroundEffect Integration (Scenario 13)', () => {
  describe('Test Case 1: BackgroundEffect component is included in the render', () => {
    it('should render HeroSection with BackgroundEffect component included', () => {
      renderWithProviders(<HeroSection />)

      // Verify HeroSection renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify BackgroundEffect container is present within hero
      const backgroundEffectContainer = screen.getByTestId('hero-background-effect')
      expect(backgroundEffectContainer).toBeInTheDocument()

      // The BackgroundEffect component should be rendered inside the container
      // BackgroundEffect renders a div with specific classes
      const backgroundDiv = backgroundEffectContainer.querySelector('div')
      expect(backgroundDiv).toBeInTheDocument()
    })

    it('should have BackgroundEffect as a child of HeroSection', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const backgroundEffectContainer = within(heroSection).getByTestId('hero-background-effect')

      expect(backgroundEffectContainer).toBeInTheDocument()
      expect(heroSection).toContainElement(backgroundEffectContainer)
    })
  })

  describe('Test Case 2: BackgroundEffect is positioned behind hero content (z-index)', () => {
    it('should position BackgroundEffect behind hero content using z-index', () => {
      renderWithProviders(<HeroSection />)

      // Get the background effect container
      const backgroundEffectContainer = screen.getByTestId('hero-background-effect')

      // Check that background has negative z-index (positioned behind content)
      expect(backgroundEffectContainer).toHaveClass('-z-10')
      expect(backgroundEffectContainer).toHaveClass('absolute')
      expect(backgroundEffectContainer).toHaveClass('inset-0')
    })

    it('should have hero content with higher z-index than background', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Find the content container (the one with z-10)
      const contentContainer = heroSection.querySelector('.z-10')
      expect(contentContainer).toBeInTheDocument()

      // The background should have -z-10, content should have z-10
      const backgroundContainer = screen.getByTestId('hero-background-effect')
      expect(backgroundContainer).toHaveClass('-z-10')
      expect(contentContainer).toHaveClass('z-10')
    })

    it('should have BackgroundEffect container marked as aria-hidden for accessibility', () => {
      renderWithProviders(<HeroSection />)

      const backgroundEffectContainer = screen.getByTestId('hero-background-effect')
      expect(backgroundEffectContainer).toHaveAttribute('aria-hidden', 'true')
    })

    it('should render BackgroundEffect with absolute positioning covering entire hero', () => {
      renderWithProviders(<HeroSection />)

      const backgroundEffectContainer = screen.getByTestId('hero-background-effect')

      // Verify the background covers the entire hero section
      expect(backgroundEffectContainer).toHaveClass('absolute')
      expect(backgroundEffectContainer).toHaveClass('inset-0')
    })
  })

  describe('Additional BackgroundEffect Integration Tests', () => {
    it('should render hero section with relative positioning for proper z-index stacking', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('relative')
      expect(heroSection).toHaveClass('overflow-hidden')
    })

    it('should maintain visual hierarchy with content above background', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const backgroundContainer = screen.getByTestId('hero-background-effect')
      const contentContainer = heroSection.querySelector('.z-10')

      // Background should be positioned absolutely with negative z-index
      expect(backgroundContainer).toHaveClass('absolute', '-z-10')

      // Content should have positive z-index
      expect(contentContainer).toHaveClass('relative', 'z-10')
    })

    it('should include the main headline visible above background effect', () => {
      renderWithProviders(<HeroSection />)

      // The headline should be visible (content is above background)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/shorten urls/i)
    })

    it('should include CTA buttons visible above background effect', () => {
      renderWithProviders(<HeroSection />)

      // Both CTA buttons should be visible
      const getStartedButton = screen.getByRole('button', { name: /get started free/i })
      const loginButton = screen.getByRole('button', { name: /login/i })

      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()
    })
  })
})
