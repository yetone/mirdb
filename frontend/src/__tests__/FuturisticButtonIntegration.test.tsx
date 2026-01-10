import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import HeroSection from '../components/HeroSection'

/**
 * Integration tests for FuturisticButton component in HeroSection
 * These tests verify that the CTA buttons correctly use the FuturisticButton component
 * and have the appropriate futuristic styling.
 */

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

describe('FuturisticButton Component Integration', () => {
  describe('Test Case 1: Primary CTA uses FuturisticButton component', () => {
    it('renders primary CTA as a FuturisticButton component', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA).toBeInTheDocument()

      // FuturisticButton should render as a link (for navigation)
      expect(primaryCTA.tagName.toLowerCase()).toBe('a')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('primary CTA has FuturisticButton styling characteristics', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')

      // Check for futuristic gradient styling (primary variant)
      expect(primaryCTA.className).toContain('bg-gradient-to-r')
      expect(primaryCTA.className).toContain('from-primary')
      expect(primaryCTA.className).toContain('to-secondary')
    })

    it('primary CTA has rounded corners (futuristic design)', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('rounded-xl')
    })

    it('primary CTA has shadow effects', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('shadow')
    })

    it('primary CTA displays correct text', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA).toHaveTextContent('Get Started Free')
    })
  })

  describe('Test Case 2: Button has appropriate hover animation from FuturisticButton', () => {
    it('has hover scale transformation for futuristic effect', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('hover:scale-105')
    })

    it('has smooth transition for animations', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('transition-all')
      expect(primaryCTA.className).toContain('duration-300')
    })

    it('has hover shadow enhancement', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('hover:shadow')
    })

    it('has backdrop blur effect for glass-like appearance', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('backdrop-blur')
    })

    it('respects reduced motion preferences', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('motion-reduce:transition-none')
      expect(primaryCTA.className).toContain('motion-reduce:hover:scale-100')
    })
  })

  describe('Test Case 3: Button styling adapts correctly to all themes', () => {
    it('uses DaisyUI theme-aware color classes (primary)', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      // Uses theme variables that adapt to light/dark/cyberpunk/synthwave
      expect(primaryCTA.className).toContain('from-primary')
      expect(primaryCTA.className).toContain('to-secondary')
      expect(primaryCTA.className).toContain('text-primary-content')
    })

    it('uses border styling that adapts to themes', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('border')
      expect(primaryCTA.className).toContain('border-primary')
    })

    it('has focus ring for keyboard accessibility across themes', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('focus-visible:ring-2')
      expect(primaryCTA.className).toContain('focus-visible:ring-primary')
    })

    it('uses theme-aware shadow colors', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      // Shadow with primary color overlay
      expect(primaryCTA.className).toContain('shadow-primary')
    })

    it('uses base classes that work across all DaisyUI themes', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      // DaisyUI btn class provides theme-aware defaults
      expect(primaryCTA.className).toContain('btn')
    })

    it('uses ring-offset with base-100 for proper focus visibility in all themes', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('focus-visible:ring-offset-base-100')
    })
  })

  describe('FuturisticButton Size and Accessibility', () => {
    it('primary CTA uses large size for hero section prominence', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('btn-lg')
      expect(primaryCTA.className).toContain('min-h-[52px]')
    })

    it('meets minimum touch target size of 44px', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      // min-h-[52px] exceeds 44px WCAG requirement
      expect(primaryCTA.className).toContain('min-h-[52px]')
    })

    it('has overflow hidden for contained shine effect', () => {
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('overflow-hidden')
    })
  })

  describe('Component Integration Verification', () => {
    it('FuturisticButton is imported and used in HeroSection', async () => {
      // This verifies the component is properly integrated by checking
      // that the characteristic classes unique to FuturisticButton are present
      renderHeroSection()

      const primaryCTA = screen.getByTestId('cta-register')

      // These specific class combinations are unique to FuturisticButton
      const hasFuturisticStyling =
        primaryCTA.className.includes('bg-gradient-to-r') &&
        primaryCTA.className.includes('from-primary') &&
        primaryCTA.className.includes('to-secondary') &&
        primaryCTA.className.includes('backdrop-blur-sm') &&
        primaryCTA.className.includes('rounded-xl')

      expect(hasFuturisticStyling).toBe(true)
    })

    it('CTA button navigates to registration page', () => {
      renderHeroSection()

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })
  })
})
