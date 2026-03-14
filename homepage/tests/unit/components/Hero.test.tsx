/**
 * Unit tests for Hero section.
 * Owner: Scenario 2 - Hero Section Implementation
 *
 * Test cases:
 * - Hero displays headline, subheadline, CTA
 * - Headline uses proper h1 heading tag
 * - Subheadline is present with supporting text
 * - CTA button meets contrast requirements
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero, HeroProps } from '../../../src/components/sections/Hero'

const defaultProps: HeroProps = {
  headline: 'Transform Your Workflow with Intelligent Automation',
  subheadline:
    'Our platform helps teams work smarter, not harder. Automate repetitive tasks, streamline collaboration, and unlock your team potential.',
  ctaButton: {
    label: 'Get Started Free',
    href: '#signup',
    variant: 'primary',
    size: 'lg',
  },
}

describe('Hero Section', () => {
  // Test Case 1: Hero displays headline, subheadline, and primary CTA button
  describe('Test Case 1: Render Hero section', () => {
    it('renders headline, subheadline, and primary CTA button', () => {
      render(<Hero {...defaultProps} />)

      // Check hero section is rendered
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check headline is rendered
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(defaultProps.headline)

      // Check subheadline is rendered
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline).toHaveTextContent(defaultProps.subheadline)

      // Check CTA button is rendered
      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent(defaultProps.ctaButton.label)
    })
  })

  // Test Case 2: Headline is present, non-empty, and uses proper h1 heading tag
  describe('Test Case 2: Check headline text content', () => {
    it('headline uses proper h1 heading tag', () => {
      render(<Hero {...defaultProps} />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline.tagName).toBe('H1')
    })

    it('headline is non-empty', () => {
      render(<Hero {...defaultProps} />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline.textContent).toBeTruthy()
      expect(headline.textContent!.length).toBeGreaterThan(0)
    })

    it('headline has proper id for accessibility', () => {
      render(<Hero {...defaultProps} />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveAttribute('id', 'hero-headline')
    })
  })

  // Test Case 3: Subheadline is present with supporting text content
  describe('Test Case 3: Check subheadline text content', () => {
    it('subheadline is present with supporting text content', () => {
      render(<Hero {...defaultProps} />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toBeTruthy()
      expect(subheadline.textContent!.length).toBeGreaterThan(0)
    })

    it('subheadline renders the provided text', () => {
      render(<Hero {...defaultProps} />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent(defaultProps.subheadline)
    })
  })

  // Test Case 5: Button meets WCAG 2.1 AA contrast requirements (4.5:1 minimum)
  describe('Test Case 5: Verify CTA button contrast ratio', () => {
    it('primary button has high-contrast styling classes', () => {
      render(<Hero {...defaultProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')

      // Primary button should have bg-primary-600 (blue) and text-white
      // This color combination provides contrast ratio > 4.5:1
      expect(ctaButton).toHaveClass('bg-primary-600')
      expect(ctaButton).toHaveClass('text-white')
    })

    it('button has appropriate size for readability', () => {
      render(<Hero {...defaultProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')

      // Large size button should have proper padding and text size
      expect(ctaButton).toHaveClass('px-8')
      expect(ctaButton).toHaveClass('py-4')
      expect(ctaButton).toHaveClass('text-lg')
    })

    it('button has proper accessible attributes', () => {
      render(<Hero {...defaultProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')

      expect(ctaButton).toHaveAttribute('role', 'button')
      expect(ctaButton).toHaveAttribute('aria-label', defaultProps.ctaButton.label)
    })

    // Test different button variants for contrast
    it('secondary button variant has high-contrast styling', () => {
      const secondaryProps: HeroProps = {
        ...defaultProps,
        ctaButton: { ...defaultProps.ctaButton, variant: 'secondary' },
      }
      render(<Hero {...secondaryProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveClass('bg-secondary-600')
      expect(ctaButton).toHaveClass('text-white')
    })

    it('outline button variant has visible styling', () => {
      const outlineProps: HeroProps = {
        ...defaultProps,
        ctaButton: { ...defaultProps.ctaButton, variant: 'outline' },
      }
      render(<Hero {...outlineProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveClass('border-2')
      expect(ctaButton).toHaveClass('border-primary-600')
      expect(ctaButton).toHaveClass('text-primary-600')
    })
  })

  // Additional tests for Hero section
  describe('Hero section accessibility', () => {
    it('section has proper aria-labelledby attribute', () => {
      render(<Hero {...defaultProps} />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')
    })

    it('CTA button has focus ring styles', () => {
      render(<Hero {...defaultProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveClass('focus:outline-none')
      expect(ctaButton).toHaveClass('focus:ring-4')
      expect(ctaButton).toHaveClass('focus:ring-offset-2')
    })
  })

  describe('Hero section with optional props', () => {
    it('renders without hero image when not provided', () => {
      render(<Hero {...defaultProps} />)

      const heroImage = screen.queryByTestId('hero-image')
      expect(heroImage).not.toBeInTheDocument()
    })

    it('renders hero image when provided', () => {
      const propsWithImage: HeroProps = {
        ...defaultProps,
        heroImage: '/images/hero.png',
      }
      render(<Hero {...propsWithImage} />)

      const heroImage = screen.getByTestId('hero-image')
      expect(heroImage).toBeInTheDocument()
      expect(heroImage).toHaveAttribute('src', '/images/hero.png')
    })
  })

  describe('Hero button sizes', () => {
    it('small button has correct size classes', () => {
      const smallProps: HeroProps = {
        ...defaultProps,
        ctaButton: { ...defaultProps.ctaButton, size: 'sm' },
      }
      render(<Hero {...smallProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveClass('px-4')
      expect(ctaButton).toHaveClass('py-2')
      expect(ctaButton).toHaveClass('text-sm')
    })

    it('medium button has correct size classes', () => {
      const mediumProps: HeroProps = {
        ...defaultProps,
        ctaButton: { ...defaultProps.ctaButton, size: 'md' },
      }
      render(<Hero {...mediumProps} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveClass('px-6')
      expect(ctaButton).toHaveClass('py-3')
      expect(ctaButton).toHaveClass('text-base')
    })
  })
})
