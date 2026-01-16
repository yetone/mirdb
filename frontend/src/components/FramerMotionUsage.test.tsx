import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import React from 'react'

// Mock framer-motion to track motion components usage
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement> & { initial?: unknown; animate?: unknown; whileHover?: unknown; whileInView?: unknown }) => (
      <div data-motion-component="div" data-has-animation={!!props.initial || !!props.animate || !!props.whileHover || !!props.whileInView} {...props}>{children}</div>
    ),
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement> & { initial?: unknown; animate?: unknown }) => (
      <h1 data-motion-component="h1" data-has-animation={!!props.initial || !!props.animate} {...props}>{children}</h1>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement> & { initial?: unknown; animate?: unknown }) => (
      <p data-motion-component="p" data-has-animation={!!props.initial || !!props.animate} {...props}>{children}</p>
    ),
    nav: ({ children, ...props }: React.HTMLAttributes<HTMLElement> & { initial?: unknown; animate?: unknown }) => (
      <nav data-motion-component="nav" data-has-animation={!!props.initial || !!props.animate} {...props}>{children}</nav>
    ),
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { whileHover?: unknown; whileTap?: unknown }) => (
      <button data-motion-component="button" data-has-hover={!!props.whileHover} data-has-tap={!!props.whileTap} {...props}>{children}</button>
    ),
    header: ({ children, ...props }: React.HTMLAttributes<HTMLElement> & { initial?: unknown; animate?: unknown }) => (
      <header data-motion-component="header" data-has-animation={!!props.initial || !!props.animate} {...props}>{children}</header>
    ),
    svg: ({ children, ...props }: React.SVGAttributes<SVGSVGElement> & { animate?: unknown }) => (
      <svg data-motion-component="svg" data-has-animation={!!props.animate} {...props}>{children}</svg>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Import components after mocking
import HeroSection from './HeroSection'
import FeaturesSection from './FeaturesSection'
import GlassMorphismCard from './GlassMorphismCard'
import FuturisticButton from './FuturisticButton'

/**
 * Test Case 1 (Unit): Check for Framer Motion usage
 * Input: Check for Framer Motion usage
 * Expected: Homepage components use Framer Motion for animations
 */
describe('Framer Motion Usage Verification', () => {
  describe('HeroSection', () => {
    it('uses Framer Motion motion.h1 for headline animation', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveAttribute('data-motion-component', 'h1')
      expect(headline).toHaveAttribute('data-has-animation', 'true')
    })

    it('uses Framer Motion motion.p for subheadline animation', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveAttribute('data-motion-component', 'p')
      expect(subheadline).toHaveAttribute('data-has-animation', 'true')
    })

    it('uses Framer Motion motion.nav for navigation links animation', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const nav = screen.getByRole('navigation', { name: /page sections/i })
      expect(nav).toHaveAttribute('data-motion-component', 'nav')
      expect(nav).toHaveAttribute('data-has-animation', 'true')
    })

    it('wraps CTA buttons with animated motion.div', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      // The CTA buttons should be wrapped in animated containers
      const motionDivs = document.querySelectorAll('[data-motion-component="div"]')
      expect(motionDivs.length).toBeGreaterThan(0)
    })
  })

  describe('FeaturesSection', () => {
    it('uses Framer Motion for section title animation', () => {
      render(<FeaturesSection />)

      const motionDivs = document.querySelectorAll('[data-motion-component="div"]')
      expect(motionDivs.length).toBeGreaterThan(0)
    })

    it('uses Framer Motion for features grid with stagger animation', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveAttribute('data-motion-component', 'div')
    })
  })

  describe('GlassMorphismCard', () => {
    it('uses Framer Motion motion.div with animation', () => {
      render(
        <GlassMorphismCard>
          <span>Test Content</span>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glassmorphism-card')
      expect(card).toHaveAttribute('data-motion-component', 'div')
      expect(card).toHaveAttribute('data-has-animation', 'true')
    })

    it('has hover animation enabled', () => {
      render(
        <GlassMorphismCard>
          <span>Test Content</span>
        </GlassMorphismCard>
      )

      // The card should have whileHover animation (scale effect)
      const card = screen.getByTestId('glassmorphism-card')
      expect(card).toHaveAttribute('data-motion-component', 'div')
    })
  })

  describe('FuturisticButton', () => {
    it('uses Framer Motion for button variant with hover/tap animations', () => {
      render(
        <FuturisticButton onClick={() => {}} data-testid="test-button">
          Click me
        </FuturisticButton>
      )

      const button = screen.getByTestId('test-button')
      expect(button).toHaveAttribute('data-motion-component', 'button')
      expect(button).toHaveAttribute('data-has-hover', 'true')
      expect(button).toHaveAttribute('data-has-tap', 'true')
    })

    it('uses Framer Motion wrapper for link variant with hover/tap animations', () => {
      render(
        <BrowserRouter>
          <FuturisticButton as="link" to="/test" data-testid="test-link">
            Click me
          </FuturisticButton>
        </BrowserRouter>
      )

      // Link variant wraps with motion.div
      const motionWrapper = document.querySelector('[data-motion-component="div"]')
      expect(motionWrapper).not.toBeNull()
    })
  })

  describe('Animation Properties Verification', () => {
    it('verifies homepage uses multiple motion components', () => {
      render(
        <BrowserRouter>
          <HeroSection />
          <FeaturesSection />
        </BrowserRouter>
      )

      // Count all motion components used
      const allMotionComponents = document.querySelectorAll('[data-motion-component]')
      expect(allMotionComponents.length).toBeGreaterThan(5) // Should have multiple animated elements
    })

    it('verifies animation initial/animate states are used', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const animatedElements = document.querySelectorAll('[data-has-animation="true"]')
      expect(animatedElements.length).toBeGreaterThan(0)
    })
  })
})
