import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../contexts/ThemeContext'
import React from 'react'

/**
 * Framer Motion Animations Unit Tests
 *
 * This test file covers Scenario 16: Framer Motion Animations
 * Test Case 1: "Check for Framer Motion usage"
 * Expected: "Homepage components use Framer Motion for animations"
 *
 * These tests verify that:
 * 1. Homepage components properly import and use Framer Motion
 * 2. Motion components are configured with appropriate animation props
 * 3. Animation variants are properly defined
 */

// Store original framer-motion module
let originalFramerMotion: any

// Track motion component usage
const motionComponentUsage: Record<string, { count: number; props: any[] }> = {}

// Mock framer-motion to track usage and verify animation configuration
vi.mock('framer-motion', () => {
  const createMotionComponent = (tag: string) => {
    const Component = React.forwardRef(({ children, ...props }: any, ref: any) => {
      // Track usage
      if (!motionComponentUsage[tag]) {
        motionComponentUsage[tag] = { count: 0, props: [] }
      }
      motionComponentUsage[tag].count++
      motionComponentUsage[tag].props.push(props)

      // Remove framer-motion specific props that React doesn't recognize
      const {
        initial,
        animate,
        exit,
        transition,
        whileHover,
        whileTap,
        whileInView,
        variants,
        viewport,
        layout,
        layoutId,
        drag,
        dragConstraints,
        onAnimationComplete,
        ...domProps
      } = props
      const Tag = tag as any
      return <Tag ref={ref} data-motion={tag} data-has-animation={!!initial || !!animate || !!variants || !!whileHover || !!whileTap} {...domProps}>{children}</Tag>
    })
    Component.displayName = `motion.${tag}`
    return Component
  }

  return {
    motion: {
      h1: createMotionComponent('h1'),
      h2: createMotionComponent('h2'),
      h3: createMotionComponent('h3'),
      p: createMotionComponent('p'),
      div: createMotionComponent('div'),
      nav: createMotionComponent('nav'),
      section: createMotionComponent('section'),
      span: createMotionComponent('span'),
      button: createMotionComponent('button'),
      svg: createMotionComponent('svg'),
      a: createMotionComponent('a'),
      li: createMotionComponent('li'),
      ul: createMotionComponent('ul'),
      header: createMotionComponent('header'),
    },
    AnimatePresence: ({ children }: any) => <>{children}</>,
  }
})

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('Framer Motion Animations - Unit Tests', () => {
  beforeEach(() => {
    // Reset tracking before each test
    Object.keys(motionComponentUsage).forEach(key => {
      delete motionComponentUsage[key]
    })
  })

  describe('Test Case 1: Check for Framer Motion usage', () => {
    it('HeroSection uses Framer Motion for animations', async () => {
      const HeroSection = (await import('./HeroSection')).default
      renderWithProviders(<HeroSection />)

      // Check that motion components are used
      const motionElements = document.querySelectorAll('[data-motion]')
      expect(motionElements.length).toBeGreaterThan(0)

      // Verify motion.h1 is used for headline
      const motionH1 = document.querySelector('[data-motion="h1"]')
      expect(motionH1).toBeTruthy()
      expect(motionH1?.textContent).toContain('Shorten')

      // Verify motion.p is used for subheadline
      const motionP = document.querySelector('[data-motion="p"]')
      expect(motionP).toBeTruthy()

      // Verify motion.div is used for CTA container
      const motionDivs = document.querySelectorAll('[data-motion="div"]')
      expect(motionDivs.length).toBeGreaterThan(0)

      // Verify animation props are configured
      const animatedElements = document.querySelectorAll('[data-has-animation="true"]')
      expect(animatedElements.length).toBeGreaterThan(0)
    })

    it('HeroSection configures entry animations correctly', async () => {
      const HeroSection = (await import('./HeroSection')).default
      renderWithProviders(<HeroSection />)

      // Check h1 animation props
      const h1Props = motionComponentUsage['h1']?.props[0]
      expect(h1Props).toBeDefined()
      expect(h1Props.initial).toEqual({ opacity: 0, y: 20 })
      expect(h1Props.animate).toEqual({ opacity: 1, y: 0 })
      expect(h1Props.transition).toBeDefined()
      expect(h1Props.transition.duration).toBe(0.6)
    })

    it('FeaturesSection uses Framer Motion container variants', async () => {
      const FeaturesSection = (await import('./FeaturesSection')).default
      renderWithProviders(<FeaturesSection />)

      // Check that motion.div is used for container
      const motionDivs = document.querySelectorAll('[data-motion="div"]')
      expect(motionDivs.length).toBeGreaterThan(0)

      // Verify container variants are configured
      const containerProps = motionComponentUsage['div']?.props.find(p => p.variants)
      expect(containerProps).toBeDefined()
      expect(containerProps.variants).toBeDefined()
      expect(containerProps.initial).toBe('hidden')
      expect(containerProps.animate).toBe('visible')
    })

    it('GlassMorphismCard uses Framer Motion for card animations', async () => {
      const GlassMorphismCard = (await import('./GlassMorphismCard')).default
      renderWithProviders(
        <GlassMorphismCard>
          <span>Test Content</span>
        </GlassMorphismCard>
      )

      // Check that motion.div is used
      const motionDiv = document.querySelector('[data-motion="div"]')
      expect(motionDiv).toBeTruthy()

      // Verify hover animation is configured
      const cardProps = motionComponentUsage['div']?.props[0]
      expect(cardProps).toBeDefined()
      expect(cardProps.whileHover).toBeDefined()
      expect(cardProps.whileHover.scale).toBe(1.02)

      // Verify entry animation is configured
      expect(cardProps.initial).toEqual({ opacity: 0, y: 20 })
      expect(cardProps.animate).toEqual({ opacity: 1, y: 0 })
    })

    it('FuturisticButton uses Framer Motion for button animations', async () => {
      const FuturisticButton = (await import('./FuturisticButton')).default
      renderWithProviders(
        <FuturisticButton>Test Button</FuturisticButton>
      )

      // Check that motion.button is used
      const motionButton = document.querySelector('[data-motion="button"]')
      expect(motionButton).toBeTruthy()

      // Verify hover and tap animations are configured
      const buttonProps = motionComponentUsage['button']?.props[0]
      expect(buttonProps).toBeDefined()
      expect(buttonProps.whileHover).toBeDefined()
      expect(buttonProps.whileTap).toBeDefined()
    })

    it('Homepage uses multiple motion components for comprehensive animations', async () => {
      const HeroSection = (await import('./HeroSection')).default
      const FeaturesSection = (await import('./FeaturesSection')).default

      renderWithProviders(
        <>
          <HeroSection />
          <FeaturesSection />
        </>
      )

      // Count unique motion component types used
      const usedMotionTypes = Object.keys(motionComponentUsage)

      // Should use at least 3 different motion component types
      expect(usedMotionTypes.length).toBeGreaterThanOrEqual(3)

      // Total motion components should be significant
      const totalMotionComponents = Object.values(motionComponentUsage)
        .reduce((sum, usage) => sum + usage.count, 0)
      expect(totalMotionComponents).toBeGreaterThan(5)
    })

    it('all animated elements have proper animation configuration', async () => {
      const HeroSection = (await import('./HeroSection')).default
      renderWithProviders(<HeroSection />)

      // Get all animation props from tracked usage
      const allProps = Object.values(motionComponentUsage).flatMap(u => u.props)

      // Count elements with valid animation configuration
      // Each animated element should have either:
      // - initial/animate props, OR
      // - variants prop, OR
      // - whileHover/whileTap props
      const animatedElements = allProps.filter(props =>
        (props.initial !== undefined && props.animate !== undefined) ||
        props.variants !== undefined ||
        props.whileHover !== undefined ||
        props.whileTap !== undefined ||
        props.whileInView !== undefined
      )

      // Majority of motion elements should have animation configuration
      // Some may be wrapper elements that just use motion for layout purposes
      expect(animatedElements.length).toBeGreaterThan(0)
      expect(animatedElements.length / allProps.length).toBeGreaterThan(0.5)
    })

    it('animations use performance-friendly durations', async () => {
      const HeroSection = (await import('./HeroSection')).default
      renderWithProviders(<HeroSection />)

      // Get all animation props with transitions
      const allProps = Object.values(motionComponentUsage).flatMap(u => u.props)
      const transitions = allProps.filter(p => p.transition).map(p => p.transition)

      // Check that durations are reasonable (not too long for smooth UX)
      transitions.forEach(transition => {
        if (transition.duration) {
          expect(transition.duration).toBeLessThanOrEqual(1) // Max 1 second
          expect(transition.duration).toBeGreaterThan(0)
        }
      })
    })

    it('stagger animations are configured for lists', async () => {
      const FeaturesSection = (await import('./FeaturesSection')).default
      renderWithProviders(<FeaturesSection />)

      // Find container with variants that includes staggerChildren
      const containerProps = motionComponentUsage['div']?.props.find(p => p.variants)
      expect(containerProps).toBeDefined()
      expect(containerProps.variants.visible).toBeDefined()
      expect(containerProps.variants.visible.transition).toBeDefined()
      expect(containerProps.variants.visible.transition.staggerChildren).toBeDefined()
      expect(containerProps.variants.visible.transition.staggerChildren).toBeGreaterThan(0)
    })
  })

  describe('Animation Import Verification', () => {
    it('Framer Motion is properly imported in HeroSection', async () => {
      const HeroSectionModule = await import('./HeroSection')
      expect(HeroSectionModule.default).toBeDefined()

      // Render to trigger the imports
      renderWithProviders(<HeroSectionModule.default />)

      // Motion components should be used
      expect(Object.keys(motionComponentUsage).length).toBeGreaterThan(0)
    })

    it('Framer Motion is properly imported in FeaturesSection', async () => {
      const FeaturesSectionModule = await import('./FeaturesSection')
      expect(FeaturesSectionModule.default).toBeDefined()

      renderWithProviders(<FeaturesSectionModule.default />)
      expect(Object.keys(motionComponentUsage).length).toBeGreaterThan(0)
    })

    it('Framer Motion is properly imported in GlassMorphismCard', async () => {
      const GlassMorphismCardModule = await import('./GlassMorphismCard')
      expect(GlassMorphismCardModule.default).toBeDefined()

      renderWithProviders(
        <GlassMorphismCardModule.default>Content</GlassMorphismCardModule.default>
      )
      expect(motionComponentUsage['div']).toBeDefined()
    })
  })
})
