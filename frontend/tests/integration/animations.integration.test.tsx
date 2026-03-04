/**
 * Animation Integration Tests - Scenario 8: Animations and Micro-interactions
 *
 * Integration tests verifying Framer Motion animations are correctly implemented
 * in homepage components:
 * - TC1: Hero section uses motion.div with initial and animate props
 * - TC2: Hero animation configuration (0.5-1s duration)
 * - TC3: Feature cards have stagger animation (100ms delay)
 * - TC4: FuturisticButton has whileHover scale transform
 * - TC5: Sections use whileInView for scroll-triggered animations
 * - TC6: Reduced motion handling
 */

import { describe, it, expect } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'

// Helper to read source files and check for animation patterns
const readSourceFile = (relativePath: string): string => {
  const fullPath = path.join(process.cwd(), 'src', relativePath)
  return fs.readFileSync(fullPath, 'utf-8')
}

describe('Scenario 8: Component Animation Integration', () => {
  describe('Test Case 1: Hero Section Framer Motion Presence', () => {
    it('HeroSection should import motion from framer-motion', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/import.*motion.*from ['"]framer-motion['"]/)
    })

    it('HeroSection should use motion.div with initial prop', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/motion\.div/)
      expect(source).toMatch(/initial=/)
    })

    it('HeroSection should use motion.div with animate prop', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/animate=/)
    })

    it('HeroSection should use variants for animation configuration', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/variants=/)
    })
  })

  describe('Test Case 2: Hero Animation Configuration', () => {
    it('HeroSection should have fadeInVariants with hidden and visible states', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/fadeInVariants|fadeIn/)
      expect(source).toMatch(/hidden/)
      expect(source).toMatch(/visible/)
    })

    it('HeroSection should have opacity animation', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      expect(source).toMatch(/opacity:\s*0/)
      expect(source).toMatch(/opacity:\s*1/)
    })

    it('HeroSection should have duration between 0.5 and 1 second', () => {
      const source = readSourceFile('components/home/HeroSection.tsx')
      // Match duration values like 0.5, 0.6, 0.7, 0.8, 0.9, or 1
      const durationMatch = source.match(/duration:\s*(0\.[5-9]|1\.0|1(?!\.))/g)
      expect(durationMatch).not.toBeNull()
      expect(durationMatch!.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 3: Feature Cards Stagger Animation', () => {
    it('FeaturesSection should import motion from framer-motion', () => {
      const source = readSourceFile('components/home/FeaturesSection.tsx')
      expect(source).toMatch(/import.*motion.*from ['"]framer-motion['"]/)
    })

    it('FeaturesSection should import featureCardVariants from animations', () => {
      const source = readSourceFile('components/home/FeaturesSection.tsx')
      expect(source).toMatch(/import.*featureCardVariants.*from.*animations/)
    })

    it('FeaturesSection should use whileInView for scroll animation', () => {
      const source = readSourceFile('components/home/FeaturesSection.tsx')
      expect(source).toMatch(/whileInView=/)
    })

    it('animations.ts should have staggerChildren: 0.1 (100ms)', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/staggerChildren:\s*0\.1/)
    })

    it('featureCardVariants should have index-based delay calculation', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/delay:\s*index\s*\*\s*0\.1/)
    })
  })

  describe('Test Case 4: CTA Button Hover Effects', () => {
    it('FuturisticButton should import motion from framer-motion', () => {
      const source = readSourceFile('components/FuturisticButton.tsx')
      expect(source).toMatch(/import.*motion.*from ['"]framer-motion['"]/)
    })

    it('FuturisticButton should use motion.button', () => {
      const source = readSourceFile('components/FuturisticButton.tsx')
      expect(source).toMatch(/motion\.button/)
    })

    it('FuturisticButton should have whileHover prop', () => {
      const source = readSourceFile('components/FuturisticButton.tsx')
      expect(source).toMatch(/whileHover=/)
    })

    it('FuturisticButton should have scale transform on hover', () => {
      const source = readSourceFile('components/FuturisticButton.tsx')
      expect(source).toMatch(/whileHover=.*scale/)
    })

    it('FuturisticButton should have whileTap for press feedback', () => {
      const source = readSourceFile('components/FuturisticButton.tsx')
      expect(source).toMatch(/whileTap=/)
    })
  })

  describe('Test Case 5: Scroll-Triggered Animations (whileInView)', () => {
    it('FeaturesSection should use whileInView prop', () => {
      const source = readSourceFile('components/home/FeaturesSection.tsx')
      expect(source).toMatch(/whileInView=/)
    })

    it('FeaturesSection should have viewport config with once: true', () => {
      const source = readSourceFile('components/home/FeaturesSection.tsx')
      expect(source).toMatch(/viewport=.*once:\s*true/)
    })

    it('FAQSection should use whileInView prop', () => {
      const source = readSourceFile('components/home/FAQSection.tsx')
      expect(source).toMatch(/whileInView=/)
    })

    it('FAQSection should have viewport config', () => {
      const source = readSourceFile('components/home/FAQSection.tsx')
      expect(source).toMatch(/viewport=/)
    })

    it('CTAFooter should use whileInView prop', () => {
      const source = readSourceFile('components/home/CTAFooter.tsx')
      expect(source).toMatch(/whileInView=/)
    })

    it('CTAFooter should have viewport config with once: true', () => {
      const source = readSourceFile('components/home/CTAFooter.tsx')
      expect(source).toMatch(/viewport=.*once:\s*true/)
    })
  })

  describe('Test Case 6: Reduced Motion Handling', () => {
    it('animations.ts should export prefersReducedMotion function', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/export\s+(const|function)\s+prefersReducedMotion/)
    })

    it('animations.ts should check for prefers-reduced-motion media query', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/prefers-reduced-motion/)
    })

    it('animations.ts should export getReducedMotionVariants helper', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/export\s+const\s+getReducedMotionVariants/)
    })

    it('animations.ts should export getMotionSafeDuration helper', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/export\s+const\s+getMotionSafeDuration/)
    })

    it('animations.ts should define REDUCED_MOTION_DURATION constant', () => {
      const source = readSourceFile('utils/animations.ts')
      expect(source).toMatch(/export\s+const\s+REDUCED_MOTION_DURATION/)
    })
  })

  describe('Animation Configuration Consistency', () => {
    it('Home page should import AnimatePresence from framer-motion', () => {
      const source = readSourceFile('pages/Home.tsx')
      expect(source).toMatch(/import.*AnimatePresence.*from ['"]framer-motion['"]/)
    })

    it('Home page should use motion.main for page container', () => {
      const source = readSourceFile('pages/Home.tsx')
      expect(source).toMatch(/motion\.main/)
    })

    it('GlassMorphismCard should support motion props', () => {
      const source = readSourceFile('components/GlassMorphismCard.tsx')
      // Should accept motion props or use forwardRef for motion compatibility
      expect(source).toMatch(/motion|forwardRef|HTMLMotionProps/)
    })
  })
})
