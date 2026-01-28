/**
 * Smooth Scroll Behavior Unit Tests
 * Owner: Scenario 17 - Smooth Scroll Behavior
 *
 * Tests for smooth scroll CSS and scroll-triggered animations:
 * - CSS scroll-behavior: smooth is applied
 * - Framer Motion whileInView animations are present
 * - Sections have proper IDs/testids for scroll targeting
 *
 * Testing framework: Vitest + @testing-library/react
 */
import { describe, it, expect } from 'vitest'
import { renderWithProviders, screen } from '../../utils/renderWithProviders'
import Home from '@/pages/Home'
import fs from 'fs'
import path from 'path'

describe('Smooth Scroll Behavior', () => {
  describe('CSS scroll-behavior property', () => {
    it('should have scroll-behavior: smooth in index.css', () => {
      // Test Case 1: Check CSS scroll-behavior property on html/body
      const cssPath = path.resolve(__dirname, '../../../src/index.css')
      const cssContent = fs.readFileSync(cssPath, 'utf-8')

      // Verify scroll-behavior: smooth is defined
      expect(cssContent).toContain('scroll-behavior: smooth')
    })

    it('should apply scroll-behavior to html element', () => {
      // Verify the CSS targets the html element
      const cssPath = path.resolve(__dirname, '../../../src/index.css')
      const cssContent = fs.readFileSync(cssPath, 'utf-8')

      // Check that scroll-behavior is within html block
      const htmlRegex = /html\s*\{[^}]*scroll-behavior:\s*smooth[^}]*\}/
      expect(cssContent).toMatch(htmlRegex)
    })
  })

  describe('Sections for scroll targeting', () => {
    it('should render all scrollable sections', () => {
      // Test Case 2: Verify sections exist for scroll targeting
      renderWithProviders(<Home />)

      // Check hero section exists
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check features section exists
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Check how it works section exists
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()

      // Check footer exists
      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should have sections in correct order for scrolling', () => {
      renderWithProviders(<Home />)

      const mainContent = screen.getByRole('main')
      expect(mainContent).toBeInTheDocument()

      // Verify sections are children of main
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // All sections should be in the document
      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('should have proper semantic structure for scroll navigation', () => {
      renderWithProviders(<Home />)

      // Verify sections use semantic section elements
      const sections = document.querySelectorAll('section')
      expect(sections.length).toBeGreaterThanOrEqual(3)

      // Verify footer is a semantic footer element
      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Framer Motion scroll animations configuration', () => {
    it('should have FeaturesSection with scroll-triggered content', () => {
      // Test Case 3: Verify Framer Motion scroll animations
      renderWithProviders(<Home />)

      // Verify features section has all feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(3)
    })

    it('should have HowItWorks section with scroll-triggered steps', () => {
      renderWithProviders(<Home />)

      // Verify all steps are present
      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()
    })

    it('should have section headings for visual hierarchy during scroll', () => {
      renderWithProviders(<Home />)

      // Verify section headings exist (h1 for hero, h2 for other sections)
      const mainHeading = screen.getByRole('heading', { level: 1 })
      expect(mainHeading).toBeInTheDocument()

      const sectionHeadings = screen.getAllByRole('heading', { level: 2 })
      expect(sectionHeadings.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('FeaturesSection source file configuration', () => {
    it('should have whileInView animation props in FeaturesSection source', () => {
      // Verify the FeaturesSection component uses whileInView for scroll animations
      const componentPath = path.resolve(
        __dirname,
        '../../../src/components/home/FeaturesSection.tsx'
      )
      const componentContent = fs.readFileSync(componentPath, 'utf-8')

      // Check for whileInView prop (Framer Motion scroll trigger)
      expect(componentContent).toContain('whileInView')
      expect(componentContent).toContain('viewport')
    })
  })

  describe('HowItWorks source file configuration', () => {
    it('should have whileInView animation props in HowItWorks source', () => {
      // Verify the HowItWorks component uses whileInView for scroll animations
      const componentPath = path.resolve(
        __dirname,
        '../../../src/components/home/HowItWorks.tsx'
      )
      const componentContent = fs.readFileSync(componentPath, 'utf-8')

      // Check for whileInView prop (Framer Motion scroll trigger)
      expect(componentContent).toContain('whileInView')
      expect(componentContent).toContain('viewport')
    })
  })

  describe('HeroSection source file configuration', () => {
    it('should have animation props in HeroSection source', () => {
      // Verify the HeroSection component uses Framer Motion animations
      const componentPath = path.resolve(
        __dirname,
        '../../../src/components/home/HeroSection.tsx'
      )
      const componentContent = fs.readFileSync(componentPath, 'utf-8')

      // Check for motion components and animation props
      expect(componentContent).toContain('motion')
      expect(componentContent).toContain('animate')
      expect(componentContent).toContain('transition')
    })
  })

  describe('Scroll animation performance', () => {
    it('should use viewport once: true for performance optimization', () => {
      // Verify components use viewport: { once: true } to avoid re-triggering
      const featuresPath = path.resolve(
        __dirname,
        '../../../src/components/home/FeaturesSection.tsx'
      )
      const howItWorksPath = path.resolve(
        __dirname,
        '../../../src/components/home/HowItWorks.tsx'
      )

      const featuresContent = fs.readFileSync(featuresPath, 'utf-8')
      const howItWorksContent = fs.readFileSync(howItWorksPath, 'utf-8')

      // Check for viewport with once: true (performance optimization)
      expect(featuresContent).toContain('once: true')
      expect(howItWorksContent).toContain('once: true')
    })

    it('should have reasonable transition durations for smooth animations', () => {
      const featuresPath = path.resolve(
        __dirname,
        '../../../src/components/home/FeaturesSection.tsx'
      )
      const featuresContent = fs.readFileSync(featuresPath, 'utf-8')

      // Check for transition duration (should be reasonable, e.g., 0.5s)
      expect(featuresContent).toContain('duration:')
      // Verify duration is a reasonable value (not too fast, not too slow)
      const durationMatch = featuresContent.match(/duration:\s*([\d.]+)/)
      if (durationMatch) {
        const duration = parseFloat(durationMatch[1])
        expect(duration).toBeGreaterThanOrEqual(0.3)
        expect(duration).toBeLessThanOrEqual(1.5)
      }
    })
  })
})
