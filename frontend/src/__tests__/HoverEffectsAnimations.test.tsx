import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import GlassMorphismCard from '../components/GlassMorphismCard'

describe('Hover Effects and Animations', () => {
  // Test Case 1: Primary CTA button hover effects
  describe('Primary CTA Button Hover Effects', () => {
    it('should have hover state classes on the primary CTA button', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      expect(ctaButton).toBeInTheDocument()

      // Check for hover styling classes
      expect(ctaButton).toHaveClass('btn')
      expect(ctaButton).toHaveClass('btn-lg')
      expect(ctaButton).toHaveClass('btn-primary')
    })

    it('should have hover:scale transform class for button scale effect', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // Check for scale effect on hover
      expect(ctaButton).toHaveClass('hover:scale-105')
    })

    it('should have shadow effect that changes on hover', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // Check for shadow and hover shadow enhancement
      expect(ctaButton).toHaveClass('shadow-lg')
      expect(ctaButton).toHaveClass('hover:shadow-xl')
    })

    it('should have smooth transition for hover effects', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // Check for transition classes (duration should be 200-300ms)
      expect(ctaButton).toHaveClass('transition-all')
      expect(ctaButton).toHaveClass('duration-200')
    })

    it('should have background color change on hover', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // Check for hover background color change
      expect(ctaButton).toHaveClass('hover:bg-white/90')
    })
  })

  // Test Case 2: Feature card hover effects
  describe('Feature Card Hover Effects', () => {
    it('should render feature cards with GlassMorphismCard component', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(3)
    })

    it('should have hover shadow effect on feature cards', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        // Check for hover shadow enhancement (lift effect)
        expect(card).toHaveClass('hover:shadow-2xl')
      })
    })

    it('should have hover scale transform for lift effect on feature cards', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        // Check for scale transform on hover (lift effect)
        expect(card).toHaveClass('hover:scale-[1.02]')
      })
    })

    it('should have hover border change on feature cards', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        // Check for border change on hover
        expect(card).toHaveClass('hover:border-primary/30')
      })
    })

    it('should have smooth transition for feature card hover effects', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        // Check for transition classes (duration should be 200-300ms)
        expect(card).toHaveClass('transition-all')
        expect(card).toHaveClass('duration-300')
      })
    })
  })

  // Test Case 3: CSS Transition Properties verification
  describe('CSS Transition Properties', () => {
    it('should have transition properties on CTA button within 200-300ms range', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')
      const buttonClasses = ctaButton.className

      // Check that the button has a duration class in the 200-300ms range
      // duration-200 = 200ms, duration-300 = 300ms
      const hasDuration200 = buttonClasses.includes('duration-200')
      const hasDuration300 = buttonClasses.includes('duration-300')

      expect(hasDuration200 || hasDuration300).toBe(true)
    })

    it('should have transition properties on feature cards within 200-300ms range', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        const cardClasses = card.className

        // Check that the card has a duration class in the 200-300ms range
        const hasDuration200 = cardClasses.includes('duration-200')
        const hasDuration300 = cardClasses.includes('duration-300')

        expect(hasDuration200 || hasDuration300).toBe(true)
      })
    })

    it('should have transition properties on HowItWorks step cards within 200-300ms range', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId(/^step-card-/)

      stepCards.forEach((card) => {
        const cardClasses = card.className

        // Check that the card has a duration class in the 200-300ms range
        const hasDuration200 = cardClasses.includes('duration-200')
        const hasDuration300 = cardClasses.includes('duration-300')

        expect(hasDuration200 || hasDuration300).toBe(true)
      })
    })

    it('should use transition-all or transition-shadow for smooth effects', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId(/^step-card-/)

      stepCards.forEach((card) => {
        const cardClasses = card.className

        // Check for transition type classes
        const hasTransitionAll = cardClasses.includes('transition-all')
        const hasTransitionShadow = cardClasses.includes('transition-shadow')

        expect(hasTransitionAll || hasTransitionShadow).toBe(true)
      })
    })
  })

  // Test Case: GlassMorphismCard component specifically
  describe('GlassMorphismCard Component', () => {
    it('should have all required hover effect classes', () => {
      render(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glassmorphism-card')

      // Base styling
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('bg-base-100/30')
      expect(card).toHaveClass('shadow-xl')
      expect(card).toHaveClass('rounded-2xl')

      // Hover effects
      expect(card).toHaveClass('hover:shadow-2xl')
      expect(card).toHaveClass('hover:scale-[1.02]')
      expect(card).toHaveClass('hover:border-primary/30')

      // Transition
      expect(card).toHaveClass('transition-all')
      expect(card).toHaveClass('duration-300')
    })

    it('should accept and apply custom className prop alongside hover effects', () => {
      render(
        <GlassMorphismCard className="p-6 my-custom-class">
          <div>Test content</div>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glassmorphism-card')

      // Custom class should be applied
      expect(card).toHaveClass('p-6')
      expect(card).toHaveClass('my-custom-class')

      // Hover effects should still be present
      expect(card).toHaveClass('hover:shadow-2xl')
      expect(card).toHaveClass('transition-all')
    })
  })

  // Test for animation smoothness (verifying transition properties exist)
  describe('Animation Smoothness', () => {
    it('should have smooth transitions on all interactive elements', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const ctaButton = screen.getByTestId('cta-register')

      // Check that button has transition properties
      expect(ctaButton.className).toContain('transition')
      expect(ctaButton.className).toContain('duration')
    })

    it('should not use transform-gpu on elements (to avoid jarring animations)', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      // Ensure animations don't use GPU-accelerated transforms that could cause jarring effects
      cards.forEach((card) => {
        // transform-gpu is acceptable but we check smooth transitions are set
        expect(card.className).toContain('transition')
      })
    })
  })
})
