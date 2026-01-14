import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FeaturesSection from '../components/FeaturesSection'
import HeroSection from '../components/HeroSection'
import DemoSection from '../components/DemoSection'
import GlassMorphismCard from '../components/GlassMorphismCard'
import FuturisticButton from '../components/FuturisticButton'

/**
 * Design Consistency - Glassmorphism Style Tests (NFR-4)
 * Verify homepage uses consistent glassmorphism design
 */

describe('Design Consistency - Glassmorphism Style (NFR-4)', () => {
  describe('Test Case 1: FeaturesSection uses GlassMorphismCard', () => {
    it('should render feature cards with GlassMorphismCard component styling', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)

      // Verify cards have glassmorphism styling classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('card')
        expect(card).toHaveClass('backdrop-blur-md')
      })
    })

    it('should have cards with semi-transparent background for glassmorphism effect', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        // Check for bg-base-100/80 class (semi-transparent)
        const hasGlassBackground = card.className.includes('bg-base-100/80')
        expect(hasGlassBackground).toBe(true)
      })
    })

    it('should have cards with border for glassmorphism definition', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        expect(card).toHaveClass('border')
      })
    })
  })

  describe('Test Case 2: Cards have backdrop-filter blur for glassmorphism', () => {
    it('should have GlassMorphismCard with backdrop-blur-md class', () => {
      render(
        <GlassMorphismCard data-testid="glass-card">
          <div>Test Content</div>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glass-card')
      expect(card).toHaveClass('backdrop-blur-md')
    })

    it('should have feature cards with backdrop-blur-md class applied', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        expect(card.className).toContain('backdrop-blur-md')
      })
    })

    it('should have GlassMorphismCard with proper shadow effects', () => {
      render(
        <GlassMorphismCard data-testid="glass-card">
          <div>Test Content</div>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glass-card')
      expect(card).toHaveClass('shadow-xl')
      expect(card).toHaveClass('hover:shadow-2xl')
    })
  })

  describe('Test Case 3: Buttons use FuturisticButton or consistent styling', () => {
    it('should render HeroSection buttons with consistent btn classes', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const getStartedBtn = screen.getByTestId('cta-get-started')
      const loginBtn = screen.getByTestId('cta-login')

      // Both buttons should have base btn class
      expect(getStartedBtn).toHaveClass('btn')
      expect(loginBtn).toHaveClass('btn')

      // Primary button styling
      expect(getStartedBtn).toHaveClass('btn-primary')
      expect(getStartedBtn).toHaveClass('btn-lg')

      // Outline button styling
      expect(loginBtn).toHaveClass('btn-outline')
      expect(loginBtn).toHaveClass('btn-lg')
    })

    it('should render DemoSection buttons with consistent styling', () => {
      render(
        <BrowserRouter>
          <DemoSection />
        </BrowserRouter>
      )

      const submitBtn = screen.getByTestId('demo-submit-button')

      expect(submitBtn).toHaveClass('btn')
      expect(submitBtn).toHaveClass('btn-primary')
    })

    it('should render FuturisticButton with correct variant classes', () => {
      render(
        <BrowserRouter>
          <FuturisticButton variant="primary" data-testid="primary-btn">
            Primary
          </FuturisticButton>
          <FuturisticButton variant="outline" data-testid="outline-btn">
            Outline
          </FuturisticButton>
          <FuturisticButton variant="secondary" data-testid="secondary-btn">
            Secondary
          </FuturisticButton>
          <FuturisticButton variant="ghost" data-testid="ghost-btn">
            Ghost
          </FuturisticButton>
        </BrowserRouter>
      )

      expect(screen.getByTestId('primary-btn')).toHaveClass('btn-primary')
      expect(screen.getByTestId('outline-btn')).toHaveClass('btn-outline')
      expect(screen.getByTestId('secondary-btn')).toHaveClass('btn-secondary')
      expect(screen.getByTestId('ghost-btn')).toHaveClass('btn-ghost')
    })

    it('should render FuturisticButton with correct size classes', () => {
      render(
        <BrowserRouter>
          <FuturisticButton size="sm" data-testid="sm-btn">
            Small
          </FuturisticButton>
          <FuturisticButton size="md" data-testid="md-btn">
            Medium
          </FuturisticButton>
          <FuturisticButton size="lg" data-testid="lg-btn">
            Large
          </FuturisticButton>
        </BrowserRouter>
      )

      expect(screen.getByTestId('sm-btn')).toHaveClass('btn-sm')
      expect(screen.getByTestId('lg-btn')).toHaveClass('btn-lg')
      // Medium has no extra size class
      expect(screen.getByTestId('md-btn')).not.toHaveClass('btn-sm')
      expect(screen.getByTestId('md-btn')).not.toHaveClass('btn-lg')
    })

    it('should render FuturisticButton as link when configured', () => {
      render(
        <BrowserRouter>
          <FuturisticButton as="link" to="/test" data-testid="link-btn">
            Link Button
          </FuturisticButton>
        </BrowserRouter>
      )

      const linkBtn = screen.getByTestId('link-btn')
      expect(linkBtn.tagName.toLowerCase()).toBe('a')
      expect(linkBtn).toHaveAttribute('href', '/test')
    })
  })

  describe('Design Consistency Validation', () => {
    it('should have consistent transition effects across components', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        expect(card).toHaveClass('transition-shadow')
        expect(card).toHaveClass('duration-300')
      })
    })

    it('should maintain DaisyUI component library consistency', () => {
      render(
        <BrowserRouter>
          <FuturisticButton data-testid="btn">Test</FuturisticButton>
        </BrowserRouter>
      )

      // All buttons use DaisyUI btn class
      expect(screen.getByTestId('btn')).toHaveClass('btn')
    })

    it('should have GlassMorphismCard maintain card structure', () => {
      render(
        <GlassMorphismCard data-testid="glass-card">
          <div className="card-body">Content</div>
        </GlassMorphismCard>
      )

      const card = screen.getByTestId('glass-card')
      expect(card).toHaveClass('card')
      expect(card.querySelector('.card-body')).toBeInTheDocument()
    })
  })
})
