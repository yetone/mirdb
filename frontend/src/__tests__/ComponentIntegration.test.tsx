import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import FuturisticButton from '../components/FuturisticButton'
import GlassMorphismCard from '../components/GlassMorphismCard'

const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('Component Integration and Rendering', () => {
  describe('Test Case 1: CTA buttons use FuturisticButton component or equivalent styling', () => {
    it('HeroSection CTA buttons use FuturisticButton component', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // FuturisticButton uses 'btn' base class from DaisyUI
      expect(primaryCta).toHaveClass('btn')
      expect(secondaryCta).toHaveClass('btn')

      // Primary CTA has btn-primary variant
      expect(primaryCta).toHaveClass('btn-primary')

      // Secondary CTA has btn-outline variant
      expect(secondaryCta).toHaveClass('btn-outline')

      // Both have transform hover:scale-105 for animation
      expect(primaryCta).toHaveClass('transform')
      expect(primaryCta).toHaveClass('hover:scale-105')
      expect(secondaryCta).toHaveClass('transform')
      expect(secondaryCta).toHaveClass('hover:scale-105')
    })

    it('FuturisticButton renders with correct base classes', () => {
      renderWithRouter(
        <FuturisticButton data-testid="test-btn">Test</FuturisticButton>
      )

      const button = screen.getByTestId('test-btn')
      expect(button).toHaveClass('btn')
      expect(button).toHaveClass('font-semibold')
      expect(button).toHaveClass('rounded-lg')
      expect(button).toHaveClass('transform')
      expect(button).toHaveClass('hover:scale-105')
    })

    it('FuturisticButton supports different variants', () => {
      const { rerender } = renderWithRouter(
        <FuturisticButton variant="primary" data-testid="btn">Primary</FuturisticButton>
      )
      expect(screen.getByTestId('btn')).toHaveClass('btn-primary')

      rerender(
        <BrowserRouter>
          <FuturisticButton variant="secondary" data-testid="btn">Secondary</FuturisticButton>
        </BrowserRouter>
      )
      expect(screen.getByTestId('btn')).toHaveClass('btn-secondary')

      rerender(
        <BrowserRouter>
          <FuturisticButton variant="outline" data-testid="btn">Outline</FuturisticButton>
        </BrowserRouter>
      )
      expect(screen.getByTestId('btn')).toHaveClass('btn-outline')
    })

    it('FuturisticButton can render as Link with correct attributes', () => {
      renderWithRouter(
        <FuturisticButton as="link" to="/test" data-testid="link-btn">
          Link Button
        </FuturisticButton>
      )

      const linkButton = screen.getByTestId('link-btn')
      expect(linkButton.tagName).toBe('A')
      expect(linkButton).toHaveAttribute('href', '/test')
      expect(linkButton).toHaveClass('btn')
    })
  })

  describe('Test Case 2: Feature cards use GlassMorphismCard component', () => {
    it('FeaturesSection renders cards with GlassMorphismCard glass-card styling', () => {
      render(<FeaturesSection />)

      // Get all feature cards - they are wrapped in GlassMorphismCard
      const featuresGrid = screen.getByTestId('features-grid')
      const cards = featuresGrid.querySelectorAll('.glass-card')

      // Should have 4 feature cards using glass-card class
      expect(cards.length).toBe(4)
    })

    it('GlassMorphismCard component renders with glass-card class', () => {
      const { container } = render(
        <GlassMorphismCard>Test Content</GlassMorphismCard>
      )

      const card = container.firstChild
      expect(card).toHaveClass('glass-card')
      expect(card).toHaveClass('p-6')
    })

    it('GlassMorphismCard accepts custom className', () => {
      const { container } = render(
        <GlassMorphismCard className="custom-class">Test Content</GlassMorphismCard>
      )

      const card = container.firstChild
      expect(card).toHaveClass('glass-card')
      expect(card).toHaveClass('custom-class')
    })

    it('Feature cards contain proper content structure', () => {
      render(<FeaturesSection />)

      const featureIds = ['url-shortening', 'analytics', 'link-management', 'security']

      featureIds.forEach((id) => {
        const card = screen.getByTestId(`feature-card-${id}`)
        expect(card).toBeInTheDocument()

        const icon = screen.getByTestId(`feature-icon-${id}`)
        expect(icon).toBeInTheDocument()
        expect(icon.querySelector('svg')).toBeInTheDocument()

        const title = screen.getByTestId(`feature-title-${id}`)
        expect(title).toBeInTheDocument()

        const description = screen.getByTestId(`feature-description-${id}`)
        expect(description).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 3: BackgroundEffect component (if used)', () => {
    it('Homepage renders without BackgroundEffect component errors', () => {
      // BackgroundEffect is optional - homepage should render without errors
      // even if BackgroundEffect is not implemented
      expect(() => renderWithRouter(<Home />)).not.toThrow()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('Homepage renders all main sections without background effect issues', () => {
      renderWithRouter(<Home />)

      // All main sections should render correctly
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Tailwind CSS classes are applied', () => {
    it('Homepage container uses Tailwind utility classes', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('min-h-screen')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('HeroSection uses Tailwind classes for layout and styling', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('hero')
      expect(heroSection).toHaveClass('min-h-[80vh]')

      // Check for gradient background classes
      const classList = heroSection.className
      expect(classList).toContain('bg-gradient-to-br')
    })

    it('FeaturesSection uses Tailwind grid classes', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('md:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-4')
      expect(grid).toHaveClass('gap-6')
    })

    it('Typography uses Tailwind text classes', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-5xl')
      expect(headline).toHaveClass('md:text-6xl')
      expect(headline).toHaveClass('font-bold')
    })

    it('FuturisticButton uses Tailwind responsive padding classes', () => {
      renderWithRouter(
        <FuturisticButton size="lg" data-testid="lg-btn">Large</FuturisticButton>
      )

      const btn = screen.getByTestId('lg-btn')
      expect(btn).toHaveClass('btn-lg')
      expect(btn).toHaveClass('text-lg')
      expect(btn).toHaveClass('px-8')
      expect(btn).toHaveClass('py-4')
    })
  })

  describe('Test Case 5: DaisyUI component classes are applied', () => {
    it('Homepage uses DaisyUI base-100 background', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('HeroSection uses DaisyUI hero component', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('hero')

      // Check for hero-content
      const heroContent = heroSection.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()
    })

    it('CTA buttons use DaisyUI btn classes', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // DaisyUI btn base class
      expect(primaryCta).toHaveClass('btn')
      expect(secondaryCta).toHaveClass('btn')

      // DaisyUI size variants
      expect(primaryCta).toHaveClass('btn-lg')
      expect(secondaryCta).toHaveClass('btn-lg')

      // DaisyUI color variants
      expect(primaryCta).toHaveClass('btn-primary')
      expect(secondaryCta).toHaveClass('btn-outline')
    })

    it('Text uses DaisyUI semantic color classes', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })

    it('Feature icons use DaisyUI semantic color classes', () => {
      render(<FeaturesSection />)

      // Icons use primary, secondary, accent, success colors
      const urlIcon = screen.getByTestId('feature-icon-url-shortening')
      expect(urlIcon.querySelector('svg')).toHaveClass('text-primary')

      const analyticsIcon = screen.getByTestId('feature-icon-analytics')
      expect(analyticsIcon.querySelector('svg')).toHaveClass('text-secondary')

      const linkIcon = screen.getByTestId('feature-icon-link-management')
      expect(linkIcon.querySelector('svg')).toHaveClass('text-accent')

      const securityIcon = screen.getByTestId('feature-icon-security')
      expect(securityIcon.querySelector('svg')).toHaveClass('text-success')
    })

    it('Feature descriptions use DaisyUI text-base-content class with opacity', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId(/^feature-description-/)
      descriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })
  })

  describe('Component Integration Smoke Tests', () => {
    it('Full homepage renders with all components integrated', () => {
      renderWithRouter(<Home />)

      // Verify all major sections are present
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Verify FuturisticButton integration in hero
      const primaryCta = screen.getByTestId('hero-cta-primary')
      expect(primaryCta).toHaveClass('btn')
      expect(primaryCta).toHaveClass('btn-primary')

      // Verify GlassMorphismCard integration in features
      const featuresGrid = screen.getByTestId('features-grid')
      const glassCards = featuresGrid.querySelectorAll('.glass-card')
      expect(glassCards.length).toBeGreaterThanOrEqual(3)
    })

    it('Components use consistent design system tokens', () => {
      renderWithRouter(<Home />)

      // Check that primary color token is used consistently
      const primaryButton = screen.getByTestId('hero-cta-primary')
      expect(primaryButton.className).toContain('primary')

      // Check that base-content color is used for text
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('base-content')
    })
  })
})
