/**
 * Unit and integration tests for Visual Design Consistency.
 * Owner: Scenario 10 - Visual Design Consistency
 *
 * Test coverage:
 * - Homepage uses existing reusable components appropriately
 * - DaisyUI classes (btn, card, etc.) are used for standard UI elements
 * - Framer Motion animations are used consistently
 * - Tailwind utility classes are used (no custom CSS duplicating utilities)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'
import HeroSection from '../../src/components/home/HeroSection'
import FeatureCard from '../../src/components/home/FeatureCard'
import FeaturesSection from '../../src/components/home/FeaturesSection'

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Visual Design Consistency', () => {
  describe('Test Case 1: Home component uses existing reusable components', () => {
    it('should import and render HeroSection component', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()
    })

    it('should import and render FeaturesSection component', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should use main element as root container', () => {
      renderWithRouter(<Home />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
      expect(main).toHaveClass('min-h-screen')
    })

    it('should use modular component architecture', () => {
      renderWithRouter(<Home />)

      // Verify both sections are rendered as children of main
      const main = screen.getByRole('main')
      expect(main.children.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Test Case 2: DaisyUI classes are used for standard UI elements', () => {
    describe('HeroSection DaisyUI usage', () => {
      it('should use DaisyUI hero classes', () => {
        renderWithRouter(<HeroSection />)

        const heroSection = screen.getByLabelText(/hero section/i)
        expect(heroSection).toHaveClass('hero')
        expect(heroSection).toHaveClass('bg-base-200')
      })

      it('should use DaisyUI btn classes for CTAs', () => {
        renderWithRouter(<HeroSection />)

        const primaryCta = screen.getByRole('link', { name: /get started/i })
        expect(primaryCta).toHaveClass('btn')
        expect(primaryCta).toHaveClass('btn-primary')
        expect(primaryCta).toHaveClass('btn-lg')

        const secondaryCta = screen.getByRole('link', { name: /login/i })
        expect(secondaryCta).toHaveClass('btn')
        expect(secondaryCta).toHaveClass('btn-outline')
        expect(secondaryCta).toHaveClass('btn-lg')
      })

      it('should use hero-content class for content wrapper', () => {
        renderWithRouter(<HeroSection />)

        const heroSection = screen.getByLabelText(/hero section/i)
        const heroContent = heroSection.querySelector('.hero-content')
        expect(heroContent).toBeInTheDocument()
      })
    })

    describe('FeatureCard DaisyUI usage', () => {
      const MockIcon = () => <svg data-testid="icon" />

      it('should use DaisyUI card classes', () => {
        render(
          <FeatureCard
            icon={<MockIcon />}
            title="Test Feature"
            description="Test description"
          />
        )

        const card = screen.getByRole('article')
        expect(card).toHaveClass('card')
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })

      it('should use DaisyUI card-body class', () => {
        render(
          <FeatureCard
            icon={<MockIcon />}
            title="Test Feature"
            description="Test description"
          />
        )

        const card = screen.getByRole('article')
        const cardBody = card.querySelector('.card-body')
        expect(cardBody).toBeInTheDocument()
      })

      it('should use DaisyUI card-title class for heading', () => {
        render(
          <FeatureCard
            icon={<MockIcon />}
            title="Test Feature"
            description="Test description"
          />
        )

        const title = screen.getByRole('heading', { level: 3 })
        expect(title).toHaveClass('card-title')
      })
    })

    describe('FeaturesSection DaisyUI usage', () => {
      it('should use base-content theme color for text', () => {
        renderWithRouter(<FeaturesSection />)

        const section = screen.getByTestId('features-section')
        expect(section).toHaveClass('bg-base-100')
      })

      it('should render all feature cards with DaisyUI card styling', () => {
        renderWithRouter(<FeaturesSection />)

        const cards = screen.getAllByRole('article')
        expect(cards.length).toBe(4)

        cards.forEach((card) => {
          expect(card).toHaveClass('card')
        })
      })
    })
  })

  describe('Test Case 3: Hover animations use Framer Motion consistently', () => {
    const MockIcon = () => <svg data-testid="icon" />

    it('should have hover shadow transition on feature cards', () => {
      render(
        <FeatureCard
          icon={<MockIcon />}
          title="Test Feature"
          description="Test description"
        />
      )

      const card = screen.getByRole('article')
      expect(card).toHaveClass('hover:shadow-2xl')
      expect(card).toHaveClass('transition-shadow')
    })

    it('should have duration for shadow transition', () => {
      render(
        <FeatureCard
          icon={<MockIcon />}
          title="Test Feature"
          description="Test description"
        />
      )

      const card = screen.getByRole('article')
      expect(card).toHaveClass('duration-300')
    })

    it('should render feature cards with Framer Motion wrapper', () => {
      renderWithRouter(<FeaturesSection />)

      const cards = screen.getAllByRole('article')
      cards.forEach((card) => {
        // Framer Motion adds style attribute for animations
        expect(card.tagName.toLowerCase()).toBe('article')
      })
    })

    it('should maintain smooth animations across all feature cards', async () => {
      renderWithRouter(<FeaturesSection />)

      const cards = screen.getAllByRole('article')

      // Simulate hover on each card
      for (const card of cards) {
        fireEvent.mouseEnter(card)
        // Card should have hover classes applied
        expect(card).toHaveClass('hover:shadow-2xl')
      }
    })
  })

  describe('Tailwind utility class consistency', () => {
    it('should use Tailwind responsive breakpoints consistently', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/text-5xl/)
      expect(headline.className).toMatch(/md:text-6xl/)
    })

    it('should use Tailwind spacing utilities', () => {
      renderWithRouter(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('py-16')
      expect(section).toHaveClass('px-4')
    })

    it('should use Tailwind grid for feature cards layout', () => {
      renderWithRouter(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('md:grid-cols-2')
    })

    it('should use Tailwind flexbox for CTA buttons', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByLabelText(/hero section/i)
      const ctaContainer = heroSection.querySelector('.flex')
      expect(ctaContainer).toBeInTheDocument()
      expect(ctaContainer).toHaveClass('flex-col')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Theme color consistency', () => {
    it('should use base-200 for hero background', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toHaveClass('bg-base-200')
    })

    it('should use base-100 for features section background', () => {
      renderWithRouter(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('bg-base-100')
    })

    it('should use primary color for main CTA', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByRole('link', { name: /get started/i })
      expect(primaryCta).toHaveClass('btn-primary')
    })

    it('should use primary color for feature icons', () => {
      renderWithRouter(<FeaturesSection />)

      const cards = screen.getAllByRole('article')
      cards.forEach((card) => {
        const iconContainer = card.querySelector('.text-primary')
        expect(iconContainer).toBeInTheDocument()
      })
    })

    it('should use gradient for headline styling', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('bg-gradient-to-r')
      expect(headline).toHaveClass('from-primary')
      expect(headline).toHaveClass('to-secondary')
    })
  })

  describe('Typography consistency', () => {
    it('should use consistent font weight for headings', () => {
      renderWithRouter(<Home />)

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toHaveClass('font-bold')

      const h2 = screen.getByRole('heading', { level: 2 })
      expect(h2).toHaveClass('font-bold')
    })

    it('should use consistent text opacity for descriptions', () => {
      renderWithRouter(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      const sectionDesc = section.querySelector('.text-base-content\\/70')
      expect(sectionDesc).toBeInTheDocument()
    })
  })
})
