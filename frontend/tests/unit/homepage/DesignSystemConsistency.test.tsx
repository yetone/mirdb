/**
 * Design System Consistency Tests
 * Owner: Scenario 15 - Design System Consistency
 *
 * Tests:
 * - Buttons use DaisyUI btn classes (btn, btn-primary, btn-outline)
 * - Inputs use DaisyUI input classes
 * - Feature cards use existing card component patterns
 * - Styling is consistent with Tailwind CSS + DaisyUI
 *
 * Requirements: NFR-4
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { HeroSection } from '../../../src/components/homepage/HeroSection'
import { InlineShortener } from '../../../src/components/homepage/InlineShortener'
import { FeatureCard } from '../../../src/components/homepage/FeatureCard'
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection'
import { Footer } from '../../../src/components/homepage/Footer'
import { Navbar } from '../../../src/components/Navbar'

// Mock the useAnonymousShorten hook
vi.mock('../../../src/hooks/useAnonymousShorten', () => ({
  useAnonymousShorten: () => ({
    shortenUrl: vi.fn(),
    isLoading: false,
    error: null,
    result: null,
    reset: vi.fn(),
  }),
}))

/**
 * Helper function to get all elements with a specific tag
 */
const getAllElementsByTag = (container: HTMLElement, tagName: string): HTMLElement[] => {
  return Array.from(container.querySelectorAll(tagName)) as HTMLElement[]
}

/**
 * Helper to check if a class string contains a DaisyUI btn class
 */
const hasDaisyUIButtonClass = (className: string): boolean => {
  const btnClasses = ['btn', 'btn-primary', 'btn-secondary', 'btn-outline', 'btn-ghost', 'btn-link', 'btn-lg', 'btn-sm', 'btn-xs']
  return btnClasses.some(cls => className.split(' ').includes(cls))
}

/**
 * Helper to check if a class string contains a DaisyUI input class
 */
const hasDaisyUIInputClass = (className: string): boolean => {
  const inputClasses = ['input', 'input-bordered', 'input-primary', 'input-secondary', 'input-error', 'input-ghost']
  return inputClasses.some(cls => className.split(' ').includes(cls))
}

/**
 * Helper to check if a class string contains DaisyUI card classes
 */
const hasDaisyUICardClass = (className: string): boolean => {
  const cardClasses = ['card', 'card-body', 'card-title', 'card-compact', 'card-side']
  return cardClasses.some(cls => className.split(' ').includes(cls))
}

describe('Design System Consistency', () => {
  describe('Test Case 1: Button Elements use DaisyUI btn classes', () => {
    it('HeroSection primary button uses btn btn-primary classes', () => {
      render(<HeroSection />)

      const primaryBtn = screen.getByTestId('primary-cta')
      expect(primaryBtn.className).toContain('btn')
      expect(primaryBtn.className).toContain('btn-primary')
    })

    it('HeroSection secondary button uses btn btn-outline classes', () => {
      render(<HeroSection />)

      const secondaryBtn = screen.getByTestId('secondary-cta')
      expect(secondaryBtn.className).toContain('btn')
      expect(secondaryBtn.className).toContain('btn-outline')
    })

    it('InlineShortener shorten button uses btn btn-primary classes', () => {
      render(<InlineShortener />)

      const shortenBtn = screen.getByTestId('shorten-button')
      expect(shortenBtn.className).toContain('btn')
      expect(shortenBtn.className).toContain('btn-primary')
    })

    it('Navbar login button uses btn btn-ghost classes', () => {
      render(
        <MemoryRouter>
          <Navbar />
        </MemoryRouter>
      )

      const loginBtn = screen.getByTestId('navbar-login-button')
      expect(loginBtn.className).toContain('btn')
      expect(loginBtn.className).toContain('btn-ghost')
    })

    it('Navbar signup button uses btn btn-primary classes', () => {
      render(
        <MemoryRouter>
          <Navbar />
        </MemoryRouter>
      )

      const signupBtn = screen.getByTestId('navbar-signup-button')
      expect(signupBtn.className).toContain('btn')
      expect(signupBtn.className).toContain('btn-primary')
    })

    it('all button elements in homepage components use DaisyUI btn class', () => {
      const { container: heroContainer } = render(<HeroSection />)
      const { container: shortenerContainer } = render(<InlineShortener />)
      const { container: navContainer } = render(
        <MemoryRouter>
          <Navbar />
        </MemoryRouter>
      )

      const heroButtons = getAllElementsByTag(heroContainer, 'button')
      const shortenerButtons = getAllElementsByTag(shortenerContainer, 'button')
      const navButtons = getAllElementsByTag(navContainer, 'button')

      const allButtons = [...heroButtons, ...shortenerButtons, ...navButtons]

      allButtons.forEach((button) => {
        expect(hasDaisyUIButtonClass(button.className)).toBe(true)
      })
    })
  })

  describe('Test Case 2: Input Elements use DaisyUI input classes', () => {
    it('InlineShortener URL input uses input input-bordered classes', () => {
      render(<InlineShortener />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput.className).toContain('input')
      expect(urlInput.className).toContain('input-bordered')
    })

    it('all text input elements use DaisyUI input class', () => {
      const { container } = render(<InlineShortener />)

      const textInputs = getAllElementsByTag(container, 'input').filter(
        (input) => input.getAttribute('type') === 'text' || !input.getAttribute('type')
      )

      textInputs.forEach((input) => {
        expect(hasDaisyUIInputClass(input.className)).toBe(true)
      })
    })

    it('input elements do not use custom CSS overrides', () => {
      const { container } = render(<InlineShortener />)

      const urlInput = screen.getByTestId('url-input')
      const className = urlInput.className

      // Check that styling uses Tailwind utility classes, not custom CSS
      const tailwindClasses = className.split(' ').filter(cls =>
        cls.match(/^(input|w-|sm:|md:|lg:|join-|flex-)/)
      )

      // Most classes should be Tailwind/DaisyUI utilities
      expect(tailwindClasses.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 4: Feature Cards use existing card component patterns', () => {
    const mockFeature = {
      icon: '📊',
      title: 'Test Feature',
      description: 'Test description for the feature.',
    }

    it('FeatureCard uses DaisyUI card class', () => {
      render(<FeatureCard {...mockFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card.className).toContain('card')
    })

    it('FeatureCard uses card-body for content', () => {
      render(<FeatureCard {...mockFeature} />)

      const card = screen.getByTestId('feature-card')
      const cardBody = card.querySelector('.card-body')
      expect(cardBody).toBeInTheDocument()
    })

    it('FeatureCard uses card-title for title', () => {
      render(<FeatureCard {...mockFeature} />)

      const title = screen.getByTestId('feature-title')
      expect(title.className).toContain('card-title')
    })

    it('FeatureCard uses bg-base-100 for consistent theming', () => {
      render(<FeatureCard {...mockFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card.className).toContain('bg-base-100')
    })

    it('FeatureCard has shadow-md for consistent elevation', () => {
      render(<FeatureCard {...mockFeature} />)

      const card = screen.getByTestId('feature-card')
      expect(card.className).toContain('shadow-md')
    })

    it('FeaturesSection contains multiple FeatureCards with consistent styling', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('feature-card')
      expect(cards.length).toBeGreaterThanOrEqual(3)

      cards.forEach((card) => {
        expect(hasDaisyUICardClass(card.className)).toBe(true)
        expect(card.className).toContain('bg-base-100')
        expect(card.className).toContain('shadow')
      })
    })

    it('all feature cards have consistent hover effects', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('feature-card')

      cards.forEach((card) => {
        expect(card.className).toContain('hover:shadow')
        expect(card.className).toContain('hover:-translate-y')
        expect(card.className).toContain('transition')
      })
    })
  })

  describe('General Tailwind CSS and DaisyUI Consistency', () => {
    it('HeroSection uses Tailwind utility classes for layout', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const className = heroSection.className

      // Should use Tailwind layout utilities
      expect(className).toContain('flex')
      expect(className).toContain('items-center')
      expect(className).toContain('justify-center')
    })

    it('components use DaisyUI theme colors (base-100, base-200, primary)', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section.className).toContain('bg-base-200')

      const cards = screen.getAllByTestId('feature-card')
      cards.forEach((card) => {
        expect(card.className).toContain('bg-base-100')
      })
    })

    it('components use DaisyUI text colors (base-content, text-primary)', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      descriptions.forEach((desc) => {
        expect(desc.className).toContain('text-base-content')
      })
    })

    it('Footer uses consistent DaisyUI styling', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer.className).toContain('bg-base-200')

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        expect(link.className).toContain('link')
      })
    })

    it('Navbar uses DaisyUI navbar classes', () => {
      render(
        <MemoryRouter>
          <Navbar />
        </MemoryRouter>
      )

      const navbar = screen.getByTestId('navbar')
      expect(navbar.className).toContain('navbar')
      expect(navbar.className).toContain('bg-base-100')
    })

    it('no inline styles are used in homepage components', () => {
      const { container: heroContainer } = render(<HeroSection />)
      const { container: shortenerContainer } = render(<InlineShortener />)
      const { container: featuresContainer } = render(<FeaturesSection />)
      const { container: footerContainer } = render(<Footer />)

      const allElements = [
        ...Array.from(heroContainer.querySelectorAll('*')),
        ...Array.from(shortenerContainer.querySelectorAll('*')),
        ...Array.from(featuresContainer.querySelectorAll('*')),
        ...Array.from(footerContainer.querySelectorAll('*')),
      ]

      allElements.forEach((element) => {
        const style = (element as HTMLElement).getAttribute('style')
        // Allow empty style or null
        if (style) {
          expect(style).toBe('')
        }
      })
    })

    it('components use consistent spacing scale (py, px, mb, gap)', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      // Should use Tailwind spacing utilities
      expect(section.className).toMatch(/py-\d+/)

      const grid = screen.getByTestId('features-grid')
      expect(grid.className).toContain('gap-')
    })

    it('typography uses Tailwind text utilities consistently', () => {
      render(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      const className = headline.className

      // Should use Tailwind typography classes
      expect(className).toContain('text-')
      expect(className).toContain('font-bold')
    })
  })

  describe('Responsive Design Classes', () => {
    it('HeroSection uses responsive typography classes', () => {
      render(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      const className = headline.className

      // Should have responsive text sizing
      expect(className).toContain('md:text-')
      expect(className).toContain('lg:text-')
    })

    it('FeaturesSection uses responsive grid classes', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      const className = grid.className

      expect(className).toContain('grid-cols-1')
      expect(className).toContain('md:grid-cols-2')
      expect(className).toContain('lg:grid-cols-3')
    })

    it('InlineShortener uses responsive flex direction', () => {
      render(<InlineShortener />)

      const inlineShortener = screen.getByTestId('inline-shortener')
      const joinContainer = inlineShortener.querySelector('.join')

      expect(joinContainer).toBeInTheDocument()
      expect(joinContainer?.className).toContain('flex-col')
      expect(joinContainer?.className).toContain('sm:flex-row')
    })

    it('Footer uses responsive flex direction', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      const container = footer.querySelector('.container > div')

      expect(container?.className).toContain('flex-col')
      expect(container?.className).toContain('md:flex-row')
    })
  })
})
