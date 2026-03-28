/**
 * Design System Compliance Tests
 * Owner: Scenario 15 - Design System Compliance
 *
 * These tests validate that the homepage uses the existing
 * Tailwind CSS + DaisyUI design system consistently:
 * - Buttons use DaisyUI button classes (btn, btn-primary, etc.)
 * - Cards use DaisyUI card classes or GlassMorphismCard
 * - Colors use Tailwind/DaisyUI theme tokens (primary, secondary, accent)
 * - Typography uses Tailwind classes with consistent sizing
 * - Spacing uses Tailwind spacing scale (p-4, m-2, gap-4, etc.)
 * - No inline styles; all styling via Tailwind classes
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '../../setup'
import Home from '@/pages/Home'
import { HeroSection } from '@/components/homepage/HeroSection'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'
import { Footer } from '@/components/homepage/Footer'
import { StatsSection } from '@/components/homepage/StatsSection'
import HowItWorksSection from '@/components/homepage/HowItWorksSection'
import { FeatureCard } from '@/components/shared/FeatureCard'
import StepIndicator from '@/components/shared/StepIndicator'
import Navbar from '@/components/Navbar'

// DaisyUI button class patterns
const DAISYUI_BUTTON_CLASSES = ['btn', 'btn-primary', 'btn-secondary', 'btn-accent', 'btn-ghost', 'btn-link', 'btn-outline', 'btn-lg', 'btn-md', 'btn-sm', 'btn-xs', 'btn-circle', 'btn-square']

// DaisyUI card class patterns
const DAISYUI_CARD_CLASSES = ['card', 'card-body', 'card-title', 'card-actions', 'card-compact', 'card-side']

// Tailwind/DaisyUI theme token patterns for colors
const THEME_COLOR_PATTERNS = [
  /bg-base-\d+/,
  /text-base-content/,
  /bg-primary/,
  /text-primary/,
  /bg-secondary/,
  /text-secondary/,
  /bg-accent/,
  /text-accent/,
  /text-primary-content/,
  /bg-base-content/,
]

// Tailwind typography class patterns
const TYPOGRAPHY_PATTERNS = [
  /text-(xs|sm|base|lg|xl|2xl|3xl|4xl|5xl|6xl)/,
  /font-(thin|extralight|light|normal|medium|semibold|bold|extrabold|black)/,
  /leading-(none|tight|snug|normal|relaxed|loose)/,
]

// Tailwind spacing patterns
const SPACING_PATTERNS = [
  /p-\d+/,
  /px-\d+/,
  /py-\d+/,
  /pt-\d+/,
  /pr-\d+/,
  /pb-\d+/,
  /pl-\d+/,
  /m-\d+/,
  /mx-\d+/,
  /my-\d+/,
  /mt-\d+/,
  /mr-\d+/,
  /mb-\d+/,
  /ml-\d+/,
  /gap-\d+/,
  /space-[xy]-\d+/,
]

// Helper function to get all elements with a specific attribute
function getAllElementsWithAttribute(container: Element, attr: string): Element[] {
  const elements: Element[] = []
  const all = container.querySelectorAll('*')
  all.forEach((el) => {
    if (el.hasAttribute(attr)) {
      elements.push(el)
    }
  })
  return elements
}

// Helper to check if a className string contains any pattern from a list
function containsAnyPattern(className: string, patterns: (string | RegExp)[]): boolean {
  return patterns.some((pattern) => {
    if (typeof pattern === 'string') {
      return className.includes(pattern)
    }
    return pattern.test(className)
  })
}

// Helper to check if element has DaisyUI button classes
function hasDaisyUIButtonClass(element: Element): boolean {
  const className = element.getAttribute('class') || ''
  return className.split(' ').some((cls) => DAISYUI_BUTTON_CLASSES.includes(cls))
}

// Helper to check if element has DaisyUI card classes
function hasDaisyUICardClass(element: Element): boolean {
  const className = element.getAttribute('class') || ''
  return className.split(' ').some((cls) => DAISYUI_CARD_CLASSES.includes(cls))
}

describe('Design System Compliance', () => {
  describe('Test Case 1: Button Components Use DaisyUI Classes', () => {
    it('HeroSection buttons use DaisyUI button classes (btn, btn-primary, etc.)', () => {
      render(<HeroSection />)

      // Get all link elements that act as buttons (primary and secondary CTAs)
      const primaryCta = screen.getByTestId('primary-cta')
      const secondaryCta = screen.getByTestId('secondary-cta')

      // Check primary CTA has btn and btn-primary classes
      expect(primaryCta.className).toContain('btn')
      expect(primaryCta.className).toContain('btn-primary')
      expect(primaryCta.className).toContain('btn-lg')

      // Check secondary CTA has btn and btn-ghost classes
      expect(secondaryCta.className).toContain('btn')
      expect(secondaryCta.className).toContain('btn-ghost')
      expect(secondaryCta.className).toContain('btn-lg')
    })

    it('Navbar buttons use DaisyUI button classes', () => {
      render(<Navbar />)

      // Check theme toggle button
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.className).toContain('btn')
      expect(themeToggle.className).toContain('btn-ghost')
      expect(themeToggle.className).toContain('btn-circle')

      // Check Get Started button
      const getStartedBtn = screen.getByTestId('nav-get-started')
      expect(getStartedBtn.className).toContain('btn')
      expect(getStartedBtn.className).toContain('btn-primary')

      // Check Login button
      const loginBtn = screen.getByTestId('nav-login')
      expect(loginBtn.className).toContain('btn')
      expect(loginBtn.className).toContain('btn-ghost')

      // Check mobile menu toggle
      const mobileToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileToggle.className).toContain('btn')
      expect(mobileToggle.className).toContain('btn-ghost')
      expect(mobileToggle.className).toContain('btn-square')
    })

    it('All button elements across homepage use DaisyUI btn class', () => {
      const { container } = render(<Home />)

      // Get all button elements and anchor elements that look like buttons
      const buttons = container.querySelectorAll('button')
      const linkButtons = container.querySelectorAll('a.btn')

      // All actual buttons should have btn class
      buttons.forEach((button) => {
        expect(hasDaisyUIButtonClass(button)).toBe(true)
      })

      // All link buttons should have btn class
      linkButtons.forEach((link) => {
        expect(hasDaisyUIButtonClass(link)).toBe(true)
      })
    })
  })

  describe('Test Case 2: Card Components Use DaisyUI Classes', () => {
    it('FeatureCard uses DaisyUI card classes', () => {
      const { container } = render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const card = screen.getByTestId('feature-card')
      expect(card.className).toContain('card')
      expect(card.className).toContain('shadow-md')

      // Check for card-body
      const cardBody = card.querySelector('.card-body')
      expect(cardBody).not.toBeNull()
    })

    it('FeaturesSection uses DaisyUI card styling', () => {
      const { container } = render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach((card) => {
        expect(card.className).toContain('card')
      })
    })

    it('Home page inline cards use DaisyUI card classes', () => {
      const { container } = render(<Home />)

      // The Home component has inline cards in the features section
      const cards = container.querySelectorAll('.card')
      expect(cards.length).toBeGreaterThan(0)

      cards.forEach((card) => {
        expect(hasDaisyUICardClass(card)).toBe(true)
      })
    })

    it('StatsSection uses DaisyUI card classes', async () => {
      render(<StatsSection fetchStats={() => Promise.resolve({ linksShortened: 100, clicksTracked: 500, activeUsers: 50 })} />)

      // Wait for stats to load
      const statsContent = await screen.findByTestId('stats-content')

      const statCards = statsContent.querySelectorAll('.card')
      expect(statCards.length).toBe(3)

      statCards.forEach((card) => {
        expect(card.className).toContain('card')
        expect(card.className).toContain('bg-base-200')
        expect(card.className).toContain('shadow-xl')
      })
    })
  })

  describe('Test Case 3: Colors Use Tailwind/DaisyUI Theme Tokens', () => {
    it('Home page uses theme-aware background colors (bg-base-100, bg-base-200)', () => {
      const { container } = render(<Home />)

      // Check main element uses base colors
      const main = container.querySelector('main')
      expect(main?.className).toContain('bg-base-100')

      // Check sections use theme colors
      const sections = container.querySelectorAll('section')
      sections.forEach((section) => {
        const hasThemeBackground = containsAnyPattern(section.className || '', THEME_COLOR_PATTERNS)
        expect(hasThemeBackground).toBe(true)
      })
    })

    it('HeroSection uses theme tokens for colors', () => {
      const { container } = render(<HeroSection />)

      const section = screen.getByTestId('hero-section')
      expect(section.className).toContain('bg-base-200')

      // Check subheading uses theme-aware text color
      const subheading = container.querySelector('h2')
      expect(subheading?.className).toContain('text-base-content')
    })

    it('FeaturesSection uses theme tokens for text colors', () => {
      const { container } = render(<FeaturesSection />)

      const heading = screen.getByTestId('features-heading')
      expect(heading.className).toContain('text-base-content')

      // Check section background
      const section = screen.getByTestId('features-section')
      expect(section.className).toContain('bg-base-200')
    })

    it('FeatureCard uses theme colors for icon and text', () => {
      render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const icon = screen.getByTestId('feature-icon')
      expect(icon.className).toContain('text-primary')

      const title = screen.getByTestId('feature-title')
      expect(title.className).toContain('text-base-content')

      const description = screen.getByTestId('feature-description')
      expect(description.className).toContain('text-base-content')
    })

    it('StepIndicator uses primary color for step number', () => {
      render(
        <StepIndicator
          stepNumber={1}
          title="Step Title"
          description="Step description"
        />
      )

      const stepNumber = screen.getByTestId('step-number-1')
      expect(stepNumber.className).toContain('bg-primary')
      expect(stepNumber.className).toContain('text-primary-content')
    })

    it('Footer uses theme tokens for styling', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer.className).toContain('bg-base-200')
      expect(footer.className).toContain('text-base-content')
    })

    it('Navbar uses theme tokens for colors', () => {
      render(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar.className).toContain('bg-base-100')

      const logo = screen.getByTestId('navbar-logo')
      expect(logo.className).toContain('text-primary')
    })
  })

  describe('Test Case 4: Typography Uses Tailwind Classes', () => {
    it('HeroSection headings use Tailwind typography classes', () => {
      const { container } = render(<HeroSection />)

      const h1 = container.querySelector('h1')
      expect(h1?.className).toMatch(/text-(4xl|5xl|6xl)/)
      expect(h1?.className).toContain('font-bold')

      const h2 = container.querySelector('h2')
      expect(h2?.className).toMatch(/text-(lg|xl)/)
    })

    it('FeaturesSection uses consistent typography', () => {
      const { container } = render(<FeaturesSection />)

      const heading = screen.getByTestId('features-heading')
      expect(heading.className).toMatch(/text-(3xl|4xl)/)
      expect(heading.className).toContain('font-bold')

      const paragraph = container.querySelector('p')
      expect(paragraph?.className).toContain('text-lg')
    })

    it('FeatureCard uses consistent typography classes', () => {
      render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const title = screen.getByTestId('feature-title')
      expect(title.className).toContain('text-lg')
      expect(title.className).toContain('font-semibold')

      const description = screen.getByTestId('feature-description')
      expect(description.className).toContain('text-sm')
      expect(description.className).toContain('leading-relaxed')
    })

    it('StepIndicator uses consistent typography', () => {
      render(
        <StepIndicator
          stepNumber={1}
          title="Step Title"
          description="Step description"
        />
      )

      const stepNumber = screen.getByTestId('step-number-1')
      expect(stepNumber.className).toContain('text-2xl')
      expect(stepNumber.className).toContain('font-bold')

      const title = screen.getByTestId('step-title-1')
      expect(title.className).toContain('text-xl')
      expect(title.className).toContain('font-semibold')
    })

    it('StatsSection uses consistent typography', async () => {
      render(<StatsSection fetchStats={() => Promise.resolve({ linksShortened: 100, clicksTracked: 500, activeUsers: 50 })} />)

      const statsContent = await screen.findByTestId('stats-content')

      // Check stat values have large text
      const statValues = statsContent.querySelectorAll('[data-testid$="-value"]')
      statValues.forEach((value) => {
        expect(value.className).toContain('text-4xl')
        expect(value.className).toContain('font-bold')
      })
    })

    it('Footer uses appropriate typography', () => {
      render(<Footer />)

      const branding = screen.getByTestId('footer-branding')
      const brandText = branding.querySelector('span')
      expect(brandText?.className).toContain('text-lg')
      expect(brandText?.className).toContain('font-bold')

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.className).toContain('text-sm')
    })
  })

  describe('Test Case 5: Spacing Uses Tailwind Spacing Scale', () => {
    it('Home page uses Tailwind spacing classes', () => {
      const { container } = render(<Home />)

      // Check that the majority of sections use Tailwind spacing
      // Some sections (like HeroSection) use DaisyUI hero class which handles spacing internally
      const sections = container.querySelectorAll('section')
      const sectionsWithSpacing = Array.from(sections).filter((section) => {
        const hasSpacing = containsAnyPattern(section.className || '', SPACING_PATTERNS)
        const hasDaisyUIComponent = section.className?.includes('hero') || section.className?.includes('footer')
        return hasSpacing || hasDaisyUIComponent
      })

      // At least 80% of sections should have proper spacing or use DaisyUI components
      expect(sectionsWithSpacing.length / sections.length).toBeGreaterThanOrEqual(0.8)

      // Additionally, check that containers within sections use spacing
      const containersWithSpacing = container.querySelectorAll('.container')
      containersWithSpacing.forEach((cont) => {
        expect(cont.className).toContain('mx-auto')
      })
    })

    it('HeroSection uses Tailwind spacing', () => {
      render(<HeroSection />)

      const heroContent = screen.getByTestId('hero-section').querySelector('.hero-content')
      expect(heroContent?.className).toContain('py-16')
      expect(heroContent?.className).toContain('px-4')

      // Check CTA buttons container has gap
      const ctaContainer = screen.getByTestId('hero-section').querySelector('.flex.gap-4')
      expect(ctaContainer).not.toBeNull()
    })

    it('FeaturesSection uses grid gap spacing', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid.className).toContain('gap-6')

      const section = screen.getByTestId('features-section')
      expect(section.className).toContain('py-16')
      expect(section.className).toContain('px-4')
    })

    it('StatsSection uses Tailwind spacing', async () => {
      render(<StatsSection fetchStats={() => Promise.resolve({ linksShortened: 100, clicksTracked: 500, activeUsers: 50 })} />)

      const section = screen.getByTestId('stats-section')
      expect(section.className).toContain('py-16')
      expect(section.className).toContain('px-4')

      const statsContent = await screen.findByTestId('stats-content')
      expect(statsContent.className).toContain('gap-8')
    })

    it('HowItWorksSection uses Tailwind spacing', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section.className).toContain('py-16')
      expect(section.className).toContain('px-4')
    })

    it('FeatureCard uses Tailwind spacing in card-body', () => {
      const { container } = render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const icon = screen.getByTestId('feature-icon')
      expect(icon.className).toContain('mb-4')
    })

    it('Footer uses proper spacing', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer.className).toContain('p-10')

      const footerContent = footer.querySelector('.flex.flex-col')
      expect(footerContent?.className).toContain('gap-4')

      const links = screen.getByTestId('footer-links')
      expect(links.className).toContain('gap-4')
    })

    it('Navbar uses proper spacing', () => {
      render(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar.className).toContain('px-4')

      // Check menu has proper gap
      const menu = navbar.querySelector('.menu.menu-horizontal')
      expect(menu?.className).toContain('gap-2')
    })
  })

  describe('Test Case 6: No Inline Styles', () => {
    it('Home page has no inline style attributes', () => {
      const { container } = render(<Home />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('HeroSection has no inline style attributes', () => {
      const { container } = render(<HeroSection />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('FeaturesSection has no inline style attributes', () => {
      const { container } = render(<FeaturesSection />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('FeatureCard has no inline style attributes', () => {
      const { container } = render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('HowItWorksSection has no inline style attributes', () => {
      const { container } = render(<HowItWorksSection />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('StepIndicator has no inline style attributes', () => {
      const { container } = render(
        <StepIndicator
          stepNumber={1}
          title="Step Title"
          description="Step description"
        />
      )

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('StatsSection has no inline style attributes', async () => {
      const { container } = render(
        <StatsSection fetchStats={() => Promise.resolve({ linksShortened: 100, clicksTracked: 500, activeUsers: 50 })} />
      )

      await screen.findByTestId('stats-content')

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('Footer has no inline style attributes', () => {
      const { container } = render(<Footer />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })

    it('Navbar has no inline style attributes', () => {
      const { container } = render(<Navbar />)

      const elementsWithStyle = getAllElementsWithAttribute(container, 'style')
      expect(elementsWithStyle.length).toBe(0)
    })
  })
})
