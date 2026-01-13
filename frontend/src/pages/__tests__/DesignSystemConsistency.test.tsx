import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import Home from '../Home'

/**
 * Design System Consistency Tests
 * Validates NFR-3: Consistent styling with existing DaisyUI/Tailwind design system
 *
 * These tests ensure that the homepage follows the established design patterns
 * and uses the existing component library consistently.
 */

// Helper to render Home with required providers
function renderHome() {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

/**
 * Test Case 1: BackgroundEffect component usage
 * Input: Check for BackgroundEffect component usage
 * Expected: BackgroundEffect component is used for visual effects if animated background is present
 * Type: unit
 *
 * Note: BackgroundEffect component is optional per PRD ("Optional animated background effect")
 * The homepage may use other visual effect methods (CSS, Framer Motion) instead
 */
describe('Test Case 1: BackgroundEffect component usage', () => {
  it('should use BackgroundEffect or equivalent visual effects if animated background is present', () => {
    const { container } = renderHome()

    // Check if animated background is present using any method:
    // 1. BackgroundEffect component (data-testid="background-effect")
    // 2. Framer Motion background animations
    // 3. CSS gradient/animation backgrounds

    // If BackgroundEffect component is used, verify it
    const backgroundEffect = container.querySelector('[data-testid="background-effect"]')

    // If there's no BackgroundEffect component, verify visual effects are achieved
    // through other acceptable means (gradients, CSS classes)
    if (backgroundEffect) {
      expect(backgroundEffect).toBeInTheDocument()
    } else {
      // Verify that visual effects exist through Tailwind/DaisyUI classes
      // The homepage uses gradient backgrounds for visual effects
      const heroSection = container.querySelector('.hero')
      expect(heroSection).toBeInTheDocument()

      // Verify background styling classes are present
      // bg-base-200, bg-base-300 are part of DaisyUI theming
      const pageContainer = container.querySelector('.bg-base-200')
      expect(pageContainer).toBeInTheDocument()
    }
  })

  it('should have consistent visual effects using the design system', () => {
    const { container } = renderHome()

    // Verify visual styling follows DaisyUI patterns
    // Hero section should use DaisyUI hero component styling
    const heroSection = container.querySelector('.hero')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveClass('hero')

    // Verify backdrop effects are used for glassmorphism
    const backdropElements = container.querySelectorAll('.backdrop-blur-md')
    expect(backdropElements.length).toBeGreaterThan(0)
  })
})

/**
 * Test Case 2: GlassMorphismCard component usage in features section
 * Input: Check for GlassMorphismCard component usage in features section
 * Expected: Feature cards use GlassMorphismCard component pattern
 * Type: unit
 */
describe('Test Case 2: GlassMorphismCard component usage in features section', () => {
  it('should use GlassMorphismCard for feature cards', () => {
    renderHome()

    // Get all feature cards
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const analyticsCard = screen.getByTestId('feature-card-analytics')
    const linkManagementCard = screen.getByTestId('feature-card-link-management')
    const themesCard = screen.getByTestId('feature-card-themes')

    // Each card should have GlassMorphismCard styling
    const cards = [urlShorteningCard, analyticsCard, linkManagementCard, themesCard]

    cards.forEach((card) => {
      // GlassMorphismCard applies these classes
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('rounded-2xl')
      expect(card).toHaveClass('shadow-xl')
    })
  })

  it('should have consistent card styling across all feature cards', () => {
    renderHome()

    const featureCardsContainer = screen.getByTestId('feature-cards-container')
    const cards = featureCardsContainer.querySelectorAll('[data-testid^="feature-card-"]')

    expect(cards.length).toBe(4)

    cards.forEach((card) => {
      // Verify GlassMorphismCard component's classes
      expect(card).toHaveClass('glassmorphism-card')
      expect(card).toHaveClass('p-6')
    })
  })

  it('should have proper card structure with title, description, and icon', () => {
    renderHome()

    // URL Shortening card structure
    expect(screen.getByTestId('feature-title-url-shortening')).toHaveTextContent('URL Shortening')
    expect(screen.getByTestId('feature-description-url-shortening')).toBeInTheDocument()
    expect(screen.getByTestId('feature-icon-url-shortening').querySelector('svg')).toBeInTheDocument()

    // Analytics card structure
    expect(screen.getByTestId('feature-title-analytics')).toHaveTextContent('Analytics Dashboard')
    expect(screen.getByTestId('feature-description-analytics')).toBeInTheDocument()
    expect(screen.getByTestId('feature-icon-analytics').querySelector('svg')).toBeInTheDocument()
  })
})

/**
 * Test Case 3: FuturisticButton component usage for CTAs
 * Input: Check for FuturisticButton component usage for CTAs
 * Expected: Primary CTA buttons use FuturisticButton component
 * Type: unit
 */
describe('Test Case 3: FuturisticButton component usage for CTAs', () => {
  it('should use FuturisticButton for the demo shorten button', () => {
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')

    // FuturisticButton applies these characteristic classes
    expect(shortenButton).toBeInTheDocument()
    expect(shortenButton.tagName).toBe('BUTTON')
    expect(shortenButton).toHaveClass('bg-gradient-to-r')
    expect(shortenButton).toHaveClass('from-primary')
    expect(shortenButton).toHaveClass('to-secondary')
  })

  it('should have FuturisticButton styling with gradient and rounded corners', () => {
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')

    // FuturisticButton's base styles
    expect(shortenButton).toHaveClass('rounded-lg')
    expect(shortenButton).toHaveClass('font-semibold')
    expect(shortenButton).toHaveClass('overflow-hidden')
    expect(shortenButton).toHaveClass('relative')
  })

  it('should use appropriate button variants from the design system', () => {
    const { container } = renderHome()

    // Get all buttons on the page
    const buttons = container.querySelectorAll('button')

    // Should have multiple buttons with design system styling
    expect(buttons.length).toBeGreaterThan(0)

    // At least one button should have primary gradient styling
    const primaryButtons = container.querySelectorAll('.from-primary.to-secondary')
    expect(primaryButtons.length).toBeGreaterThan(0)
  })

  it('should have consistent button sizing from FuturisticButton variants', () => {
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')

    // FuturisticButton medium size classes
    expect(shortenButton).toHaveClass('px-6')
    expect(shortenButton).toHaveClass('py-3')
  })
})

/**
 * Test Case 4: Tailwind CSS classes usage for styling
 * Input: Verify Tailwind CSS classes are used for styling
 * Expected: Styling uses Tailwind utility classes, not inline styles
 * Type: unit
 */
describe('Test Case 4: Tailwind CSS classes are used for styling', () => {
  it('should use Tailwind classes instead of inline styles for layout', () => {
    const { container } = renderHome()

    // Verify Tailwind layout classes are used
    const flexElements = container.querySelectorAll('.flex, .grid')
    expect(flexElements.length).toBeGreaterThan(0)

    // Verify responsive classes are used
    const responsiveElements = container.querySelectorAll('[class*="sm:"], [class*="md:"], [class*="lg:"]')
    expect(responsiveElements.length).toBeGreaterThan(0)
  })

  it('should minimize inline style usage', () => {
    const { container } = renderHome()

    // Get all elements with inline styles
    const elementsWithInlineStyle = container.querySelectorAll('[style]')

    // Some inline styles from framer-motion are acceptable for animations
    // But they should be minimal (< 30 for animation-related elements)
    expect(elementsWithInlineStyle.length).toBeLessThan(30)
  })

  it('should use Tailwind spacing utilities', () => {
    const { container } = renderHome()

    // Check for Tailwind spacing classes
    const paddingElements = container.querySelectorAll('[class*="p-"], [class*="px-"], [class*="py-"]')
    const marginElements = container.querySelectorAll('[class*="m-"], [class*="mx-"], [class*="my-"], [class*="mb-"], [class*="mt-"]')
    const gapElements = container.querySelectorAll('[class*="gap-"]')

    expect(paddingElements.length).toBeGreaterThan(0)
    expect(marginElements.length).toBeGreaterThan(0)
    expect(gapElements.length).toBeGreaterThan(0)
  })

  it('should use Tailwind typography utilities', () => {
    const { container } = renderHome()

    // Check for typography classes
    const textElements = container.querySelectorAll('[class*="text-"], [class*="font-"]')
    expect(textElements.length).toBeGreaterThan(0)

    // Verify headings use appropriate text sizes
    const headings = container.querySelectorAll('h1, h2, h3')
    headings.forEach((heading) => {
      expect(heading.className).toMatch(/text-\d?xl|text-lg|font-/)
    })
  })

  it('should use Tailwind color utilities instead of hardcoded colors', () => {
    const { container } = renderHome()

    // Check for DaisyUI semantic color classes
    const semanticColorElements = container.querySelectorAll(
      '[class*="text-base-content"], [class*="bg-base-"], [class*="text-primary"], [class*="bg-primary"]'
    )
    expect(semanticColorElements.length).toBeGreaterThan(0)

    // Verify no hardcoded color values in inline styles
    const allElements = container.querySelectorAll('*')
    allElements.forEach((element) => {
      const style = element.getAttribute('style')
      if (style) {
        // Inline styles should not contain hardcoded hex colors
        // (allow framer-motion transform/opacity styles)
        const hasHardcodedColor = /(?<!opacity|transform).*#[0-9A-Fa-f]{6}/.test(style)
        expect(hasHardcodedColor).toBe(false)
      }
    })
  })
})

/**
 * Test Case 5: DaisyUI component classes are used appropriately
 * Input: Verify DaisyUI component classes are used appropriately
 * Expected: DaisyUI classes (btn, card, etc.) are used for consistent styling
 * Type: unit
 */
describe('Test Case 5: DaisyUI component classes are used appropriately', () => {
  it('should use DaisyUI btn classes for buttons', () => {
    const { container } = renderHome()

    // Get all elements with DaisyUI btn class
    const btnElements = container.querySelectorAll('.btn')
    expect(btnElements.length).toBeGreaterThan(0)

    // Check for btn variants
    const btnPrimary = container.querySelectorAll('.btn-primary')
    const btnGhost = container.querySelectorAll('.btn-ghost')
    const btnOutline = container.querySelectorAll('.btn-outline')

    // Should have at least one primary button
    expect(btnPrimary.length).toBeGreaterThan(0)
    // Should have navigation buttons
    expect(btnGhost.length + btnOutline.length).toBeGreaterThan(0)
  })

  it('should use DaisyUI navbar component for navigation', () => {
    const { container } = renderHome()

    const navbar = container.querySelector('.navbar')
    expect(navbar).toBeInTheDocument()

    // Navbar should have proper DaisyUI structure
    expect(navbar).toHaveClass('navbar')
  })

  it('should use DaisyUI hero component for hero section', () => {
    const { container } = renderHome()

    const heroSection = container.querySelector('.hero')
    expect(heroSection).toBeInTheDocument()

    // Should have hero-content for centering
    const heroContent = container.querySelector('.hero-content')
    expect(heroContent).toBeInTheDocument()
  })

  it('should use DaisyUI footer component', () => {
    renderHome()

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
    expect(footer).toHaveClass('footer')
    expect(footer).toHaveClass('footer-center')
  })

  it('should use DaisyUI input classes for form inputs', () => {
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    expect(urlInput).toHaveClass('input')
    expect(urlInput).toHaveClass('input-bordered')
  })

  it('should use DaisyUI link classes for navigation links', () => {
    const { container } = renderHome()

    // Footer links should use link-hover class
    const linkHoverElements = container.querySelectorAll('.link-hover')
    expect(linkHoverElements.length).toBeGreaterThan(0)
  })

  it('should use DaisyUI semantic colors consistently', () => {
    const { container } = renderHome()

    // Check for base-content text color usage
    const baseContentElements = container.querySelectorAll('[class*="text-base-content"]')
    expect(baseContentElements.length).toBeGreaterThan(0)

    // Check for base background color usage
    const baseBgElements = container.querySelectorAll('[class*="bg-base-"]')
    expect(baseBgElements.length).toBeGreaterThan(0)
  })

  it('should use DaisyUI theming system', () => {
    const { container } = renderHome()

    // The page container should use base colors for theming
    const pageContainer = container.firstChild as HTMLElement

    // Should have DaisyUI base color classes
    expect(pageContainer.className).toContain('bg-base-')
  })
})

/**
 * Additional Test: Overall Design System Integration
 */
describe('Overall Design System Integration', () => {
  it('should maintain consistent styling patterns across all sections', () => {
    const { container } = renderHome()

    // All sections should use consistent text colors
    const textContentElements = container.querySelectorAll('[class*="text-base-content"]')
    expect(textContentElements.length).toBeGreaterThan(0)

    // All cards should have consistent styling
    const cards = container.querySelectorAll('.glassmorphism-card')
    expect(cards.length).toBeGreaterThan(0)
  })

  it('should use design system components without custom overrides', () => {
    const { container } = renderHome()

    // Verify we're using the established component patterns
    // FuturisticButton gradient pattern
    const gradientButtons = container.querySelectorAll('.bg-gradient-to-r.from-primary')
    expect(gradientButtons.length).toBeGreaterThan(0)

    // GlassMorphismCard pattern
    const glassmorphismCards = container.querySelectorAll('.glassmorphism-card')
    expect(glassmorphismCards.length).toBeGreaterThan(0)
  })
})
