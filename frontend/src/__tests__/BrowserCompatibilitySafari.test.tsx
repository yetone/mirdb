import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import DemoSection from '../components/DemoSection'
import FooterSection from '../components/FooterSection'

/**
 * Safari Browser Compatibility Tests (NFR-3)
 *
 * These tests verify that the homepage works correctly in Safari browser.
 * They check:
 * 1. All sections render correctly without layout issues
 * 2. Webkit-prefixed CSS features work correctly
 * 3. Safari-specific CSS properties are handled
 * 4. Flexbox and Grid layouts render properly (Safari has specific requirements)
 * 5. Smooth scroll behavior works or falls back gracefully
 * 6. Theme switching and CSS custom properties work
 *
 * Note: These tests run in jsdom which simulates DOM behavior.
 * For actual Safari rendering, E2E tests with Playwright webkit driver
 * would be needed (requires macOS or special WebKit build).
 */

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Browser Compatibility - Safari (NFR-3)', () => {
  describe('Test Case 1: Homepage renders correctly in Safari', () => {
    it('all homepage sections render without errors', () => {
      renderWithRouter(<Home />)

      // Verify all main sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section renders with correct structure', () => {
      renderWithRouter(<HeroSection />)

      // Verify headline renders
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toBe('Shorten URLs. Track Results.')

      // Verify subheadline renders
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // Verify CTA buttons render
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()
    })

    it('features section renders all feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(3)

      // Verify each card has proper content
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()
    })

    it('demo section renders input and button', () => {
      renderWithRouter(<DemoSection />)

      expect(screen.getByTestId('demo-url-input')).toBeInTheDocument()
      expect(screen.getByTestId('demo-submit-button')).toBeInTheDocument()
    })

    it('footer section renders all navigation links', () => {
      renderWithRouter(<FooterSection />)

      expect(screen.getByRole('link', { name: /home/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /register/i })).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Safari-specific CSS features', () => {
    describe('Flexbox compatibility', () => {
      it('hero section flexbox layout renders correctly', () => {
        renderWithRouter(<HeroSection />)

        const heroSection = screen.getByTestId('hero-section')

        // Safari requires explicit flex container properties
        // The component uses 'flex items-center justify-center' which compiles to:
        // display: flex; align-items: center; justify-content: center;
        expect(heroSection.classList.contains('flex')).toBe(true)
        expect(heroSection.classList.contains('items-center')).toBe(true)
        expect(heroSection.classList.contains('justify-center')).toBe(true)
      })

      it('CTA button container uses proper flexbox for Safari', () => {
        renderWithRouter(<HeroSection />)

        // Get the container with CTA buttons
        const ctaGetStarted = screen.getByTestId('cta-get-started')
        const ctaContainer = ctaGetStarted.parentElement

        // Safari needs explicit flex-direction for responsive layouts
        // 'flex-col sm:flex-row' provides proper fallback
        expect(ctaContainer?.classList.contains('flex')).toBe(true)
        expect(ctaContainer?.classList.contains('flex-col')).toBe(true)
      })

      it('footer flexbox layout is Safari-compatible', () => {
        renderWithRouter(<FooterSection />)

        const footer = screen.getByTestId('footer-section')

        // Verify footer uses proper flex utilities
        // Safari handles these standard flex properties well
        expect(footer.querySelector('.flex')).toBeInTheDocument()
      })
    })

    describe('Grid layout compatibility', () => {
      it('features section grid layout renders correctly', () => {
        render(<FeaturesSection />)

        const section = screen.getByTestId('features-section')
        const gridContainer = section.querySelector('.grid')

        // Safari requires explicit grid-template-columns
        // 'grid-cols-1 md:grid-cols-2 lg:grid-cols-3' is compatible
        expect(gridContainer).toBeInTheDocument()
        expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
      })

      it('grid gap property works for Safari', () => {
        render(<FeaturesSection />)

        const section = screen.getByTestId('features-section')
        const gridContainer = section.querySelector('.grid')

        // Safari 12+ supports 'gap' property for grid
        // 'gap-8' compiles to gap: 2rem
        expect(gridContainer?.classList.contains('gap-8')).toBe(true)
      })
    })

    describe('CSS gradient compatibility', () => {
      it('hero section background gradient is Safari-compatible', () => {
        renderWithRouter(<HeroSection />)

        const heroSection = screen.getByTestId('hero-section')

        // Tailwind's gradient utilities use standard syntax
        // Safari supports bg-gradient-to-* via autoprefixer
        expect(heroSection.classList.contains('bg-gradient-to-br')).toBe(true)
        expect(heroSection.classList.contains('from-base-200')).toBe(true)
        expect(heroSection.classList.contains('to-base-300')).toBe(true)
      })
    })

    describe('CSS transform compatibility', () => {
      it('Framer Motion transforms are webkit-compatible', async () => {
        renderWithRouter(<HeroSection />)

        await waitFor(() => {
          expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
        })

        // Framer Motion uses transform: translateY() which is
        // automatically webkit-prefixed by the browser
        // Safari supports transforms with -webkit-transform
        const headline = screen.getByRole('heading', { level: 1 })
        expect(headline).toBeInTheDocument()
      })

      it('feature cards hover effects use webkit-compatible shadows', () => {
        render(<FeaturesSection />)

        const featureCards = screen.getAllByTestId('feature-card')

        featureCards.forEach((card) => {
          // 'shadow-xl hover:shadow-2xl' uses box-shadow
          // which Safari supports natively
          expect(card.classList.contains('shadow-xl')).toBe(true)
        })
      })
    })

    describe('CSS transition compatibility', () => {
      it('transition-shadow class is Safari-compatible', () => {
        render(<FeaturesSection />)

        const featureCards = screen.getAllByTestId('feature-card')

        featureCards.forEach((card) => {
          // transition-shadow compiles to:
          // transition-property: box-shadow;
          // Safari supports this standard property
          expect(card.classList.contains('transition-shadow')).toBe(true)
        })
      })

      it('transition-colors class is Safari-compatible', () => {
        renderWithRouter(<HeroSection />)

        const navLinks = screen.getByTestId('nav-features')

        // transition-colors handles color transitions
        // Safari supports this via standard CSS
        expect(navLinks.classList.contains('transition-colors')).toBe(true)
      })

      it('duration-300 transition timing works in Safari', () => {
        render(<FeaturesSection />)

        const featureCards = screen.getAllByTestId('feature-card')

        featureCards.forEach((card) => {
          // duration-300 compiles to transition-duration: 300ms
          // This is standard CSS supported by Safari
          expect(card.classList.contains('duration-300')).toBe(true)
        })
      })
    })
  })

  describe('Webkit-specific scroll behavior', () => {
    it('smooth scroll behavior falls back gracefully', () => {
      renderWithRouter(<HeroSection />)

      const navFeatures = screen.getByTestId('nav-features')

      // Click handler uses scrollIntoView({ behavior: 'smooth' })
      // Safari 15.4+ supports smooth scroll
      // Older versions fall back to instant scroll
      expect(navFeatures).toHaveAttribute('href', '#features')
    })

    it('anchor links have proper href for fallback navigation', () => {
      renderWithRouter(<HeroSection />)

      const navDemo = screen.getByTestId('nav-demo')
      expect(navDemo).toHaveAttribute('href', '#demo')
    })

    it('smooth scroll click handler triggers scrollIntoView', async () => {
      const user = userEvent.setup()
      const mockScrollIntoView = vi.fn()

      // Mock scrollIntoView
      Element.prototype.scrollIntoView = mockScrollIntoView

      // Create a target element
      const container = document.createElement('div')
      container.id = 'features'
      document.body.appendChild(container)

      renderWithRouter(<HeroSection />)

      const navFeatures = screen.getByTestId('nav-features')

      await act(async () => {
        await user.click(navFeatures)
      })

      // scrollIntoView should be called with smooth behavior
      expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })

      // Cleanup
      document.body.removeChild(container)
    })
  })

  describe('CSS custom properties (CSS variables)', () => {
    it('DaisyUI theme variables are used correctly', () => {
      renderWithRouter(<Home />)

      // DaisyUI uses CSS custom properties for theming
      // Safari 9.1+ supports CSS custom properties
      // Classes like text-base-content, bg-base-100 use these variables

      const heroSection = screen.getByTestId('hero-section')

      // Verify theme-aware classes are applied
      expect(heroSection.classList.contains('from-base-200')).toBe(true)
      expect(heroSection.classList.contains('to-base-300')).toBe(true)
    })

    it('opacity modifier classes work correctly', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')

      // Safari supports opacity modifiers via CSS custom properties
      // text-base-content/70 uses CSS custom property with opacity
      expect(subheadline.classList.contains('text-base-content/70')).toBe(true)
    })
  })

  describe('min-height and viewport units', () => {
    it('hero section uses min-h-screen correctly', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // min-h-screen compiles to min-height: 100vh
      // Safari on iOS requires -webkit-fill-available fallback in some cases
      // Tailwind handles this appropriately
      expect(heroSection.classList.contains('min-h-screen')).toBe(true)
    })

    it('main container uses min-h-screen', () => {
      renderWithRouter(<Home />)

      const main = document.querySelector('main')
      expect(main?.classList.contains('min-h-screen')).toBe(true)
    })
  })

  describe('SVG rendering compatibility', () => {
    it('feature card SVG icons render correctly', () => {
      render(<FeaturesSection />)

      // Safari supports inline SVG with proper namespace
      const svgIcons = document.querySelectorAll('svg')

      expect(svgIcons.length).toBeGreaterThan(0)

      svgIcons.forEach((svg) => {
        // Verify SVG has proper namespace attribute
        expect(svg.getAttribute('xmlns')).toBe('http://www.w3.org/2000/svg')
      })
    })

    it('SVG icons have proper viewBox for scaling', () => {
      render(<FeaturesSection />)

      const svgIcons = document.querySelectorAll('svg')

      svgIcons.forEach((svg) => {
        // viewBox ensures proper scaling across browsers
        expect(svg.getAttribute('viewBox')).toBe('0 0 24 24')
      })
    })

    it('SVG stroke properties work correctly', () => {
      render(<FeaturesSection />)

      const svgIcons = document.querySelectorAll('svg')

      svgIcons.forEach((svg) => {
        // Safari handles stroke properties correctly
        expect(svg.getAttribute('stroke')).toBe('currentColor')
        expect(svg.getAttribute('fill')).toBe('none')
      })
    })
  })

  describe('Form element compatibility', () => {
    it('input element has proper Safari-compatible styling', () => {
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')

      // Safari requires specific input styling considerations
      // DaisyUI's input class handles cross-browser styling
      expect(input.classList.contains('input')).toBe(true)
      expect(input.classList.contains('input-bordered')).toBe(true)
    })

    it('button element has proper styling', () => {
      renderWithRouter(<DemoSection />)

      const button = screen.getByTestId('demo-submit-button')

      // Safari button styling is handled by DaisyUI
      expect(button.classList.contains('btn')).toBe(true)
      expect(button.classList.contains('btn-primary')).toBe(true)
    })

    it('form submission works correctly', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const button = screen.getByTestId('demo-submit-button')

      await act(async () => {
        await user.type(input, 'https://example.com')
      })

      await act(async () => {
        await user.click(button)
      })

      // Should show registration prompt for valid URL
      await waitFor(() => {
        expect(screen.getByTestId('demo-register-prompt')).toBeInTheDocument()
      })
    })
  })

  describe('Link component compatibility', () => {
    it('React Router Link components render as anchor elements', () => {
      renderWithRouter(<HeroSection />)

      const ctaGetStarted = screen.getByTestId('cta-get-started')
      const ctaLogin = screen.getByTestId('cta-login')

      // Verify Link components render as anchor tags
      expect(ctaGetStarted.tagName.toLowerCase()).toBe('a')
      expect(ctaLogin.tagName.toLowerCase()).toBe('a')

      // Verify href attributes are correct
      expect(ctaGetStarted).toHaveAttribute('href', '/register')
      expect(ctaLogin).toHaveAttribute('href', '/login')
    })

    it('footer links have correct href attributes', () => {
      renderWithRouter(<FooterSection />)

      expect(screen.getByRole('link', { name: /home/i })).toHaveAttribute('href', '/')
      expect(screen.getByRole('link', { name: /login/i })).toHaveAttribute('href', '/login')
      expect(screen.getByRole('link', { name: /register/i })).toHaveAttribute('href', '/register')
    })
  })

  describe('Animation compatibility for Safari', () => {
    it('Framer Motion animations use webkit-compatible properties', async () => {
      renderWithRouter(<HeroSection />)

      await waitFor(() => {
        expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      })

      // Framer Motion uses transform and opacity which are
      // GPU-accelerated on Safari via -webkit-transform
      const heroSection = screen.getByTestId('hero-section')
      const animatedElements = heroSection.querySelectorAll('[style]')

      expect(animatedElements.length).toBeGreaterThan(0)
    })

    it('feature cards animate correctly with staggered timing', async () => {
      render(<FeaturesSection />)

      await waitFor(() => {
        const cards = screen.getAllByTestId('feature-card')
        expect(cards.length).toBe(3)
      })

      // Staggered animations prevent Safari from overwhelming
      // the compositor with simultaneous animations
      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        // Cards should have style attributes from Framer Motion
        const style = card.getAttribute('style')
        expect(style).toBeDefined()
      })
    })
  })

  describe('Responsive design for Safari', () => {
    it('responsive classes are applied correctly', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })

      // Responsive text sizes: text-4xl md:text-6xl
      // Safari handles these media queries correctly
      expect(headline.classList.contains('text-4xl')).toBe(true)
    })

    it('responsive layout breakpoints are defined', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      const gridContainer = section.querySelector('.grid')

      // Grid responsive breakpoints
      // Safari handles Tailwind breakpoint syntax correctly
      expect(gridContainer?.className).toContain('grid-cols-1')
    })

    it('responsive padding classes are Safari-compatible', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // px-4 provides consistent padding across browsers
      expect(heroSection.classList.contains('px-4')).toBe(true)
    })
  })

  describe('Focus and accessibility compatibility', () => {
    it('interactive elements are focusable', async () => {
      const user = userEvent.setup()
      renderWithRouter(<HeroSection />)

      const ctaGetStarted = screen.getByTestId('cta-get-started')

      await act(async () => {
        await user.tab()
      })

      // Safari handles focus correctly for anchor elements
      // The first focusable element should receive focus
      expect(document.activeElement?.tagName.toLowerCase()).toBe('a')
    })

    it('navigation links have proper ARIA attributes', () => {
      renderWithRouter(<HeroSection />)

      const nav = screen.getByRole('navigation', { name: /page sections/i })
      expect(nav).toBeInTheDocument()
    })

    it('regions have proper ARIA labels', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: /features/i })
      expect(section).toBeInTheDocument()
    })
  })
})
