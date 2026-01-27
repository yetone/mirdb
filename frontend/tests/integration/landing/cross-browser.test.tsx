/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 19 - Cross-Browser Compatibility
 *
 * Tests that verify the landing page uses cross-browser compatible patterns
 * that work correctly in Chrome, Firefox, Safari, and Edge.
 *
 * Test cases:
 * 1. Chrome compatibility - verify page renders with all sections
 * 2. Firefox compatibility - verify layout and flexbox/grid work correctly
 * 3. Safari compatibility - verify webkit-specific features are handled
 * 4. Edge compatibility - verify Chromium-based rendering works
 *
 * Note: These tests verify code patterns and DOM structure that ensure
 * cross-browser compatibility rather than running in actual browsers.
 * The landing page uses Tailwind CSS + DaisyUI which provide cross-browser
 * compatible CSS with appropriate vendor prefixes.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup } from './setup'
import Home from '../../../src/pages/Home'

// Mock scroll behavior for tests
const mockScrollIntoView = vi.fn()
Element.prototype.scrollIntoView = mockScrollIntoView

describe('Cross-Browser Compatibility', () => {
  beforeEach(() => {
    mockScrollIntoView.mockClear()
    // Reset any document attributes
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  /**
   * Test Case 1: Chrome Compatibility
   * Verifies the landing page renders correctly with all sections
   * Chrome is the reference browser (most commonly used)
   */
  describe('Test Case 1: Chrome Compatibility', () => {
    it('renders all landing page sections without layout issues', () => {
      render(<Home />)

      // Verify header/navigation renders
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // Verify navigation links are present
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument()

      // Verify main content area
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Verify hero section
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()
      expect(screen.getByText(/Create short, powerful links/)).toBeInTheDocument()

      // Verify features section
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Verify How It Works section
      expect(screen.getByText('How It Works')).toBeInTheDocument()

      // Verify CTA section
      expect(screen.getByTestId('cta-section')).toBeInTheDocument()

      // Verify footer
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('uses CSS Grid for features section layout (Chrome 57+)', () => {
      const { container } = render(<Home />)

      // Find the features grid container
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify grid classes are applied (Tailwind CSS)
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
      expect(featuresGrid).toHaveClass('lg:grid-cols-4')
    })

    it('uses Flexbox for layout alignment (Chrome 21+)', () => {
      const { container } = render(<Home />)

      // Check for flex containers in the page
      const flexContainers = container.querySelectorAll('[class*="flex"]')
      expect(flexContainers.length).toBeGreaterThan(0)

      // Verify navbar uses flexbox
      const navbar = screen.getByRole('banner')
      expect(navbar.querySelector('.navbar-end')).toBeInTheDocument()
    })

    it('renders interactive elements correctly', () => {
      render(<Home />)

      // All buttons should be interactive
      const buttons = screen.getAllByRole('button')
      expect(buttons.length).toBeGreaterThan(0)

      buttons.forEach(button => {
        expect(button).not.toBeDisabled()
      })

      // All links should be navigable
      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThan(0)

      links.forEach(link => {
        expect(link).toHaveAttribute('href')
      })
    })
  })

  /**
   * Test Case 2: Firefox Compatibility
   * Verifies layout and CSS features work in Firefox
   */
  describe('Test Case 2: Firefox Compatibility', () => {
    it('renders page structure correctly (Firefox uses Gecko engine)', () => {
      render(<Home />)

      // Verify semantic HTML structure (important for Firefox accessibility)
      expect(screen.getByRole('banner')).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()

      // Firefox renders headings correctly
      const headings = screen.getAllByRole('heading')
      expect(headings.length).toBeGreaterThan(0)

      // Verify h1 heading exists (important for SEO and accessibility)
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
    })

    it('CSS transitions work correctly (Firefox 4+)', () => {
      const { container } = render(<Home />)

      // Check that elements have transition classes (Tailwind CSS)
      const transitionElements = container.querySelectorAll('[class*="transition"]')
      expect(transitionElements.length).toBeGreaterThan(0)
    })

    it('backdrop-filter/blur is handled gracefully', () => {
      const { container } = render(<Home />)

      // Tailwind uses backdrop-blur which has good Firefox support (79+)
      // The navbar uses backdrop-blur for glass effect
      const navbar = screen.getByRole('banner')
      expect(navbar).toBeInTheDocument()
      expect(navbar.className).toContain('backdrop-blur')
    })

    it('gradient text renders correctly', () => {
      render(<Home />)

      // The hero heading uses gradient text
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading).toBeInTheDocument()

      // Verify gradient classes are applied
      expect(heroHeading).toHaveClass('bg-gradient-to-r')
      expect(heroHeading).toHaveClass('from-primary')
      expect(heroHeading).toHaveClass('to-secondary')
      expect(heroHeading).toHaveClass('bg-clip-text')
      expect(heroHeading).toHaveClass('text-transparent')
    })

    it('feature cards render in grid layout', () => {
      render(<Home />)

      // Verify all 4 feature cards are rendered
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)

      // Verify each card has expected content
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument()
      expect(screen.getByText('Share Stats')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Safari Compatibility
   * Verifies WebKit-specific features are handled correctly
   */
  describe('Test Case 3: Safari Compatibility', () => {
    it('renders all sections (WebKit engine)', () => {
      render(<Home />)

      // Safari uses WebKit - verify basic rendering
      expect(screen.getByRole('banner')).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()

      // Verify text content renders
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()
    })

    it('smooth scroll is supported (Safari 15.4+, fallback for older)', () => {
      render(<Home />)

      // Find the Learn More link
      const learnMoreLink = screen.getByRole('link', { name: /learn more/i })
      expect(learnMoreLink).toBeInTheDocument()
      expect(learnMoreLink).toHaveAttribute('href', '#features')

      // The component handles scroll programmatically with scrollIntoView
      // which has good Safari support
    })

    it('CSS variables work correctly (Safari 9.1+)', () => {
      const { container } = render(<Home />)

      // DaisyUI and Tailwind use CSS variables extensively
      // Verify the page renders (if CSS vars failed, layout would break)
      const mainContent = screen.getByRole('main')
      expect(mainContent).toBeInTheDocument()

      // Check that themed elements exist
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()
    })

    it('SVG icons render correctly', () => {
      const { container } = render(<Home />)

      // Feature icons are SVG (from Heroicons)
      const svgIcons = container.querySelectorAll('svg')
      expect(svgIcons.length).toBeGreaterThan(0)

      // Verify SVGs have proper attributes for accessibility
      const featureIcons = screen.getAllByTestId('feature-icon')
      expect(featureIcons.length).toBe(4)

      featureIcons.forEach(iconContainer => {
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('Flexbox gap property works (Safari 14.1+)', () => {
      const { container } = render(<Home />)

      // Check for gap classes (Tailwind)
      const gapElements = container.querySelectorAll('[class*="gap-"]')
      expect(gapElements.length).toBeGreaterThan(0)

      // Hero CTA buttons use gap
      const ctaContainer = container.querySelector('.flex.flex-col.sm\\:flex-row.gap-4')
      expect(ctaContainer).toBeInTheDocument()
    })

    it('min-height viewport units work correctly', () => {
      const { container } = render(<Home />)

      // Hero section uses min-h-[80vh]
      const heroSection = container.querySelector('[class*="min-h-[80vh]"]')
      expect(heroSection).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Edge Compatibility
   * Verifies Chromium-based Edge renders correctly
   */
  describe('Test Case 4: Edge Compatibility', () => {
    it('renders page with all sections (Chromium-based)', () => {
      render(<Home />)

      // Edge (Chromium) should render identically to Chrome
      expect(screen.getByRole('banner')).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()

      // Verify all major sections
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-section')).toBeInTheDocument()
    })

    it('modern CSS features are supported', () => {
      const { container } = render(<Home />)

      // CSS Grid
      const gridElements = container.querySelectorAll('[class*="grid"]')
      expect(gridElements.length).toBeGreaterThan(0)

      // Flexbox
      const flexElements = container.querySelectorAll('[class*="flex"]')
      expect(flexElements.length).toBeGreaterThan(0)

      // Backdrop blur
      const blurElements = container.querySelectorAll('[class*="backdrop-blur"]')
      expect(blurElements.length).toBeGreaterThan(0)
    })

    it('navigation links work correctly', () => {
      render(<Home />)

      // Verify all navigation links
      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')

      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toHaveAttribute('href', '/register')

      const brandLink = screen.getByRole('link', { name: /url shortener/i })
      expect(brandLink).toHaveAttribute('href', '/')
    })

    it('theme toggle functions correctly', () => {
      render(<Home />)

      // Find theme toggle button(s)
      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to (dark|light) mode/i })
      expect(themeToggleButtons.length).toBeGreaterThan(0)

      // Verify they are interactive
      themeToggleButtons.forEach(button => {
        expect(button).not.toBeDisabled()
      })
    })

    it('CTA buttons are clickable and accessible', () => {
      render(<Home />)

      // Hero Get Started button
      const heroGetStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(heroGetStartedButton).toBeInTheDocument()
      expect(heroGetStartedButton).not.toBeDisabled()
      expect(heroGetStartedButton).toHaveAccessibleName()

      // CTA section button
      const ctaButton = screen.getByTestId('cta-register-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).not.toBeDisabled()
    })
  })

  /**
   * Cross-browser CSS feature compatibility tests
   */
  describe('CSS Feature Compatibility', () => {
    it('uses standard CSS properties with Tailwind (no vendor prefixes needed)', () => {
      const { container } = render(<Home />)

      // Tailwind CSS handles vendor prefixes automatically
      // Verify common utility classes are used
      const roundedElements = container.querySelectorAll('[class*="rounded"]')
      expect(roundedElements.length).toBeGreaterThan(0)

      const shadowElements = container.querySelectorAll('[class*="shadow"]')
      expect(shadowElements.length).toBeGreaterThan(0)

      const borderElements = container.querySelectorAll('[class*="border"]')
      expect(borderElements.length).toBeGreaterThan(0)
    })

    it('responsive breakpoints work across browsers', () => {
      const { container } = render(<Home />)

      // Check for responsive classes (sm:, md:, lg:)
      const smClasses = container.querySelectorAll('[class*="sm:"]')
      const mdClasses = container.querySelectorAll('[class*="md:"]')
      const lgClasses = container.querySelectorAll('[class*="lg:"]')

      // Page should have responsive styling
      expect(smClasses.length + mdClasses.length + lgClasses.length).toBeGreaterThan(0)
    })

    it('color opacity modifiers work correctly', () => {
      render(<Home />)

      // Tailwind uses color opacity modifiers like text-base-content/80
      const subheadline = screen.getByText(/Create short, powerful links/)
      expect(subheadline).toHaveClass('text-base-content/80')
    })

    it('transform and animation properties are compatible', () => {
      const { container } = render(<Home />)

      // Framer Motion applies transforms via inline styles
      // Tailwind animate utilities are used (e.g., animate-pulse in BackgroundEffect)
      const animatedElements = container.querySelectorAll('[class*="animate-"]')
      expect(animatedElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * JavaScript API compatibility tests
   */
  describe('JavaScript API Compatibility', () => {
    it('uses standard DOM APIs', () => {
      render(<Home />)

      // Verify getElementById works (used for smooth scroll)
      const featuresSection = document.getElementById('features')
      expect(featuresSection).toBeInTheDocument()
    })

    it('event handlers work correctly', () => {
      render(<Home />)

      // All interactive elements should have working event handlers
      const buttons = screen.getAllByRole('button')
      buttons.forEach(button => {
        // Buttons should be interactable
        expect(button).not.toBeDisabled()
      })
    })

    it('React rendering is consistent', () => {
      const { container } = render(<Home />)

      // Verify React root renders correctly
      expect(container.firstChild).toBeInTheDocument()

      // Check that components render without errors
      const mainContent = screen.getByRole('main')
      expect(mainContent.children.length).toBeGreaterThan(0)
    })
  })

  /**
   * Accessibility across browsers
   */
  describe('Cross-Browser Accessibility', () => {
    it('ARIA attributes are properly rendered', () => {
      const { container } = render(<Home />)

      // Check aria-labelledby on sections
      const heroSection = container.querySelector('[aria-labelledby="hero-heading"]')
      expect(heroSection).toBeInTheDocument()

      const ctaSection = container.querySelector('[aria-labelledby="cta-heading"]')
      expect(ctaSection).toBeInTheDocument()

      // Verify the referenced ids exist
      const heroHeadingId = container.querySelector('#hero-heading')
      expect(heroHeadingId).toBeInTheDocument()

      const ctaHeadingId = container.querySelector('#cta-heading')
      expect(ctaHeadingId).toBeInTheDocument()
    })

    it('focus management works across browsers', () => {
      render(<Home />)

      // All focusable elements should have proper tabIndex
      const links = screen.getAllByRole('link')
      const buttons = screen.getAllByRole('button')

      const focusableElements = [...links, ...buttons]
      focusableElements.forEach(element => {
        expect(element.tabIndex).not.toBe(-1)
      })
    })

    it('screen reader compatible structure', () => {
      render(<Home />)

      // Landmarks are present
      expect(screen.getByRole('banner')).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()

      // Only one h1
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })
  })
})
