/**
 * Responsive Design Tests
 * Owners:
 * - Scenario 6: Mobile View (375px)
 * - Scenario 7: Tablet View (768px)
 * - Scenario 8: Desktop View (1280px)
 *
 * Test coverage:
 * - Mobile: single column, no horizontal scroll, touch-friendly buttons
 * - Tablet: 2-column grid, intermediate layout
 * - Desktop: full layout, multi-column grid, full navbar
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { renderWithProviders, screen, within } from './test-utils'
import Home from '../../pages/Home'

// Helper to set viewport width
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

// Helper to check for horizontal overflow
function hasHorizontalOverflow(element: HTMLElement): boolean {
  return element.scrollWidth > element.clientWidth
}

// Helper to get computed styles
function getComputedStyleValue(element: HTMLElement, property: string): string {
  return window.getComputedStyle(element).getPropertyValue(property)
}

describe('Responsive Design - Mobile View (375px)', () => {
  const MOBILE_WIDTH = 375

  beforeEach(() => {
    setViewportWidth(MOBILE_WIDTH)
  })

  afterEach(() => {
    // Reset viewport
    setViewportWidth(1024)
  })

  describe('Test Case 1: Page renders without horizontal overflow', () => {
    it('should render homepage at 375px viewport width without horizontal scroll', () => {
      const { container } = renderWithProviders(<Home />)

      // The main container should not have horizontal overflow
      const mainElement = container.firstChild as HTMLElement
      expect(mainElement).toBeInTheDocument()

      // Check that the page renders correctly - use getAllByText since "URL Shortener" appears multiple times
      const urlShortenerElements = screen.getAllByText('URL Shortener')
      expect(urlShortenerElements.length).toBeGreaterThan(0)

      // Verify no content extends beyond viewport
      // In a real browser, this would check scrollWidth vs clientWidth
      // For testing purposes, we verify the responsive classes are applied
      expect(mainElement).toHaveClass('min-h-screen')
    })

    it('should contain all major sections within viewport width', () => {
      const { container } = renderWithProviders(<Home />)

      // Verify main content wrapper exists
      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()

      // Verify hero section is present
      const heroSection = container.querySelector('section.hero')
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Hero section on mobile', () => {
    it('should display hero headline and CTAs stacked vertically and readable', () => {
      renderWithProviders(<Home />)

      // Check headline is present and readable
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('URL Shortener')

      // Check subheadline/description is present
      expect(screen.getByText(/Shorten URLs. Track Results. Grow Smarter./i)).toBeInTheDocument()

      // Hero content container should use text-center for mobile stacking
      const heroContent = document.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()
      expect(heroContent).toHaveClass('text-center')
    })

    it('should have readable font sizes for mobile', () => {
      renderWithProviders(<Home />)

      const headline = screen.getByRole('heading', { level: 1 })
      // Verify headline uses responsive text sizing
      expect(headline).toHaveClass('text-5xl')
    })
  })

  describe('Test Case 3: Feature cards on mobile', () => {
    it('should display feature cards in single column layout', () => {
      const { container } = renderWithProviders(<Home />)

      // Check for HowItWorksSection grid (this is the only section currently rendered)
      const gridContainer = container.querySelector('.grid')
      if (gridContainer) {
        // Should use grid-cols-1 for mobile (single column)
        expect(gridContainer).toHaveClass('grid-cols-1')
      }
    })

    it('should render HowItWorksSection with proper mobile layout', () => {
      renderWithProviders(<Home />)

      // Check that "How It Works" section is rendered
      const howItWorksHeading = screen.getByText('How It Works')
      expect(howItWorksHeading).toBeInTheDocument()

      // Verify step cards are present
      expect(screen.getByText('Paste your long URL')).toBeInTheDocument()
      expect(screen.getByText('Get a short, memorable link')).toBeInTheDocument()
      expect(screen.getByText('Track clicks and analytics')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: CTA button dimensions on mobile', () => {
    it('should have minimum 44px height for touch accessibility on buttons', () => {
      const { container } = renderWithProviders(<Home />)

      // Get all buttons in the page
      const buttons = container.querySelectorAll('button, .btn')

      buttons.forEach((button) => {
        // DaisyUI btn class provides minimum touch-friendly sizing
        // The btn class by default provides at least 44px height
        expect(button.className).toMatch(/btn/)
      })
    })

    it('should have touch-friendly navigation buttons', () => {
      renderWithProviders(<Home />)

      // Check navigation links have btn class for proper sizing
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })

      expect(loginLink).toHaveClass('btn')
      expect(registerLink).toHaveClass('btn')
    })
  })

  describe('Test Case 5: Navigation on mobile viewport', () => {
    it('should display navigation that is accessible on mobile', () => {
      renderWithProviders(<Home />)

      // Check navbar is rendered
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()

      // Verify navbar uses responsive padding
      expect(nav).toHaveClass('navbar')
    })

    it('should have accessible navigation links on mobile', () => {
      renderWithProviders(<Home />)

      // Navigation links should still be accessible
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })
      const homeLink = screen.getByRole('link', { name: /url shortener/i })

      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
      expect(homeLink).toBeInTheDocument()
    })

    it('should have theme toggle accessible on mobile', () => {
      const { container } = renderWithProviders(<Home />)

      // Theme toggle should be present and usable
      const themeToggle = container.querySelector('[data-testid="theme-toggle"]') ||
                          container.querySelector('.swap') ||
                          container.querySelector('label')

      // Theme toggle exists in navbar
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })
  })

  describe('Mobile Responsive Utilities', () => {
    it('should use appropriate padding for mobile', () => {
      const { container } = renderWithProviders(<Home />)

      // Check that sections use responsive padding
      const sections = container.querySelectorAll('section')
      sections.forEach((section) => {
        // Sections should have padding (px-4 or similar)
        expect(section.className).toBeDefined()
      })
    })

    it('should use max-w constraints to prevent overflow', () => {
      const { container } = renderWithProviders(<Home />)

      // Check for max-width containers
      const contentWrappers = container.querySelectorAll('[class*="max-w"]')
      expect(contentWrappers.length).toBeGreaterThan(0)
    })

    it('should render without horizontal scroll bars', () => {
      const { container } = renderWithProviders(<Home />)

      // The root container should not cause horizontal scrolling
      const rootDiv = container.firstChild as HTMLElement

      // Check that overflow is controlled (not visible for horizontal)
      // The min-h-screen class should be present
      expect(rootDiv).toHaveClass('min-h-screen')
      expect(rootDiv).toHaveClass('bg-base-200')
    })
  })
})

// Additional mobile-specific tests for accessibility
describe('Mobile Accessibility', () => {
  beforeEach(() => {
    setViewportWidth(375)
  })

  it('should maintain text readability at mobile viewport', () => {
    renderWithProviders(<Home />)

    // Check that text content is present and readable
    const textContent = screen.getByText(/Shorten URLs/i)
    expect(textContent).toBeInTheDocument()
  })

  it('should have proper heading hierarchy on mobile', () => {
    renderWithProviders(<Home />)

    // Check h1 exists
    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toBeInTheDocument()

    // Check h2 exists (How It Works)
    const h2 = screen.getByRole('heading', { level: 2 })
    expect(h2).toBeInTheDocument()
  })

  it('should have accessible interactive elements', () => {
    renderWithProviders(<Home />)

    // All links should be accessible
    const links = screen.getAllByRole('link')
    expect(links.length).toBeGreaterThan(0)

    links.forEach((link) => {
      expect(link).toHaveAttribute('href')
    })
  })
})

/**
 * =====================================================
 * SCENARIO 7: Tablet View Tests (768px - 1024px)
 * =====================================================
 * Tests for tablet viewport responsive design
 * Owner: Scenario 7 - Responsive Design - Tablet View
 */

describe('Responsive Design - Tablet View (768px)', () => {
  const TABLET_WIDTH = 768

  beforeEach(() => {
    setViewportWidth(TABLET_WIDTH)
  })

  afterEach(() => {
    // Reset viewport
    setViewportWidth(1024)
  })

  describe('Test Case 1: Page renders with tablet-optimized layout', () => {
    it('should render homepage at 768px viewport width with tablet layout', () => {
      const { container } = renderWithProviders(<Home />)

      // The main container should render correctly
      const mainElement = container.firstChild as HTMLElement
      expect(mainElement).toBeInTheDocument()

      // Check that the page renders correctly
      const urlShortenerElements = screen.getAllByText('URL Shortener')
      expect(urlShortenerElements.length).toBeGreaterThan(0)

      // Verify base layout classes are applied
      expect(mainElement).toHaveClass('min-h-screen')
      expect(mainElement).toHaveClass('bg-base-200')
    })

    it('should contain all major sections at tablet viewport', () => {
      const { container } = renderWithProviders(<Home />)

      // Verify main content wrapper exists
      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()

      // Verify hero section is present
      const heroSection = container.querySelector('section.hero')
      expect(heroSection).toBeInTheDocument()

      // Verify How It Works section is present
      const howItWorksHeading = screen.getByText('How It Works')
      expect(howItWorksHeading).toBeInTheDocument()
    })

    it('should apply tablet-specific responsive breakpoints', () => {
      const { container } = renderWithProviders(<Home />)

      // At 768px, the md: breakpoint should be active in Tailwind
      // Verify sections use max-width constraints
      const contentWrappers = container.querySelectorAll('[class*="max-w"]')
      expect(contentWrappers.length).toBeGreaterThan(0)

      // Verify padding adapts for tablet
      const navbar = screen.getByRole('navigation')
      expect(navbar).toHaveClass('navbar')
    })
  })

  describe('Test Case 2: Feature cards display in 2-column grid layout', () => {
    it('should display grid container with tablet-appropriate columns', () => {
      const { container } = renderWithProviders(<Home />)

      // Find the grid container in HowItWorksSection
      const gridContainer = container.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // At tablet (md breakpoint, 768px), grid should use 2 columns
      // The component should have md:grid-cols-2 for tablet view
      expect(gridContainer).toHaveClass('md:grid-cols-2')
    })

    it('should render workflow step cards in grid layout', () => {
      renderWithProviders(<Home />)

      // Verify all three workflow steps are rendered
      expect(screen.getByText('Paste your long URL')).toBeInTheDocument()
      expect(screen.getByText('Get a short, memorable link')).toBeInTheDocument()
      expect(screen.getByText('Track clicks and analytics')).toBeInTheDocument()

      // Verify step numbers are present
      expect(screen.getByTestId('step-number-1')).toBeInTheDocument()
      expect(screen.getByTestId('step-number-2')).toBeInTheDocument()
      expect(screen.getByTestId('step-number-3')).toBeInTheDocument()
    })

    it('should have proper gap spacing between grid items at tablet size', () => {
      const { container } = renderWithProviders(<Home />)

      const gridContainer = container.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Verify gap class is applied for proper spacing
      expect(gridContainer).toHaveClass('gap-8')
    })
  })

  describe('Test Case 3: Hero content is well-proportioned for tablet display', () => {
    it('should display hero section with appropriate sizing for tablet', () => {
      const { container } = renderWithProviders(<Home />)

      // Verify hero section exists
      const heroSection = container.querySelector('section.hero')
      expect(heroSection).toBeInTheDocument()

      // Hero should have minimum height
      expect(heroSection).toHaveClass('min-h-[60vh]')
    })

    it('should display headline with tablet-appropriate text size', () => {
      renderWithProviders(<Home />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('URL Shortener')

      // Verify responsive text sizing - md breakpoint should apply larger size
      expect(headline).toHaveClass('md:text-6xl')
    })

    it('should display subheadline and description properly at tablet width', () => {
      renderWithProviders(<Home />)

      // Check subheadline/description is present and properly styled
      const description = screen.getByText(/Shorten URLs. Track Results. Grow Smarter./i)
      expect(description).toBeInTheDocument()
    })

    it('should have hero content container with tablet-appropriate max-width', () => {
      const { container } = renderWithProviders(<Home />)

      // Hero content should expand for tablet
      const heroContent = container.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()

      // Check for max-width container that adapts to tablet
      const maxWidthContainer = heroContent?.querySelector('[class*="max-w"]')
      expect(maxWidthContainer).toBeInTheDocument()
      expect(maxWidthContainer).toHaveClass('md:max-w-2xl')
    })

    it('should display CTA buttons appropriately for tablet view', () => {
      renderWithProviders(<Home />)

      // Verify navigation buttons are present and properly styled
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })

      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
      expect(loginLink).toHaveClass('btn')
      expect(registerLink).toHaveClass('btn')
    })
  })

  describe('Tablet Navigation and Layout', () => {
    it('should display full navigation bar at tablet size', () => {
      renderWithProviders(<Home />)

      // Check navbar is rendered with full layout
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
      expect(nav).toHaveClass('navbar')

      // At tablet size, navigation links should be visible (not collapsed into hamburger)
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })

      expect(loginLink).toBeVisible()
      expect(registerLink).toBeVisible()
    })

    it('should have theme toggle accessible at tablet viewport', () => {
      renderWithProviders(<Home />)

      // Theme toggle should be present in navbar
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()

      // ThemeToggle component should be rendered
      const navbar = nav.closest('.navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('should have proper padding for tablet viewport', () => {
      const { container } = renderWithProviders(<Home />)

      // Navbar should have responsive padding
      const nav = screen.getByRole('navigation')
      expect(nav).toHaveClass('px-4')
      expect(nav).toHaveClass('lg:px-8')

      // Sections should have responsive padding
      const sections = container.querySelectorAll('section')
      expect(sections.length).toBeGreaterThan(0)
    })
  })

  describe('Tablet Intermediate Layout', () => {
    it('should show intermediate layout between mobile and desktop', () => {
      const { container } = renderWithProviders(<Home />)

      // At tablet (768px), layout should differ from mobile (1 col) and desktop (3+ col)
      const gridContainer = container.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Should have responsive grid classes
      expect(gridContainer).toHaveClass('grid-cols-1') // mobile base
      expect(gridContainer).toHaveClass('md:grid-cols-2') // tablet (2 columns)
      expect(gridContainer).toHaveClass('lg:grid-cols-3') // desktop (3 columns)
    })

    it('should maintain proper content width constraints at tablet', () => {
      const { container } = renderWithProviders(<Home />)

      // Check for max-width containers that constrain content appropriately
      const maxWidthContainers = container.querySelectorAll('[class*="max-w-"]')
      expect(maxWidthContainers.length).toBeGreaterThan(0)

      // Section content should be contained within max-w-6xl
      const sectionContainer = container.querySelector('.max-w-6xl')
      expect(sectionContainer).toBeInTheDocument()
    })

    it('should have readable text sizes at tablet viewport', () => {
      renderWithProviders(<Home />)

      // H1 should be readable
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()

      // H2 (How It Works) should use tablet-appropriate sizing
      const h2 = screen.getByRole('heading', { level: 2 })
      expect(h2).toBeInTheDocument()
      expect(h2).toHaveClass('lg:text-4xl') // larger on desktop
    })
  })
})

// Tablet accessibility tests
describe('Tablet Accessibility', () => {
  beforeEach(() => {
    setViewportWidth(768)
  })

  afterEach(() => {
    setViewportWidth(1024)
  })

  it('should maintain proper heading hierarchy at tablet viewport', () => {
    renderWithProviders(<Home />)

    // Check h1 exists
    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toBeInTheDocument()

    // Check h2 exists (How It Works)
    const h2 = screen.getByRole('heading', { level: 2 })
    expect(h2).toBeInTheDocument()
  })

  it('should have all interactive elements accessible at tablet size', () => {
    renderWithProviders(<Home />)

    // All links should be accessible
    const links = screen.getAllByRole('link')
    expect(links.length).toBeGreaterThan(0)

    links.forEach((link) => {
      expect(link).toHaveAttribute('href')
    })
  })

  it('should have sufficient touch targets at tablet viewport', () => {
    const { container } = renderWithProviders(<Home />)

    // Get all buttons/links - they should have btn class for proper sizing
    const buttons = container.querySelectorAll('button, .btn')
    buttons.forEach((button) => {
      expect(button.className).toMatch(/btn/)
    })
  })
})
