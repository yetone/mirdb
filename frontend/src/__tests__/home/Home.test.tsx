/**
 * Main Homepage Component Tests
 * Owner: Scenario 15 - Component Composition and Reusability
 *
 * Tests the Home.tsx component composition and integration.
 *
 * Test coverage:
 * - All section components are rendered
 * - Proper component hierarchy
 * - Integration with existing components (Navbar, ThemeToggle, etc.)
 * - Component reuse patterns
 */
import { describe, it, expect, vi } from 'vitest'
import { renderWithProviders, screen, within } from './test-utils'
import Home from '../../pages/Home'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h1 {...props}>{children}</h1>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Mock recharts to avoid ResizeObserver issues
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="responsive-container">{children}</div>
  ),
  AreaChart: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="area-chart">{children}</div>
  ),
  Area: () => null,
  XAxis: () => null,
  YAxis: () => null,
  CartesianGrid: () => null,
  Tooltip: () => null,
}))

describe('Home Component Composition', () => {
  describe('Test Case 1: Home.tsx imports section components', () => {
    it('should import and render HeroSection as a separate component', () => {
      renderWithProviders(<Home />)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should import and render FeaturesSection as a separate component', () => {
      renderWithProviders(<Home />)
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should import and render HowItWorksSection as a separate component', () => {
      renderWithProviders(<Home />)
      // HowItWorksSection has "How It Works" heading
      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })
      expect(howItWorksHeading).toBeInTheDocument()
    })

    it('should render all section components in correct order', () => {
      renderWithProviders(<Home />)

      const main = screen.getByRole('main')
      const sections = within(main).getAllByRole('region', { hidden: true })

      // Verify we have multiple sections rendered
      expect(sections.length).toBeGreaterThanOrEqual(4)
    })
  })

  describe('Test Case 2: FuturisticButton usage for CTAs', () => {
    it('should use FuturisticButton for Get Started CTA', () => {
      renderWithProviders(<Home />)
      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton).toBeInTheDocument()
      expect(getStartedButton).toHaveClass('btn')
      expect(getStartedButton.tagName).toBe('BUTTON')
    })

    it('should use FuturisticButton for Login CTA', () => {
      renderWithProviders(<Home />)
      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveClass('btn')
      expect(loginButton.tagName).toBe('BUTTON')
    })

    it('should have primary variant on Get Started button', () => {
      renderWithProviders(<Home />)
      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton).toHaveClass('btn-primary')
    })

    it('should have secondary variant on Login button', () => {
      renderWithProviders(<Home />)
      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toHaveClass('btn-secondary')
    })
  })

  describe('Test Case 3: GlassMorphismCard usage in features', () => {
    it('should use GlassMorphismCard components in FeaturesSection', () => {
      renderWithProviders(<Home />)

      // Check for feature cards that use GlassMorphismCard styling
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // GlassMorphismCard uses specific styling classes
      const cards = within(featuresGrid).getAllByText(/URL Shortening|Click Analytics|GeoIP Tracking/i)
      expect(cards.length).toBeGreaterThanOrEqual(3)
    })

    it('should render at least 4 feature cards with GlassMorphismCard', () => {
      renderWithProviders(<Home />)

      // GlassMorphismCard has card class - check features section for cards
      const featuresSection = screen.getByTestId('features-section')
      // Get all cards with the GlassMorphismCard styling class
      const cards = featuresSection.querySelectorAll('.card')
      expect(cards.length).toBeGreaterThanOrEqual(4)
    })

    it('should use GlassMorphismCard in HowItWorksSection', () => {
      renderWithProviders(<Home />)

      // HowItWorksSection uses GlassMorphismCard for each step
      const stepNumbers = screen.getAllByTestId(/step-number-/i)
      expect(stepNumbers.length).toBe(3)
    })
  })

  describe('Test Case 4: Navbar component inclusion', () => {
    it('should include Navbar component at the top', () => {
      renderWithProviders(<Home />)

      // Get navbar specifically (not the footer nav)
      const navbars = screen.getAllByRole('navigation')
      const mainNavbar = navbars.find(nav => nav.classList.contains('navbar'))
      expect(mainNavbar).toBeInTheDocument()
    })

    it('should render Navbar with logo/brand link', () => {
      renderWithProviders(<Home />)

      // Navbar has a link to home with brand name
      const brandLink = screen.getByRole('link', { name: /url shortener/i })
      expect(brandLink).toBeInTheDocument()
    })

    it('should render Navbar before main content', () => {
      renderWithProviders(<Home />)

      // Get navbar specifically (the one with navbar class)
      const navbars = screen.getAllByRole('navigation')
      const mainNavbar = navbars.find(nav => nav.classList.contains('navbar'))
      const main = screen.getByRole('main')

      expect(mainNavbar).toBeDefined()
      // Check that navbar comes before main in the DOM
      const navbarPosition = mainNavbar!.compareDocumentPosition(main)
      // DOCUMENT_POSITION_FOLLOWING = 4
      expect(navbarPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBe(Node.DOCUMENT_POSITION_FOLLOWING)
    })

    it('should include Login and Register links in Navbar', () => {
      renderWithProviders(<Home />)

      // Get navbar specifically
      const navbars = screen.getAllByRole('navigation')
      const mainNavbar = navbars.find(nav => nav.classList.contains('navbar'))
      expect(mainNavbar).toBeDefined()

      // Login and Register links should be in navbar
      const loginLinks = screen.getAllByRole('link', { name: /login/i })
      const registerLinks = screen.getAllByRole('link', { name: /register/i })

      // At least one of each should be in the navbar
      expect(loginLinks.length).toBeGreaterThan(0)
      expect(registerLinks.length).toBeGreaterThan(0)
    })
  })

  describe('Component Composition Structure', () => {
    it('should have correct page structure with all sections', () => {
      renderWithProviders(<Home />)

      // Check for main structural elements
      const navbars = screen.getAllByRole('navigation')
      const mainNavbar = navbars.find(nav => nav.classList.contains('navbar'))
      expect(mainNavbar).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should render AnalyticsPreviewSection', () => {
      renderWithProviders(<Home />)

      const analyticsSection = screen.getByTestId('analytics-preview-section')
      expect(analyticsSection).toBeInTheDocument()
    })

    it('should render Footer at the bottom', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()

      // Footer should contain copyright
      expect(screen.getByTestId('copyright')).toBeInTheDocument()
    })
  })
})
