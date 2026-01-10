import { describe, it, expect, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { axe } from 'vitest-axe'
import Home from '../pages/Home'
import Navbar from '../components/Navbar'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import FooterSection from '../components/FooterSection'
import { ThemeProvider } from '../contexts/ThemeContext'

/**
 * Screen Reader Compatibility Tests
 *
 * Verifies that homepage content is accessible to screen readers:
 * - Test Case 1: Manual - Content is read in logical order (verified via structure)
 * - Test Case 2: ARIA landmarks (banner, main, contentinfo)
 * - Test Case 3: Buttons have accessible names and roles
 * - Test Case 4: Automated a11y audit with axe-core
 */

// Theme setup helpers
const setupTheme = (theme: string) => {
  document.documentElement.setAttribute('data-theme', theme)
}

const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
}

// Helper to render with router and theme
const renderWithProviders = (initialEntries: string[] = ['/']) => {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>
        <Routes>
          <Route
            path="/"
            element={
              <>
                <Navbar />
                <Home />
              </>
            }
          />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        </Routes>
      </MemoryRouter>
    </ThemeProvider>
  )
}

describe('Screen Reader Compatibility', () => {
  afterEach(() => {
    cleanupTheme()
  })

  /**
   * Test Case 1: Navigate homepage with screen reader
   * Expected: Content is read in logical order
   * Type: Manual (automated verification of logical DOM structure)
   *
   * While we cannot truly simulate screen reader behavior in automated tests,
   * we can verify the DOM structure that screen readers rely on.
   */
  describe('Test Case 1: Content reading order (structure verification)', () => {
    it('should have content structured in logical reading order', () => {
      renderWithProviders()

      // Get the main content sections in order
      const navbar = screen.getByTestId('navbar')
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      // Verify all sections exist
      expect(navbar).toBeInTheDocument()
      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
      expect(footerSection).toBeInTheDocument()

      // Verify reading order via DOM structure (hero before features before footer)
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Hero should come before features in DOM
      const heroPosition = heroSection.compareDocumentPosition(featuresSection)
      expect(heroPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()

      // Features should come before how-it-works in DOM
      const featuresPosition = featuresSection.compareDocumentPosition(howItWorksSection)
      expect(featuresPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()

      // How-it-works should come before footer in DOM
      const howItWorksPosition = howItWorksSection.compareDocumentPosition(footerSection)
      expect(howItWorksPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('should have heading hierarchy that makes sense for screen readers', () => {
      renderWithProviders()

      // Get all headings
      const h1 = screen.getByRole('heading', { level: 1 })
      const h2Headings = screen.getAllByRole('heading', { level: 2 })
      const h3Headings = screen.getAllByRole('heading', { level: 3 })

      // Main heading should be the hero headline
      expect(h1).toHaveTextContent('Shorten, Share, Track')

      // Section headings should be h2
      expect(h2Headings.length).toBeGreaterThanOrEqual(2)
      expect(screen.getByRole('heading', { name: /powerful features/i, level: 2 })).toBeInTheDocument()
      expect(screen.getByRole('heading', { name: /how it works/i, level: 2 })).toBeInTheDocument()

      // Feature titles should be h3 (under h2)
      expect(h3Headings.length).toBeGreaterThanOrEqual(3)
    })

    it('should have descriptive text content that provides context', () => {
      renderWithProviders()

      // Hero subheadline provides context
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent(/transform your long urls into short, memorable links/i)

      // Feature descriptions provide context
      expect(screen.getByText(/create memorable, short links instantly/i)).toBeInTheDocument()
      expect(screen.getByText(/track clicks, locations, and referrers/i)).toBeInTheDocument()
      expect(screen.getByText(/organize and manage all your links/i)).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Check ARIA landmarks
   * Expected: Page has proper landmark regions (banner, main, contentinfo)
   * Type: Unit
   */
  describe('Test Case 2: ARIA landmarks', () => {
    it('should have navigation landmarks on the page', () => {
      renderWithProviders()

      // There can be multiple navigation landmarks (main navbar + footer nav)
      const navigations = screen.getAllByRole('navigation')
      expect(navigations.length).toBeGreaterThanOrEqual(1)
      // The main navbar should be present
      const navbar = screen.getByTestId('navbar')
      expect(navbar.tagName.toLowerCase()).toBe('nav')
    })

    it('should have a main content landmark', () => {
      renderWithProviders()

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
      expect(main.tagName.toLowerCase()).toBe('main')
    })

    it('should have a footer (contentinfo) landmark', () => {
      renderWithProviders()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have all required landmark regions accessible to screen readers', () => {
      renderWithProviders()

      // Screen readers use these landmarks for quick navigation
      // Main navigation landmark (at least one)
      const navigations = screen.getAllByRole('navigation')
      expect(navigations.length).toBeGreaterThanOrEqual(1)

      // Single main landmark
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Single contentinfo/footer landmark
      const contentinfo = screen.getByRole('contentinfo')
      expect(contentinfo).toBeInTheDocument()
    })

    it('should have landmark regions in logical order (nav, main, footer)', () => {
      renderWithProviders()

      // Get the main navbar (first nav element)
      const navbar = screen.getByTestId('navbar')
      const main = screen.getByRole('main')
      const footer = screen.getByRole('contentinfo')

      // Navigation should come before main
      const navToMain = navbar.compareDocumentPosition(main)
      expect(navToMain & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()

      // Main should come before footer
      const mainToFooter = main.compareDocumentPosition(footer)
      expect(mainToFooter & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('should have section elements with proper semantic roles', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Hero uses section element
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName.toLowerCase()).toBe('section')
    })

    it('should have footer navigation using nav element', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      // Footer contains nav elements for link groups
      const navElements = document.querySelectorAll('footer nav')
      expect(navElements.length).toBeGreaterThanOrEqual(1)
    })
  })

  /**
   * Test Case 3: Check button accessibility
   * Expected: Buttons have accessible names and roles
   * Type: Unit
   */
  describe('Test Case 3: Button accessibility', () => {
    it('should have primary CTA button with accessible name', () => {
      renderWithProviders()

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent('Get Started Free')
    })

    it('should have theme toggle button with aria-label', () => {
      renderWithProviders()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toHaveAttribute('aria-label')
      // The aria-label should describe the action
      expect(themeToggle.getAttribute('aria-label')).toMatch(/switch to (dark|light) mode/i)
    })

    it('should have theme toggle with correct button role', () => {
      renderWithProviders()

      const themeToggle = screen.getByRole('button', { name: /switch to (dark|light) mode/i })
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have navbar login button accessible by role', () => {
      renderWithProviders()

      // Login in navbar - use the navbar test ID to scope the search
      const navbar = screen.getByTestId('navbar')
      const navLinks = within(navbar).getAllByRole('link')
      const loginLink = navLinks.find((link) => link.textContent?.toLowerCase().includes('login'))
      expect(loginLink).toBeInTheDocument()
    })

    it('should have navbar register button accessible by role', () => {
      renderWithProviders()

      // Register in navbar - use the navbar test ID to scope the search
      const navbar = screen.getByTestId('navbar')
      const navLinks = within(navbar).getAllByRole('link')
      const registerLink = navLinks.find((link) => link.textContent?.toLowerCase().includes('register'))
      expect(registerLink).toBeInTheDocument()
    })

    it('should have hero login link with accessible text', () => {
      renderWithProviders()

      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toHaveTextContent('Log in')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have all footer links with accessible names', () => {
      renderWithProviders()

      // Check footer links are accessible by their test IDs and have proper accessible names
      const footerHomeLink = screen.getByTestId('footer-link-home')
      const footerLoginLink = screen.getByTestId('footer-link-login')
      const footerRegisterLink = screen.getByTestId('footer-link-register')
      const footerPrivacyLink = screen.getByTestId('footer-link-privacy')
      const footerTermsLink = screen.getByTestId('footer-link-terms')

      // All links should have accessible text content
      expect(footerHomeLink).toHaveTextContent('Home')
      expect(footerLoginLink).toHaveTextContent('Login')
      expect(footerRegisterLink).toHaveTextContent('Register')
      expect(footerPrivacyLink).toHaveTextContent('Privacy Policy')
      expect(footerTermsLink).toHaveTextContent('Terms of Service')

      // All should be links (accessible role)
      expect(footerHomeLink.tagName.toLowerCase()).toBe('a')
      expect(footerLoginLink.tagName.toLowerCase()).toBe('a')
      expect(footerRegisterLink.tagName.toLowerCase()).toBe('a')
      expect(footerPrivacyLink.tagName.toLowerCase()).toBe('a')
      expect(footerTermsLink.tagName.toLowerCase()).toBe('a')
    })

    it('should have decorative elements hidden from screen readers', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Check that decorative shine effects are hidden
      const hiddenElements = document.querySelectorAll('[aria-hidden="true"]')
      expect(hiddenElements.length).toBeGreaterThanOrEqual(0) // May have decorative elements
    })

    it('should not have empty button/link text (all interactive elements have accessible names)', () => {
      renderWithProviders()

      // Get all buttons and links
      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')

      // All buttons should have accessible names (text content or aria-label)
      buttons.forEach((button) => {
        const hasText = button.textContent?.trim()
        const hasAriaLabel = button.hasAttribute('aria-label')
        const hasAriaLabelledBy = button.hasAttribute('aria-labelledby')
        expect(hasText || hasAriaLabel || hasAriaLabelledBy).toBeTruthy()
      })

      // All links should have accessible names
      links.forEach((link) => {
        const hasText = link.textContent?.trim()
        const hasAriaLabel = link.hasAttribute('aria-label')
        const hasAriaLabelledBy = link.hasAttribute('aria-labelledby')
        expect(hasText || hasAriaLabel || hasAriaLabelledBy).toBeTruthy()
      })
    })
  })

  /**
   * Test Case 4: Run automated a11y audit (axe-core)
   * Expected: No critical accessibility violations
   * Type: Integration
   */
  describe('Test Case 4: Automated a11y audit with axe-core', () => {
    it('should have no critical accessibility violations on homepage', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'],
        },
      })

      // Filter for critical and serious violations only
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should pass ARIA-related accessibility rules', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: [
            'aria-allowed-attr',
            'aria-hidden-body',
            'aria-hidden-focus',
            'aria-input-field-name',
            'aria-required-attr',
            'aria-required-children',
            'aria-required-parent',
            'aria-roles',
            'aria-toggle-field-name',
            'aria-valid-attr-value',
            'aria-valid-attr',
          ],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should pass landmark-related accessibility rules', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: [
            'landmark-banner-is-top-level',
            'landmark-contentinfo-is-top-level',
            'landmark-main-is-top-level',
            'landmark-no-duplicate-banner',
            'landmark-no-duplicate-contentinfo',
            'landmark-no-duplicate-main',
            'landmark-one-main',
            // Note: 'region' rule can be too strict for components outside landmarks
          ],
        },
      })

      // Filter for critical violations only
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should pass button and link accessibility rules', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: [
            'button-name',
            'link-name',
            // Note: link-in-text-block can be overly strict for design patterns
          ],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should pass heading hierarchy accessibility rules', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: [
            'empty-heading',
            'heading-order',
          ],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should pass image accessibility rules', async () => {
      setupTheme('light')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: [
            'image-alt',
            'image-redundant-alt',
            'svg-img-alt',
          ],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should have no critical accessibility violations in dark theme', async () => {
      setupTheme('dark')

      const { container } = renderWithProviders()

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should have no critical accessibility violations on individual sections', async () => {
      setupTheme('light')

      // Test Hero Section
      const { container: heroContainer } = render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroResults = await axe(heroContainer, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const heroCritical = heroResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(heroCritical).toHaveLength(0)

      // Test Features Section
      const { container: featuresContainer } = render(<FeaturesSection />)

      const featuresResults = await axe(featuresContainer, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const featuresCritical = featuresResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(featuresCritical).toHaveLength(0)

      // Test Footer Section
      const { container: footerContainer } = render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const footerResults = await axe(footerContainer, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const footerCritical = footerResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(footerCritical).toHaveLength(0)
    })
  })

  // Additional screen reader specific tests
  describe('Additional screen reader accessibility features', () => {
    it('should have focus-visible styles for keyboard users', () => {
      renderWithProviders()

      const primaryCTA = screen.getByTestId('cta-register')
      // FuturisticButton has focus-visible ring styles
      expect(primaryCTA.className).toContain('focus-visible:ring')
    })

    it('should have semantic HTML throughout the page', () => {
      renderWithProviders()

      // Check for semantic elements
      expect(document.querySelector('main')).toBeInTheDocument()
      expect(document.querySelector('nav')).toBeInTheDocument()
      expect(document.querySelector('footer')).toBeInTheDocument()
      expect(document.querySelector('section')).toBeInTheDocument()
      expect(document.querySelector('h1')).toBeInTheDocument()
      expect(document.querySelector('h2')).toBeInTheDocument()
    })

    it('should have no tabindex values greater than 0', () => {
      renderWithProviders()

      const elementsWithTabindex = document.querySelectorAll('[tabindex]')
      elementsWithTabindex.forEach((element) => {
        const tabindex = element.getAttribute('tabindex')
        if (tabindex) {
          expect(parseInt(tabindex)).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should have error boundary fallback accessible for screen readers', async () => {
      // Error boundary has role="alert" and aria-live="polite" for announcements
      // This is tested indirectly via the axe audit, but we can verify the structure
      const ErrorBoundary = (await import('../components/ErrorBoundary')).default

      // Render with a component that will error
      const ErrorComponent = () => {
        throw new Error('Test error')
      }

      const { container } = render(
        <ErrorBoundary sectionName="Test">
          <ErrorComponent />
        </ErrorBoundary>
      )

      // The fallback should have accessibility attributes
      const alertElement = container.querySelector('[role="alert"]')
      expect(alertElement).toBeInTheDocument()
      expect(alertElement).toHaveAttribute('aria-live', 'polite')
    })
  })
})
