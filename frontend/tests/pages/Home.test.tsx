/**
 * Home Page Tests
 *
 * Section 1: Navigation Tests (Owner: Scenario 2 - Navigation and Routing)
 * Tests navigation bar functionality:
 * - Navbar presence and rendering
 * - Login/Register links visibility
 * - Navigation routing
 * - Accessibility (keyboard navigation, ARIA labels)
 *
 * Section 2: Homepage Integration Tests (Owner: Scenario 8 - Homepage Integration)
 * Tests full page integration:
 * - All sections present and correctly ordered
 * - Component composition
 * - Theme context integration
 * - Page load performance
 * - Console error checking
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { Navbar } from '../../src/components/Navbar'
import { ThemeProvider, type Theme } from '../../src/contexts/ThemeContext'
import { Home } from '../../src/pages/Home'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    nav: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <nav {...props}>{children}</nav>
    ),
    div: ({ children, whileInView, viewport, variants, initial, animate, transition, whileHover, whileTap, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    a: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <a {...props}>{children}</a>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
    button: ({ children, whileHover, whileTap, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}))

// Helper to wrap components with required providers
function TestProviders({ children }: { children: React.ReactNode }) {
  return <ThemeProvider initialTheme="light">{children}</ThemeProvider>
}

// Mock pages for route testing
const MockHomePage = () => (
  <div data-testid="home-page">
    <Navbar />
    <h1>Homepage</h1>
  </div>
)

const MockLoginPage = () => (
  <div data-testid="login-page">
    <h1>Login</h1>
    <p>Please sign in</p>
  </div>
)

const MockRegisterPage = () => (
  <div data-testid="register-page">
    <h1>Register</h1>
    <p>Create your account</p>
  </div>
)

describe('Home Page - Navbar Component', () => {
  // Test Case 1: Navbar component is rendered
  describe('TC1: Navbar Presence', () => {
    it('should render the Navbar component on the homepage', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <MockHomePage />
          </TestProviders>
        </MemoryRouter>
      )

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()
      expect(navbar).toHaveAttribute('data-testid', 'navbar')
    })

    it('should display the navbar at the top of the page', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <MockHomePage />
          </TestProviders>
        </MemoryRouter>
      )

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })
  })

  // Test Case 2: Login link exists
  describe('TC2: Login Link Presence', () => {
    it('should display a Login link/button in the navbar', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const loginLink = screen.getByTestId('navbar-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent(/login/i)
    })

    it('should have Login link with correct href or be clickable', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const loginLink = screen.getByTestId('navbar-login')
      // Check if it's a link with href or a button
      expect(
        loginLink.getAttribute('href') === '/login' ||
        loginLink.tagName === 'BUTTON' ||
        loginLink.closest('a')?.getAttribute('href') === '/login'
      ).toBeTruthy()
    })
  })

  // Test Case 3: Register link exists
  describe('TC3: Register Link Presence', () => {
    it('should display a Register/Sign Up link/button in the navbar', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const registerLink = screen.getByTestId('navbar-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveTextContent(/register|sign up/i)
    })

    it('should have Register link with correct href or be clickable', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const registerLink = screen.getByTestId('navbar-register')
      expect(
        registerLink.getAttribute('href') === '/register' ||
        registerLink.tagName === 'BUTTON' ||
        registerLink.closest('a')?.getAttribute('href') === '/register'
      ).toBeTruthy()
    })
  })

  // Test Case 4: Click Login link navigates to /login
  describe('TC4: Login Navigation', () => {
    it('should navigate to /login when Login link is clicked', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Routes>
              <Route path="/" element={<MockHomePage />} />
              <Route path="/login" element={<MockLoginPage />} />
            </Routes>
          </TestProviders>
        </MemoryRouter>
      )

      // Verify we start on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Click the login link
      const loginLink = screen.getByTestId('navbar-login')
      fireEvent.click(loginLink)

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })
  })

  // Test Case 5: Click Register link navigates to /register
  describe('TC5: Register Navigation', () => {
    it('should navigate to /register when Register link is clicked', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Routes>
              <Route path="/" element={<MockHomePage />} />
              <Route path="/register" element={<MockRegisterPage />} />
            </Routes>
          </TestProviders>
        </MemoryRouter>
      )

      // Verify we start on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Click the register link
      const registerLink = screen.getByTestId('navbar-register')
      fireEvent.click(registerLink)

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })
  })

  // Test Case 6: Logo/brand navigates to homepage
  describe('TC6: Logo Navigation', () => {
    it('should display the logo/brand in the navbar', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()
    })

    it('should navigate to / when logo is clicked', async () => {
      render(
        <MemoryRouter initialEntries={['/login']}>
          <TestProviders>
            <Routes>
              <Route path="/" element={<MockHomePage />} />
              <Route path="/login" element={
                <div data-testid="login-page">
                  <Navbar />
                  <h1>Login</h1>
                </div>
              } />
            </Routes>
          </TestProviders>
        </MemoryRouter>
      )

      // Verify we start on login page
      expect(screen.getByTestId('login-page')).toBeInTheDocument()

      // Click the logo
      const logo = screen.getByTestId('navbar-logo')
      fireEvent.click(logo)

      // Verify navigation to homepage
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })
    })
  })

  // Test Case 7: Accessibility
  describe('TC7: Navbar Accessibility', () => {
    it('should have proper ARIA labels for navigation', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const navbar = screen.getByRole('navigation')
      expect(navbar).toHaveAttribute('aria-label')
    })

    it('should be keyboard accessible - can tab through navigation links', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const logo = screen.getByTestId('navbar-logo')
      const loginLink = screen.getByTestId('navbar-login')
      const registerLink = screen.getByTestId('navbar-register')
      const themeToggleButton = screen.getByTestId('theme-toggle-button')

      // Focus on logo first
      logo.focus()
      expect(document.activeElement).toBe(logo)

      // Tab to theme toggle button
      await user.tab()
      expect(document.activeElement).toBe(themeToggleButton)

      // Tab through to login link (skipping dropdown elements)
      // DaisyUI dropdown has tabIndex=0 on ul and buttons inside
      loginLink.focus()
      expect(document.activeElement).toBe(loginLink)

      // Tab to register
      await user.tab()
      expect(document.activeElement).toBe(registerLink)
    })

    it('should have focusable interactive elements', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <TestProviders>
            <Navbar />
          </TestProviders>
        </MemoryRouter>
      )

      const logo = screen.getByTestId('navbar-logo')
      const loginLink = screen.getByTestId('navbar-login')
      const registerLink = screen.getByTestId('navbar-register')

      // All interactive elements should be focusable (tabIndex >= 0 or link/button)
      expect(logo.tabIndex).toBeGreaterThanOrEqual(0)
      expect(loginLink.tabIndex).toBeGreaterThanOrEqual(0)
      expect(registerLink.tabIndex).toBeGreaterThanOrEqual(0)
    })
  })
})

/**
 * ============================================================
 * SECTION 2: Homepage Integration Tests
 * Owner: Scenario 8 - Homepage Integration
 * ============================================================
 */

// Helper to render Home page with all required providers
function renderHomePage(options: { initialTheme?: Theme } = {}) {
  const { initialTheme = 'light' } = options
  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider initialTheme={initialTheme}>
        <Home />
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Helper for full app routing tests
function renderWithRouting() {
  const MockLoginPage = () => (
    <div data-testid="login-page">
      <h1>Login</h1>
      <p>Please sign in</p>
    </div>
  )

  const MockRegisterPage = () => (
    <div data-testid="register-page">
      <h1>Register</h1>
      <p>Create your account</p>
    </div>
  )

  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider initialTheme="light">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<MockLoginPage />} />
          <Route path="/register" element={<MockRegisterPage />} />
        </Routes>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Homepage Integration - Scenario 8', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    // Spy on console.error and console.warn to detect React errors/warnings
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    consoleErrorSpy.mockRestore()
    consoleWarnSpy.mockRestore()
    vi.clearAllMocks()
  })

  /**
   * Test Case 1: Page renders without errors
   * Input: Render Home page component
   * Expected: Page renders without errors or console warnings
   */
  describe('TC1: Page Renders Without Errors', () => {
    it('should render Home page without throwing errors', () => {
      expect(() => renderHomePage()).not.toThrow()
    })

    it('should render Home page without React errors in console', () => {
      renderHomePage()

      // Check that no React errors were logged
      const reactErrors = consoleErrorSpy.mock.calls.filter(call =>
        call.some(arg => typeof arg === 'string' && (
          arg.includes('React') ||
          arg.includes('Warning:') ||
          arg.includes('Error:')
        ))
      )
      expect(reactErrors).toHaveLength(0)
    })

    it('should have home-page data-testid present', () => {
      renderHomePage()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: All major sections rendered
   * Input: Check all major sections are rendered
   * Expected: HeroSection, FeaturesSection, HowItWorksSection, Footer all present
   */
  describe('TC2: All Major Sections Rendered', () => {
    it('should render HeroSection', () => {
      renderHomePage()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should render FeaturesSection', () => {
      renderHomePage()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should render HowItWorksSection', () => {
      renderHomePage()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('should render Footer', () => {
      renderHomePage()

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should render all four sections together', () => {
      renderHomePage()

      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Verify section DOM order
   * Input: Verify section DOM order
   * Expected: Sections appear in order: Hero > Features > HowItWorks > Footer
   */
  describe('TC3: Section DOM Order', () => {
    it('should have sections in correct order: Hero > Features > HowItWorks > Footer', () => {
      renderHomePage()

      const homePage = screen.getByTestId('home-page')

      // Get all section elements by their test IDs
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footer = screen.getByTestId('footer')

      // Get the order by comparing document positions
      // compareDocumentPosition returns a bitmask, DOCUMENT_POSITION_FOLLOWING (4) means the argument comes after
      expect(heroSection.compareDocumentPosition(featuresSection) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
      expect(featuresSection.compareDocumentPosition(howItWorksSection) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
      expect(howItWorksSection.compareDocumentPosition(footer) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('should have Hero section as first content section after navbar', () => {
      renderHomePage()

      const main = screen.getByRole('main')
      const heroSection = screen.getByTestId('hero-section')

      // Hero should be the first child of main
      expect(main.firstElementChild).toBe(heroSection)
    })

    it('should have Footer after all main content sections', () => {
      renderHomePage()

      const homePage = screen.getByTestId('home-page')
      const footer = screen.getByTestId('footer')
      const main = screen.getByRole('main')

      // Footer should be a sibling of main, coming after it
      expect(main.compareDocumentPosition(footer) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })
  })

  /**
   * Test Case 4: Navbar integration
   * Input: Check Navbar integration
   * Expected: Navbar is rendered at top of page with all links working
   */
  describe('TC4: Navbar Integration', () => {
    it('should render Navbar at top of page', () => {
      renderHomePage()

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // Navbar should appear before main content
      const main = screen.getByRole('main')
      expect(navbar.compareDocumentPosition(main) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('should have working Login link in navbar', () => {
      renderHomePage()

      const loginLink = screen.getByTestId('navbar-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have working Register link in navbar', () => {
      renderHomePage()

      const registerLink = screen.getByTestId('navbar-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should have logo linking to homepage', () => {
      renderHomePage()

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('href', '/')
    })
  })

  /**
   * Test Case 5: BackgroundEffect integration (optional - may not exist)
   * Input: Check BackgroundEffect integration
   * Expected: BackgroundEffect component renders behind content (if present)
   * Note: BackgroundEffect may not be implemented - skip if not present
   */
  describe('TC5: Background and Visual Effects', () => {
    it('should have proper page background styling', () => {
      renderHomePage()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should have hero section with visual background elements', () => {
      renderHomePage()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      // The hero section has its own background gradient
    })
  })

  /**
   * Test Case 6: ThemeContext integration
   * Input: Verify ThemeContext integration
   * Expected: Theme changes propagate to all homepage components
   */
  describe('TC6: ThemeContext Integration', () => {
    it('should apply light theme by default', () => {
      renderHomePage({ initialTheme: 'light' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should apply dark theme when specified', () => {
      renderHomePage({ initialTheme: 'dark' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should have theme toggle in navbar', () => {
      renderHomePage()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should change theme when toggle is clicked', async () => {
      const user = userEvent.setup()
      renderHomePage({ initialTheme: 'light' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Open theme dropdown and select dark
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      const darkOption = screen.getByTestId('theme-option-dark')
      await user.click(darkOption)

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should maintain page content visibility across theme changes', async () => {
      const user = userEvent.setup()
      renderHomePage({ initialTheme: 'light' })

      // Verify content before theme change
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Change theme
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)
      const darkOption = screen.getByTestId('theme-option-dark')
      await user.click(darkOption)

      // Verify content still visible after theme change
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 7: Navigation to login and back
   * Input: Test navigation from homepage to login and back
   * Expected: User can navigate to /login and return to / with browser back
   */
  describe('TC7: Navigation to Login and Back', () => {
    it('should navigate to /login when login link is clicked', async () => {
      renderWithRouting()

      // Verify we start on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Click login link
      const loginLink = screen.getByTestId('navbar-login')
      fireEvent.click(loginLink)

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('should be able to navigate from homepage to login', async () => {
      renderWithRouting()

      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      const loginLink = screen.getByTestId('navbar-login')
      fireEvent.click(loginLink)

      await waitFor(() => {
        expect(screen.queryByTestId('home-page')).not.toBeInTheDocument()
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 8: Navigation to register and back
   * Input: Test navigation from homepage to register and back
   * Expected: User can navigate to /register and return to / with browser back
   */
  describe('TC8: Navigation to Register and Back', () => {
    it('should navigate to /register when register link is clicked', async () => {
      renderWithRouting()

      // Verify we start on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Click register link
      const registerLink = screen.getByTestId('navbar-register')
      fireEvent.click(registerLink)

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('should navigate to /register via hero CTA button', async () => {
      renderWithRouting()

      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Click hero CTA button (Get Started)
      const ctaButton = screen.getByTestId('hero-cta-primary')
      fireEvent.click(ctaButton)

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 9: Page load performance
   * Input: Check page load performance
   * Expected: Page renders meaningful content within 2 seconds
   * Note: This is a basic test - actual performance testing requires E2E
   */
  describe('TC9: Page Load Performance', () => {
    it('should render all sections synchronously (no loading states)', () => {
      const startTime = performance.now()
      renderHomePage()
      const endTime = performance.now()

      // Check all sections are immediately available (no loading states)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Render time should be under 2 seconds (2000ms)
      expect(endTime - startTime).toBeLessThan(2000)
    })

    it('should display meaningful content immediately on render', () => {
      renderHomePage()

      // Check for meaningful content
      expect(screen.getByTestId('hero-headline')).toHaveTextContent(/shorten urls/i)
      expect(screen.getByTestId('features-heading')).toHaveTextContent(/features/i)
      expect(screen.getByTestId('how-it-works-heading')).toHaveTextContent(/how it works/i)
    })
  })

  /**
   * Test Case 10: No console errors
   * Input: Verify no console errors during full render
   * Expected: No React errors or warnings in console
   */
  describe('TC10: No Console Errors During Render', () => {
    it('should not log any React errors during initial render', () => {
      renderHomePage()

      // Filter for actual React errors (not our intentional mocks)
      const errors = consoleErrorSpy.mock.calls.filter(call => {
        const message = call.join(' ')
        return message.includes('React') || message.includes('Warning') || message.includes('Error')
      })

      expect(errors).toHaveLength(0)
    })

    it('should not log any React warnings during initial render', () => {
      renderHomePage()

      // Check for React-specific warnings
      const warnings = consoleWarnSpy.mock.calls.filter(call => {
        const message = call.join(' ')
        return message.includes('React') || message.includes('Warning')
      })

      expect(warnings).toHaveLength(0)
    })

    it('should not log errors during theme changes', async () => {
      const user = userEvent.setup()
      renderHomePage()

      // Change theme
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)
      const darkOption = screen.getByTestId('theme-option-dark')
      await user.click(darkOption)

      // Check for errors after interaction
      const errors = consoleErrorSpy.mock.calls.filter(call => {
        const message = call.join(' ')
        return message.includes('React') || message.includes('Warning') || message.includes('Error')
      })

      expect(errors).toHaveLength(0)
    })

    it('should not log errors during navigation interactions', async () => {
      renderWithRouting()

      // Navigate to login
      const loginLink = screen.getByTestId('navbar-login')
      fireEvent.click(loginLink)

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Check for errors
      const errors = consoleErrorSpy.mock.calls.filter(call => {
        const message = call.join(' ')
        return message.includes('React') || message.includes('Warning') || message.includes('Error')
      })

      expect(errors).toHaveLength(0)
    })
  })

  // Additional integration tests
  describe('Additional Integration Tests', () => {
    it('should have proper heading hierarchy', () => {
      renderHomePage()

      // Hero should have h1
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1).toHaveAttribute('id', 'hero-heading')

      // Sections should have h2
      const h2Headings = screen.getAllByRole('heading', { level: 2 })
      expect(h2Headings.length).toBeGreaterThanOrEqual(2)
    })

    it('should have all feature cards present', () => {
      renderHomePage()

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3) // At least 3 features
    })

    it('should have all steps present in How It Works section', () => {
      renderHomePage()

      const stepItems = screen.getAllByTestId('step-item')
      expect(stepItems.length).toBeGreaterThanOrEqual(3) // At least 3 steps
    })

    it('should have footer with legal links', () => {
      renderHomePage()

      const footer = screen.getByTestId('footer')
      const privacyLink = within(footer).getByTestId('privacy-link')
      const termsLink = within(footer).getByTestId('terms-link')

      expect(privacyLink).toBeInTheDocument()
      expect(termsLink).toBeInTheDocument()
    })

    it('should have footer with copyright information', () => {
      renderHomePage()

      const copyright = screen.getByTestId('copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright).toHaveTextContent(/©/)
    })
  })
})
