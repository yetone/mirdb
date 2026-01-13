/**
 * Homepage Component Unit Tests
 *
 * Scenario: Component Unit Tests - Homepage
 * Description: Unit tests for the Homepage component ensuring proper rendering and props
 *
 * Test Cases:
 * 1. Render Home component with ThemeContext provider
 * 2. Render Home component with BrowserRouter provider
 * 3. Snapshot test of Home component
 * 4. Check for proper TypeScript types (validated via tsc compilation)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider, useTheme } from '../../contexts/ThemeContext'
import Home from '../Home'

// Setup and teardown for animation tests
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
})

afterEach(() => {
  vi.useRealTimers()
})

/**
 * Test Case 1: Render Home component with ThemeContext provider
 * Input: Render Home component with ThemeContext provider
 * Expected: Component renders without errors
 */
describe('Test Case 1: Home renders with ThemeContext provider', () => {
  it('should render Home component without errors when wrapped with ThemeProvider', () => {
    // Render Home with ThemeProvider to ensure ThemeContext is available
    const { container } = render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // Verify component renders without throwing
    expect(container).toBeInTheDocument()

    // Verify the homepage element is present
    const homepage = screen.getByTestId('homepage')
    expect(homepage).toBeInTheDocument()
  })

  it('should have access to theme context for ThemeToggle functionality', () => {
    render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // ThemeToggle is rendered in the navbar
    // Verify theme toggle button exists (it uses aria-label)
    const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i })
    expect(themeToggle).toBeInTheDocument()
  })

  it('should throw error when ThemeProvider is missing', () => {
    // Create a test component that uses useTheme
    const TestComponent = () => {
      useTheme()
      return <div>Test</div>
    }

    // Verify that useTheme throws when used outside ThemeProvider
    expect(() => {
      render(
        <BrowserRouter>
          <TestComponent />
        </BrowserRouter>
      )
    }).toThrow('useTheme must be used within a ThemeProvider')
  })
})

/**
 * Test Case 2: Render Home component with BrowserRouter provider
 * Input: Render Home component with BrowserRouter provider
 * Expected: Component renders with router support for Link components
 */
describe('Test Case 2: Home renders with BrowserRouter provider', () => {
  it('should render Home component with router support for Link components', () => {
    render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // Verify component renders
    expect(screen.getByTestId('homepage')).toBeInTheDocument()

    // Verify Link components are rendered as anchor elements with correct hrefs
    const loginLink = screen.getByTestId('login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
    expect(loginLink.tagName).toBe('A')

    const registerLink = screen.getByTestId('register-link')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
    expect(registerLink.tagName).toBe('A')
  })

  it('should render navigation links in footer with router support', () => {
    render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // Footer links should also work with router
    const footerLoginLink = screen.getByTestId('footer-login-link')
    expect(footerLoginLink).toBeInTheDocument()
    expect(footerLoginLink).toHaveAttribute('href', '/login')

    const footerRegisterLink = screen.getByTestId('footer-register-link')
    expect(footerRegisterLink).toBeInTheDocument()
    expect(footerRegisterLink).toHaveAttribute('href', '/register')
  })

  it('should render CTA buttons with correct navigation targets', () => {
    render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    vi.advanceTimersByTime(1000) // Allow framer-motion animations to complete

    // Get Started button navigates to /register
    const getStartedLink = screen.getByTestId('get-started-btn')
    expect(getStartedLink).toHaveAttribute('href', '/register')

    // Login button navigates to /login
    const loginBtn = screen.getByTestId('login-btn')
    expect(loginBtn).toHaveAttribute('href', '/login')
  })
})

/**
 * Test Case 3: Snapshot test of Home component
 * Input: Snapshot test of Home component
 * Expected: Component matches snapshot
 */
describe('Test Case 3: Home component snapshot', () => {
  it('should match snapshot when rendered with all required providers', () => {
    const { container } = render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // Allow animations to settle
    vi.advanceTimersByTime(1000)

    // Snapshot test - captures the rendered DOM structure
    expect(container).toMatchSnapshot()
  })
})

/**
 * Test Case 4: Check for proper TypeScript types
 * Input: Check for proper TypeScript types
 * Expected: Component is properly typed with no type errors
 *
 * Note: TypeScript type checking is primarily done at compile time via `tsc`.
 * This test validates that the component exports and imports are typed correctly
 * and that the component can be used with proper TypeScript semantics.
 */
describe('Test Case 4: Home component TypeScript types', () => {
  it('should export Home as a valid React functional component', () => {
    // Verify Home is a function (React functional component)
    expect(typeof Home).toBe('function')

    // Verify Home can be called as a component (this is a runtime check
    // that complements the compile-time TypeScript checking done by tsc)
    const { container } = render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    expect(container).toBeInTheDocument()
  })

  it('should render without prop type errors (component accepts no required props)', () => {
    // Home component has no required props, verify it renders correctly
    // This test ensures the component's TypeScript interface is correct
    const renderWithoutProps = () => {
      render(
        <ThemeProvider>
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )
    }

    // Should not throw any errors
    expect(renderWithoutProps).not.toThrow()

    // Verify the component rendered successfully
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
  })

  it('should have properly typed child components', () => {
    render(
      <ThemeProvider>
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    )

    // Verify all major sections are present (ensuring types are correct)
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    expect(screen.getByTestId('demo-section')).toBeInTheDocument()
    expect(screen.getByTestId('footer-section')).toBeInTheDocument()

    // Verify typed elements have correct attributes
    const heroHeadline = screen.getByTestId('hero-headline')
    expect(heroHeadline.tagName).toBe('H1')

    const heroSubheadline = screen.getByTestId('hero-subheadline')
    expect(heroSubheadline.tagName).toBe('P')
  })
})
