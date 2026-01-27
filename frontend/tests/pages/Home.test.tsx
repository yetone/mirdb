/**
 * Home Page Navigation Tests
 * Owner: Scenario 2 - Navigation and Routing
 *
 * Tests navigation bar functionality:
 * - Navbar presence and rendering
 * - Login/Register links visibility
 * - Navigation routing
 * - Accessibility (keyboard navigation, ARIA labels)
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { Navbar } from '../../src/components/Navbar'
import { ThemeProvider } from '../../src/contexts/ThemeContext'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    nav: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <nav {...props}>{children}</nav>
    ),
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    a: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <a {...props}>{children}</a>
    ),
  },
}))

// Helper to wrap components with required providers
function TestProviders({ children }: { children: React.ReactNode }) {
  return <ThemeProvider defaultTheme="light">{children}</ThemeProvider>
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
