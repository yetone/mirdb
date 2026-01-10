import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../contexts/ThemeContext'
import App from '../App'

// Test wrapper that provides ThemeProvider
const TestWrapper = ({ children, initialEntries = ['/'] }: { children: React.ReactNode; initialEntries?: string[] }) => (
  <MemoryRouter initialEntries={initialEntries}>
    <ThemeProvider>{children}</ThemeProvider>
  </MemoryRouter>
)

/**
 * Navbar Integration Tests
 *
 * Verifies the homepage integrates with the existing Navbar component:
 * - Navbar component is visible at top of page
 * - Theme toggle in Navbar functions correctly
 * - Login and Register links are accessible from Navbar
 */

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
  localStorage.clear()
}

describe('Navbar Integration', () => {
  beforeEach(() => {
    cleanupTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Render homepage - Navbar component is visible at top of page
  describe('Navbar Presence', () => {
    it('should render Navbar component at top of homepage', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify Navbar is present
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // Navbar should be at the top (rendered before main content)
      const heroSection = screen.getByTestId('hero-section')
      expect(navbar.compareDocumentPosition(heroSection) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('should display brand name in Navbar', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const brandName = within(navbar).getByText(/URL Shortener/i)
      expect(brandName).toBeInTheDocument()
    })

    it('should render Navbar with navigation role', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveAttribute('role', 'navigation')
    })
  })

  // Test Case 2: Toggle theme via Navbar - Homepage sections update to reflect new theme
  describe('Theme Toggle', () => {
    it('should have a theme toggle button in Navbar', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const themeToggle = within(navbar).getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should change theme when toggle is clicked', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Initial theme should be light (default)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click theme toggle to switch to dark
      await user.click(themeToggle)

      // Theme should now be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should persist theme preference after toggle', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Click to change theme
      await user.click(themeToggle)

      // Theme should be stored in localStorage
      expect(localStorage.getItem('theme')).toBe('dark')
    })

    it('should update homepage sections when theme changes', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify homepage sections are present with initial theme
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      expect(featuresSection).toHaveClass('bg-base-200')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // Toggle theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Sections should still use semantic DaisyUI classes that adapt to theme
      expect(featuresSection).toHaveClass('bg-base-200')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // The data-theme attribute should have changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should show sun icon when in dark mode and moon icon when in light mode', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // In light mode, should show moon icon (to switch to dark)
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument()

      // Toggle to dark mode
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // In dark mode, should show sun icon (to switch to light)
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()
    })
  })

  // Test Case 3: Check Navbar navigation links - Login and Register links are accessible
  describe('Navigation Links', () => {
    it('should have Login link in Navbar', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have Register link in Navbar', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const registerLink = within(navbar).getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should navigate to login page when Login link is clicked', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      await user.click(loginLink)

      // Should show login page content
      expect(screen.getByText('Login Page')).toBeInTheDocument()
    })

    it('should navigate to register page when Register link is clicked', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const registerLink = within(navbar).getByRole('link', { name: /register/i })
      await user.click(registerLink)

      // Should show register page content
      expect(screen.getByText('Register Page')).toBeInTheDocument()
    })

    it('should have accessible touch targets for navigation links', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      const registerLink = within(navbar).getByRole('link', { name: /register/i })

      // Links should have proper styling for touch targets
      expect(loginLink).toHaveClass('btn')
      expect(registerLink).toHaveClass('btn')
    })
  })

  // Additional integration tests
  describe('Navbar Persistence Across Pages', () => {
    it('should show Navbar on login page', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Navigate to login using navbar link
      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      await user.click(loginLink)

      // Navbar should still be visible
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
    })

    it('should show Navbar on register page', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Navigate to register using navbar link
      const navbar = screen.getByTestId('navbar')
      const registerLink = within(navbar).getByRole('link', { name: /register/i })
      await user.click(registerLink)

      // Navbar should still be visible
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
    })

    it('should maintain theme across page navigation', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Toggle theme to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Navigate to login using navbar link
      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      await user.click(loginLink)

      // Theme should still be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Theme Toggle Accessibility', () => {
    it('should have accessible label for theme toggle', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')
    })

    it('should be keyboard accessible', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Focus on theme toggle
      themeToggle.focus()
      expect(document.activeElement).toBe(themeToggle)

      // Should be able to activate with Enter key
      await user.keyboard('{Enter}')

      // Theme should have changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })
})
