import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '../contexts/ThemeContext'
import App from '../App'
import Home from '../pages/Home'

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
  localStorage.clear()
}

// Test wrapper that provides ThemeProvider
const TestWrapper = ({ children, initialEntries = ['/'] }: { children: React.ReactNode; initialEntries?: string[] }) => (
  <MemoryRouter initialEntries={initialEntries}>
    <ThemeProvider>{children}</ThemeProvider>
  </MemoryRouter>
)

describe('React Router Integration - Homepage at "/" route', () => {
  beforeEach(() => {
    cleanupTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Navigate to '/' route - Home page component is rendered
  describe('Test Case 1: Navigate to "/" route', () => {
    it('renders the Home page component at the root "/" route', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify Home component renders - it contains HeroSection, FeaturesSection, etc.
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('displays the hero section headline', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
    })

    it('displays all main sections of the homepage', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Features section
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // How it works section
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Footer section
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })
  })

  // Test Case 2: Access '/' route without authentication - Homepage displays without redirect
  describe('Test Case 2: Access "/" route without authentication', () => {
    it('renders homepage directly without redirecting to login', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Homepage should be visible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Should NOT see login-related elements that would indicate a redirect
      expect(screen.queryByTestId('login-form')).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /sign in/i })).not.toBeInTheDocument()
    })

    it('shows "Get Started" CTA button accessible without authentication', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // The "Get Started" CTA should be present, indicating the homepage loaded
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toBeInTheDocument()
      expect(getStartedLink).toHaveAttribute('href', '/register')
    })

    it('shows "Log in" link for existing users (public route)', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // The login link should be present on the public homepage
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('does not require authentication context to render', () => {
      // Render with ThemeProvider (required by Navbar)
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Homepage renders successfully
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })
  })

  // Test Case 3: Check route configuration in App.tsx
  describe('Test Case 3: Route configuration verification', () => {
    it('Route for "/" points to Home component (renders Home content)', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify Home component content is rendered at root route
      // Home component renders HeroSection with specific structure
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify the SEO title is set (Home component behavior)
      expect(document.title).toBe('URL Shortener - Shorten, Share, Track Your Links')
    })

    it('navigating to "/" does not render login or register content', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // App.tsx has placeholder content for /login and /register
      // At "/" route, those should NOT be visible
      expect(screen.queryByText('Login Page')).not.toBeInTheDocument()
      expect(screen.queryByText('Register Page')).not.toBeInTheDocument()
    })

    it('App component includes a Route for "/" with Home element', () => {
      // This test verifies the routing structure by checking behavior
      // When we navigate to "/", the Home component's unique content appears
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Home component has specific sections that wouldn't appear if route was misconfigured
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // Verify all four sections are children of main (Home component structure)
      expect(mainElement.querySelector('[data-testid="hero-section"]')).toBeInTheDocument()
      expect(mainElement.querySelector('[data-testid="features-section"]')).toBeInTheDocument()
      expect(mainElement.querySelector('[data-testid="how-it-works-section"]')).toBeInTheDocument()
      expect(mainElement.querySelector('[data-testid="footer-section"]')).toBeInTheDocument()
    })
  })

  // Additional integration tests for routing behavior
  describe('Navigation between routes', () => {
    it('other routes (/login, /register) render different content than homepage', () => {
      const { unmount } = render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // At "/" - Homepage content visible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Unmount and remount at /login route
      unmount()

      render(
        <TestWrapper initialEntries={['/login']}>
          <App />
        </TestWrapper>
      )

      // At "/login" - Login placeholder visible, no homepage
      expect(screen.getByText('Login Page')).toBeInTheDocument()
      expect(screen.queryByTestId('hero-section')).not.toBeInTheDocument()
    })

    it('homepage route "/" is distinct from other routes', () => {
      // Render at a non-existent route to verify "/" is specifically mapped
      render(
        <TestWrapper initialEntries={['/some-random-path']}>
          <App />
        </TestWrapper>
      )

      // With current routing, unmatched routes render nothing
      expect(screen.queryByTestId('hero-section')).not.toBeInTheDocument()
      expect(screen.queryByText('Login Page')).not.toBeInTheDocument()
    })
  })
})
