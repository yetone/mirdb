/**
 * Integration tests for homepage navigation.
 * Owner: Scenario 4 - Navigation Links to Login and Register
 *
 * Tests navigation from homepage to Login and Register pages,
 * verifying that navigation links are present for unauthenticated users
 * and route correctly when clicked.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import React from 'react'
import Navbar from '@/components/Navbar'
import { HeroSection } from '@/components/homepage'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'

/**
 * Custom render helper for navigation tests using MemoryRouter
 * Allows tracking of current route location
 */
const renderWithRouter = (
  ui: React.ReactElement,
  { initialEntries = ['/'] } = {}
) => {
  let currentLocation: string | null = null

  const LocationDisplay = () => {
    const location = require('react-router-dom').useLocation()
    currentLocation = location.pathname
    return <div data-testid="location-display">{location.pathname}</div>
  }

  const result = render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>
          {ui}
          <Routes>
            <Route path="*" element={<LocationDisplay />} />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )

  return {
    ...result,
    getCurrentLocation: () => currentLocation,
  }
}

describe('Homepage Navigation - Login and Register Links', () => {
  beforeEach(() => {
    // Clear any stored auth tokens
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Navigation contains Login link for unauthenticated users', () => {
    it('should render a Login link pointing to /login', () => {
      renderWithRouter(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have Login link visible in the navbar', () => {
      renderWithRouter(<Navbar />)

      const navbar = screen.getByRole('navigation')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })

      expect(loginLink).toBeVisible()
    })
  })

  describe('Test Case 2: Navigation contains Register/Sign Up link for unauthenticated users', () => {
    it('should render a Sign Up link pointing to /register', () => {
      renderWithRouter(<Navbar />)

      const signUpLink = screen.getByRole('link', { name: /sign up/i })
      expect(signUpLink).toBeInTheDocument()
      expect(signUpLink).toHaveAttribute('href', '/register')
    })

    it('should have Sign Up link visible in the navbar', () => {
      renderWithRouter(<Navbar />)

      const navbar = screen.getByRole('navigation')
      const signUpLink = within(navbar).getByRole('link', { name: /sign up/i })

      expect(signUpLink).toBeVisible()
    })

    it('should style Sign Up link as primary button', () => {
      renderWithRouter(<Navbar />)

      const signUpLink = screen.getByRole('link', { name: /sign up/i })
      expect(signUpLink).toHaveClass('btn-primary')
    })
  })

  describe('Test Case 3: Click Login link navigates to /login route', () => {
    it('should navigate to /login when Login link is clicked', async () => {
      const user = userEvent.setup()
      const { getCurrentLocation } = renderWithRouter(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      await user.click(loginLink)

      expect(getCurrentLocation()).toBe('/login')
    })

    it('should update location display when Login is clicked', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      await user.click(loginLink)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/login')
    })
  })

  describe('Test Case 4: Click Register/Sign Up link navigates to /register route', () => {
    it('should navigate to /register when Sign Up link is clicked', async () => {
      const user = userEvent.setup()
      const { getCurrentLocation } = renderWithRouter(<Navbar />)

      const signUpLink = screen.getByRole('link', { name: /sign up/i })
      await user.click(signUpLink)

      expect(getCurrentLocation()).toBe('/register')
    })

    it('should update location display when Sign Up is clicked', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navbar />)

      const signUpLink = screen.getByRole('link', { name: /sign up/i })
      await user.click(signUpLink)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/register')
    })
  })

  describe('Test Case 5: Click Sign Up CTA button in hero section navigates to /register', () => {
    it('should navigate to /register when Get Started button is clicked', async () => {
      const user = userEvent.setup()
      const mockOnUrlSubmit = vi.fn()

      const { getCurrentLocation } = renderWithRouter(
        <HeroSection onUrlSubmit={mockOnUrlSubmit} isAuthenticated={false} />
      )

      // Find the Get Started button which links to /register
      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      await user.click(getStartedButton)

      expect(getCurrentLocation()).toBe('/register')
    })

    it('should have Get Started CTA visible for unauthenticated users', () => {
      const mockOnUrlSubmit = vi.fn()

      renderWithRouter(
        <HeroSection onUrlSubmit={mockOnUrlSubmit} isAuthenticated={false} />
      )

      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toBeVisible()
      expect(getStartedLink).toHaveAttribute('href', '/register')
    })

    it('should not show Get Started CTA for authenticated users', () => {
      const mockOnUrlSubmit = vi.fn()

      renderWithRouter(
        <HeroSection onUrlSubmit={mockOnUrlSubmit} isAuthenticated={true} />
      )

      const getStartedLink = screen.queryByRole('link', { name: /get started/i })
      expect(getStartedLink).not.toBeInTheDocument()
    })
  })

  describe('Navigation accessibility', () => {
    it('should have accessible Login link with proper role', () => {
      renderWithRouter(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
    })

    it('should have accessible Sign Up link with proper role', () => {
      renderWithRouter(<Navbar />)

      const signUpLink = screen.getByRole('link', { name: /sign up/i })
      expect(signUpLink).toBeInTheDocument()
    })

    it('should have navigation landmark', () => {
      renderWithRouter(<Navbar />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })
  })

  describe('Navigation with full app structure', () => {
    it('should maintain navigation links alongside home page content', () => {
      renderWithRouter(
        <>
          <Navbar />
          <HeroSection onUrlSubmit={vi.fn()} isAuthenticated={false} />
        </>
      )

      // Navbar links
      const loginLink = screen.getByRole('link', { name: /login/i })
      const navSignUpLink = within(screen.getByRole('navigation')).getByRole('link', { name: /sign up/i })

      // Hero section CTA
      const getStartedLink = screen.getByRole('link', { name: /get started/i })

      expect(loginLink).toBeInTheDocument()
      expect(navSignUpLink).toBeInTheDocument()
      expect(getStartedLink).toBeInTheDocument()
    })
  })
})
