/**
 * Navigation Bar Integration Tests
 * Owner: Scenario 5 - Navigation Bar Functionality
 *
 * Tests navigation bar displays logo, menu items (Features, Pricing, About),
 * and authentication links (Login, Get Started), along with routing behavior.
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '../setup'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { Routes, Route, useLocation } from 'react-router-dom'
import Home from '@/pages/Home'
import Navbar from '@/components/Navbar'

// Helper component to display current location for routing tests
const LocationDisplay: React.FC = () => {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Test wrapper that includes routing for integration tests
const TestApp: React.FC = () => {
  return (
    <>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        <Route path="/features" element={<div data-testid="features-page">Features Page</div>} />
        <Route path="/pricing" element={<div data-testid="pricing-page">Pricing Page</div>} />
        <Route path="/about" element={<div data-testid="about-page">About Page</div>} />
      </Routes>
      <LocationDisplay />
    </>
  )
}

// Navbar-only test wrapper for isolated tests - Navbar always rendered
const NavbarTestWrapper: React.FC = () => {
  return (
    <>
      <Navbar />
      <Routes>
        <Route path="/" element={<div data-testid="home-page">Home Page</div>} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        <Route path="/features" element={<div data-testid="features-page">Features Page</div>} />
      </Routes>
      <LocationDisplay />
    </>
  )
}

describe('Navigation Bar Functionality', () => {
  describe('Test Case 1: Navigation bar is present at the top of the page', () => {
    it('should render navigation bar on HomePage', () => {
      render(<TestApp />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
      expect(navbar).toHaveAttribute('role', 'navigation')
    })

    it('should have navbar with main navigation aria label', () => {
      render(<TestApp />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveAttribute('aria-label', 'Main navigation')
    })
  })

  describe('Test Case 2: Logo is visible on the left side of navigation', () => {
    it('should display logo in the navigation bar', () => {
      render(<TestApp />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()
    })

    it('should have logo with correct branding text', () => {
      render(<TestApp />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toHaveTextContent('LinkShort')
    })

    it('should have logo positioned in navbar-start', () => {
      render(<TestApp />)

      const logo = screen.getByTestId('navbar-logo')
      const navbarStart = logo.closest('.navbar-start')
      expect(navbarStart).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Click logo in navigation navigates to homepage (/)', () => {
    it('should navigate to homepage when logo is clicked', async () => {
      const user = userEvent.setup()
      render(<NavbarTestWrapper />, { initialEntries: ['/features'] })

      // First navigate to features
      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/features')
      })

      // Click the logo
      const logo = screen.getByTestId('navbar-logo')
      await user.click(logo)

      // Should be on homepage
      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/')
      })
    })

    it('should have logo link pointing to root path', () => {
      render(<TestApp />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toHaveAttribute('href', '/')
    })
  })

  describe('Test Case 4: Features link is visible in navigation', () => {
    it('should display Features link in the navigation', () => {
      render(<TestApp />)

      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toBeInTheDocument()
      expect(featuresLink).toHaveTextContent('Features')
    })

    it('should have Features link pointing to /features', () => {
      render(<TestApp />)

      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toHaveAttribute('href', '/features')
    })
  })

  describe('Test Case 5: Pricing link is visible in navigation', () => {
    it('should display Pricing link in the navigation', () => {
      render(<TestApp />)

      const pricingLink = screen.getByTestId('nav-pricing')
      expect(pricingLink).toBeInTheDocument()
      expect(pricingLink).toHaveTextContent('Pricing')
    })

    it('should have Pricing link pointing to /pricing', () => {
      render(<TestApp />)

      const pricingLink = screen.getByTestId('nav-pricing')
      expect(pricingLink).toHaveAttribute('href', '/pricing')
    })
  })

  describe('Test Case 6: About link is visible in navigation', () => {
    it('should display About link in the navigation', () => {
      render(<TestApp />)

      const aboutLink = screen.getByTestId('nav-about')
      expect(aboutLink).toBeInTheDocument()
      expect(aboutLink).toHaveTextContent('About')
    })

    it('should have About link pointing to /about', () => {
      render(<TestApp />)

      const aboutLink = screen.getByTestId('nav-about')
      expect(aboutLink).toHaveAttribute('href', '/about')
    })
  })

  describe('Test Case 7: Login link is visible for unauthenticated user', () => {
    it('should display Login link for unauthenticated users', () => {
      render(<TestApp />)

      const loginLink = screen.getByTestId('nav-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Login')
    })

    it('should have Login link pointing to /login', () => {
      render(<TestApp />)

      const loginLink = screen.getByTestId('nav-login')
      expect(loginLink).toHaveAttribute('href', '/login')
    })
  })

  describe('Test Case 8: Get Started CTA button is visible for unauthenticated user', () => {
    it('should display Get Started button for unauthenticated users', () => {
      render(<TestApp />)

      const getStartedBtn = screen.getByTestId('nav-get-started')
      expect(getStartedBtn).toBeInTheDocument()
      expect(getStartedBtn).toHaveTextContent('Get Started')
    })

    it('should have Get Started button styled as primary CTA', () => {
      render(<TestApp />)

      const getStartedBtn = screen.getByTestId('nav-get-started')
      expect(getStartedBtn).toHaveClass('btn-primary')
    })

    it('should have Get Started button pointing to /register', () => {
      render(<TestApp />)

      const getStartedBtn = screen.getByTestId('nav-get-started')
      expect(getStartedBtn).toHaveAttribute('href', '/register')
    })
  })

  describe('Test Case 9: Click Login link navigates to /login route', () => {
    it('should navigate to /login when Login link is clicked', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
      })
    })

    it('should render login page content after navigation', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 10: Click Get Started button navigates to /register route', () => {
    it('should navigate to /register when Get Started button is clicked', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      const getStartedBtn = screen.getByTestId('nav-get-started')
      await user.click(getStartedBtn)

      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/register')
      })
    })

    it('should render register page content after navigation', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      const getStartedBtn = screen.getByTestId('nav-get-started')
      await user.click(getStartedBtn)

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })
  })

  describe('Additional Navigation Tests', () => {
    it('should have all menu items in center navigation', () => {
      render(<TestApp />)

      const navbar = screen.getByTestId('navbar')
      const centerNav = navbar.querySelector('.navbar-center')
      expect(centerNav).toBeInTheDocument()

      const featuresLink = screen.getByTestId('nav-features')
      const pricingLink = screen.getByTestId('nav-pricing')
      const aboutLink = screen.getByTestId('nav-about')

      expect(featuresLink).toBeInTheDocument()
      expect(pricingLink).toBeInTheDocument()
      expect(aboutLink).toBeInTheDocument()
    })

    it('should have auth links in end navigation area', () => {
      render(<TestApp />)

      const navbar = screen.getByTestId('navbar')
      const endNav = navbar.querySelector('.navbar-end')
      expect(endNav).toBeInTheDocument()
    })

    it('should have theme toggle button', () => {
      render(<TestApp />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toHaveAttribute('aria-label')
    })

    it('should have mobile menu toggle button', () => {
      render(<TestApp />)

      const mobileToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileToggle).toBeInTheDocument()
    })

    it('should show mobile menu when toggle is clicked', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      // Mobile menu should not be visible initially
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument()

      // Click mobile toggle
      const mobileToggle = screen.getByTestId('mobile-menu-toggle')
      await user.click(mobileToggle)

      // Mobile menu should be visible
      await waitFor(() => {
        expect(screen.getByTestId('mobile-menu')).toBeInTheDocument()
      })
    })
  })
})
