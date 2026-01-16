import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Footer from './Footer'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock framer-motion to avoid animation-related issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
  },
}))

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('Footer', () => {
  describe('Component Rendering', () => {
    it('renders footer with navigation, legal links, and copyright', () => {
      renderWithProviders(<Footer />)

      // Check footer element exists
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Check navigation links section exists
      expect(screen.getByTestId('footer-navigation')).toBeInTheDocument()

      // Check legal links section exists
      expect(screen.getByTestId('footer-legal-links')).toBeInTheDocument()

      // Check copyright exists
      expect(screen.getByTestId('footer-copyright')).toBeInTheDocument()
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<Footer data-testid="custom-footer" />)
      expect(screen.getByTestId('custom-footer')).toBeInTheDocument()
    })

    it('uses semantic footer element', () => {
      renderWithProviders(<Footer />)
      const footer = screen.getByTestId('footer')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })
  })

  describe('Navigation Links', () => {
    it('contains link to Home', () => {
      renderWithProviders(<Footer />)
      const homeLink = screen.getByTestId('footer-nav-home')
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
      expect(homeLink).toHaveTextContent('Home')
    })

    it('contains link to Login', () => {
      renderWithProviders(<Footer />)
      const loginLink = screen.getByTestId('footer-nav-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
      expect(loginLink).toHaveTextContent('Login')
    })

    it('contains link to Register', () => {
      renderWithProviders(<Footer />)
      const registerLink = screen.getByTestId('footer-nav-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
      expect(registerLink).toHaveTextContent('Register')
    })

    it('contains link to Dashboard', () => {
      renderWithProviders(<Footer />)
      const dashboardLink = screen.getByTestId('footer-nav-dashboard')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
      expect(dashboardLink).toHaveTextContent('Dashboard')
    })

    it('renders Quick Links title', () => {
      renderWithProviders(<Footer />)
      const navTitle = screen.getByTestId('footer-nav-title')
      expect(navTitle).toBeInTheDocument()
      expect(navTitle).toHaveTextContent('Quick Links')
    })
  })

  describe('Legal Links', () => {
    it('contains Terms of Service link', () => {
      renderWithProviders(<Footer />)
      const termsLink = screen.getByTestId('footer-legal-terms-of-service')
      expect(termsLink).toBeInTheDocument()
      expect(termsLink).toHaveAttribute('href', '/terms')
      expect(termsLink).toHaveTextContent('Terms of Service')
    })

    it('contains Privacy Policy link', () => {
      renderWithProviders(<Footer />)
      const privacyLink = screen.getByTestId('footer-legal-privacy-policy')
      expect(privacyLink).toBeInTheDocument()
      expect(privacyLink).toHaveAttribute('href', '/privacy')
      expect(privacyLink).toHaveTextContent('Privacy Policy')
    })

    it('renders Legal section title', () => {
      renderWithProviders(<Footer />)
      const legalTitle = screen.getByTestId('footer-legal-title')
      expect(legalTitle).toBeInTheDocument()
      expect(legalTitle).toHaveTextContent('Legal')
    })
  })

  describe('Copyright Notice', () => {
    it('displays copyright notice', () => {
      renderWithProviders(<Footer />)
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
    })

    it('contains current year in copyright', () => {
      renderWithProviders(<Footer />)
      const copyright = screen.getByTestId('footer-copyright')
      const currentYear = new Date().getFullYear().toString()
      expect(copyright.textContent).toContain(currentYear)
    })

    it('contains URL Shortener in copyright', () => {
      renderWithProviders(<Footer />)
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.textContent).toContain('URL Shortener')
    })

    it('contains "All rights reserved" in copyright', () => {
      renderWithProviders(<Footer />)
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.textContent).toContain('All rights reserved')
    })
  })

  describe('Brand Section', () => {
    it('displays brand title', () => {
      renderWithProviders(<Footer />)
      const brandTitle = screen.getByTestId('footer-brand-title')
      expect(brandTitle).toBeInTheDocument()
      expect(brandTitle).toHaveTextContent('URL Shortener')
    })

    it('displays brand description', () => {
      renderWithProviders(<Footer />)
      const brandDescription = screen.getByTestId('footer-brand-description')
      expect(brandDescription).toBeInTheDocument()
    })
  })

  describe('Layout and Styling', () => {
    it('footer has proper background styling', () => {
      renderWithProviders(<Footer />)
      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('navigation links are wrapped in nav element', () => {
      renderWithProviders(<Footer />)
      const navElement = screen.getByTestId('footer-navigation')
      expect(navElement.tagName.toLowerCase()).toBe('nav')
    })

    it('legal links are wrapped in nav element', () => {
      renderWithProviders(<Footer />)
      const legalNav = screen.getByTestId('footer-legal-links')
      expect(legalNav.tagName.toLowerCase()).toBe('nav')
    })
  })

  describe('Accessibility', () => {
    it('navigation links are keyboard accessible (they are anchor elements)', () => {
      renderWithProviders(<Footer />)
      const homeLink = screen.getByTestId('footer-nav-home')
      expect(homeLink.tagName.toLowerCase()).toBe('a')
    })

    it('legal links are keyboard accessible (they are anchor elements)', () => {
      renderWithProviders(<Footer />)
      const termsLink = screen.getByTestId('footer-legal-terms-of-service')
      expect(termsLink.tagName.toLowerCase()).toBe('a')
    })
  })
})
