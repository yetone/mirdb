import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FooterCTA from './FooterCTA'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock framer-motion to avoid animation-related issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
    nav: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <nav {...props}>{children}</nav>
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

describe('FooterCTA', () => {
  describe('Component Rendering', () => {
    it('renders without errors', () => {
      renderWithProviders(<FooterCTA />)
      expect(screen.getByTestId('footer-cta')).toBeInTheDocument()
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<FooterCTA data-testid="custom-footer" />)
      expect(screen.getByTestId('custom-footer')).toBeInTheDocument()
    })

    it('renders headline text', () => {
      renderWithProviders(<FooterCTA />)
      const headline = screen.getByTestId('footer-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Ready to Get Started?')
    })

    it('renders subheadline text', () => {
      renderWithProviders(<FooterCTA />)
      const subheadline = screen.getByTestId('footer-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toContain('Join thousands of users')
    })

    it('renders copyright text', () => {
      renderWithProviders(<FooterCTA />)
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright.textContent).toContain('URL Shortener')
    })

    it('renders footer with navigation, legal links, and copyright (Test Case 1)', () => {
      renderWithProviders(<FooterCTA />)
      // Footer renders
      expect(screen.getByTestId('footer-cta')).toBeInTheDocument()
      // Navigation links section exists
      expect(screen.getByTestId('footer-navigation')).toBeInTheDocument()
      // Legal links section exists
      expect(screen.getByTestId('footer-legal')).toBeInTheDocument()
      // Copyright exists
      expect(screen.getByTestId('footer-copyright')).toBeInTheDocument()
    })
  })

  describe('Navigation Links (Test Case 2)', () => {
    it('footer contains link to Home', () => {
      renderWithProviders(<FooterCTA />)
      const homeLink = screen.getByTestId('footer-nav-home')
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
      expect(homeLink).toHaveTextContent('Home')
    })

    it('footer contains link to Login', () => {
      renderWithProviders(<FooterCTA />)
      const loginLink = screen.getByTestId('footer-nav-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
      expect(loginLink).toHaveTextContent('Login')
    })

    it('footer contains link to Register', () => {
      renderWithProviders(<FooterCTA />)
      const registerLink = screen.getByTestId('footer-nav-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
      expect(registerLink).toHaveTextContent('Register')
    })

    it('footer contains link to Dashboard', () => {
      renderWithProviders(<FooterCTA />)
      const dashboardLink = screen.getByTestId('footer-nav-dashboard')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
      expect(dashboardLink).toHaveTextContent('Dashboard')
    })

    it('all navigation links are within the navigation section', () => {
      renderWithProviders(<FooterCTA />)
      const navSection = screen.getByTestId('footer-navigation')
      expect(navSection).toContainElement(screen.getByTestId('footer-nav-home'))
      expect(navSection).toContainElement(screen.getByTestId('footer-nav-login'))
      expect(navSection).toContainElement(screen.getByTestId('footer-nav-register'))
      expect(navSection).toContainElement(screen.getByTestId('footer-nav-dashboard'))
    })
  })

  describe('Legal Links (Test Case 3)', () => {
    it('footer contains Terms of Service link', () => {
      renderWithProviders(<FooterCTA />)
      const termsLink = screen.getByTestId('footer-legal-terms')
      expect(termsLink).toBeInTheDocument()
      expect(termsLink).toHaveAttribute('href', '/terms')
      expect(termsLink).toHaveTextContent('Terms of Service')
    })

    it('footer contains Privacy Policy link', () => {
      renderWithProviders(<FooterCTA />)
      const privacyLink = screen.getByTestId('footer-legal-privacy')
      expect(privacyLink).toBeInTheDocument()
      expect(privacyLink).toHaveAttribute('href', '/privacy')
      expect(privacyLink).toHaveTextContent('Privacy Policy')
    })

    it('all legal links are within the legal section', () => {
      renderWithProviders(<FooterCTA />)
      const legalSection = screen.getByTestId('footer-legal')
      expect(legalSection).toContainElement(screen.getByTestId('footer-legal-terms'))
      expect(legalSection).toContainElement(screen.getByTestId('footer-legal-privacy'))
    })
  })

  describe('CTA Button', () => {
    it('renders Get Started Free button', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent('Get Started Free')
    })

    it('CTA button links to /register route', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('CTA button has primary styling', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toHaveClass('bg-primary')
    })

    it('CTA button is a Link element for SPA navigation', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Layout and Styling', () => {
    it('footer has proper background styling', () => {
      renderWithProviders(<FooterCTA />)
      const footer = screen.getByTestId('footer-cta')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('footer uses semantic footer element', () => {
      renderWithProviders(<FooterCTA />)
      const footer = screen.getByTestId('footer-cta')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('navigation section uses semantic nav element', () => {
      renderWithProviders(<FooterCTA />)
      const navSection = screen.getByTestId('footer-navigation')
      expect(navSection.tagName.toLowerCase()).toBe('nav')
    })
  })

  describe('Accessibility', () => {
    it('has proper heading element', () => {
      renderWithProviders(<FooterCTA />)
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
    })

    it('navigation section has aria-label', () => {
      renderWithProviders(<FooterCTA />)
      const navSection = screen.getByTestId('footer-navigation')
      expect(navSection).toHaveAttribute('aria-label', 'Footer navigation')
    })

    it('all footer links are accessible', () => {
      renderWithProviders(<FooterCTA />)
      const links = screen.getAllByRole('link')
      // Should have at least: Home, Login, Register, Dashboard, Terms, Privacy, Get Started
      expect(links.length).toBeGreaterThanOrEqual(7)
    })
  })
})
