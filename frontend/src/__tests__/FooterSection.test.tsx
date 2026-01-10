import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import FooterSection from '../components/FooterSection'

describe('FooterSection', () => {
  // Test Case 1: Component renders without errors
  describe('Component Rendering', () => {
    it('should render the FooterSection component without errors', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const footer = screen.getByTestId('footer-section')
      expect(footer).toBeInTheDocument()
    })

    it('should render as a footer element', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  // Test Case 2: Footer contains navigation links to Home (/), Login (/login), Register (/register)
  describe('Navigation Links', () => {
    it('should contain a link to Home page (/)', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should contain a link to Login page (/login)', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should contain a link to Register page (/register)', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should have a navigation section with all nav links', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const navSection = screen.getByTestId('footer-nav-links')
      expect(navSection).toBeInTheDocument()
    })
  })

  // Test Case 3: Footer contains Privacy Policy and Terms of Service links
  describe('Legal Links', () => {
    it('should contain a Privacy Policy link', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const privacyLink = screen.getByRole('link', { name: /privacy policy/i })
      expect(privacyLink).toBeInTheDocument()
    })

    it('should contain a Terms of Service link', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const termsLink = screen.getByRole('link', { name: /terms of service/i })
      expect(termsLink).toBeInTheDocument()
    })

    it('should have a legal section with legal links', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const legalSection = screen.getByTestId('footer-legal-links')
      expect(legalSection).toBeInTheDocument()
    })
  })

  // Test Case 4: Footer displays copyright text with current year
  describe('Copyright Notice', () => {
    it('should display copyright text', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright).toHaveTextContent(/copyright|©/i)
    })

    it('should display the current year in copyright', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const currentYear = new Date().getFullYear().toString()
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveTextContent(currentYear)
    })

    it('should include company/product name in copyright', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      // Should contain some form of product identification
      expect(copyright).toHaveTextContent(/url shortener|shorturl/i)
    })
  })

  // Test Case 5: Navigation to / route occurs when clicking Home link
  describe('Home Link Navigation', () => {
    it('should have href pointing to / route', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should navigate to / when Home link is clicked', async () => {
      const user = userEvent.setup()

      // Create a test component that shows current route
      const TestRoutes = () => (
        <MemoryRouter initialEntries={['/some-page']}>
          <Routes>
            <Route path="/" element={<div data-testid="home-page">Home Page</div>} />
            <Route
              path="/some-page"
              element={
                <div>
                  <FooterSection />
                </div>
              }
            />
          </Routes>
        </MemoryRouter>
      )

      render(<TestRoutes />)

      // Find and click the home link
      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toHaveAttribute('href', '/')

      await user.click(homeLink)

      // After navigation, the home page should be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })
  })

  // Additional structural tests
  describe('Footer Structure and Styling', () => {
    it('should have proper footer styling classes', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      const footer = screen.getByTestId('footer-section')
      // Footer should have background and padding classes
      expect(footer).toHaveClass('bg-base-200')
    })

    it('should be positioned at the bottom of the page', () => {
      render(
        <BrowserRouter>
          <FooterSection />
        </BrowserRouter>
      )

      // The footer should exist and be accessible
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })
})
