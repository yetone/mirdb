import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import FooterSection from '../FooterSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('FooterSection', () => {
  // Test Case 1: Footer element is rendered with navigation links
  describe('Test Case 1: Footer element rendering', () => {
    it('should render footer element with navigation links', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByTestId('footer-section')
      expect(footer).toBeInTheDocument()

      // Verify footer element is a semantic footer tag
      expect(footer.tagName.toLowerCase()).toBe('footer')

      // Verify navigation links are present
      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toBeInTheDocument()
    })

    it('should have footer as a semantic element', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  // Test Case 2: Links to Home (/), Login (/login), Register (/register) are present
  describe('Test Case 2: Navigation links presence', () => {
    it('should have Home link pointing to /', () => {
      renderWithRouter(<FooterSection />)

      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should have Login link pointing to /login', () => {
      renderWithRouter(<FooterSection />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have Register link pointing to /register', () => {
      renderWithRouter(<FooterSection />)

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should have all three navigation links in the footer', () => {
      renderWithRouter(<FooterSection />)

      const links = screen.getAllByRole('link')
      const hrefs = links.map((link) => link.getAttribute('href'))

      expect(hrefs).toContain('/')
      expect(hrefs).toContain('/login')
      expect(hrefs).toContain('/register')
    })
  })

  // Test Case 3: Copyright notice text is displayed
  describe('Test Case 3: Copyright notice display', () => {
    it('should display copyright notice', () => {
      renderWithRouter(<FooterSection />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()

      // Check that copyright text contains year and common copyright indicators
      const copyrightText = copyright.textContent?.toLowerCase() || ''
      expect(
        copyrightText.includes('©') ||
          copyrightText.includes('copyright') ||
          copyrightText.includes('all rights reserved')
      ).toBe(true)
    })

    it('should display current year in copyright', () => {
      renderWithRouter(<FooterSection />)

      const copyright = screen.getByTestId('footer-copyright')
      const currentYear = new Date().getFullYear().toString()

      expect(copyright.textContent).toContain(currentYear)
    })
  })

  // Test Case 4: Navigation to /login route occurs when clicking Login link
  describe('Test Case 4: Login link navigation', () => {
    it('should navigate to /login when clicking Login link', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<FooterSection />} />
            <Route
              path="/login"
              element={<div data-testid="login-page">Login Page</div>}
            />
          </Routes>
        </MemoryRouter>
      )

      const loginLink = screen.getByRole('link', { name: /login/i })
      await user.click(loginLink)

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })
  })

  // Legal links tests (Privacy, Terms)
  describe('Legal Links', () => {
    it('should have Privacy link present', () => {
      renderWithRouter(<FooterSection />)

      const privacyLink = screen.getByRole('link', { name: /privacy/i })
      expect(privacyLink).toBeInTheDocument()
    })

    it('should have Terms link present', () => {
      renderWithRouter(<FooterSection />)

      const termsLink = screen.getByRole('link', { name: /terms/i })
      expect(termsLink).toBeInTheDocument()
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('should have footer with proper role contentinfo', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should have a navigation section for footer links', () => {
      renderWithRouter(<FooterSection />)

      const nav = screen.getByRole('navigation', { name: /footer/i })
      expect(nav).toBeInTheDocument()
    })

    it('should have all links accessible by keyboard', () => {
      renderWithRouter(<FooterSection />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        // All links should be focusable (no negative tabindex)
        expect(link).not.toHaveAttribute('tabindex', '-1')
      })
    })
  })

  // Responsive design tests
  describe('Responsive Design', () => {
    it('should use flex layout for footer content', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByTestId('footer-section')
      const container = footer.querySelector('.container, [class*="max-w-"]')
      expect(container).toBeInTheDocument()
    })
  })
})
