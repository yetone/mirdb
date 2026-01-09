import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Footer from '../src/components/Footer'

// Wrapper component to provide BrowserRouter context
const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Footer', () => {
  // Test Case 1: Footer renders with copyright notice and navigation links
  describe('Test Case 1: Footer renders with copyright notice and navigation links', () => {
    it('renders the Footer component', () => {
      renderWithRouter(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('contains copyright notice', () => {
      renderWithRouter(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
    })

    it('contains navigation links container', () => {
      renderWithRouter(<Footer />)

      const linksContainer = screen.getByTestId('footer-links')
      expect(linksContainer).toBeInTheDocument()
    })
  })

  // Test Case 2: Footer displays copyright notice with current year
  describe('Test Case 2: Copyright text with current year', () => {
    it('displays copyright notice with current year', () => {
      renderWithRouter(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      const currentYear = new Date().getFullYear().toString()

      expect(copyright).toHaveTextContent(currentYear)
      expect(copyright).toHaveTextContent(/©/i)
    })

    it('copyright notice includes service/company name', () => {
      renderWithRouter(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveTextContent(/url shortener|shortener/i)
    })
  })

  // Test Case 3: About link is present and clickable
  describe('Test Case 3: About link', () => {
    it('displays About link in footer', () => {
      renderWithRouter(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      expect(aboutLink).toBeInTheDocument()
      expect(aboutLink).toHaveTextContent(/about/i)
    })

    it('About link is a valid anchor element', () => {
      renderWithRouter(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      expect(aboutLink.tagName).toBe('A')
      expect(aboutLink).toHaveAttribute('href')
    })

    it('About link has accessible name', () => {
      renderWithRouter(<Footer />)

      const aboutLink = screen.getByRole('link', { name: /about/i })
      expect(aboutLink).toBeInTheDocument()
    })
  })

  // Test Case 4: Privacy Policy link is present and clickable
  describe('Test Case 4: Privacy Policy link', () => {
    it('displays Privacy Policy link in footer', () => {
      renderWithRouter(<Footer />)

      const privacyLink = screen.getByTestId('footer-link-privacy')
      expect(privacyLink).toBeInTheDocument()
      expect(privacyLink).toHaveTextContent(/privacy/i)
    })

    it('Privacy Policy link is a valid anchor element', () => {
      renderWithRouter(<Footer />)

      const privacyLink = screen.getByTestId('footer-link-privacy')
      expect(privacyLink.tagName).toBe('A')
      expect(privacyLink).toHaveAttribute('href')
    })

    it('Privacy Policy link has accessible name', () => {
      renderWithRouter(<Footer />)

      const privacyLink = screen.getByRole('link', { name: /privacy/i })
      expect(privacyLink).toBeInTheDocument()
    })
  })

  // Test Case 5: Terms of Service link is present and clickable
  describe('Test Case 5: Terms of Service link', () => {
    it('displays Terms of Service link in footer', () => {
      renderWithRouter(<Footer />)

      const termsLink = screen.getByTestId('footer-link-terms')
      expect(termsLink).toBeInTheDocument()
      expect(termsLink).toHaveTextContent(/terms/i)
    })

    it('Terms of Service link is a valid anchor element', () => {
      renderWithRouter(<Footer />)

      const termsLink = screen.getByTestId('footer-link-terms')
      expect(termsLink.tagName).toBe('A')
      expect(termsLink).toHaveAttribute('href')
    })

    it('Terms of Service link has accessible name', () => {
      renderWithRouter(<Footer />)

      const termsLink = screen.getByRole('link', { name: /terms/i })
      expect(termsLink).toBeInTheDocument()
    })
  })

  // Additional: Accessibility tests
  describe('Accessibility', () => {
    it('footer has proper semantic role', () => {
      renderWithRouter(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('footer links are organized in a navigation element', () => {
      renderWithRouter(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer/i })
      expect(nav).toBeInTheDocument()
    })

    it('all footer links are keyboard accessible', () => {
      renderWithRouter(<Footer />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        expect(link).toHaveAttribute('href')
        // Links should be focusable (not have tabIndex=-1)
        expect(link).not.toHaveAttribute('tabindex', '-1')
      })
    })
  })

  // Additional: Verify all links render together
  describe('All footer links', () => {
    it('renders all three required links (About, Privacy, Terms)', () => {
      renderWithRouter(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      const privacyLink = screen.getByTestId('footer-link-privacy')
      const termsLink = screen.getByTestId('footer-link-terms')

      expect(aboutLink).toBeInTheDocument()
      expect(privacyLink).toBeInTheDocument()
      expect(termsLink).toBeInTheDocument()
    })

    it('footer contains at least 3 navigation links', () => {
      renderWithRouter(<Footer />)

      const linksContainer = screen.getByTestId('footer-links')
      const links = linksContainer.querySelectorAll('a')
      expect(links.length).toBeGreaterThanOrEqual(3)
    })
  })
})
