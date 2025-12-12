import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import {
  Footer,
  FOOTER_NAV_LINKS,
  LEGAL_LINKS,
  SOCIAL_LINKS,
  CONTACT_INFO,
} from './Footer'

// Helper to render with router context
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(<MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>)
}

describe('Footer Component', () => {
  // Test Case 1: Unit test - Footer is rendered with all required sections
  describe('TC1: Render Footer component', () => {
    it('should render the footer element', () => {
      renderWithRouter(<Footer />)
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should render the footer with data-testid', () => {
      renderWithRouter(<Footer />)
      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should render the Site Map section', () => {
      renderWithRouter(<Footer />)
      const siteMapTitle = screen.getByText('Site Map')
      expect(siteMapTitle).toBeInTheDocument()
    })

    it('should render the Legal section', () => {
      renderWithRouter(<Footer />)
      const legalTitle = screen.getByText('Legal')
      expect(legalTitle).toBeInTheDocument()
    })

    it('should render the Follow Us (social) section', () => {
      renderWithRouter(<Footer />)
      const socialTitle = screen.getByText('Follow Us')
      expect(socialTitle).toBeInTheDocument()
    })

    it('should render the Contact Us section', () => {
      renderWithRouter(<Footer />)
      const contactTitle = screen.getByText('Contact Us')
      expect(contactTitle).toBeInTheDocument()
    })

    it('should render all site map navigation links', () => {
      renderWithRouter(<Footer />)
      FOOTER_NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toBeInTheDocument()
      })
    })

    it('should render all legal links', () => {
      renderWithRouter(<Footer />)
      LEGAL_LINKS.forEach((link) => {
        const legalLink = screen.getByRole('link', { name: link.label })
        expect(legalLink).toBeInTheDocument()
      })
    })

    it('should render all social links', () => {
      renderWithRouter(<Footer />)
      SOCIAL_LINKS.forEach((link) => {
        const socialLink = screen.getByRole('link', { name: link.label })
        expect(socialLink).toBeInTheDocument()
      })
    })

    it('should render contact information', () => {
      renderWithRouter(<Footer />)
      expect(screen.getByText(CONTACT_INFO.email)).toBeInTheDocument()
      expect(screen.getByText(CONTACT_INFO.phone)).toBeInTheDocument()
      expect(screen.getByText(CONTACT_INFO.address)).toBeInTheDocument()
    })

    it('should render copyright notice', () => {
      renderWithRouter(<Footer />)
      const currentYear = new Date().getFullYear()
      const copyright = screen.getByText(new RegExp(`${currentYear}.*Homepage Enhancement`))
      expect(copyright).toBeInTheDocument()
    })
  })

  // Test Case 2: E2E-style test - Privacy policy link is present and has valid href
  describe('TC2: Privacy policy link', () => {
    it('should render privacy policy link', () => {
      renderWithRouter(<Footer />)
      const privacyLink = screen.getByRole('link', { name: 'Privacy Policy' })
      expect(privacyLink).toBeInTheDocument()
    })

    it('should have valid href for privacy policy link', () => {
      renderWithRouter(<Footer />)
      const privacyLink = screen.getByRole('link', { name: 'Privacy Policy' })
      expect(privacyLink).toHaveAttribute('href', '/privacy')
    })

    it('should have non-empty href for privacy policy link', () => {
      renderWithRouter(<Footer />)
      const privacyLink = screen.getByRole('link', { name: 'Privacy Policy' })
      const href = privacyLink.getAttribute('href')
      expect(href).toBeTruthy()
      expect(href!.length).toBeGreaterThan(0)
    })

    it('should be clickable and navigate when clicked', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const privacyLink = screen.getByRole('link', { name: 'Privacy Policy' })
      await user.click(privacyLink)
      expect(privacyLink).toHaveAttribute('href', '/privacy')
    })
  })

  // Test Case 3: E2E-style test - Terms of service link is present and has valid href
  describe('TC3: Terms of service link', () => {
    it('should render terms of service link', () => {
      renderWithRouter(<Footer />)
      const termsLink = screen.getByRole('link', { name: 'Terms of Service' })
      expect(termsLink).toBeInTheDocument()
    })

    it('should have valid href for terms of service link', () => {
      renderWithRouter(<Footer />)
      const termsLink = screen.getByRole('link', { name: 'Terms of Service' })
      expect(termsLink).toHaveAttribute('href', '/terms')
    })

    it('should have non-empty href for terms of service link', () => {
      renderWithRouter(<Footer />)
      const termsLink = screen.getByRole('link', { name: 'Terms of Service' })
      const href = termsLink.getAttribute('href')
      expect(href).toBeTruthy()
      expect(href!.length).toBeGreaterThan(0)
    })

    it('should be clickable and navigate when clicked', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const termsLink = screen.getByRole('link', { name: 'Terms of Service' })
      await user.click(termsLink)
      expect(termsLink).toHaveAttribute('href', '/terms')
    })
  })

  // Test Case 4: E2E-style test - Footer navigation links navigate correctly
  describe('TC4: Footer navigation links', () => {
    it('should navigate to Home when clicking Home link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const homeLink = screen.getByRole('link', { name: 'Home' })
      await user.click(homeLink)
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should navigate to Features when clicking Features link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const featuresLink = screen.getByRole('link', { name: 'Features' })
      await user.click(featuresLink)
      expect(featuresLink).toHaveAttribute('href', '/features')
    })

    it('should navigate to About when clicking About link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const aboutLink = screen.getByRole('link', { name: 'About' })
      await user.click(aboutLink)
      expect(aboutLink).toHaveAttribute('href', '/about')
    })

    it('should navigate to Contact when clicking Contact link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)
      const contactLink = screen.getByRole('link', { name: 'Contact' })
      await user.click(contactLink)
      expect(contactLink).toHaveAttribute('href', '/contact')
    })

    it('should have all footer nav links with valid href attributes', () => {
      renderWithRouter(<Footer />)
      FOOTER_NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toHaveAttribute('href', link.href)
      })
    })

    it('should have all footer nav links with href starting with /', () => {
      renderWithRouter(<Footer />)
      FOOTER_NAV_LINKS.forEach((link) => {
        expect(link.href).toMatch(/^\//)
      })
    })

    it('should have all legal links with href starting with /', () => {
      renderWithRouter(<Footer />)
      LEGAL_LINKS.forEach((link) => {
        expect(link.href).toMatch(/^\//)
      })
    })

    it('should have all links clickable without errors', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)

      // Test site map links
      for (const link of FOOTER_NAV_LINKS) {
        const navLink = screen.getByRole('link', { name: link.label })
        await user.click(navLink)
      }

      // Test legal links
      for (const link of LEGAL_LINKS) {
        const legalLink = screen.getByRole('link', { name: link.label })
        await user.click(legalLink)
      }
    })
  })

  // Social links tests
  describe('Social Links', () => {
    it('should have social links with external URLs', () => {
      renderWithRouter(<Footer />)
      SOCIAL_LINKS.forEach((link) => {
        const socialLink = screen.getByRole('link', { name: link.label })
        expect(socialLink).toHaveAttribute('href', link.href)
        expect(link.href).toMatch(/^https?:\/\//)
      })
    })

    it('should have social links open in new tab', () => {
      renderWithRouter(<Footer />)
      SOCIAL_LINKS.forEach((link) => {
        const socialLink = screen.getByRole('link', { name: link.label })
        expect(socialLink).toHaveAttribute('target', '_blank')
        expect(socialLink).toHaveAttribute('rel', 'noopener noreferrer')
      })
    })
  })

  // Contact info tests
  describe('Contact Information', () => {
    it('should have email link with mailto href', () => {
      renderWithRouter(<Footer />)
      const emailLink = screen.getByRole('link', { name: CONTACT_INFO.email })
      expect(emailLink).toHaveAttribute('href', `mailto:${CONTACT_INFO.email}`)
    })

    it('should have phone link with tel href', () => {
      renderWithRouter(<Footer />)
      const phoneLink = screen.getByRole('link', { name: CONTACT_INFO.phone })
      const expectedTel = `tel:${CONTACT_INFO.phone.replace(/\D/g, '')}`
      expect(phoneLink).toHaveAttribute('href', expectedTel)
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('should use semantic footer element', () => {
      renderWithRouter(<Footer />)
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it('should have navigation landmark with accessible name', () => {
      renderWithRouter(<Footer />)
      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('should have accessible names for social links', () => {
      renderWithRouter(<Footer />)
      SOCIAL_LINKS.forEach((link) => {
        const socialLink = screen.getByRole('link', { name: link.label })
        expect(socialLink).toHaveAttribute('aria-label', link.label)
      })
    })

    it('should have proper heading hierarchy', () => {
      renderWithRouter(<Footer />)
      const headings = screen.getAllByRole('heading', { level: 3 })
      expect(headings.length).toBeGreaterThanOrEqual(4)
    })

    it('should allow Tab key navigation through footer links', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Footer />)

      // Get all focusable links in the footer
      const allLinks = screen.getAllByRole('link')

      // Start tabbing through links
      for (let i = 0; i < Math.min(allLinks.length, 5); i++) {
        await user.tab()
        expect(document.activeElement).toBe(allLinks[i])
      }
    })

    it('should have all links focusable', () => {
      renderWithRouter(<Footer />)
      const allLinks = screen.getAllByRole('link')
      allLinks.forEach((link) => {
        const tabIndex = link.getAttribute('tabindex')
        // tabindex should be null (default 0) or a non-negative number
        if (tabIndex !== null) {
          expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0)
        }
      })
    })
  })

  // Responsive structure test
  describe('Footer Structure', () => {
    it('should contain four main sections', () => {
      renderWithRouter(<Footer />)
      const sectionTitles = ['Site Map', 'Legal', 'Follow Us', 'Contact Us']
      sectionTitles.forEach((title) => {
        expect(screen.getByText(title)).toBeInTheDocument()
      })
    })

    it('should render address element for contact info', () => {
      renderWithRouter(<Footer />)
      const addressElement = document.querySelector('address')
      expect(addressElement).toBeInTheDocument()
    })
  })
})
