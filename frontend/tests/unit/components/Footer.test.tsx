/**
 * Footer Component Tests
 * Owner: Scenario 6 - Footer Section Display
 *
 * Test cases:
 * 1. Product name and copyright year are displayed
 * 2. Links to About, Privacy Policy, and Terms of Service are present
 * 3. Social media icon placeholders are displayed
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../test-utils'
import Footer from '../../../src/components/Footer'

describe('Footer', () => {
  describe('Test Case 1: Product name and copyright year are displayed', () => {
    it('renders the product name', () => {
      render(<Footer />)

      const productName = screen.getByTestId('footer-product-name')
      expect(productName).toBeInTheDocument()
      expect(productName).toHaveTextContent('URL Shortener')
    })

    it('renders the copyright text with current year', () => {
      render(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()

      const currentYear = new Date().getFullYear()
      expect(copyright).toHaveTextContent(`© ${currentYear}`)
      expect(copyright).toHaveTextContent('URL Shortener')
      expect(copyright).toHaveTextContent('All rights reserved')
    })

    it('footer brand section contains both product name and copyright', () => {
      render(<Footer />)

      const brand = screen.getByTestId('footer-brand')
      const brandScope = within(brand)

      expect(brandScope.getByTestId('footer-product-name')).toBeInTheDocument()
      expect(brandScope.getByTestId('footer-copyright')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Links to About, Privacy Policy, and Terms of Service are present', () => {
    it('renders all three navigation links', () => {
      render(<Footer />)

      const linksContainer = screen.getByTestId('footer-links')
      expect(linksContainer).toBeInTheDocument()

      const links = within(linksContainer).getAllByRole('link')
      expect(links).toHaveLength(3)
    })

    it('renders About link with correct href', () => {
      render(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      expect(aboutLink).toBeInTheDocument()
      expect(aboutLink).toHaveTextContent('About')
      expect(aboutLink).toHaveAttribute('href', '/about')
    })

    it('renders Privacy Policy link with correct href', () => {
      render(<Footer />)

      const privacyLink = screen.getByTestId('footer-link-privacy-policy')
      expect(privacyLink).toBeInTheDocument()
      expect(privacyLink).toHaveTextContent('Privacy Policy')
      expect(privacyLink).toHaveAttribute('href', '/privacy')
    })

    it('renders Terms of Service link with correct href', () => {
      render(<Footer />)

      const termsLink = screen.getByTestId('footer-link-terms-of-service')
      expect(termsLink).toBeInTheDocument()
      expect(termsLink).toHaveTextContent('Terms of Service')
      expect(termsLink).toHaveAttribute('href', '/terms')
    })

    it('all links have proper hover styling classes', () => {
      render(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      const privacyLink = screen.getByTestId('footer-link-privacy-policy')
      const termsLink = screen.getByTestId('footer-link-terms-of-service')

      ;[aboutLink, privacyLink, termsLink].forEach((link) => {
        expect(link).toHaveClass('link')
        expect(link).toHaveClass('link-hover')
      })
    })
  })

  describe('Test Case 3: Social media icon placeholders are displayed', () => {
    it('renders social media section', () => {
      render(<Footer />)

      const socialSection = screen.getByTestId('footer-social')
      expect(socialSection).toBeInTheDocument()
    })

    it('renders Twitter social icon link', () => {
      render(<Footer />)

      const twitterLink = screen.getByTestId('footer-social-twitter')
      expect(twitterLink).toBeInTheDocument()
      expect(twitterLink).toHaveAttribute('href', 'https://twitter.com')
      expect(twitterLink).toHaveAttribute('aria-label', 'Twitter')
      expect(twitterLink).toHaveAttribute('target', '_blank')
      expect(twitterLink).toHaveAttribute('rel', 'noopener noreferrer')

      // Check for SVG icon
      const svg = twitterLink.querySelector('svg')
      expect(svg).toBeInTheDocument()
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('renders GitHub social icon link', () => {
      render(<Footer />)

      const githubLink = screen.getByTestId('footer-social-github')
      expect(githubLink).toBeInTheDocument()
      expect(githubLink).toHaveAttribute('href', 'https://github.com')
      expect(githubLink).toHaveAttribute('aria-label', 'GitHub')
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

      // Check for SVG icon
      const svg = githubLink.querySelector('svg')
      expect(svg).toBeInTheDocument()
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('renders LinkedIn social icon link', () => {
      render(<Footer />)

      const linkedinLink = screen.getByTestId('footer-social-linkedin')
      expect(linkedinLink).toBeInTheDocument()
      expect(linkedinLink).toHaveAttribute('href', 'https://linkedin.com')
      expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn')
      expect(linkedinLink).toHaveAttribute('target', '_blank')
      expect(linkedinLink).toHaveAttribute('rel', 'noopener noreferrer')

      // Check for SVG icon
      const svg = linkedinLink.querySelector('svg')
      expect(svg).toBeInTheDocument()
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('renders three social media icons total', () => {
      render(<Footer />)

      const socialSection = screen.getByTestId('footer-social')
      const socialLinks = within(socialSection).getAllByRole('link')
      expect(socialLinks).toHaveLength(3)
    })
  })

  describe('Accessibility', () => {
    it('footer has proper semantic element', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer.tagName).toBe('FOOTER')
    })

    it('navigation section has aria-label', () => {
      render(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('social icons have aria-labels for screen readers', () => {
      render(<Footer />)

      const twitterLink = screen.getByTestId('footer-social-twitter')
      const githubLink = screen.getByTestId('footer-social-github')
      const linkedinLink = screen.getByTestId('footer-social-linkedin')

      expect(twitterLink).toHaveAttribute('aria-label', 'Twitter')
      expect(githubLink).toHaveAttribute('aria-label', 'GitHub')
      expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn')
    })

    it('social icons have aria-hidden on SVG elements', () => {
      render(<Footer />)

      const socialSection = screen.getByTestId('footer-social')
      const svgs = socialSection.querySelectorAll('svg')

      svgs.forEach((svg) => {
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Styling and Layout', () => {
    it('footer has proper DaisyUI styling classes', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('footer')
      expect(footer).toHaveClass('footer-center')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('footer links have responsive gap classes', () => {
      render(<Footer />)

      const linksContainer = screen.getByTestId('footer-links')
      expect(linksContainer).toHaveClass('flex')
      expect(linksContainer).toHaveClass('gap-4')
      expect(linksContainer).toHaveClass('md:gap-8')
    })

    it('social links are displayed in a flex container with gap', () => {
      render(<Footer />)

      const socialSection = screen.getByTestId('footer-social')
      const flexContainer = socialSection.querySelector('.flex')
      expect(flexContainer).toBeInTheDocument()
      expect(flexContainer).toHaveClass('gap-4')
    })
  })
})
