/**
 * Footer Component Unit Tests
 * Owner: Scenario 5 - Navigation Header and Footer
 *
 * Test Cases:
 * - TC6: Footer renders with GitHub repository link
 * - TC7: Footer includes copyright information
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer } from '@/components/layout/Footer'
import { PRODUCT_NAME, GITHUB_URL, GITHUB_ISSUES_URL, SECTIONS } from '@/utils/constants'

describe('Footer Component', () => {
  /**
   * Test Case 6: Footer renders with GitHub repository link
   */
  describe('TC6: Footer renders with GitHub repository link', () => {
    it('should render the footer element', () => {
      render(<Footer />)
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should display GitHub Repository link in resources section', () => {
      render(<Footer />)
      const githubLinks = screen.getAllByRole('link', { name: /github/i })
      const textLink = githubLinks.find((link) =>
        link.textContent?.toLowerCase().includes('github repository')
      )
      expect(textLink).toBeInTheDocument()
      expect(textLink).toHaveAttribute('href', GITHUB_URL)
      expect(textLink).toHaveAttribute('target', '_blank')
      expect(textLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('should have GitHub icon link in footer bottom', () => {
      render(<Footer />)
      const githubIconLink = screen.getByRole('link', { name: /view on github/i })
      expect(githubIconLink).toBeInTheDocument()
      expect(githubIconLink).toHaveAttribute('href', GITHUB_URL)
      expect(githubIconLink).toHaveAttribute('target', '_blank')
    })

    it('should display Report Issues link', () => {
      render(<Footer />)
      const issuesLink = screen.getByRole('link', { name: /report issues/i })
      expect(issuesLink).toBeInTheDocument()
      expect(issuesLink).toHaveAttribute('href', GITHUB_ISSUES_URL)
      expect(issuesLink).toHaveAttribute('target', '_blank')
    })

    it('should display Documentation link', () => {
      render(<Footer />)
      const docsLink = screen.getByRole('link', { name: /documentation/i })
      expect(docsLink).toBeInTheDocument()
    })
  })

  /**
   * Test Case 7: Footer includes copyright information
   */
  describe('TC7: Footer includes copyright information', () => {
    let originalDate: typeof Date

    beforeEach(() => {
      originalDate = global.Date
    })

    afterEach(() => {
      global.Date = originalDate
    })

    it('should display copyright text', () => {
      render(<Footer />)
      const copyrightText = screen.getByText(/all rights reserved/i)
      expect(copyrightText).toBeInTheDocument()
    })

    it('should include the product name in copyright', () => {
      render(<Footer />)
      // Use more specific text pattern to match copyright line
      const copyrightText = screen.getByText(/All rights reserved/i)
      expect(copyrightText.textContent).toContain(PRODUCT_NAME)
    })

    it('should include the current year in copyright', () => {
      render(<Footer />)
      const currentYear = new Date().getFullYear()
      const copyrightText = screen.getByText(new RegExp(`© ${currentYear}`, 'i'))
      expect(copyrightText).toBeInTheDocument()
    })

    it('should display copyright symbol', () => {
      render(<Footer />)
      const footerText = screen.getByRole('contentinfo').textContent
      expect(footerText).toContain('©')
    })
  })

  /**
   * Brand information
   */
  describe('Brand information', () => {
    it('should display the product logo', () => {
      render(<Footer />)
      const logo = screen.getByAltText(`${PRODUCT_NAME} logo`)
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('src', '/logo.gif')
    })

    it('should display the product name', () => {
      render(<Footer />)
      // Multiple elements contain product name, use getAllBy
      const productNames = screen.getAllByText(PRODUCT_NAME)
      expect(productNames.length).toBeGreaterThan(0)
    })

    it('should display product description', () => {
      render(<Footer />)
      const description = screen.getByText(/persistent key-value store/i)
      expect(description).toBeInTheDocument()
    })
  })

  /**
   * Quick links section
   */
  describe('Quick links section', () => {
    it('should have Quick Links heading', () => {
      render(<Footer />)
      const heading = screen.getByText(/quick links/i)
      expect(heading).toBeInTheDocument()
    })

    it('should render all section links', () => {
      render(<Footer />)
      SECTIONS.forEach((section) => {
        const links = screen.getAllByRole('link', { name: new RegExp(section.label, 'i') })
        expect(links.length).toBeGreaterThan(0)
      })
    })
  })

  /**
   * Resources section
   */
  describe('Resources section', () => {
    it('should have Resources heading', () => {
      render(<Footer />)
      const heading = screen.getByText(/resources/i)
      expect(heading).toBeInTheDocument()
    })
  })

  /**
   * Accessibility tests
   */
  describe('Accessibility', () => {
    it('should have contentinfo role on footer', () => {
      render(<Footer />)
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should have proper aria-labels on external links', () => {
      render(<Footer />)
      const githubIconLink = screen.getByRole('link', { name: /view on github/i })
      expect(githubIconLink).toHaveAttribute('aria-label')
    })
  })

  /**
   * Custom className support
   */
  describe('Custom className support', () => {
    it('should apply custom className', () => {
      render(<Footer className="custom-footer-class" />)
      const footer = screen.getByRole('contentinfo')
      expect(footer.className).toContain('custom-footer-class')
    })
  })
})
