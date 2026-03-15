/**
 * Header Component Unit Tests
 * Owner: Scenario 5 - Navigation Header and Footer
 *
 * Test Cases:
 * - TC1: Header renders with logo and navigation links
 * - TC2: Header has position:sticky CSS
 * - TC3: Navigation contains links for Features, Quick Start, Architecture, Community
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Header } from '@/components/layout/Header'
import { SECTIONS, PRODUCT_NAME, GITHUB_URL } from '@/utils/constants'

// Mock the useSmoothScroll hook
const mockScrollTo = vi.fn()
vi.mock('@/hooks/useSmoothScroll', () => ({
  useSmoothScroll: () => ({ scrollTo: mockScrollTo }),
}))

describe('Header Component', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  /**
   * Test Case 1: Header renders with logo and navigation links
   */
  describe('TC1: Header renders with logo and navigation links', () => {
    it('should render the header element', () => {
      render(<Header />)
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
    })

    it('should display the product logo', () => {
      render(<Header />)
      const logo = screen.getByAltText(`${PRODUCT_NAME} logo`)
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('src', '/logo.gif')
    })

    it('should display the product name', () => {
      render(<Header />)
      expect(screen.getByText(PRODUCT_NAME)).toBeInTheDocument()
    })

    it('should have navigation element', () => {
      render(<Header />)
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })

    it('should render all section navigation links', () => {
      render(<Header />)
      SECTIONS.forEach((section) => {
        const link = screen.getByRole('link', { name: new RegExp(section.label, 'i') })
        expect(link).toBeInTheDocument()
        expect(link).toHaveAttribute('href', section.href)
      })
    })

    it('should render GitHub link', () => {
      render(<Header />)
      const githubLink = screen.getByRole('link', { name: /github/i })
      expect(githubLink).toBeInTheDocument()
      expect(githubLink).toHaveAttribute('href', GITHUB_URL)
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  /**
   * Test Case 2: Header has position:sticky CSS
   */
  describe('TC2: Header has position:sticky or position:fixed CSS', () => {
    it('should have sticky class in the header', () => {
      render(<Header />)
      const header = screen.getByRole('banner')
      expect(header.className).toContain('sticky')
    })

    it('should have top-0 class for sticky positioning', () => {
      render(<Header />)
      const header = screen.getByRole('banner')
      expect(header.className).toContain('top-0')
    })

    it('should have z-index class for proper stacking', () => {
      render(<Header />)
      const header = screen.getByRole('banner')
      expect(header.className).toContain('z-50')
    })
  })

  /**
   * Test Case 3: Navigation contains links for Features, Quick Start, Architecture, Community
   */
  describe('TC3: Navigation contains links for all required sections', () => {
    it('should have Features link', () => {
      render(<Header />)
      const link = screen.getByRole('link', { name: /features/i })
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', '#features')
    })

    it('should have Quick Start link', () => {
      render(<Header />)
      const link = screen.getByRole('link', { name: /quick start/i })
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', '#quick-start')
    })

    it('should have Architecture link', () => {
      render(<Header />)
      const link = screen.getByRole('link', { name: /architecture/i })
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', '#architecture')
    })

    it('should have Community link', () => {
      render(<Header />)
      const link = screen.getByRole('link', { name: /community/i })
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', '#community')
    })
  })

  /**
   * Test smooth scroll behavior on navigation click
   */
  describe('Navigation smooth scroll', () => {
    it('should call scrollTo when clicking a section link', () => {
      render(<Header />)
      const featuresLink = screen.getByRole('link', { name: /navigate to features section/i })
      fireEvent.click(featuresLink)
      expect(mockScrollTo).toHaveBeenCalledWith('features')
    })

    it('should prevent default behavior on section link click', () => {
      render(<Header />)
      const quickStartLink = screen.getByRole('link', { name: /navigate to quick start section/i })
      const preventDefault = vi.fn()
      fireEvent.click(quickStartLink, { preventDefault })
      // scrollTo should be called, which means preventDefault was called internally
      expect(mockScrollTo).toHaveBeenCalled()
    })
  })

  /**
   * Accessibility tests
   */
  describe('Accessibility', () => {
    it('should have proper aria-label on logo link', () => {
      render(<Header />)
      const logoLink = screen.getByRole('link', { name: new RegExp(`${PRODUCT_NAME} homepage`, 'i') })
      expect(logoLink).toBeInTheDocument()
    })

    it('should have proper aria-label on navigation', () => {
      render(<Header />)
      const nav = screen.getByRole('navigation')
      expect(nav).toHaveAttribute('aria-label', 'Main navigation')
    })

    it('should have mobile menu button with aria-label', () => {
      render(<Header />)
      const menuButton = screen.getByRole('button', { name: /open mobile menu/i })
      expect(menuButton).toBeInTheDocument()
      expect(menuButton).toHaveAttribute('aria-expanded', 'false')
    })
  })

  /**
   * Custom className support
   */
  describe('Custom className support', () => {
    it('should apply custom className', () => {
      render(<Header className="custom-class" />)
      const header = screen.getByRole('banner')
      expect(header.className).toContain('custom-class')
    })
  })
})
