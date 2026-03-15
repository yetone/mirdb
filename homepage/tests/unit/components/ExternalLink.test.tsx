/**
 * ExternalLink Component Unit Tests
 * Owner: Scenario 6 - GitHub Repository Integration
 *
 * Test Cases:
 * - TC1: At least one GitHub link is present on the page
 * - TC2: Link has target='_blank' attribute
 * - TC3: Link has rel='noopener noreferrer' for security
 * - TC4: Link has href pointing to GitHub repository
 * - TC5: External link icon or indicator is visible
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ExternalLink } from '@/components/ui/ExternalLink'
import { GITHUB_URL } from '@/utils/constants'

describe('ExternalLink Component', () => {
  /**
   * Test Case 1: At least one GitHub link is present on the page
   */
  describe('TC1: GitHub link presence', () => {
    it('should render a GitHub link when provided GitHub URL', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link', { name: /github repository/i })
      expect(link).toBeInTheDocument()
    })

    it('should render the link with children content', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          View Source Code
        </ExternalLink>
      )

      expect(screen.getByText('View Source Code')).toBeInTheDocument()
    })

    it('should render multiple links when used multiple times', () => {
      render(
        <div>
          <ExternalLink href={GITHUB_URL}>Link 1</ExternalLink>
          <ExternalLink href={GITHUB_URL}>Link 2</ExternalLink>
        </div>
      )

      const links = screen.getAllByRole('link')
      expect(links).toHaveLength(2)
    })
  })

  /**
   * Test Case 2: Link has target='_blank' attribute
   */
  describe('TC2: target="_blank" attribute', () => {
    it('should have target="_blank" attribute', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('target', '_blank')
    })

    it('should always have target="_blank" regardless of showIcon prop', () => {
      render(
        <ExternalLink href={GITHUB_URL} showIcon={false}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('target', '_blank')
    })
  })

  /**
   * Test Case 3: Link has rel='noopener noreferrer' for security
   */
  describe('TC3: rel="noopener noreferrer" for security', () => {
    it('should have rel="noopener noreferrer" attribute', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('should include noopener in rel attribute', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      const relValue = link.getAttribute('rel') || ''
      expect(relValue).toContain('noopener')
    })

    it('should include noreferrer in rel attribute', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      const relValue = link.getAttribute('rel') || ''
      expect(relValue).toContain('noreferrer')
    })
  })

  /**
   * Test Case 4: Link has href pointing to GitHub repository
   */
  describe('TC4: href points to correct GitHub repository', () => {
    it('should have href pointing to https://github.com/yetone/mirdb', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
    })

    it('should preserve the exact href provided', () => {
      const customUrl = 'https://github.com/yetone/mirdb/issues'
      render(
        <ExternalLink href={customUrl}>
          Report Issues
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', customUrl)
    })

    it('should work with any external URL', () => {
      const externalUrl = 'https://example.com'
      render(
        <ExternalLink href={externalUrl}>
          External Site
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', externalUrl)
    })
  })

  /**
   * Test Case 5: External link icon or indicator is visible
   */
  describe('TC5: External link indicator', () => {
    it('should display external link icon by default', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const icon = screen.getByTestId('external-link-icon')
      expect(icon).toBeInTheDocument()
    })

    it('should have aria-hidden on the icon for accessibility', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const icon = screen.getByTestId('external-link-icon')
      expect(icon).toHaveAttribute('aria-hidden', 'true')
    })

    it('should hide icon when showIcon is false', () => {
      render(
        <ExternalLink href={GITHUB_URL} showIcon={false}>
          GitHub Repository
        </ExternalLink>
      )

      const icon = screen.queryByTestId('external-link-icon')
      expect(icon).not.toBeInTheDocument()
    })

    it('should show icon when showIcon is explicitly true', () => {
      render(
        <ExternalLink href={GITHUB_URL} showIcon={true}>
          GitHub Repository
        </ExternalLink>
      )

      const icon = screen.getByTestId('external-link-icon')
      expect(icon).toBeInTheDocument()
    })

    it('should render icon as SVG element', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const icon = screen.getByTestId('external-link-icon')
      expect(icon.tagName.toLowerCase()).toBe('svg')
    })
  })

  /**
   * Additional test cases for props and accessibility
   */
  describe('Props and accessibility', () => {
    it('should apply custom className', () => {
      render(
        <ExternalLink href={GITHUB_URL} className="custom-class">
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link.className).toContain('custom-class')
    })

    it('should support aria-label prop', () => {
      render(
        <ExternalLink
          href={GITHUB_URL}
          aria-label="View MirDB on GitHub (opens in new tab)"
        >
          GitHub
        </ExternalLink>
      )

      const link = screen.getByRole('link', { name: /view mirdb on github/i })
      expect(link).toBeInTheDocument()
    })

    it('should render children of various types', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          <span data-testid="child-span">Nested Content</span>
        </ExternalLink>
      )

      expect(screen.getByTestId('child-span')).toBeInTheDocument()
      expect(screen.getByText('Nested Content')).toBeInTheDocument()
    })

    it('should maintain inline-flex display for proper alignment', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link.className).toContain('inline-flex')
    })

    it('should have items-center class for vertical alignment', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          GitHub Repository
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link.className).toContain('items-center')
    })
  })

  /**
   * Integration with real GitHub URL from constants
   */
  describe('Integration with constants', () => {
    it('should work correctly with GITHUB_URL constant', () => {
      render(
        <ExternalLink href={GITHUB_URL}>
          View on GitHub
        </ExternalLink>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
      expect(screen.getByTestId('external-link-icon')).toBeInTheDocument()
    })
  })
})
