/**
 * External Link Security Tests
 * Owner: Scenario 13 - External Link Security
 *
 * Requirements:
 * - NFR-6: All external links shall use rel="noopener noreferrer" for security
 * - Prevent tab-napping attacks on external links
 * - External links should open in new tab with target="_blank"
 *
 * Test cases:
 * 1. Find GitHub repository link - verify security attributes
 * 2. Find CircleCI badge link - verify security attributes
 * 3. Find all anchor tags with external href - verify all have security attributes
 * 4. Check documentation external links - verify proper security attributes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'

// Components with external links
import { Badge } from '../../../src/components/ui/Badge'
import { Navigation } from '../../../src/components/layout/Navigation'
import { Footer } from '../../../src/components/layout/Footer'
import { MobileMenu } from '../../../src/components/layout/MobileMenu'
import { Button } from '../../../src/components/ui/Button'

// Configuration and types
import {
  GITHUB_URL,
  CIRCLECI_BADGE_URL,
  CIRCLECI_BUILD_URL,
  navItems,
  footerLinks
} from '../../../src/config/content'
import type { NavItem } from '../../../src/types'

/**
 * Helper function to check if a link is external (starts with http/https)
 */
function isExternalLink(href: string): boolean {
  return href.startsWith('http://') || href.startsWith('https://')
}

describe('External Link Security', () => {
  beforeEach(() => {
    // Mock the current year for Footer tests
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-15'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('Test Case 1: GitHub Repository Link Security', () => {
    it('Navigation GitHub link has rel="noopener noreferrer" and target="_blank"', () => {
      render(<Navigation items={navItems} />)

      const githubLink = screen.getByRole('link', { name: 'GitHub' })
      expect(githubLink).toBeInTheDocument()
      expect(githubLink).toHaveAttribute('href', GITHUB_URL)
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('Footer GitHub link has rel="noopener noreferrer" and target="_blank"', () => {
      render(<Footer />)

      const githubLink = screen.getByRole('link', { name: 'GitHub' })
      expect(githubLink).toBeInTheDocument()
      expect(githubLink).toHaveAttribute('href', GITHUB_URL)
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('MobileMenu GitHub link has rel="noopener noreferrer" and target="_blank"', () => {
      render(<MobileMenu items={navItems} isOpen={true} onClose={() => {}} />)

      const githubLink = screen.getByRole('link', { name: 'GitHub' })
      expect(githubLink).toBeInTheDocument()
      expect(githubLink).toHaveAttribute('href', GITHUB_URL)
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('Button component with external GitHub link has security attributes', () => {
      render(
        <Button href={GITHUB_URL} external>
          View on GitHub
        </Button>
      )

      const button = screen.getByRole('link', { name: 'View on GitHub' })
      expect(button).toHaveAttribute('href', GITHUB_URL)
      expect(button).toHaveAttribute('target', '_blank')
      expect(button).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('Test Case 2: CircleCI Badge Link Security', () => {
    it('Badge component link has rel="noopener noreferrer" and target="_blank"', () => {
      render(
        <Badge
          src={CIRCLECI_BADGE_URL}
          alt="Build Status"
          href={CIRCLECI_BUILD_URL}
        />
      )

      const link = screen.getByRole('link')
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', CIRCLECI_BUILD_URL)
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('CircleCI badge URL is external (starts with https)', () => {
      expect(isExternalLink(CIRCLECI_BUILD_URL)).toBe(true)
    })
  })

  describe('Test Case 3: All External Links Have Security Attributes', () => {
    it('all Navigation external links have proper security attributes', () => {
      render(<Navigation items={navItems} />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const href = link.getAttribute('href') || ''
        if (isExternalLink(href)) {
          expect(link).toHaveAttribute('target', '_blank')
          expect(link).toHaveAttribute('rel', 'noopener noreferrer')
        }
      })
    })

    it('all Footer external links have proper security attributes', () => {
      render(<Footer />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const href = link.getAttribute('href') || ''
        if (isExternalLink(href)) {
          expect(link).toHaveAttribute('target', '_blank')
          expect(link).toHaveAttribute('rel', 'noopener noreferrer')
        }
      })
    })

    it('all MobileMenu external links have proper security attributes', () => {
      render(<MobileMenu items={navItems} isOpen={true} onClose={() => {}} />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const href = link.getAttribute('href') || ''
        if (isExternalLink(href)) {
          expect(link).toHaveAttribute('target', '_blank')
          expect(link).toHaveAttribute('rel', 'noopener noreferrer')
        }
      })
    })

    it('navItems marked as external have http/https URLs', () => {
      const externalNavItems = navItems.filter(item => item.external)
      externalNavItems.forEach((item) => {
        expect(isExternalLink(item.href)).toBe(true)
      })
    })

    it('footerLinks marked as external have http/https URLs', () => {
      const externalFooterLinks = footerLinks.filter(link => link.external)
      externalFooterLinks.forEach((link) => {
        expect(isExternalLink(link.href)).toBe(true)
      })
    })
  })

  describe('Test Case 4: Documentation External Links Security', () => {
    it('Documentation link in Navigation has proper security attributes when external', () => {
      // Documentation links to GitHub readme anchor
      render(<Navigation items={navItems} />)

      const docsLink = screen.getByRole('link', { name: 'Documentation' })
      expect(docsLink).toBeInTheDocument()

      const href = docsLink.getAttribute('href') || ''
      if (isExternalLink(href)) {
        expect(docsLink).toHaveAttribute('target', '_blank')
        expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer')
      }
    })

    it('Examples link in Navigation has proper security attributes when external', () => {
      render(<Navigation items={navItems} />)

      const examplesLink = screen.getByRole('link', { name: 'Examples' })
      expect(examplesLink).toBeInTheDocument()

      const href = examplesLink.getAttribute('href') || ''
      if (isExternalLink(href)) {
        expect(examplesLink).toHaveAttribute('target', '_blank')
        expect(examplesLink).toHaveAttribute('rel', 'noopener noreferrer')
      }
    })

    it('Issues link in Footer has proper security attributes', () => {
      render(<Footer />)

      const issuesLink = screen.getByRole('link', { name: 'Issues' })
      expect(issuesLink).toBeInTheDocument()
      expect(issuesLink).toHaveAttribute('target', '_blank')
      expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('License link in Footer has proper security attributes', () => {
      render(<Footer />)

      const licenseLink = screen.getByRole('link', { name: 'License' })
      expect(licenseLink).toBeInTheDocument()
      expect(licenseLink).toHaveAttribute('target', '_blank')
      expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('Internal Links Do Not Have External Attributes', () => {
    it('internal Navigation links do not have target="_blank"', () => {
      render(<Navigation items={navItems} />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const href = link.getAttribute('href') || ''
        if (!isExternalLink(href)) {
          expect(link).not.toHaveAttribute('target', '_blank')
          expect(link).not.toHaveAttribute('rel', 'noopener noreferrer')
        }
      })
    })

    it('internal MobileMenu links do not have target="_blank"', () => {
      render(<MobileMenu items={navItems} isOpen={true} onClose={() => {}} />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const href = link.getAttribute('href') || ''
        if (!isExternalLink(href)) {
          expect(link).not.toHaveAttribute('target', '_blank')
          expect(link).not.toHaveAttribute('rel', 'noopener noreferrer')
        }
      })
    })

    it('Button without external prop does not have security attributes', () => {
      render(
        <Button href="#about">
          About
        </Button>
      )

      const button = screen.getByRole('link', { name: 'About' })
      expect(button).not.toHaveAttribute('target', '_blank')
      expect(button).not.toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('Button Component External Link Handling', () => {
    it('Button with external=true has security attributes', () => {
      render(
        <Button href="https://example.com" external>
          External Link
        </Button>
      )

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('Button with external=false does not have security attributes', () => {
      render(
        <Button href="#section">
          Internal Link
        </Button>
      )

      const link = screen.getByRole('link')
      expect(link).not.toHaveAttribute('target', '_blank')
      expect(link).not.toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('Configuration Consistency', () => {
    it('GITHUB_URL starts with https', () => {
      expect(GITHUB_URL).toMatch(/^https:\/\//)
    })

    it('CIRCLECI_BUILD_URL starts with https', () => {
      expect(CIRCLECI_BUILD_URL).toMatch(/^https:\/\//)
    })

    it('all footerLinks with external=true point to GitHub domain', () => {
      const externalFooterLinks = footerLinks.filter(link => link.external)
      externalFooterLinks.forEach((link) => {
        expect(link.href).toContain('github.com')
      })
    })
  })
})
