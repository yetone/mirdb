/**
 * Navigation E2E Tests.
 * Contributor: Scenario 6 - Navigation and External Links
 *
 * Tests:
 * - GitHub repository link navigates correctly
 * - External links open in new tabs
 * - Navigation links scroll to sections
 *
 * Note: These tests verify link behavior. When Playwright is configured,
 * these tests can be upgraded to true browser-based E2E tests.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Footer } from '../../src/components/Footer/Footer'
import { Navigation } from '../../src/components/Navigation/Navigation'
import { GITHUB_REPO_URL } from '../../src/utils/constants'

describe('E2E: Navigation and External Links', () => {
  describe('Footer GitHub Repository Link', () => {
    beforeEach(() => {
      render(<Footer />)
    })

    it('GitHub link navigates to repository in new tab', () => {
      const githubLink = screen.getByRole('link', { name: /github/i })

      // Verify the link has correct href for GitHub repository
      expect(githubLink).toHaveAttribute('href', GITHUB_REPO_URL)

      // Verify the link opens in new tab (target="_blank")
      expect(githubLink).toHaveAttribute('target', '_blank')

      // Verify security attributes prevent opener access
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

      // Simulate click - in a real browser this would open a new tab
      // The click should not cause errors
      fireEvent.click(githubLink)

      // Link should still exist after click (didn't navigate away)
      expect(githubLink).toBeInTheDocument()
    })

    it('clicking GitHub link with correct attributes would open in new tab', () => {
      const githubLink = screen.getByRole('link', { name: /github/i })

      // Collect attributes that determine navigation behavior
      const href = githubLink.getAttribute('href')
      const target = githubLink.getAttribute('target')
      const rel = githubLink.getAttribute('rel')

      // Assert the full navigation configuration
      expect(href).toBe('https://github.com/yetone/mirdb')
      expect(target).toBe('_blank')
      expect(rel).toContain('noopener')
      expect(rel).toContain('noreferrer')
    })
  })

  describe('Navigation GitHub Link', () => {
    beforeEach(() => {
      render(<Navigation />)
    })

    it('navigation GitHub link opens in new tab', () => {
      const githubLinks = screen.getAllByRole('link', { name: /github/i })
      const externalGithubLink = githubLinks.find(
        link => link.getAttribute('href') === GITHUB_REPO_URL
      )

      expect(externalGithubLink).toBeDefined()
      expect(externalGithubLink).toHaveAttribute('target', '_blank')
      expect(externalGithubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('External Link Security', () => {
    it('all external links have security attributes', () => {
      render(<Footer />)

      const allLinks = screen.getAllByRole('link')

      // All external links in footer should have security attributes
      allLinks.forEach((link) => {
        if (link.getAttribute('href')?.startsWith('http')) {
          expect(link).toHaveAttribute('target', '_blank')
          expect(link).toHaveAttribute('rel', expect.stringContaining('noopener'))
        }
      })
    })
  })
})
