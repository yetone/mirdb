import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Community } from '../../../src/components/sections/Community'

describe('Community', () => {
  // Test Case 1: Component renders with engagement options
  describe('renders with engagement options', () => {
    it('renders the community section', () => {
      render(<Community />)

      const section = screen.getByTestId('community-section')
      expect(section).toBeInTheDocument()
    })

    it('renders the section heading', () => {
      render(<Community />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent(/community/i)
    })

    it('renders community cards with engagement options', () => {
      render(<Community />)

      const communityCards = screen.getAllByTestId('community-card')
      expect(communityCards.length).toBeGreaterThanOrEqual(2)
    })

    it('renders icons for community cards', () => {
      render(<Community />)

      const communityIcons = screen.getAllByTestId('community-icon')
      expect(communityIcons.length).toBeGreaterThanOrEqual(2)
    })

    it('is accessible with proper ARIA attributes', () => {
      render(<Community />)

      const section = screen.getByRole('region', { name: /community/i })
      expect(section).toBeInTheDocument()
      expect(section).toHaveAttribute('aria-labelledby', 'community-heading')
    })
  })

  // Test Case 2: Link to GitHub issues is present
  describe('GitHub issues link', () => {
    it('has a link to GitHub issues', () => {
      render(<Community />)

      const issuesLink = screen.getByRole('link', { name: /open an issue/i })
      expect(issuesLink).toBeInTheDocument()
      expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues')
    })

    it('issues link opens in new tab', () => {
      render(<Community />)

      const issuesLink = screen.getByRole('link', { name: /open an issue/i })
      expect(issuesLink).toHaveAttribute('target', '_blank')
      expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('has accessible label for issues link', () => {
      render(<Community />)

      const issuesLink = screen.getByRole('link', { name: /open an issue/i })
      expect(issuesLink).toHaveAttribute('aria-label')
      expect(issuesLink.getAttribute('aria-label')).toContain('opens in new tab')
    })

    it('has text content about reporting issues', () => {
      render(<Community />)

      const section = screen.getByTestId('community-section')
      expect(section).toHaveTextContent(/issue/i)
      expect(section).toHaveTextContent(/bug/i)
    })
  })

  // Test Case 3: Contribution guidelines or link to CONTRIBUTING.md is present
  describe('contribution guidelines', () => {
    it('has a link to contribution guidelines', () => {
      render(<Community />)

      const contributeLink = screen.getByRole('link', { name: /contributing guide/i })
      expect(contributeLink).toBeInTheDocument()
      expect(contributeLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/CONTRIBUTING.md')
    })

    it('contribute link opens in new tab', () => {
      render(<Community />)

      const contributeLink = screen.getByRole('link', { name: /contributing guide/i })
      expect(contributeLink).toHaveAttribute('target', '_blank')
      expect(contributeLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('has text content about contributing', () => {
      render(<Community />)

      const section = screen.getByTestId('community-section')
      expect(section).toHaveTextContent(/contribut/i)
    })

    it('has accessible label for contribute link', () => {
      render(<Community />)

      const contributeLink = screen.getByRole('link', { name: /contributing guide/i })
      expect(contributeLink).toHaveAttribute('aria-label')
      expect(contributeLink.getAttribute('aria-label')).toContain('opens in new tab')
    })
  })

  // Additional tests for comprehensive coverage
  describe('community engagement features', () => {
    it('has a link to GitHub discussions', () => {
      render(<Community />)

      const discussionsLink = screen.getByRole('link', { name: /join discussions/i })
      expect(discussionsLink).toBeInTheDocument()
      expect(discussionsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/discussions')
    })

    it('has a link to the main GitHub repository', () => {
      render(<Community />)

      const repoLink = screen.getByRole('link', { name: /view on github/i })
      expect(repoLink).toBeInTheDocument()
      expect(repoLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
    })

    it('has a license link', () => {
      render(<Community />)

      const licenseLink = screen.getByRole('link', { name: /license/i })
      expect(licenseLink).toBeInTheDocument()
      expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE')
    })

    it('renders the community grid', () => {
      render(<Community />)

      const grid = screen.getByTestId('community-grid')
      expect(grid).toBeInTheDocument()
    })

    it('all external links have proper security attributes', () => {
      render(<Community />)

      const externalLinks = screen.getAllByRole('link')

      externalLinks.forEach((link) => {
        expect(link).toHaveAttribute('target', '_blank')
        expect(link).toHaveAttribute('rel', 'noopener noreferrer')
      })
    })
  })
})
