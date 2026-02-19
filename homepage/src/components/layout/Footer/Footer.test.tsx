/**
 * Footer Component Unit Tests
 * Owner: Scenario 8 - Footer & External Links
 *
 * Test cases covering:
 * - Test Case 1: Render Footer component
 * - Test Case 2: Check copyright text
 * - Test Case 3: Check GitHub link href
 * - Test Case 5: Check CircleCI badge presence
 * - Test Case 6: Verify footer links have proper external attributes
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { Footer } from './Footer'

const GITHUB_URL = 'https://github.com/yetone/mirdb'

describe('Footer', () => {
  // Test Case 1: Render Footer component
  it('renders footer with copyright, links, and badges', () => {
    render(<Footer />)

    // Footer should be in the document
    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()
    expect(footer).toHaveAttribute('role', 'contentinfo')

    // Check copyright is present
    expect(screen.getByTestId('copyright')).toBeInTheDocument()

    // Check GitHub link is present
    expect(screen.getByTestId('github-link')).toBeInTheDocument()

    // Check CircleCI badge is present
    expect(screen.getByTestId('circleci-badge')).toBeInTheDocument()
  })

  // Test Case 2: Check copyright text
  it('contains copyright notice with current year and MirDB', () => {
    render(<Footer />)

    const copyright = screen.getByTestId('copyright')
    const currentYear = new Date().getFullYear()

    expect(copyright).toHaveTextContent(`© ${currentYear} MirDB`)
    expect(copyright).toHaveTextContent('All rights reserved')
  })

  // Test Case 3: Check GitHub link href
  it('has GitHub link with correct href', () => {
    render(<Footer />)

    const githubLink = screen.getByTestId('github-link')
    expect(githubLink).toHaveAttribute('href', GITHUB_URL)
  })

  // Test Case 5: Check CircleCI badge presence
  it('renders CircleCI badge image with link to build status', () => {
    render(<Footer />)

    // Check badge image
    const badge = screen.getByTestId('circleci-badge')
    expect(badge).toBeInTheDocument()
    expect(badge.tagName).toBe('IMG')
    expect(badge).toHaveAttribute('alt', 'CircleCI build status')
    expect(badge).toHaveAttribute(
      'src',
      'https://dl.circleci.com/status-badge/img/gh/yetone/mirdb/tree/master.svg?style=svg'
    )

    // Check badge link
    const badgeLink = screen.getByTestId('circleci-badge-link')
    expect(badgeLink).toHaveAttribute(
      'href',
      'https://dl.circleci.com/status-badge/redirect/gh/yetone/mirdb/tree/master'
    )
    expect(badgeLink).toHaveAttribute('aria-label', 'View CircleCI build status')
  })

  // Test Case 6: Verify footer links have proper external attributes
  it('has external links with target="_blank" and rel="noopener noreferrer"', () => {
    render(<Footer />)

    // GitHub Repository link
    const githubLink = screen.getByTestId('github-link')
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Documentation link
    const docsLink = screen.getByTestId('docs-link')
    expect(docsLink).toHaveAttribute('target', '_blank')
    expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Issues link
    const issuesLink = screen.getByTestId('issues-link')
    expect(issuesLink).toHaveAttribute('target', '_blank')
    expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer')

    // CircleCI badge link
    const badgeLink = screen.getByTestId('circleci-badge-link')
    expect(badgeLink).toHaveAttribute('target', '_blank')
    expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer')

    // License link
    const licenseLink = screen.getByTestId('license-link')
    expect(licenseLink).toHaveAttribute('target', '_blank')
    expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('renders license information with link', () => {
    render(<Footer />)

    const license = screen.getByTestId('license')
    expect(license).toHaveTextContent('Released under the')
    expect(license).toHaveTextContent('MIT License')

    const licenseLink = screen.getByTestId('license-link')
    expect(licenseLink).toHaveAttribute('href', `${GITHUB_URL}/blob/master/LICENSE`)
  })

  it('renders brand name and description', () => {
    render(<Footer />)

    expect(screen.getByText('MirDB')).toBeInTheDocument()
    expect(
      screen.getByText(/high-performance persistent key-value store/i)
    ).toBeInTheDocument()
  })

  it('renders Links section with correct links', () => {
    render(<Footer />)

    expect(screen.getByText('Links')).toBeInTheDocument()
    expect(screen.getByText('GitHub Repository')).toBeInTheDocument()
    expect(screen.getByText('Documentation')).toBeInTheDocument()
    expect(screen.getByText('Report Issues')).toBeInTheDocument()

    // Check documentation link href
    const docsLink = screen.getByTestId('docs-link')
    expect(docsLink).toHaveAttribute('href', `${GITHUB_URL}#readme`)

    // Check issues link href
    const issuesLink = screen.getByTestId('issues-link')
    expect(issuesLink).toHaveAttribute('href', `${GITHUB_URL}/issues`)
  })

  it('renders Build Status section', () => {
    render(<Footer />)

    expect(screen.getByText('Build Status')).toBeInTheDocument()
  })
})
