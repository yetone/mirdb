/**
 * Footer Component Tests.
 * Owner: Scenario 6 - Navigation and External Links
 *
 * Tests:
 * - GitHub repository link is present and correct
 * - External links have proper security attributes
 * - License information is displayed
 * - Contribution link is present
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer } from './Footer'
import { GITHUB_REPO_URL } from '../../utils/constants'

describe('Footer', () => {
  it('renders with GitHub link containing correct URL', () => {
    render(<Footer />)

    const githubLink = screen.getByRole('link', { name: /github/i })
    expect(githubLink).toBeInTheDocument()
    expect(githubLink).toHaveAttribute('href', GITHUB_REPO_URL)
  })

  it('GitHub link contains https://github.com/yetone/mirdb', () => {
    render(<Footer />)

    const githubLink = screen.getByRole('link', { name: /github/i })
    expect(githubLink.getAttribute('href')).toContain('https://github.com/yetone/mirdb')
  })

  it('external links have target="_blank" and rel="noopener noreferrer"', () => {
    render(<Footer />)

    const externalLinks = screen.getAllByRole('link')

    externalLinks.forEach((link) => {
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  it('displays license information', () => {
    render(<Footer />)

    const licenseInfo = screen.getByTestId('license-info')
    expect(licenseInfo).toBeInTheDocument()
    expect(licenseInfo).toHaveTextContent(/MIT License/i)
  })

  it('has link to MIT License on GitHub', () => {
    render(<Footer />)

    const licenseLink = screen.getByRole('link', { name: /MIT License/i })
    expect(licenseLink).toBeInTheDocument()
    expect(licenseLink).toHaveAttribute('href', `${GITHUB_REPO_URL}/blob/master/LICENSE`)
  })

  it('has link to contribution guidelines', () => {
    render(<Footer />)

    const contributingLink = screen.getByRole('link', { name: /contribute to MirDB/i })
    expect(contributingLink).toBeInTheDocument()
    expect(contributingLink.getAttribute('href')).toContain('CONTRIBUTING')
  })

  it('has link to issues page for contributions', () => {
    render(<Footer />)

    const issuesLink = screen.getByRole('link', { name: /issues/i })
    expect(issuesLink).toBeInTheDocument()
    expect(issuesLink.getAttribute('href')).toContain('/issues')
  })

  it('has link to documentation', () => {
    render(<Footer />)

    const docsLink = screen.getByRole('link', { name: /documentation/i })
    expect(docsLink).toBeInTheDocument()
    expect(docsLink.getAttribute('href')).toContain('#readme')
  })

  it('displays copyright information', () => {
    render(<Footer />)

    const currentYear = new Date().getFullYear().toString()
    expect(screen.getByText(new RegExp(`© ${currentYear} MirDB`))).toBeInTheDocument()
  })

  it('has accessible navigation label', () => {
    render(<Footer />)

    const nav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(nav).toBeInTheDocument()
  })
})
