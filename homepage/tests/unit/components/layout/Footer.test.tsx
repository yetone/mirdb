/**
 * Unit tests for Footer component
 * Scenario 7 - Footer Section
 *
 * Test cases:
 * 1. Footer contains link to GitHub repository with proper href
 * 2. Footer contains link to GitHub issues page
 * 3. Footer displays license type or links to LICENSE file
 * 4. All external links have rel='noopener noreferrer' attribute
 * 5. Footer contains copyright notice with current year
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer } from '../../../../src/components/layout/Footer'

describe('Footer', () => {
  beforeEach(() => {
    // Mock the current year to ensure consistent test results
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-15'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('renders the footer element', () => {
    render(<Footer />)

    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
  })

  // Test Case 1: Footer contains link to GitHub repository with proper href
  it('contains a link to GitHub repository with proper href', () => {
    render(<Footer />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toBeInTheDocument()
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
  })

  // Test Case 2: Footer contains link to GitHub issues page
  it('contains a link to GitHub issues page', () => {
    render(<Footer />)

    const issuesLink = screen.getByRole('link', { name: 'Issues' })
    expect(issuesLink).toBeInTheDocument()
    expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues')
  })

  // Test Case 3: Footer displays license type or links to LICENSE file
  it('contains a link to LICENSE file', () => {
    render(<Footer />)

    const licenseLink = screen.getByRole('link', { name: 'License' })
    expect(licenseLink).toBeInTheDocument()
    expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE')
  })

  it('displays MIT License text in copyright', () => {
    render(<Footer />)

    const copyrightText = screen.getByText(/MIT License/i)
    expect(copyrightText).toBeInTheDocument()
  })

  // Test Case 4: All external links have rel='noopener noreferrer' attribute
  it('has security attributes on GitHub link', () => {
    render(<Footer />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('has security attributes on Issues link', () => {
    render(<Footer />)

    const issuesLink = screen.getByRole('link', { name: 'Issues' })
    expect(issuesLink).toHaveAttribute('target', '_blank')
    expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('has security attributes on License link', () => {
    render(<Footer />)

    const licenseLink = screen.getByRole('link', { name: 'License' })
    expect(licenseLink).toHaveAttribute('target', '_blank')
    expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('all external links have noopener noreferrer', () => {
    render(<Footer />)

    const links = screen.getAllByRole('link')
    links.forEach((link) => {
      if (link.getAttribute('target') === '_blank') {
        expect(link).toHaveAttribute('rel', 'noopener noreferrer')
      }
    })
  })

  // Test Case 5: Footer contains copyright notice with current year
  it('contains copyright notice with current year', () => {
    render(<Footer />)

    const currentYear = new Date().getFullYear()
    const copyrightText = screen.getByText(new RegExp(`© ${currentYear}`, 'i'))
    expect(copyrightText).toBeInTheDocument()
  })

  it('displays MirDB in copyright text', () => {
    render(<Footer />)

    const copyrightText = screen.getByText(/MirDB/i)
    expect(copyrightText).toBeInTheDocument()
  })

  // Accessibility tests
  it('has accessible navigation with proper aria-label', () => {
    render(<Footer />)

    const nav = screen.getByRole('navigation', { name: 'Footer navigation' })
    expect(nav).toBeInTheDocument()
  })
})
