import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer } from './Footer'

describe('Footer', () => {
  // Test Case 1: Footer component renders without errors
  it('renders without errors', () => {
    const { container } = render(<Footer />)
    const footer = container.querySelector('footer')
    expect(footer).toBeInTheDocument()
  })

  // Test Case 2: Footer contains copyright notice with 'MirDB'
  it('contains copyright notice with MirDB', () => {
    render(<Footer />)
    const copyrightText = screen.getByText(/MirDB/i)
    expect(copyrightText).toBeInTheDocument()

    // Check for copyright symbol and year
    const currentYear = new Date().getFullYear()
    expect(screen.getByText(new RegExp(`© ${currentYear} MirDB`, 'i'))).toBeInTheDocument()
  })

  // Test Case 3: Footer contains link to GitHub repository
  it('contains link to GitHub repository', () => {
    render(<Footer />)
    const githubLinks = screen.getAllByRole('link', { name: /github/i })
    expect(githubLinks.length).toBeGreaterThan(0)

    // Check at least one GitHub link has proper href
    const hasGithubUrl = githubLinks.some((link) =>
      link.getAttribute('href')?.includes('github.com')
    )
    expect(hasGithubUrl).toBe(true)
  })

  // Test Case 4: Footer uses semantic footer element
  it('uses semantic footer element', () => {
    render(<Footer />)
    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  // Additional tests for accessibility and structure
  it('has navigation with proper aria-label', () => {
    render(<Footer />)
    const nav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(nav).toBeInTheDocument()
  })

  it('contains links to documentation', () => {
    render(<Footer />)
    const docsLink = screen.getByRole('link', { name: /documentation/i })
    expect(docsLink).toBeInTheDocument()
    expect(docsLink).toHaveAttribute('href')
  })

  it('contains links to Memcached Protocol', () => {
    render(<Footer />)
    const protocolLink = screen.getByRole('link', { name: /memcached protocol/i })
    expect(protocolLink).toBeInTheDocument()
    expect(protocolLink).toHaveAttribute('href')
  })

  it('external links open in new tab', () => {
    render(<Footer />)
    const externalLinks = screen.getAllByRole('link')
    externalLinks.forEach((link) => {
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  it('displays tagline about MirDB', () => {
    render(<Footer />)
    expect(
      screen.getByText(/persistent key-value store/i)
    ).toBeInTheDocument()
  })
})
