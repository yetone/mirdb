/**
 * Unit tests for ExternalLink component.
 * Owner: Scenario 6 - External Links and Resources
 *
 * Test Cases:
 * - TC2: Check GitHub link target='_blank'
 * - TC5: External links have rel='noopener noreferrer' for security
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ExternalLink, SimpleExternalLink } from './ExternalLink'
import type { ExternalLink as ExternalLinkType } from '../../types'

const mockGitHubLink: ExternalLinkType = {
  label: 'GitHub',
  url: 'https://github.com/theseus-rs/mirdb',
  type: 'github',
}

const mockDocsLink: ExternalLinkType = {
  label: 'Documentation',
  url: 'https://github.com/theseus-rs/mirdb#readme',
  type: 'docs',
}

describe('ExternalLink Component', () => {
  // Test Case 2: GitHub link opens in new tab (target='_blank')
  describe('Target Attribute', () => {
    it('renders with target="_blank" attribute', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link', { name: /github/i })
      expect(link).toHaveAttribute('target', '_blank')
    })

    it('GitHub link specifically has target="_blank"', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', expect.stringContaining('github.com'))
      expect(link).toHaveAttribute('target', '_blank')
    })
  })

  // Test Case 5: External links have rel='noopener noreferrer' for security
  describe('Security Attributes', () => {
    it('renders with rel="noopener noreferrer" attribute', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('all external links have security attributes', () => {
      const links = [mockGitHubLink, mockDocsLink]
      links.forEach((linkData) => {
        const { unmount } = render(<ExternalLink link={linkData} />)
        const link = screen.getByRole('link')
        expect(link).toHaveAttribute('rel', 'noopener noreferrer')
        unmount()
      })
    })
  })

  // Basic rendering tests
  describe('Rendering', () => {
    it('renders link with correct href', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', mockGitHubLink.url)
    })

    it('renders link label', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      expect(screen.getByText('GitHub')).toBeInTheDocument()
    })

    it('can render custom children instead of label', () => {
      render(
        <ExternalLink link={mockGitHubLink}>
          View on GitHub
        </ExternalLink>
      )
      expect(screen.getByText('View on GitHub')).toBeInTheDocument()
    })

    it('applies custom className', () => {
      render(<ExternalLink link={mockGitHubLink} className="custom-class" />)
      const link = screen.getByRole('link')
      expect(link).toHaveClass('custom-class')
    })
  })

  // Accessibility
  describe('Accessibility', () => {
    it('has aria-label indicating opens in new tab', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('aria-label', expect.stringContaining('opens in new tab'))
    })

    it('includes label in aria-label', () => {
      render(<ExternalLink link={mockGitHubLink} />)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('aria-label', expect.stringContaining('GitHub'))
    })
  })

  // Icon rendering
  describe('Icon Display', () => {
    it('renders external link icon when showIcon is true', () => {
      const { container } = render(<ExternalLink link={mockGitHubLink} showIcon />)
      const svg = container.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('does not render icon by default', () => {
      const { container } = render(<ExternalLink link={mockGitHubLink} />)
      const svg = container.querySelector('svg')
      expect(svg).not.toBeInTheDocument()
    })
  })
})

describe('SimpleExternalLink Component', () => {
  // Test Case 2 & 5 for SimpleExternalLink variant
  it('renders with target="_blank" and rel="noopener noreferrer"', () => {
    render(
      <SimpleExternalLink href="https://github.com/example">
        GitHub Link
      </SimpleExternalLink>
    )
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('renders children as link text', () => {
    render(
      <SimpleExternalLink href="https://example.com">
        Example Link
      </SimpleExternalLink>
    )
    expect(screen.getByText('Example Link')).toBeInTheDocument()
  })

  it('applies custom className', () => {
    render(
      <SimpleExternalLink href="https://example.com" className="my-class">
        Link
      </SimpleExternalLink>
    )
    const link = screen.getByRole('link')
    expect(link).toHaveClass('my-class')
  })

  it('renders icon when showIcon is true', () => {
    const { container } = render(
      <SimpleExternalLink href="https://example.com" showIcon>
        Link
      </SimpleExternalLink>
    )
    const svg = container.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })
})
