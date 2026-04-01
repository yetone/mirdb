/**
 * Navigation Component Tests.
 * Owner: Scenario 6 - Navigation and External Links
 *
 * Tests:
 * - Navigation renders correctly
 * - Internal section links are present
 * - External GitHub link has proper attributes
 * - Mobile menu functionality
 */

import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Navigation } from './Navigation'
import { GITHUB_REPO_URL } from '../../utils/constants'

describe('Navigation', () => {
  it('renders navigation component', () => {
    render(<Navigation />)

    const nav = screen.getByRole('navigation')
    expect(nav).toBeInTheDocument()
  })

  it('displays MirDB logo/brand link', () => {
    render(<Navigation />)

    const brandLink = screen.getByRole('link', { name: /mirdb/i })
    expect(brandLink).toBeInTheDocument()
    expect(brandLink).toHaveAttribute('href', '/')
  })

  it('has link to Features section', () => {
    render(<Navigation />)

    const featuresLinks = screen.getAllByRole('link', { name: /features/i })
    expect(featuresLinks.length).toBeGreaterThan(0)
    expect(featuresLinks[0]).toHaveAttribute('href', '#features')
  })

  it('has link to Usage section', () => {
    render(<Navigation />)

    const usageLinks = screen.getAllByRole('link', { name: /usage/i })
    expect(usageLinks.length).toBeGreaterThan(0)
    expect(usageLinks[0]).toHaveAttribute('href', '#usage')
  })

  it('has link to Roadmap section', () => {
    render(<Navigation />)

    const roadmapLinks = screen.getAllByRole('link', { name: /roadmap/i })
    expect(roadmapLinks.length).toBeGreaterThan(0)
    expect(roadmapLinks[0]).toHaveAttribute('href', '#roadmap')
  })

  it('GitHub link has target="_blank" and rel="noopener noreferrer"', () => {
    render(<Navigation />)

    const githubLinks = screen.getAllByRole('link', { name: /github/i })
    const externalGithubLink = githubLinks.find(link =>
      link.getAttribute('href') === GITHUB_REPO_URL
    )

    expect(externalGithubLink).toBeInTheDocument()
    expect(externalGithubLink).toHaveAttribute('target', '_blank')
    expect(externalGithubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('has mobile menu button', () => {
    render(<Navigation />)

    const menuButton = screen.getByRole('button', { name: /open menu/i })
    expect(menuButton).toBeInTheDocument()
    expect(menuButton).toHaveAttribute('aria-expanded', 'false')
  })

  it('mobile menu button toggles aria-expanded', () => {
    render(<Navigation />)

    const menuButton = screen.getByRole('button', { name: /open menu/i })
    fireEvent.click(menuButton)

    const closeButton = screen.getByRole('button', { name: /close menu/i })
    expect(closeButton).toHaveAttribute('aria-expanded', 'true')
  })

  it('internal links do not have external link attributes', () => {
    render(<Navigation />)

    const featuresLinks = screen.getAllByRole('link', { name: /features/i })
    expect(featuresLinks[0]).not.toHaveAttribute('target', '_blank')
    expect(featuresLinks[0]).not.toHaveAttribute('rel', 'noopener noreferrer')
  })
})
