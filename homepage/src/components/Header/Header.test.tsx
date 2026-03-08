import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Header } from './Header'

describe('Header', () => {
  // Test Case 1: Header component renders without errors
  it('renders without errors', () => {
    const { container } = render(<Header />)
    const header = container.querySelector('header')
    expect(header).toBeInTheDocument()
  })

  // Test Case 2: Logo image is present with src attribute pointing to valid image
  it('displays logo image with valid src attribute', () => {
    render(<Header />)
    const logo = screen.getByRole('img', { name: /mirdb logo/i })
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('src')
    expect(logo.getAttribute('src')).toMatch(/logo\.svg$/)
  })

  // Test Case 3: Logo has descriptive alt text containing 'MirDB'
  it('has logo with descriptive alt text containing MirDB', () => {
    render(<Header />)
    const logo = screen.getByRole('img', { name: /mirdb/i })
    expect(logo).toBeInTheDocument()
    const altText = logo.getAttribute('alt')
    expect(altText).toBeTruthy()
    expect(altText?.toLowerCase()).toContain('mirdb')
  })

  // Test Case 4: Text 'MirDB' is visible in header area
  it('displays MirDB project name prominently', () => {
    render(<Header />)
    const projectName = screen.getByText('MirDB')
    expect(projectName).toBeInTheDocument()
    expect(projectName).toBeVisible()
  })

  // Additional tests for accessibility and structure
  it('has proper semantic structure with banner role', () => {
    render(<Header />)
    const banner = screen.getByRole('banner')
    expect(banner).toBeInTheDocument()
  })

  it('has navigation with proper aria-label', () => {
    render(<Header />)
    const nav = screen.getByRole('navigation', { name: /main navigation/i })
    expect(nav).toBeInTheDocument()
  })

  it('contains navigation links', () => {
    render(<Header />)
    expect(screen.getByText('Features')).toBeInTheDocument()
    expect(screen.getByText('Getting Started')).toBeInTheDocument()
    expect(screen.getByText('Roadmap')).toBeInTheDocument()
  })

  it('has GitHub link with proper accessibility attributes', () => {
    render(<Header />)
    const githubLink = screen.getByRole('link', { name: /github/i })
    expect(githubLink).toBeInTheDocument()
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  // Scenario 7: Project Status Badges - Unit Tests
  describe('CircleCI Status Badge', () => {
    // Test Case 1: Check for CircleCI badge image
    it('displays CircleCI badge image with src containing circleci', () => {
      render(<Header />)
      const badgeImage = screen.getByTestId('circleci-badge-image')
      expect(badgeImage).toBeInTheDocument()
      expect(badgeImage).toHaveAttribute('src')
      expect(badgeImage.getAttribute('src')).toContain('circleci')
    })

    // Test Case 2: Check badge alt text
    it('has badge with descriptive alt text mentioning build status', () => {
      render(<Header />)
      const badgeImage = screen.getByTestId('circleci-badge-image')
      const altText = badgeImage.getAttribute('alt')
      expect(altText).toBeTruthy()
      expect(altText?.toLowerCase()).toContain('build status')
    })

    // Test Case 3: Check badge link
    it('has badge wrapped in link to CircleCI project page', () => {
      render(<Header />)
      const badgeLink = screen.getByTestId('circleci-badge-link')
      expect(badgeLink).toBeInTheDocument()
      expect(badgeLink).toHaveAttribute('href')
      expect(badgeLink.getAttribute('href')).toContain('circleci')
      expect(badgeLink.getAttribute('href')).toContain('theseus-rs/mirdb')
    })

    it('opens CircleCI page in new tab', () => {
      render(<Header />)
      const badgeLink = screen.getByTestId('circleci-badge-link')
      expect(badgeLink).toHaveAttribute('target', '_blank')
      expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('has status badges container', () => {
      render(<Header />)
      const badgesContainer = screen.getByTestId('status-badges')
      expect(badgesContainer).toBeInTheDocument()
    })
  })
})
