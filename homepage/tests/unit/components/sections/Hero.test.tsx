/**
 * Unit tests for Hero component.
 * Owner: Scenario 2 - Hero Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero } from '../../../../src/components/sections/Hero'

describe('Hero Component', () => {
  // Test Case 1: Hero section contains h1 headline with text about MirDB or 'Persistent Key-Value Store'
  it('should render headline with MirDB and Persistent Key-Value Store text', () => {
    render(<Hero />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // Check for MirDB in the headline
    expect(heading).toHaveTextContent(/MirDB/i)

    // Check for subtitle about Persistent Key-Value Store
    expect(heading).toHaveTextContent(/Persistent Key-Value Store/i)
  })

  // Test Case 2: Subheadline mentions 'Memcached protocol' and 'persistence' or 'durable'
  it('should render subheadline mentioning Memcached protocol and persistence', () => {
    render(<Hero />)

    // The description text should mention Memcached and persistence/durable
    // Use queryAllByText since multiple elements may match
    const memcachedElements = screen.queryAllByText(/Memcached/i)
    expect(memcachedElements.length).toBeGreaterThan(0)

    // Check for persistence or durable - use queryAllByText since multiple elements may match
    const persistedElements = screen.queryAllByText(/persist/i)
    const durableElements = screen.queryAllByText(/durable/i)
    const hasPersistenceContent = persistedElements.length > 0 || durableElements.length > 0
    expect(hasPersistenceContent).toBe(true)
  })

  // Test Case 3: Button with text 'Get Started' or 'Install Now' is visible and has primary styling
  it('should render primary CTA button with "Get Started" text and primary styling', () => {
    render(<Hero />)

    const primaryButton = screen.getByRole('button', { name: /Get Started/i })
    expect(primaryButton).toBeInTheDocument()
    expect(primaryButton).toBeVisible()

    // Check for primary styling class
    expect(primaryButton).toHaveClass('button--primary')
  })

  // Test Case 4: Button or link with text containing 'GitHub' links to repository
  it('should render secondary CTA button linking to GitHub repository', () => {
    render(<Hero />)

    const githubLink = screen.getByRole('link', { name: /GitHub/i })
    expect(githubLink).toBeInTheDocument()
    expect(githubLink).toBeVisible()

    // Check that it links to GitHub
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')

    // Check for security attributes on external link
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('should render hero section with correct semantic structure', () => {
    render(<Hero />)

    // Check that the hero section exists
    const heroSection = document.querySelector('section.hero')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveAttribute('id', 'hero')
  })

  it('should render CTA buttons in a container', () => {
    render(<Hero />)

    // Both buttons should be present
    const primaryButton = screen.getByRole('button', { name: /Get Started/i })
    const secondaryButton = screen.getByRole('link', { name: /GitHub/i })

    expect(primaryButton).toBeInTheDocument()
    expect(secondaryButton).toBeInTheDocument()

    // They should be in the same CTA container
    const ctaContainer = document.querySelector('.hero__cta')
    expect(ctaContainer).toContainElement(primaryButton)
    expect(ctaContainer).toContainElement(secondaryButton)
  })
})
