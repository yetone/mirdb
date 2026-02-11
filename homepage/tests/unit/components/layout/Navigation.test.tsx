/**
 * Unit tests for Navigation component
 * Scenario 1 - Header and Navigation
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Navigation } from '../../../../src/components/layout/Navigation'
import type { NavItem } from '../../../../src/types'

const mockNavItems: NavItem[] = [
  { label: 'Documentation', href: '#documentation' },
  { label: 'Examples', href: '#examples' },
  { label: 'GitHub', href: 'https://github.com/yetone/mirdb', external: true },
  { label: 'About', href: '#about' },
]

describe('Navigation', () => {
  it('renders all navigation links', () => {
    render(<Navigation items={mockNavItems} />)

    expect(screen.getByRole('link', { name: 'Documentation' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Examples' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'GitHub' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'About' })).toBeInTheDocument()
  })

  it('renders navigation with correct aria-label', () => {
    render(<Navigation items={mockNavItems} />)

    const nav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(nav).toBeInTheDocument()
  })

  it('renders GitHub link with correct href', () => {
    render(<Navigation items={mockNavItems} />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
  })

  it('renders GitHub link with rel="noopener noreferrer" for security', () => {
    render(<Navigation items={mockNavItems} />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('renders GitHub link with target="_blank"', () => {
    render(<Navigation items={mockNavItems} />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toHaveAttribute('target', '_blank')
  })

  it('renders internal links without target or rel attributes', () => {
    render(<Navigation items={mockNavItems} />)

    const docsLink = screen.getByRole('link', { name: 'Documentation' })
    expect(docsLink).not.toHaveAttribute('target')
    expect(docsLink).not.toHaveAttribute('rel')
  })
})
