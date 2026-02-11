/**
 * Unit tests for Header component
 * Scenario 1 - Header and Navigation
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Header } from '../../../../src/components/layout/Header'

describe('Header', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('renders the MirDB logo with correct alt text', () => {
    render(<Header />)

    const logo = screen.getByAltText('MirDB')
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('src', '/assets/logo.gif')
  })

  it('renders the header element', () => {
    render(<Header />)

    const header = screen.getByRole('banner')
    expect(header).toBeInTheDocument()
  })

  it('renders logo link with correct aria-label', () => {
    render(<Header />)

    const logoLink = screen.getByRole('link', { name: 'MirDB Home' })
    expect(logoLink).toBeInTheDocument()
    expect(logoLink).toHaveAttribute('href', '/')
  })

  it('renders the theme toggle button', () => {
    render(<Header />)

    const themeToggle = screen.getByRole('button', { name: /switch to .* mode/i })
    expect(themeToggle).toBeInTheDocument()
  })

  it('renders the hamburger menu button on mobile', () => {
    render(<Header />)

    const hamburger = screen.getByRole('button', { name: /open menu/i })
    expect(hamburger).toBeInTheDocument()
  })
})
