/**
 * BackgroundEffect component tests.
 * Scenario 13 - Component Integration
 *
 * Tests:
 * - Component renders with animated visual effects
 * - BackgroundEffect is integrated in Home page for visual consistency
 * - Theme-aware color classes
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { BackgroundEffect } from '../../../../src/components/common/BackgroundEffect'
import { Home } from '../../../../src/pages/Home'
import { ThemeProvider } from '../../../../src/contexts/ThemeContext'

// Helper to wrap components with necessary providers
function renderWithProviders(ui: React.ReactElement, { route = '/' } = {}) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <ThemeProvider>{ui}</ThemeProvider>
    </MemoryRouter>
  )
}

describe('BackgroundEffect', () => {
  it('renders background container', () => {
    const { container } = render(<BackgroundEffect />)

    // Should have a fixed position container
    const bgContainer = container.firstChild as HTMLElement
    expect(bgContainer).toHaveClass('fixed')
    expect(bgContainer).toHaveClass('inset-0')
    expect(bgContainer).toHaveClass('-z-10')
  })

  it('renders animated gradient orbs', () => {
    const { container } = render(<BackgroundEffect />)

    // Should have multiple animated orbs with blur effects
    const orbs = container.querySelectorAll('.blur-3xl')
    expect(orbs.length).toBeGreaterThanOrEqual(3)
  })

  it('uses theme-aware color classes', () => {
    const { container } = render(<BackgroundEffect />)

    // Should use theme color variables
    const primaryOrb = container.querySelector('.bg-primary\\/20')
    const secondaryOrb = container.querySelector('.bg-secondary\\/20')
    const accentOrb = container.querySelector('.bg-accent\\/10')

    expect(primaryOrb).toBeInTheDocument()
    expect(secondaryOrb).toBeInTheDocument()
    expect(accentOrb).toBeInTheDocument()
  })

  it('applies pulse animation to orbs', () => {
    const { container } = render(<BackgroundEffect />)

    // Should have animate-pulse class for subtle animations
    const animatedOrbs = container.querySelectorAll('.animate-pulse')
    expect(animatedOrbs.length).toBeGreaterThanOrEqual(3)
  })

  it('renders grid pattern overlay', () => {
    const { container } = render(<BackgroundEffect />)

    // Should have a grid pattern element
    const gridPattern = container.querySelector('[class*="bg-\\[linear-gradient"]')
    expect(gridPattern).toBeInTheDocument()
  })

  it('uses performance-optimized CSS transforms', () => {
    const { container } = render(<BackgroundEffect />)

    // Center orb should use CSS transforms for performance
    const centerOrb = container.querySelector('.-translate-x-1\\/2')
    expect(centerOrb).toBeInTheDocument()
    expect(centerOrb).toHaveClass('-translate-y-1/2')
  })
})

describe('BackgroundEffect Integration - Home Page', () => {
  it('BackgroundEffect component is integrated for visual consistency', () => {
    const { container } = renderWithProviders(<Home />)

    // Home page should include BackgroundEffect
    // BackgroundEffect renders a fixed, full-screen background
    const bgElement = container.querySelector('.fixed.inset-0.-z-10')
    expect(bgElement).toBeInTheDocument()

    // Should have the gradient orbs
    const orbs = container.querySelectorAll('.blur-3xl')
    expect(orbs.length).toBeGreaterThanOrEqual(3)
  })

  it('BackgroundEffect is positioned behind content', () => {
    const { container } = renderWithProviders(<Home />)

    // Background should have negative z-index
    const bgElement = container.querySelector('.fixed.inset-0.-z-10')
    expect(bgElement).toBeInTheDocument()
    expect(bgElement).toHaveClass('-z-10')
  })

  it('Home page content is visible above BackgroundEffect', () => {
    const { container } = renderWithProviders(<Home />)

    // Hero section content should be visible
    expect(screen.getByText('Shorten URLs, Track Clicks, Grow Your Reach')).toBeInTheDocument()

    // Navbar (main navigation) should be visible - it has the 'navbar' class
    const navbar = container.querySelector('nav.navbar')
    expect(navbar).toBeInTheDocument()
  })
})
