/**
 * Header Component Unit Tests
 * Owner: Scenario 2 - Navigation & Header
 *
 * Test Case 1: Render Header component
 * Expected: Header renders with logo, nav links, and theme toggle
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Header } from './Header'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock window.scrollTo
Object.defineProperty(window, 'scrollTo', {
  value: vi.fn(),
  writable: true,
})

describe('Header', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders header with logo', () => {
    render(<Header />)

    // Check logo image is present
    const logoImg = screen.getByRole('img', { hidden: true })
    expect(logoImg).toBeInTheDocument()
    expect(logoImg).toHaveAttribute('src', '/assets/logo.gif')

    // Check logo text
    expect(screen.getByText('MirDB')).toBeInTheDocument()
  })

  it('renders navigation links', () => {
    render(<Header />)

    // Check all nav links are present in the main navigation
    const mainNav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(mainNav.querySelector('a[href="#features"]')).toBeInTheDocument()
    expect(mainNav.querySelector('a[href="#quick-start"]')).toBeInTheDocument()
    expect(mainNav.querySelector('a[href="#usage"]')).toBeInTheDocument()
    expect(mainNav.querySelector('a[href="https://github.com/yetone/mirdb"]')).toBeInTheDocument()
  })

  it('renders theme toggle placeholder slot', () => {
    render(<Header />)

    // Check theme toggle slot exists for Scenario 6
    const themeToggleSlot = screen.getByTestId('theme-toggle-slot')
    expect(themeToggleSlot).toBeInTheDocument()
  })

  it('renders hamburger button for mobile menu', () => {
    render(<Header />)

    const hamburgerButton = screen.getByTestId('hamburger-button')
    expect(hamburgerButton).toBeInTheDocument()
    expect(hamburgerButton).toHaveAttribute('aria-label', 'Open navigation menu')
    expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')
  })

  it('toggles mobile menu when hamburger button is clicked', () => {
    render(<Header />)

    const hamburgerButton = screen.getByTestId('hamburger-button')

    // Initially menu is closed
    expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')

    // Click to open
    fireEvent.click(hamburgerButton)
    expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true')
    expect(hamburgerButton).toHaveAttribute('aria-label', 'Close navigation menu')

    // Click to close
    fireEvent.click(hamburgerButton)
    expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')
    expect(hamburgerButton).toHaveAttribute('aria-label', 'Open navigation menu')
  })

  it('has proper accessibility attributes on header', () => {
    render(<Header />)

    const header = screen.getByRole('banner')
    expect(header).toBeInTheDocument()
  })

  it('renders main navigation with correct aria-label', () => {
    render(<Header />)

    const mainNav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(mainNav).toBeInTheDocument()
  })

  it('logo click scrolls to top of page', () => {
    render(<Header />)

    const logoLink = screen.getByRole('link', { name: /go to top/i })
    fireEvent.click(logoLink)

    expect(window.scrollTo).toHaveBeenCalledWith({ top: 0, behavior: 'smooth' })
  })

  it('GitHub link has external link attributes', () => {
    render(<Header />)

    const mainNav = screen.getByRole('navigation', { name: 'Main navigation' })
    const githubLink = mainNav.querySelector('a[href="https://github.com/yetone/mirdb"]')
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('internal nav links have correct href attributes', () => {
    render(<Header />)

    const mainNav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(mainNav.querySelector('a[href="#features"]')).toBeInTheDocument()
    expect(mainNav.querySelector('a[href="#quick-start"]')).toBeInTheDocument()
    expect(mainNav.querySelector('a[href="#usage"]')).toBeInTheDocument()
  })
})
