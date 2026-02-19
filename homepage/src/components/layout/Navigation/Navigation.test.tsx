/**
 * Navigation Component Unit Tests
 * Owner: Scenario 2 - Navigation & Header
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Navigation, MobileMenu } from './Navigation'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

describe('Navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    document.body.innerHTML = `
      <div id="features"></div>
      <div id="quick-start"></div>
      <div id="usage"></div>
    `
  })

  it('renders all navigation links', () => {
    render(<Navigation />)

    // Links have role="menuitem" for accessible navigation
    expect(screen.getByRole('menuitem', { name: /features/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /quick start/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /usage/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /github/i })).toBeInTheDocument()
  })

  it('has proper navigation accessibility attributes', () => {
    render(<Navigation />)

    const nav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(nav).toBeInTheDocument()

    const menubar = screen.getByRole('menubar')
    expect(menubar).toBeInTheDocument()
  })

  it('calls scrollIntoView when internal link is clicked', () => {
    render(<Navigation />)

    const featuresLink = screen.getByRole('menuitem', { name: /features/i })
    fireEvent.click(featuresLink)

    const featuresSection = document.getElementById('features')
    expect(featuresSection?.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
  })

  it('calls onNavClick callback when link is clicked', () => {
    const onNavClick = vi.fn()
    render(<Navigation onNavClick={onNavClick} />)

    const featuresLink = screen.getByRole('menuitem', { name: /features/i })
    fireEvent.click(featuresLink)

    expect(onNavClick).toHaveBeenCalled()
  })

  it('external links have proper attributes', () => {
    render(<Navigation />)

    const githubLink = screen.getByRole('menuitem', { name: /github/i })
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('renders with mobile styling when isMobile is true', () => {
    const { container } = render(<Navigation isMobile />)

    // Check that nav element exists (we can't easily test CSS classes in jsdom)
    const nav = container.querySelector('nav')
    expect(nav).toBeInTheDocument()
  })
})

describe('MobileMenu', () => {
  const onClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    document.body.innerHTML = `
      <div id="features"></div>
      <div id="quick-start"></div>
      <div id="usage"></div>
    `
  })

  it('renders all navigation links when open', () => {
    render(<MobileMenu isOpen={true} onClose={onClose} />)

    // Links have role="menuitem" for accessible navigation
    expect(screen.getByRole('menuitem', { name: /features/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /quick start/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /usage/i })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: /github/i })).toBeInTheDocument()
  })

  it('has proper dialog accessibility attributes when open', () => {
    render(<MobileMenu isOpen={true} onClose={onClose} />)

    const dialog = screen.getByRole('dialog')
    expect(dialog).toBeInTheDocument()
    expect(dialog).toHaveAttribute('aria-modal', 'true')
    expect(dialog).toHaveAttribute('aria-label', 'Mobile navigation menu')
  })

  it('is hidden when closed', () => {
    const { container } = render(<MobileMenu isOpen={false} onClose={onClose} />)

    // When closed, the dialog has aria-hidden="true" which makes it inaccessible to role queries
    const dialog = container.querySelector('[role="dialog"]')
    expect(dialog).toHaveAttribute('aria-hidden', 'true')
  })

  it('calls onClose when Escape key is pressed', () => {
    render(<MobileMenu isOpen={true} onClose={onClose} />)

    fireEvent.keyDown(document, { key: 'Escape' })

    expect(onClose).toHaveBeenCalled()
  })

  it('calls onClose when clicking outside the menu', () => {
    render(
      <div>
        <div data-testid="outside">Outside</div>
        <MobileMenu isOpen={true} onClose={onClose} />
      </div>
    )

    const outside = screen.getByTestId('outside')
    fireEvent.mouseDown(outside)

    expect(onClose).toHaveBeenCalled()
  })

  it('calls onClose and scrolls when internal link is clicked', () => {
    render(<MobileMenu isOpen={true} onClose={onClose} />)

    const featuresLink = screen.getByRole('menuitem', { name: /features/i })
    fireEvent.click(featuresLink)

    const featuresSection = document.getElementById('features')
    expect(featuresSection?.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
    expect(onClose).toHaveBeenCalled()
  })

  it('links are focusable when menu is open', () => {
    render(<MobileMenu isOpen={true} onClose={onClose} />)

    const links = screen.getAllByRole('menuitem')
    links.forEach((link) => {
      expect(link).toHaveAttribute('tabIndex', '0')
    })
  })

  it('links are not focusable when menu is closed', () => {
    const { container } = render(<MobileMenu isOpen={false} onClose={onClose} />)

    // When closed, links are inside aria-hidden element, so use querySelector
    const links = container.querySelectorAll('[role="dialog"] a')
    links.forEach((link) => {
      expect(link).toHaveAttribute('tabIndex', '-1')
    })
  })
})
