/**
 * Unit tests for MobileMenu component
 * Scenario 1 - Header and Navigation
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MobileMenu, HamburgerButton } from '../../../../src/components/layout/MobileMenu'
import type { NavItem } from '../../../../src/types'

const mockNavItems: NavItem[] = [
  { label: 'Documentation', href: '#documentation' },
  { label: 'Examples', href: '#examples' },
  { label: 'GitHub', href: 'https://github.com/yetone/mirdb', external: true },
  { label: 'About', href: '#about' },
]

describe('MobileMenu', () => {
  it('renders mobile menu with all navigation links when open', () => {
    const onClose = vi.fn()
    render(<MobileMenu items={mockNavItems} isOpen={true} onClose={onClose} />)

    expect(screen.getByRole('link', { name: 'Documentation' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Examples' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'GitHub' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'About' })).toBeInTheDocument()
  })

  it('renders GitHub link with security attributes', () => {
    const onClose = vi.fn()
    render(<MobileMenu items={mockNavItems} isOpen={true} onClose={onClose} />)

    const githubLink = screen.getByRole('link', { name: 'GitHub' })
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(githubLink).toHaveAttribute('target', '_blank')
  })

  it('calls onClose when a link is clicked', () => {
    const onClose = vi.fn()
    render(<MobileMenu items={mockNavItems} isOpen={true} onClose={onClose} />)

    const docLink = screen.getByRole('link', { name: 'Documentation' })
    fireEvent.click(docLink)

    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('calls onClose when overlay is clicked', () => {
    const onClose = vi.fn()
    render(<MobileMenu items={mockNavItems} isOpen={true} onClose={onClose} />)

    const overlay = document.querySelector('.mobile-menu__overlay')
    expect(overlay).toBeInTheDocument()

    fireEvent.click(overlay!)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('does not show overlay when menu is closed', () => {
    const onClose = vi.fn()
    render(<MobileMenu items={mockNavItems} isOpen={false} onClose={onClose} />)

    const overlay = document.querySelector('.mobile-menu__overlay')
    expect(overlay).not.toBeInTheDocument()
  })
})

describe('HamburgerButton', () => {
  it('renders hamburger button with correct aria-label when closed', () => {
    const onClick = vi.fn()
    render(<HamburgerButton isOpen={false} onClick={onClick} />)

    const button = screen.getByRole('button', { name: 'Open menu' })
    expect(button).toBeInTheDocument()
    expect(button).toHaveAttribute('aria-expanded', 'false')
  })

  it('renders hamburger button with correct aria-label when open', () => {
    const onClick = vi.fn()
    render(<HamburgerButton isOpen={true} onClick={onClick} />)

    const button = screen.getByRole('button', { name: 'Close menu' })
    expect(button).toBeInTheDocument()
    expect(button).toHaveAttribute('aria-expanded', 'true')
  })

  it('calls onClick when clicked', () => {
    const onClick = vi.fn()
    render(<HamburgerButton isOpen={false} onClick={onClick} />)

    const button = screen.getByRole('button', { name: 'Open menu' })
    fireEvent.click(button)

    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
