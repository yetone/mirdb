/**
 * Theme Toggle Component Tests.
 * Owner: Scenario 10 - Dark Mode Theme Toggle
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ThemeToggle } from './ThemeToggle'
import { ThemeProvider } from '../../context/ThemeContext'
import { THEME_STORAGE_KEY } from '../../utils/constants'

// Wrapper component for tests
function renderWithProvider(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      {ui}
    </ThemeProvider>
  )
}

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.classList.remove('dark')
  })

  it('renders theme toggle button', () => {
    renderWithProvider(<ThemeToggle />)

    expect(screen.getByTestId('theme-toggle')).toBeInTheDocument()
  })

  it('has accessible aria-label', () => {
    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')
  })

  it('shows moon icon in light mode', () => {
    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    const svg = button.querySelector('svg')
    expect(svg).toBeInTheDocument()
    // Moon icon has the specific moon path
    expect(svg?.innerHTML).toContain('21.752')
  })

  it('shows sun icon in dark mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    const svg = button.querySelector('svg')
    expect(svg).toBeInTheDocument()
    // Sun icon has the specific path pattern (12 3v2.25 for top ray)
    expect(svg?.innerHTML).toContain('12 3v2.25')
  })

  it('toggles from light to dark mode on click', () => {
    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')

    fireEvent.click(button)

    expect(button).toHaveAttribute('aria-label', 'Switch to light mode')
    expect(document.documentElement.classList.contains('dark')).toBe(true)
  })

  it('toggles from dark to light mode on click', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    expect(button).toHaveAttribute('aria-label', 'Switch to light mode')

    fireEvent.click(button)

    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')
    expect(document.documentElement.classList.contains('dark')).toBe(false)
  })

  it('saves preference to localStorage after toggle', () => {
    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    fireEvent.click(button)

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')

    fireEvent.click(button)

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light')
  })

  it('applies custom className', () => {
    renderWithProvider(<ThemeToggle className="custom-class" />)

    const button = screen.getByTestId('theme-toggle')
    expect(button.className).toContain('custom-class')
  })

  it('has screen reader text', () => {
    renderWithProvider(<ThemeToggle />)

    expect(screen.getByText('Switch to dark mode')).toBeInTheDocument()
  })

  it('has focus styles for keyboard accessibility', () => {
    renderWithProvider(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    expect(button.className).toContain('focus:outline-none')
    expect(button.className).toContain('focus:ring-2')
  })
})
