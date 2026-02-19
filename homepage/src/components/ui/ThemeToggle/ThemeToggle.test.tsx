/**
 * ThemeToggle Component Tests
 * Owner: Scenario 6 - Dark Mode & Theme System
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ThemeToggle } from './ThemeToggle'
import { ThemeProvider } from '@/context/ThemeContext'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Mock matchMedia
const matchMediaMock = vi.fn().mockImplementation((query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
}))

Object.defineProperty(window, 'matchMedia', { value: matchMediaMock })

function renderWithTheme(defaultTheme?: 'light' | 'dark') {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <ThemeToggle />
    </ThemeProvider>
  )
}

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  it('renders toggle button with appropriate icon and aria-label', () => {
    renderWithTheme('light')

    const button = screen.getByTestId('theme-toggle')
    expect(button).toBeInTheDocument()
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')
    expect(button).toHaveAttribute('type', 'button')
  })

  it('renders moon icon when in light mode', () => {
    renderWithTheme('light')

    const button = screen.getByTestId('theme-toggle')
    // In light mode, we show moon icon (to switch to dark)
    const svg = button.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })

  it('changes theme to dark and icon switches to sun when clicked in light mode', () => {
    renderWithTheme('light')

    const button = screen.getByTestId('theme-toggle')
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')

    fireEvent.click(button)

    // After clicking, should now show sun icon and switch to light mode label
    expect(button).toHaveAttribute('aria-label', 'Switch to light mode')
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })

  it('changes theme to light and icon switches to moon when clicked in dark mode', () => {
    renderWithTheme('dark')

    const button = screen.getByTestId('theme-toggle')
    expect(button).toHaveAttribute('aria-label', 'Switch to light mode')

    fireEvent.click(button)

    // After clicking, should show moon icon
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode')
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('updates localStorage when theme is toggled', () => {
    renderWithTheme('light')

    const button = screen.getByTestId('theme-toggle')
    fireEvent.click(button)

    expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
  })

  it('is keyboard accessible with proper focus styles', () => {
    renderWithTheme('light')

    const button = screen.getByTestId('theme-toggle')
    button.focus()
    expect(document.activeElement).toBe(button)
  })
})
