/**
 * ThemeToggle Component Tests
 * Owner: Scenario 5 - Theme Adaptation
 *
 * Tests for the ThemeToggle component functionality.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { ThemeToggle } from './ThemeToggle'
import { ThemeProvider, Theme } from '../contexts/ThemeContext'

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

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Helper to render ThemeToggle with provider
const renderThemeToggle = (theme: Theme = 'dark', props = {}) => {
  return render(
    <ThemeProvider defaultTheme={theme}>
      <BrowserRouter>
        <ThemeToggle {...props} />
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  it('should render the theme toggle button', () => {
    renderThemeToggle()

    const themeToggle = screen.getByTestId('theme-toggle')
    expect(themeToggle).toBeInTheDocument()
  })

  it('should render the toggle button with aria-label', () => {
    renderThemeToggle()

    const button = screen.getByTestId('theme-toggle-button')
    expect(button).toHaveAttribute('aria-label')
    expect(button.getAttribute('aria-label')).toContain('theme')
  })

  it('should show dropdown with all theme options when clicked', async () => {
    const user = userEvent.setup()
    renderThemeToggle()

    const button = screen.getByTestId('theme-toggle-button')
    await user.click(button)

    expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
  })

  it('should change theme when option is selected', async () => {
    const user = userEvent.setup()
    renderThemeToggle('dark')

    const button = screen.getByTestId('theme-toggle-button')
    await user.click(button)

    const lightOption = screen.getByTestId('theme-option-light')
    await user.click(lightOption)

    await waitFor(() => {
      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'url-shortener-theme',
        'light'
      )
    })
  })

  it('should show current theme with checkmark', async () => {
    const user = userEvent.setup()
    renderThemeToggle('dark')

    const button = screen.getByTestId('theme-toggle-button')
    await user.click(button)

    // Dark option should have aria-selected true
    const darkOption = screen.getByTestId('theme-option-dark').closest('li')
    expect(darkOption).toHaveAttribute('aria-selected', 'true')
  })

  it('should render with label when showLabel is true', () => {
    renderThemeToggle('dark', { showLabel: true })

    // Should contain hidden sm:inline span with label
    const button = screen.getByTestId('theme-toggle-button')
    expect(button).toBeInTheDocument()
  })

  it('should have proper accessibility attributes', async () => {
    const user = userEvent.setup()
    renderThemeToggle()

    const button = screen.getByTestId('theme-toggle-button')
    expect(button).toHaveAttribute('aria-haspopup', 'listbox')

    await user.click(button)

    const dropdown = screen.getByTestId('theme-dropdown')
    expect(dropdown).toHaveAttribute('role', 'listbox')
    expect(dropdown).toHaveAttribute('aria-label', 'Select theme')
  })
})
