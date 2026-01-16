import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ThemeToggle from './ThemeToggle'
import { ThemeProvider } from '../contexts/ThemeContext'

// Helper to render with ThemeProvider
const renderWithTheme = (component: React.ReactNode) => {
  return render(
    <ThemeProvider>
      {component}
    </ThemeProvider>
  )
}

describe('ThemeToggle', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset data-theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('renders without crashing', () => {
    renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)
    expect(screen.getByTestId('theme-toggle')).toBeInTheDocument()
  })

  it('displays theme toggle button', () => {
    renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)
    expect(screen.getByTestId('theme-toggle-button')).toBeInTheDocument()
  })

  it('displays dropdown trigger', () => {
    renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)
    expect(screen.getByTestId('theme-dropdown-trigger')).toBeInTheDocument()
  })

  it('displays dropdown menu with all theme options', () => {
    renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)
    const menu = screen.getByTestId('theme-dropdown-menu')
    expect(menu).toBeInTheDocument()

    // Check all theme options are present
    expect(screen.getByTestId('theme-option-system')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
    expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
  })

  it('has correct ARIA attributes for accessibility', () => {
    renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

    const toggleButton = screen.getByTestId('theme-toggle-button')
    expect(toggleButton).toHaveAttribute('aria-label')

    const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
    expect(dropdownTrigger).toHaveAttribute('aria-haspopup', 'listbox')

    const menu = screen.getByTestId('theme-dropdown-menu')
    expect(menu).toHaveAttribute('role', 'listbox')
  })

  describe('Theme cycling with toggle button', () => {
    it('cycles through themes when toggle button is clicked', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      const toggleButton = screen.getByTestId('theme-toggle-button')

      // Default is system, click to go to light
      await user.click(toggleButton)
      expect(localStorage.getItem('theme-preference')).toBe('light')

      // Click to go to dark
      await user.click(toggleButton)
      expect(localStorage.getItem('theme-preference')).toBe('dark')

      // Click to go to cyberpunk
      await user.click(toggleButton)
      expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')

      // Click to go to synthwave
      await user.click(toggleButton)
      expect(localStorage.getItem('theme-preference')).toBe('synthwave')

      // Click to go back to system
      await user.click(toggleButton)
      expect(localStorage.getItem('theme-preference')).toBe('system')
    })
  })

  describe('Theme selection from dropdown', () => {
    it('selects light theme when clicking light option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      await user.click(screen.getByTestId('theme-option-light'))

      expect(localStorage.getItem('theme-preference')).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('selects dark theme when clicking dark option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      await user.click(screen.getByTestId('theme-option-dark'))

      expect(localStorage.getItem('theme-preference')).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('selects cyberpunk theme when clicking cyberpunk option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('selects synthwave theme when clicking synthwave option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })

  describe('Toggle theme from light to dark', () => {
    it('updates homepage to dark theme colors', async () => {
      const user = userEvent.setup()

      // Set initial theme to light
      localStorage.setItem('theme-preference', 'light')

      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      // Select dark theme
      await user.click(screen.getByTestId('theme-option-dark'))

      // Verify localStorage and DOM attribute are updated
      expect(localStorage.getItem('theme-preference')).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Toggle theme from dark to light', () => {
    it('updates homepage to light theme colors', async () => {
      const user = userEvent.setup()

      // Set initial theme to dark
      localStorage.setItem('theme-preference', 'dark')

      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      // Select light theme
      await user.click(screen.getByTestId('theme-option-light'))

      // Verify localStorage and DOM attribute are updated
      expect(localStorage.getItem('theme-preference')).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('Theme shows selected state', () => {
    it('marks the currently selected theme option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<ThemeToggle data-testid="theme-toggle" />)

      // Select dark theme
      await user.click(screen.getByTestId('theme-option-dark'))

      // Check dark option is marked as selected
      const darkOption = screen.getByTestId('theme-option-dark')
      expect(darkOption).toHaveAttribute('aria-selected', 'true')

      // Check light option is not selected
      const lightOption = screen.getByTestId('theme-option-light')
      expect(lightOption).toHaveAttribute('aria-selected', 'false')
    })
  })
})
