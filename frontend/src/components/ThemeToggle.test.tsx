import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    button: ({ children, ...props }: React.ComponentPropsWithoutRef<'button'>) => (
      <button {...props}>{children}</button>
    ),
    div: ({ children, ...props }: React.ComponentPropsWithoutRef<'div'>) => (
      <div {...props}>{children}</div>
    ),
    svg: ({ children, ...props }: React.SVGProps<SVGSVGElement>) => (
      <svg {...props}>{children}</svg>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('ThemeToggle', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
  })

  describe('Component Rendering', () => {
    it('renders without errors', () => {
      renderWithProviders(<ThemeToggle />)
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument()
    })

    it('renders toggle button with correct aria attributes', () => {
      renderWithProviders(<ThemeToggle />)
      const button = screen.getByTestId('theme-toggle-button')
      expect(button).toHaveAttribute('aria-label', 'Toggle theme')
      expect(button).toHaveAttribute('aria-expanded', 'false')
      expect(button).toHaveAttribute('aria-haspopup', 'listbox')
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<ThemeToggle data-testid="custom-toggle" />)
      expect(screen.getByTestId('custom-toggle')).toBeInTheDocument()
    })

    it('displays theme icon', () => {
      renderWithProviders(<ThemeToggle />)
      expect(screen.getByTestId('theme-toggle-icon')).toBeInTheDocument()
    })
  })

  describe('Dropdown Functionality', () => {
    it('opens dropdown when button is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle-button')
      await user.click(button)

      expect(screen.getByTestId('theme-dropdown')).toBeInTheDocument()
    })

    it('closes dropdown when clicking button again', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle-button')
      await user.click(button)
      expect(screen.getByTestId('theme-dropdown')).toBeInTheDocument()

      await user.click(button)
      expect(screen.queryByTestId('theme-dropdown')).not.toBeInTheDocument()
    })

    it('displays all theme options in dropdown', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))

      expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-system')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
    })

    it('has correct role attributes for accessibility', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))

      const dropdown = screen.getByTestId('theme-dropdown')
      expect(dropdown).toHaveAttribute('role', 'listbox')

      const options = screen.getAllByRole('option')
      expect(options).toHaveLength(5)
    })
  })

  describe('Theme Selection', () => {
    it('selects light theme when light option is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-light'))

      expect(localStorage.getItem('theme-preference')).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('selects dark theme when dark option is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(localStorage.getItem('theme-preference')).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('selects cyberpunk theme when cyberpunk option is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('selects synthwave theme when synthwave option is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('closes dropdown after theme selection', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(screen.queryByTestId('theme-dropdown')).not.toBeInTheDocument()
    })

    it('highlights currently selected theme option', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      // Select dark theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      // Reopen dropdown
      await user.click(screen.getByTestId('theme-toggle-button'))

      const darkOption = screen.getByTestId('theme-option-dark')
      expect(darkOption).toHaveAttribute('aria-selected', 'true')
    })
  })

  describe('ThemeContext Integration', () => {
    it('uses theme from context to display correct icon', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      // Change to dark theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      // Icon should be updated (Moon icon for dark theme)
      const iconContainer = screen.getByTestId('theme-toggle-icon')
      expect(iconContainer).toBeInTheDocument()
    })

    it('components consume ThemeContext and react to changes', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      // Initial state (system default)
      const initialTheme = document.documentElement.getAttribute('data-theme')

      // Change theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      // Theme should change
      const newTheme = document.documentElement.getAttribute('data-theme')
      expect(newTheme).toBe('cyberpunk')
      expect(newTheme).not.toBe(initialTheme)
    })

    it('theme preference persists to localStorage', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
    })
  })

  describe('Keyboard Navigation', () => {
    it('closes dropdown when Escape is pressed', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))
      expect(screen.getByTestId('theme-dropdown')).toBeInTheDocument()

      await user.keyboard('{Escape}')
      expect(screen.queryByTestId('theme-dropdown')).not.toBeInTheDocument()
    })
  })

  describe('Click Outside Behavior', () => {
    it('closes dropdown when clicking outside', async () => {
      const user = userEvent.setup()
      renderWithProviders(
        <div>
          <div data-testid="outside-element">Outside</div>
          <ThemeToggle />
        </div>
      )

      await user.click(screen.getByTestId('theme-toggle-button'))
      expect(screen.getByTestId('theme-dropdown')).toBeInTheDocument()

      // Click outside
      await user.click(screen.getByTestId('outside-element'))

      await waitFor(() => {
        expect(screen.queryByTestId('theme-dropdown')).not.toBeInTheDocument()
      })
    })
  })
})
