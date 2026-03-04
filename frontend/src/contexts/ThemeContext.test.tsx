/**
 * ThemeContext Tests
 * Owner: Scenario 5 - Theme Adaptation
 *
 * Tests for the ThemeContext and ThemeProvider functionality.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import { ThemeProvider, useTheme, Theme } from './ThemeContext'

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

// Test component that uses the theme context
function TestComponent() {
  const { theme, setTheme, toggleTheme, availableThemes } = useTheme()
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="available-themes">{availableThemes.join(',')}</span>
      <button data-testid="set-light" onClick={() => setTheme('light')}>
        Set Light
      </button>
      <button data-testid="set-dark" onClick={() => setTheme('dark')}>
        Set Dark
      </button>
      <button data-testid="set-cyberpunk" onClick={() => setTheme('cyberpunk')}>
        Set Cyberpunk
      </button>
      <button data-testid="set-synthwave" onClick={() => setTheme('synthwave')}>
        Set Synthwave
      </button>
      <button data-testid="toggle" onClick={toggleTheme}>
        Toggle
      </button>
    </div>
  )
}

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    // Reset the data-theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  describe('ThemeProvider', () => {
    it('should provide default theme when no localStorage value exists', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('should use localStorage value if it exists', () => {
      localStorageMock.getItem.mockReturnValue('light')

      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('should provide all available themes', () => {
      render(
        <ThemeProvider>
          <TestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('available-themes')).toHaveTextContent(
        'light,dark,cyberpunk,synthwave'
      )
    })
  })

  describe('setTheme', () => {
    it('should update theme to light', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      const button = screen.getByTestId('set-light')
      await act(async () => {
        button.click()
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('should update theme to dark', async () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      const button = screen.getByTestId('set-dark')
      await act(async () => {
        button.click()
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('should update theme to cyberpunk', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      const button = screen.getByTestId('set-cyberpunk')
      await act(async () => {
        button.click()
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
    })

    it('should update theme to synthwave', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      const button = screen.getByTestId('set-synthwave')
      await act(async () => {
        button.click()
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')
    })

    it('should save theme to localStorage', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <TestComponent />
        </ThemeProvider>
      )

      const button = screen.getByTestId('set-light')
      await act(async () => {
        button.click()
      })

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'url-shortener-theme',
        'light'
      )
    })
  })

  describe('toggleTheme', () => {
    it('should cycle through themes in order', async () => {
      render(
        <ThemeProvider defaultTheme="light">
          <TestComponent />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('toggle')

      // light -> dark
      await act(async () => {
        toggleButton.click()
      })
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      // dark -> cyberpunk
      await act(async () => {
        toggleButton.click()
      })
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')

      // cyberpunk -> synthwave
      await act(async () => {
        toggleButton.click()
      })
      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')

      // synthwave -> light (wrap around)
      await act(async () => {
        toggleButton.click()
      })
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })
  })

  describe('useTheme hook', () => {
    it('should throw error when used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(<TestComponent />)
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleSpy.mockRestore()
    })
  })
})
