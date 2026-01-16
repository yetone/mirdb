import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { ThemeProvider, useTheme } from './ThemeContext'

// Helper component to test the hook
function ThemeConsumer() {
  const { theme, themePreference, setThemePreference } = useTheme()
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="theme-preference">{themePreference}</span>
      <button data-testid="set-light" onClick={() => setThemePreference('light')}>Light</button>
      <button data-testid="set-dark" onClick={() => setThemePreference('dark')}>Dark</button>
      <button data-testid="set-cyberpunk" onClick={() => setThemePreference('cyberpunk')}>Cyberpunk</button>
      <button data-testid="set-synthwave" onClick={() => setThemePreference('synthwave')}>Synthwave</button>
      <button data-testid="set-system" onClick={() => setThemePreference('system')}>System</button>
    </div>
  )
}

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('ThemeProvider', () => {
    it('provides default system theme preference', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
    })

    it('resolves system preference to light by default (matchMedia mock returns false)', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      // Our test setup mocks matchMedia to return matches: false (light mode)
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('loads stored theme preference from localStorage', () => {
      localStorage.setItem('theme-preference', 'dark')

      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('sets data-theme attribute on document.documentElement', () => {
      localStorage.setItem('theme-preference', 'cyberpunk')

      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  describe('setThemePreference', () => {
    it('updates theme when setThemePreference is called', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-dark').click()
      })

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('persists theme preference to localStorage', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-synthwave').click()
      })

      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
    })

    it('updates data-theme attribute when theme changes', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-cyberpunk').click()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  describe('Theme persistence', () => {
    it('theme preference persists in localStorage on reload simulation', () => {
      // First render - set theme
      const { unmount } = render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-dark').click()
      })

      expect(localStorage.getItem('theme-preference')).toBe('dark')

      // Unmount (simulate page close)
      unmount()

      // Re-render (simulate reload)
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      // Theme should be restored from localStorage
      expect(screen.getByTestId('theme-preference')).toHaveTextContent('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Multiple theme variants support', () => {
    it('supports light theme', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-light').click()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('supports dark theme', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-dark').click()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('supports cyberpunk theme', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-cyberpunk').click()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('supports synthwave theme', () => {
      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      act(() => {
        screen.getByTestId('set-synthwave').click()
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })

  describe('useTheme hook fallback', () => {
    it('returns fallback values when used outside ThemeProvider', () => {
      // Render without ThemeProvider
      render(<ThemeConsumer />)

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
    })
  })

  describe('Invalid localStorage values', () => {
    it('defaults to system when localStorage has invalid value', () => {
      localStorage.setItem('theme-preference', 'invalid-theme')

      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
    })
  })
})
