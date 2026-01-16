import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ThemeProvider, useTheme, ThemePreference } from './ThemeContext'

// Test component that exposes theme context
function ThemeTestComponent() {
  const { theme, themePreference, setThemePreference } = useTheme()
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="theme-preference">{themePreference}</span>
      <button
        data-testid="set-light"
        onClick={() => setThemePreference('light')}
      >
        Light
      </button>
      <button
        data-testid="set-dark"
        onClick={() => setThemePreference('dark')}
      >
        Dark
      </button>
      <button
        data-testid="set-system"
        onClick={() => setThemePreference('system')}
      >
        System
      </button>
      <button
        data-testid="set-cyberpunk"
        onClick={() => setThemePreference('cyberpunk')}
      >
        Cyberpunk
      </button>
      <button
        data-testid="set-synthwave"
        onClick={() => setThemePreference('synthwave')}
      >
        Synthwave
      </button>
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
  })

  describe('Initial State', () => {
    it('defaults to system preference when no stored value', () => {
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
    })

    it('loads stored theme preference from localStorage', () => {
      localStorage.setItem('theme-preference', 'dark')

      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('dark')
    })

    it('resolves system preference to actual theme', () => {
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      // Our test setup mocks matchMedia to return false (light mode)
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })
  })

  describe('Theme Persistence in localStorage', () => {
    it('stores light theme preference in localStorage', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-light'))
      expect(localStorage.getItem('theme-preference')).toBe('light')
    })

    it('stores dark theme preference in localStorage', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-dark'))
      expect(localStorage.getItem('theme-preference')).toBe('dark')
    })

    it('stores cyberpunk theme preference in localStorage', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-cyberpunk'))
      expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')
    })

    it('stores synthwave theme preference in localStorage', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-synthwave'))
      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
    })

    it('theme preference persists across component remounts', async () => {
      const user = userEvent.setup()
      const { unmount } = render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-dark'))
      unmount()

      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      expect(screen.getByTestId('theme-preference')).toHaveTextContent('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })
  })

  describe('Document Attribute Updates', () => {
    it('sets data-theme attribute on document when theme changes', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-dark'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('updates data-theme attribute for all theme variants', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-light'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      await user.click(screen.getByTestId('set-dark'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      await user.click(screen.getByTestId('set-cyberpunk'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      await user.click(screen.getByTestId('set-synthwave'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })

  describe('Theme Resolution', () => {
    it('resolves light preference to light theme', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-light'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('resolves dark preference to dark theme', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-dark'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('resolves cyberpunk preference to cyberpunk theme', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-cyberpunk'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
    })

    it('resolves synthwave preference to synthwave theme', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      await user.click(screen.getByTestId('set-synthwave'))
      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')
    })
  })

  describe('useTheme Hook Fallback', () => {
    it('returns fallback values when used outside provider', () => {
      // Render without provider
      render(<ThemeTestComponent />)

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
    })
  })

  describe('Multiple Theme Variants Support', () => {
    it('supports light, dark, cyberpunk, and synthwave themes', async () => {
      const user = userEvent.setup()
      render(
        <ThemeProvider>
          <ThemeTestComponent />
        </ThemeProvider>
      )

      const themes: ThemePreference[] = ['light', 'dark', 'cyberpunk', 'synthwave']

      for (const theme of themes) {
        await user.click(screen.getByTestId(`set-${theme}`))
        expect(screen.getByTestId('current-theme')).toHaveTextContent(theme)
        expect(localStorage.getItem('theme-preference')).toBe(theme)
      }
    })
  })
})
