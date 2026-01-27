/**
 * Homepage Theme Integration Tests
 * Owner: Scenario 7 - Theme Support and Persistence
 *
 * Tests for:
 * - Theme persistence in localStorage
 * - Theme switching without page reload
 * - Theme consistency across page navigation
 * - All DaisyUI theme options render correctly
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '../utils/test-utils'
import { render as rawRender } from '@testing-library/react'
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext'
import { MemoryRouter, Routes, Route, useNavigate } from 'react-router-dom'
import Home from '../../src/pages/Home'
import { GlassMorphismCard } from '../../src/components/GlassMorphismCard'
import { BackgroundEffect } from '../../src/components/BackgroundEffect'
import { ReactNode } from 'react'

// Available themes as defined in ThemeContext
const AVAILABLE_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const
type Theme = typeof AVAILABLE_THEMES[number]

// Mock localStorage for controlled testing
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
    _getStore: () => store,
    _setStore: (newStore: Record<string, string>) => {
      store = newStore
    },
  }
})()

// Helper component that exposes theme controls for testing
function ThemeTestControls({ children }: { children: ReactNode }) {
  const { theme, setTheme } = useTheme()
  return (
    <div>
      <div data-testid="current-theme">{theme}</div>
      <button
        data-testid="toggle-theme"
        onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
      >
        Toggle Theme
      </button>
      {AVAILABLE_THEMES.map((t) => (
        <button
          key={t}
          data-testid={`set-theme-${t}`}
          onClick={() => setTheme(t)}
        >
          Set {t}
        </button>
      ))}
      {children}
    </div>
  )
}

// Custom render with theme controls
function renderWithThemeControls(ui: ReactNode) {
  return render(
    <ThemeTestControls>
      {ui}
    </ThemeTestControls>
  )
}

describe('Homepage Theme Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.clear()
    localStorageMock._setStore({})
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    })
    // Reset document theme
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Render homepage with dark theme in localStorage', () => {
    it('displays homepage in dark mode when localStorage has dark theme', async () => {
      // Set dark theme in localStorage before rendering
      localStorageMock._setStore({ theme: 'dark' })

      renderWithThemeControls(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Verify localStorage was read
      expect(localStorageMock.getItem).toHaveBeenCalledWith('theme')

      // Verify document has dark theme attribute
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('renders all homepage sections correctly in dark mode', async () => {
      localStorageMock._setStore({ theme: 'dark' })

      renderWithThemeControls(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify main sections are rendered
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByTestId('background-effect')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Render homepage with light theme in localStorage', () => {
    it('displays homepage in light mode when localStorage has light theme', async () => {
      localStorageMock._setStore({ theme: 'light' })

      renderWithThemeControls(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('defaults to light theme when localStorage is empty', async () => {
      // localStorage is empty
      localStorageMock._setStore({})

      renderWithThemeControls(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      })
    })
  })

  describe('Test Case 3: Toggle theme from light to dark', () => {
    it('updates all components to dark theme without page reload', async () => {
      localStorageMock._setStore({ theme: 'light' })

      renderWithThemeControls(<Home />)

      // Verify initial light theme
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      })
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle to dark theme
      await act(async () => {
        fireEvent.click(screen.getByTestId('toggle-theme'))
      })

      // Verify theme changed to dark
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify localStorage was updated
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
    })

    it('maintains component state when theme changes', async () => {
      localStorageMock._setStore({ theme: 'light' })

      renderWithThemeControls(<Home />)

      // Get initial content
      const mainContent = screen.getByRole('main')
      expect(mainContent).toBeInTheDocument()

      // Toggle theme
      await act(async () => {
        fireEvent.click(screen.getByTestId('toggle-theme'))
      })

      // Content should still be present
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByTestId('background-effect')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Theme persistence across page reload', () => {
    it('persists theme preference in localStorage', async () => {
      localStorageMock._setStore({ theme: 'light' })

      const { unmount } = renderWithThemeControls(<Home />)

      // Change to dark theme
      await act(async () => {
        fireEvent.click(screen.getByTestId('set-theme-dark'))
      })

      // Verify localStorage was updated
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')

      // Unmount to simulate page unload
      unmount()

      // Simulate reload by reading from localStorage
      const persistedTheme = localStorageMock._getStore()['theme']
      expect(persistedTheme).toBe('dark')

      // Re-render (simulating page reload)
      localStorageMock._setStore({ theme: persistedTheme })
      renderWithThemeControls(<Home />)

      // Theme should be restored from localStorage
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
    })

    it('stores theme immediately on change', async () => {
      localStorageMock._setStore({ theme: 'light' })

      renderWithThemeControls(<Home />)

      // Change theme
      await act(async () => {
        fireEvent.click(screen.getByTestId('set-theme-cyberpunk'))
      })

      // Verify immediate localStorage update
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'cyberpunk')
    })
  })

  describe('Test Case 4 (Extended): All DaisyUI themes render correctly', () => {
    AVAILABLE_THEMES.forEach((themeName) => {
      it(`renders correctly with ${themeName} theme`, async () => {
        localStorageMock._setStore({ theme: themeName })

        renderWithThemeControls(<Home />)

        await waitFor(() => {
          expect(screen.getByTestId('current-theme')).toHaveTextContent(themeName)
        })

        // Verify document theme attribute
        expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)

        // Verify homepage sections are rendered
        expect(screen.getByRole('main')).toBeInTheDocument()
      })
    })

    it('can cycle through all available themes', async () => {
      localStorageMock._setStore({ theme: 'light' })

      renderWithThemeControls(<Home />)

      for (const themeName of AVAILABLE_THEMES) {
        await act(async () => {
          fireEvent.click(screen.getByTestId(`set-theme-${themeName}`))
        })

        await waitFor(() => {
          expect(screen.getByTestId('current-theme')).toHaveTextContent(themeName)
        })
        expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
      }
    })
  })

  describe('Test Case 7: Theme persists across navigation', () => {
    // Helper component that simulates dashboard navigation
    function MockDashboard() {
      const { theme } = useTheme()
      return (
        <div data-testid="dashboard">
          <h1>Dashboard</h1>
          <div data-testid="dashboard-theme">{theme}</div>
        </div>
      )
    }

    function AppWithRoutes() {
      const { theme, setTheme } = useTheme()
      const NavigationHelper = () => {
        const navigate = useNavigate()
        return (
          <>
            <div data-testid="current-theme">{theme}</div>
            <button
              data-testid="nav-to-dashboard"
              onClick={() => navigate('/dashboard')}
            >
              Go to Dashboard
            </button>
            <button
              data-testid="nav-to-home"
              onClick={() => navigate('/')}
            >
              Go to Home
            </button>
            <button
              data-testid="set-theme-dark"
              onClick={() => setTheme('dark')}
            >
              Set Dark
            </button>
          </>
        )
      }

      return (
        <>
          <NavigationHelper />
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<MockDashboard />} />
          </Routes>
        </>
      )
    }

    it('maintains theme when navigating from homepage to dashboard', async () => {
      localStorageMock._setStore({ theme: 'dark' })

      rawRender(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AppWithRoutes />
          </ThemeProvider>
        </MemoryRouter>
      )

      // Verify initial dark theme on homepage
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Navigate to dashboard
      await act(async () => {
        fireEvent.click(screen.getByTestId('nav-to-dashboard'))
      })

      // Verify theme persists on dashboard
      await waitFor(() => {
        expect(screen.getByTestId('dashboard')).toBeInTheDocument()
      })
      expect(screen.getByTestId('dashboard-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('maintains theme change during navigation', async () => {
      localStorageMock._setStore({ theme: 'light' })

      rawRender(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AppWithRoutes />
          </ThemeProvider>
        </MemoryRouter>
      )

      // Change theme to dark
      await act(async () => {
        fireEvent.click(screen.getByTestId('set-theme-dark'))
      })

      // Navigate to dashboard
      await act(async () => {
        fireEvent.click(screen.getByTestId('nav-to-dashboard'))
      })

      // Verify theme persists
      expect(screen.getByTestId('dashboard-theme')).toHaveTextContent('dark')

      // Navigate back to home
      await act(async () => {
        fireEvent.click(screen.getByTestId('nav-to-home'))
      })

      // Verify theme still persists
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })
  })

  describe('Test Case 5: GlassMorphismCard renders correctly in dark theme', () => {
    it('renders card with proper styling classes', () => {
      localStorageMock._setStore({ theme: 'dark' })

      render(
        <GlassMorphismCard>
          <span data-testid="card-content">Test Content</span>
        </GlassMorphismCard>
      )

      // Verify card content renders
      expect(screen.getByTestId('card-content')).toBeInTheDocument()
      expect(screen.getByTestId('card-content')).toHaveTextContent('Test Content')
    })

    it('card has backdrop blur and glass morphism classes', () => {
      localStorageMock._setStore({ theme: 'dark' })

      const { container } = render(
        <GlassMorphismCard>
          <span>Content</span>
        </GlassMorphismCard>
      )

      // GlassMorphismCard wraps content in a motion.div with backdrop-blur classes
      const card = container.firstChild as HTMLElement
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('rounded-2xl')
      expect(card).toHaveClass('shadow-xl')
    })

    it('card uses DaisyUI base colors that adapt to theme', () => {
      localStorageMock._setStore({ theme: 'dark' })

      const { container } = render(
        <GlassMorphismCard>
          <span>Content</span>
        </GlassMorphismCard>
      )

      const card = container.firstChild as HTMLElement
      // Uses bg-base-100/50 which adapts to theme
      expect(card.className).toContain('bg-base-100/50')
      // Uses border-base-300/50 which adapts to theme
      expect(card.className).toContain('border-base-300/50')
    })

    it('accepts custom className prop', () => {
      const { container } = render(
        <GlassMorphismCard className="custom-class">
          <span>Content</span>
        </GlassMorphismCard>
      )

      const card = container.firstChild as HTMLElement
      expect(card).toHaveClass('custom-class')
    })

    it('renders children content correctly in any theme', () => {
      AVAILABLE_THEMES.forEach((themeName) => {
        localStorageMock._setStore({ theme: themeName })

        const { unmount } = render(
          <GlassMorphismCard>
            <h2>Title</h2>
            <p>Description</p>
          </GlassMorphismCard>
        )

        expect(screen.getByText('Title')).toBeInTheDocument()
        expect(screen.getByText('Description')).toBeInTheDocument()

        unmount()
      })
    })
  })

  describe('Test Case 6: BackgroundEffect adapts to dark theme', () => {
    it('renders background effect container', () => {
      localStorageMock._setStore({ theme: 'dark' })

      render(<BackgroundEffect />)

      expect(screen.getByTestId('background-effect')).toBeInTheDocument()
    })

    it('has fixed positioning for full-screen background', () => {
      localStorageMock._setStore({ theme: 'dark' })

      render(<BackgroundEffect />)

      const container = screen.getByTestId('background-effect')
      expect(container).toHaveClass('fixed')
      expect(container).toHaveClass('inset-0')
      expect(container).toHaveClass('-z-10')
    })

    it('uses DaisyUI primary/secondary/accent colors that adapt to theme', () => {
      localStorageMock._setStore({ theme: 'dark' })

      const { container } = render(<BackgroundEffect />)

      const effectContainer = screen.getByTestId('background-effect')
      const gradientOrbs = effectContainer.querySelectorAll('div > div')

      // Background effect uses primary, secondary, and accent colors
      // These classes reference DaisyUI color variables that adapt per theme
      const classNames = Array.from(gradientOrbs).map((orb) => orb.className).join(' ')
      expect(classNames).toContain('bg-primary/20')
      expect(classNames).toContain('bg-secondary/20')
      expect(classNames).toContain('bg-accent/10')
    })

    it('renders correctly across all themes', () => {
      AVAILABLE_THEMES.forEach((themeName) => {
        localStorageMock._setStore({ theme: themeName })
        document.documentElement.setAttribute('data-theme', themeName)

        const { unmount } = render(<BackgroundEffect />)

        const container = screen.getByTestId('background-effect')
        expect(container).toBeInTheDocument()

        // Gradient orbs should be present in any theme
        const gradientOrbs = container.querySelectorAll('div > div')
        expect(gradientOrbs.length).toBeGreaterThanOrEqual(3) // Primary, secondary, accent orbs

        unmount()
      })
    })

    it('is non-interactive (pointer-events-none)', () => {
      render(<BackgroundEffect />)

      const container = screen.getByTestId('background-effect')
      expect(container).toHaveClass('pointer-events-none')
    })

    it('has overflow hidden to prevent scroll issues', () => {
      render(<BackgroundEffect />)

      const container = screen.getByTestId('background-effect')
      expect(container).toHaveClass('overflow-hidden')
    })
  })
})
