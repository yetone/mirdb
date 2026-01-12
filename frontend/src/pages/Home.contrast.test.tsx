import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import { AuthProvider } from '../contexts/AuthContext'
import {
  hexToRgb,
  parseColor,
  getContrastRatio,
  meetsWcagAA,
  meetsWcagAALargeText,
  DAISYUI_THEMES,
  ThemeName,
} from '../utils/colorContrast'

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

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <div {...rest}>{children}</div>
    },
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <section {...rest}>{children}</section>
    },
    ul: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <ul {...rest}>{children}</ul>
    },
    ol: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <ol {...rest}>{children}</ol>
    },
    li: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <li {...rest}>{children}</li>
    },
    svg: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <svg {...rest}>{children}</svg>
    },
    circle: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <circle {...rest}>{children}</circle>
    },
    rect: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <rect {...rest}>{children}</rect>
    },
    g: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <g {...rest}>{children}</g>
    },
    text: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <text {...rest}>{children}</text>
    },
  },
}))

// Helper to set document theme attribute
const setDocumentTheme = (theme: string) => {
  document.documentElement.setAttribute('data-theme', theme)
}

// Helper to render with router
const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Home />
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Home Page - Accessibility - Color Contrast (WCAG AA)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.clear()
    setDocumentTheme('light')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: Body text contrast ratio in light theme
  describe('Test Case 1: Body text contrast ratio in light theme', () => {
    it('base-content text on base-100 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-100'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('base-content text on base-200 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-200'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('base-content text on base-300 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-300'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('light theme renders page with proper contrast classes', () => {
      setDocumentTheme('light')
      renderWithRouter()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify body text uses theme-aware classes
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })
  })

  // Test Case 2: Body text contrast ratio in dark theme
  describe('Test Case 2: Body text contrast ratio in dark theme', () => {
    it('base-content text on base-100 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.dark
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-100'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('base-content text on base-200 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.dark
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-200'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('base-content text on base-300 background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.dark
      const textColor = hexToRgb(theme['base-content'])!
      const bgColor = hexToRgb(theme['base-300'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('dark theme renders page with proper contrast classes', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify body text uses theme-aware classes
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })
  })

  // Test Case 3: Heading (large text) contrast ratio
  describe('Test Case 3: Heading text contrast ratio', () => {
    it('primary color on base-100 background has at least 3:1 contrast ratio (large text)', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['primary'])!
      const bgColor = hexToRgb(theme['base-100'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAALargeText(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(3.0)
    })

    it('secondary color on base-100 background has at least 3:1 contrast ratio (large text)', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['secondary'])!
      const bgColor = hexToRgb(theme['base-100'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAALargeText(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(3.0)
    })

    it('dark theme primary color on base-100 has at least 3:1 contrast ratio (large text)', () => {
      const theme = DAISYUI_THEMES.dark
      const textColor = hexToRgb(theme['primary'])!
      const bgColor = hexToRgb(theme['base-100'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAALargeText(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(3.0)
    })

    it('headline uses gradient text with primary/secondary colors', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      expect(headline.className).toContain('from-primary')
      expect(headline.className).toContain('to-secondary')
      expect(headline.className).toContain('bg-gradient-to-r')
      expect(headline.tagName.toLowerCase()).toBe('h1')
    })

    it('all themes have sufficient large text contrast', () => {
      const themes: ThemeName[] = ['light', 'dark', 'cyberpunk', 'synthwave']

      themes.forEach((themeName) => {
        const theme = DAISYUI_THEMES[themeName]
        const primaryColor = hexToRgb(theme['primary'])!
        const baseColor = hexToRgb(theme['base-100'])!

        const contrastRatio = getContrastRatio(primaryColor, baseColor)

        expect(
          meetsWcagAALargeText(contrastRatio),
          `${themeName} theme primary color should meet large text contrast requirement (got ${contrastRatio.toFixed(2)}:1)`
        ).toBe(true)
      })
    })
  })

  // Test Case 4: CTA button text contrast
  describe('Test Case 4: CTA button text contrast', () => {
    it('primary-content on primary background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['primary-content'])!
      const bgColor = hexToRgb(theme['primary'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('dark theme primary-content on primary background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.dark
      const textColor = hexToRgb(theme['primary-content'])!
      const bgColor = hexToRgb(theme['primary'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('secondary-content on secondary background has at least 4.5:1 contrast ratio', () => {
      const theme = DAISYUI_THEMES.light
      const textColor = hexToRgb(theme['secondary-content'])!
      const bgColor = hexToRgb(theme['secondary'])!

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(meetsWcagAA(contrastRatio)).toBe(true)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    })

    it('CTA button uses btn-primary class for proper contrast', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA.className).toContain('btn-primary')
    })

    it('all themes have sufficient button contrast', () => {
      const themes: ThemeName[] = ['light', 'dark', 'cyberpunk', 'synthwave']

      themes.forEach((themeName) => {
        const theme = DAISYUI_THEMES[themeName]
        const contentColor = hexToRgb(theme['primary-content'])!
        const primaryColor = hexToRgb(theme['primary'])!

        const contrastRatio = getContrastRatio(contentColor, primaryColor)

        expect(
          meetsWcagAA(contrastRatio),
          `${themeName} theme button contrast should meet WCAG AA (got ${contrastRatio.toFixed(2)}:1)`
        ).toBe(true)
      })
    })
  })

  // Additional comprehensive tests
  describe('All DaisyUI themes meet WCAG AA contrast requirements', () => {
    const themes: ThemeName[] = ['light', 'dark', 'cyberpunk', 'synthwave']

    themes.forEach((themeName) => {
      describe(`${themeName} theme`, () => {
        it('body text meets minimum 4.5:1 contrast ratio', () => {
          const theme = DAISYUI_THEMES[themeName]
          const textColor = hexToRgb(theme['base-content'])!
          const bgColor = hexToRgb(theme['base-100'])!

          const contrastRatio = getContrastRatio(textColor, bgColor)

          expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
        })

        it('primary button text meets minimum 4.5:1 contrast ratio', () => {
          const theme = DAISYUI_THEMES[themeName]
          const textColor = hexToRgb(theme['primary-content'])!
          const bgColor = hexToRgb(theme['primary'])!

          const contrastRatio = getContrastRatio(textColor, bgColor)

          expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
        })

        it('renders page correctly with theme applied', () => {
          setDocumentTheme(themeName)
          renderWithRouter()

          expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
          expect(screen.getByTestId('home-page')).toBeInTheDocument()
        })
      })
    })
  })

  // Test color contrast utility functions
  describe('Color contrast utility functions', () => {
    it('hexToRgb correctly parses hex colors', () => {
      expect(hexToRgb('#ffffff')).toEqual({ r: 255, g: 255, b: 255 })
      expect(hexToRgb('#000000')).toEqual({ r: 0, g: 0, b: 0 })
      expect(hexToRgb('#ff0000')).toEqual({ r: 255, g: 0, b: 0 })
      expect(hexToRgb('fff')).toEqual({ r: 255, g: 255, b: 255 })
    })

    it('getContrastRatio returns correct values for known color pairs', () => {
      const white = { r: 255, g: 255, b: 255 }
      const black = { r: 0, g: 0, b: 0 }

      // Maximum contrast ratio is 21:1 (black on white)
      const maxContrast = getContrastRatio(white, black)
      expect(maxContrast).toBeCloseTo(21, 0)

      // Same color has contrast ratio of 1:1
      const sameColor = getContrastRatio(white, white)
      expect(sameColor).toBeCloseTo(1, 0)
    })

    it('meetsWcagAA returns correct boolean for threshold', () => {
      expect(meetsWcagAA(4.5)).toBe(true)
      expect(meetsWcagAA(4.4)).toBe(false)
      expect(meetsWcagAA(7)).toBe(true)
    })

    it('meetsWcagAALargeText returns correct boolean for threshold', () => {
      expect(meetsWcagAALargeText(3.0)).toBe(true)
      expect(meetsWcagAALargeText(2.9)).toBe(false)
      expect(meetsWcagAALargeText(4.5)).toBe(true)
    })
  })
})
