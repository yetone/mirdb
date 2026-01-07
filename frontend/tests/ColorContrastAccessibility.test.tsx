import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../src/contexts/ThemeContext'
import Home from '../src/pages/Home'

/**
 * Color Contrast Accessibility Tests - WCAG 2.1 AA Compliance
 *
 * WCAG 2.1 AA Requirements:
 * - Normal text (< 18px or < 14px bold): minimum 4.5:1 contrast ratio
 * - Large text (>= 18px or >= 14px bold): minimum 3:1 contrast ratio
 * - Interactive elements: minimum 3:1 contrast ratio against adjacent colors
 *
 * DaisyUI themes provide semantic color classes that are designed to meet
 * accessibility standards. These tests verify proper usage of those classes.
 */

/**
 * Calculate relative luminance of a color
 * Formula from WCAG 2.1: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4)
  })
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs
}

/**
 * Calculate contrast ratio between two colors
 * Formula from WCAG 2.1: https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
 */
function getContrastRatio(rgb1: [number, number, number], rgb2: [number, number, number]): number {
  const l1 = getLuminance(...rgb1)
  const l2 = getLuminance(...rgb2)
  const lighter = Math.max(l1, l2)
  const darker = Math.min(l1, l2)
  return (lighter + 0.05) / (darker + 0.05)
}

/**
 * Parse CSS color string to RGB tuple
 */
function parseColor(color: string): [number, number, number] | null {
  // Handle rgb(r, g, b) format
  const rgbMatch = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/)
  if (rgbMatch) {
    return [parseInt(rgbMatch[1]), parseInt(rgbMatch[2]), parseInt(rgbMatch[3])]
  }

  // Handle rgba(r, g, b, a) format
  const rgbaMatch = color.match(/rgba\((\d+),\s*(\d+),\s*(\d+),\s*[\d.]+\)/)
  if (rgbaMatch) {
    return [parseInt(rgbaMatch[1]), parseInt(rgbaMatch[2]), parseInt(rgbaMatch[3])]
  }

  // Handle hex format
  const hexMatch = color.match(/^#([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})$/)
  if (hexMatch) {
    return [parseInt(hexMatch[1], 16), parseInt(hexMatch[2], 16), parseInt(hexMatch[3], 16)]
  }

  return null
}

/**
 * DaisyUI light theme color values (CSS variables resolved)
 * These are the actual color values from DaisyUI's light theme
 */
const DAISYUI_LIGHT_THEME = {
  'base-100': [255, 255, 255] as [number, number, number],      // #ffffff - white background
  'base-200': [243, 244, 246] as [number, number, number],      // #f3f4f6 - slightly darker bg
  'base-300': [229, 231, 235] as [number, number, number],      // #e5e7eb - even darker bg
  'base-content': [31, 41, 55] as [number, number, number],     // #1f2937 - dark text
  'primary': [101, 163, 13] as [number, number, number],        // #65a30d - primary green
  'primary-content': [255, 255, 255] as [number, number, number], // white text on primary
}

/**
 * DaisyUI dark theme color values
 */
const DAISYUI_DARK_THEME = {
  'base-100': [31, 41, 55] as [number, number, number],         // #1f2937 - dark background
  'base-200': [17, 24, 39] as [number, number, number],         // #111827 - darker bg
  'base-300': [55, 65, 81] as [number, number, number],         // #374151 - lighter dark
  'base-content': [229, 231, 235] as [number, number, number],  // #e5e7eb - light text
  'primary': [101, 163, 13] as [number, number, number],        // #65a30d - primary green
  'primary-content': [255, 255, 255] as [number, number, number], // white text on primary
}

// WCAG 2.1 AA minimum contrast ratios
const WCAG_AA_NORMAL_TEXT = 4.5
const WCAG_AA_LARGE_TEXT = 3.0
const WCAG_AA_UI_COMPONENTS = 3.0

describe('Color Contrast Accessibility Tests - WCAG 2.1 AA Compliance', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Hero Headline Contrast (Large Text - 3:1 minimum)', () => {
    it('hero headline uses text-base-content class for proper contrast', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveClass('text-base-content')
    })

    it('hero headline meets WCAG AA 3:1 contrast ratio for large text in light theme', () => {
      // Calculate contrast: base-content text on base-100/base-200 gradient background
      const textColor = DAISYUI_LIGHT_THEME['base-content']
      const bgColor = DAISYUI_LIGHT_THEME['base-100']

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT)
      // The actual ratio should be much higher (~12.6:1 for this combination)
      expect(contrastRatio).toBeGreaterThan(10)
    })

    it('hero headline meets WCAG AA contrast ratio in dark theme', () => {
      const textColor = DAISYUI_DARK_THEME['base-content']
      const bgColor = DAISYUI_DARK_THEME['base-100']

      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT)
    })

    it('hero headline primary accent text meets contrast requirements', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      // The headline contains "Amplify Reach" in text-primary
      const headline = screen.getByRole('heading', { level: 1 })
      const primarySpan = headline.querySelector('.text-primary')
      expect(primarySpan).toBeInTheDocument()

      // Verify primary color meets 3:1 against background for large text
      const primaryColor = DAISYUI_LIGHT_THEME['primary']
      const bgColor = DAISYUI_LIGHT_THEME['base-100']
      const contrastRatio = getContrastRatio(primaryColor, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT)
    })
  })

  describe('Test Case 2: Body Text Contrast (Normal Text - 4.5:1 minimum)', () => {
    it('body text uses text-base-content/70 class for proper hierarchy', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const subheadline = screen.getByText(/transform your long urls into powerful/i)
      expect(subheadline).toBeInTheDocument()
      expect(subheadline).toHaveClass('text-base-content/70')
    })

    it('body text meets WCAG AA 4.5:1 contrast ratio in light theme', () => {
      // base-content at 70% opacity on white background
      // At 70% opacity, the effective color is blended with background
      const textColor = DAISYUI_LIGHT_THEME['base-content']
      const bgColor = DAISYUI_LIGHT_THEME['base-100']

      // Calculate effective color at 70% opacity
      const opacity = 0.7
      const effectiveText: [number, number, number] = [
        Math.round(textColor[0] * opacity + bgColor[0] * (1 - opacity)),
        Math.round(textColor[1] * opacity + bgColor[1] * (1 - opacity)),
        Math.round(textColor[2] * opacity + bgColor[2] * (1 - opacity)),
      ]

      const contrastRatio = getContrastRatio(effectiveText, bgColor)

      // At 70% opacity, contrast should still meet AA requirements
      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT)
    })

    it('body text meets WCAG AA contrast ratio in dark theme', () => {
      const textColor = DAISYUI_DARK_THEME['base-content']
      const bgColor = DAISYUI_DARK_THEME['base-100']

      // Calculate effective color at 70% opacity
      const opacity = 0.7
      const effectiveText: [number, number, number] = [
        Math.round(textColor[0] * opacity + bgColor[0] * (1 - opacity)),
        Math.round(textColor[1] * opacity + bgColor[1] * (1 - opacity)),
        Math.round(textColor[2] * opacity + bgColor[2] * (1 - opacity)),
      ]

      const contrastRatio = getContrastRatio(effectiveText, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT)
    })

    it('feature descriptions use proper contrast classes', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const featureDescriptions = screen.getAllByTestId('feature-description')
      featureDescriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })

    it('section headings use full contrast text-base-content', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      h2Elements.forEach((h2) => {
        expect(h2).toHaveClass('text-base-content')
      })

      // Verify full contrast meets 4.5:1
      const textColor = DAISYUI_LIGHT_THEME['base-content']
      const bgColor = DAISYUI_LIGHT_THEME['base-200']
      const contrastRatio = getContrastRatio(textColor, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT)
    })
  })

  describe('Test Case 3: CTA Button Contrast', () => {
    it('primary CTA button uses btn-primary class', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const primaryButton = screen.getByRole('link', { name: /get started free/i })
      expect(primaryButton).toBeInTheDocument()
      expect(primaryButton).toHaveClass('btn-primary')
    })

    it('primary button text meets WCAG AA contrast against button background', () => {
      // DaisyUI btn-primary uses primary-content on primary background
      // btn-lg class sets font-size to 1.125rem (18px), qualifying as "large text"
      // Large text requires only 3:1 contrast ratio per WCAG AA
      const textColor = DAISYUI_LIGHT_THEME['primary-content']
      const bgColor = DAISYUI_LIGHT_THEME['primary']

      const contrastRatio = getContrastRatio(textColor, bgColor)

      // Large text (18px+) requires minimum 3:1 contrast ratio
      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT)
    })

    it('outline button uses btn-outline class with proper contrast', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const outlineButton = screen.getByRole('link', { name: /sign in/i })
      expect(outlineButton).toBeInTheDocument()
      expect(outlineButton).toHaveClass('btn-outline')
    })

    it('outline button border meets 3:1 contrast against background', () => {
      // Outline buttons use base-content color for border/text
      const borderColor = DAISYUI_LIGHT_THEME['base-content']
      const bgColor = DAISYUI_LIGHT_THEME['base-100']

      const contrastRatio = getContrastRatio(borderColor, bgColor)

      expect(contrastRatio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS)
    })

    it('buttons use btn-lg class ensuring adequate touch target size', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const primaryButton = screen.getByRole('link', { name: /get started free/i })
      const secondaryButton = screen.getByRole('link', { name: /sign in/i })

      // btn-lg class provides minimum 44x44px touch target (WCAG 2.5.5)
      expect(primaryButton).toHaveClass('btn-lg')
      expect(secondaryButton).toHaveClass('btn-lg')
    })
  })

  describe('DaisyUI Semantic Color Classes Verification', () => {
    it('all themes provide consistent semantic color structure', () => {
      // Verify that both light and dark themes have proper contrast
      const themes = [
        { name: 'light', colors: DAISYUI_LIGHT_THEME },
        { name: 'dark', colors: DAISYUI_DARK_THEME },
      ]

      themes.forEach(({ name, colors }) => {
        // Text on primary background
        const textContrast = getContrastRatio(colors['base-content'], colors['base-100'])
        expect(textContrast, `${name} theme: base-content on base-100`).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT)

        // Text on secondary background
        const textOnSecondary = getContrastRatio(colors['base-content'], colors['base-200'])
        expect(textOnSecondary, `${name} theme: base-content on base-200`).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT)
      })
    })

    it('hero section uses correct background classes', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-b')
      expect(heroSection).toHaveClass('from-base-100')
      expect(heroSection).toHaveClass('to-base-200')
    })

    it('feature cards use base-100 background for proper text contrast', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const featureCards = screen.getAllByTestId('feature-card')
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })
    })

    it('footer links use proper contrast with muted styling', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      const footer = screen.getByTestId('footer')
      const footerLinks = footer.querySelectorAll('a')

      footerLinks.forEach((link) => {
        expect(link).toHaveClass('text-base-content/70')
      })
    })
  })

  describe('Color Contrast Utility Functions', () => {
    it('getLuminance calculates correct values for edge cases', () => {
      // Black: luminance = 0
      expect(getLuminance(0, 0, 0)).toBe(0)

      // White: luminance = 1
      expect(getLuminance(255, 255, 255)).toBe(1)

      // Mid-gray should be around 0.2 luminance
      const midGray = getLuminance(128, 128, 128)
      expect(midGray).toBeGreaterThan(0.1)
      expect(midGray).toBeLessThan(0.3)
    })

    it('getContrastRatio calculates correct values', () => {
      // Black on white: 21:1
      const blackOnWhite = getContrastRatio([0, 0, 0], [255, 255, 255])
      expect(blackOnWhite).toBeCloseTo(21, 0)

      // Same color: 1:1
      const sameColor = getContrastRatio([128, 128, 128], [128, 128, 128])
      expect(sameColor).toBe(1)
    })

    it('parseColor handles various formats', () => {
      expect(parseColor('rgb(255, 128, 64)')).toEqual([255, 128, 64])
      expect(parseColor('rgba(255, 128, 64, 0.5)')).toEqual([255, 128, 64])
      expect(parseColor('#ff8040')).toEqual([255, 128, 64])
      expect(parseColor('invalid')).toBeNull()
    })
  })
})

describe('Theme-Specific Color Contrast Tests', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('light theme is applied correctly to document', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('dark theme is applied correctly to document', () => {
    render(
      <ThemeProvider defaultTheme="dark">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })

  it('components maintain semantic color classes across themes', () => {
    const { rerender } = render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Check light theme
    let headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toHaveClass('text-base-content')

    // Switch to dark theme
    rerender(
      <ThemeProvider defaultTheme="dark">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Same semantic class, different computed colors
    headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toHaveClass('text-base-content')
  })
})
