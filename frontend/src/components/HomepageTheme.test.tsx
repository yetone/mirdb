import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from './HeroSection'
import { ThemeProvider } from '../contexts/ThemeContext'

// Helper to render with all required providers
const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('Homepage Theme Integration', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('ThemeToggle presence on homepage', () => {
    it('renders ThemeToggle component in HeroSection', () => {
      renderWithProviders(<HeroSection />)
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument()
    })

    it('ThemeToggle is positioned in the header area', () => {
      renderWithProviders(<HeroSection />)
      const themeToggle = screen.getByTestId('theme-toggle')
      // Verify parent has absolute positioning (top-right)
      const container = themeToggle.closest('.absolute.top-4.right-4')
      expect(container).toBeInTheDocument()
    })
  })

  describe('Test Case 1: Toggle theme from light to dark', () => {
    it('homepage updates to dark theme colors throughout all sections', async () => {
      const user = userEvent.setup()

      // Set initial theme to light
      localStorage.setItem('theme-preference', 'light')

      renderWithProviders(<HeroSection />)

      // Verify initial light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click dark theme option
      await user.click(screen.getByTestId('theme-option-dark'))

      // Verify dark theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(localStorage.getItem('theme-preference')).toBe('dark')
    })
  })

  describe('Test Case 2: Toggle theme from dark to light', () => {
    it('homepage updates to light theme colors throughout all sections', async () => {
      const user = userEvent.setup()

      // Set initial theme to dark
      localStorage.setItem('theme-preference', 'dark')

      renderWithProviders(<HeroSection />)

      // Verify initial dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Click light theme option
      await user.click(screen.getByTestId('theme-option-light'))

      // Verify light theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(localStorage.getItem('theme-preference')).toBe('light')
    })
  })

  describe('Test Case 3: Theme persistence in localStorage', () => {
    it('theme preference is stored in localStorage and persists on reload', async () => {
      const user = userEvent.setup()

      // First render - set theme
      const { unmount } = renderWithProviders(<HeroSection />)

      // Change to dark theme
      await user.click(screen.getByTestId('theme-option-dark'))
      expect(localStorage.getItem('theme-preference')).toBe('dark')

      // Unmount (simulate page close)
      unmount()

      // Re-render (simulate reload)
      renderWithProviders(<HeroSection />)

      // Theme should be restored from localStorage
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('theme-option-dark')).toHaveAttribute('aria-selected', 'true')
    })
  })

  describe('Test Case 4: ThemeContext integration', () => {
    it('homepage components consume ThemeContext and react to changes', async () => {
      const user = userEvent.setup()

      renderWithProviders(<HeroSection />)

      // The BackgroundEffect component uses useTheme() hook
      const backgroundEffect = screen.getByTestId('hero-background')
      expect(backgroundEffect).toBeInTheDocument()

      // Change theme and verify components update
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      // Verify theme is applied globally
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  describe('Test Case 5: Multiple theme variants support', () => {
    it('homepage supports cyberpunk theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<HeroSection />)

      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')
    })

    it('homepage supports synthwave theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<HeroSection />)

      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      expect(localStorage.getItem('theme-preference')).toBe('synthwave')
    })

    it('all DaisyUI themes are available in dropdown', () => {
      renderWithProviders(<HeroSection />)

      expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
    })
  })

  describe('Theme-aware styling', () => {
    it('HeroSection uses theme-aware CSS classes', () => {
      renderWithProviders(<HeroSection />)

      // Check for DaisyUI theme-aware classes
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-base-content')

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })

    it('navigation links use theme-aware hover colors', () => {
      renderWithProviders(<HeroSection />)

      const featuresNav = screen.getByTestId('nav-features')
      expect(featuresNav).toHaveClass('hover:text-primary')
    })
  })

  describe('Accessibility', () => {
    it('theme toggle has proper aria-label', () => {
      renderWithProviders(<HeroSection />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('aria-label')
      expect(toggleButton.getAttribute('aria-label')).toContain('theme')
    })

    it('theme options have proper role attributes', () => {
      renderWithProviders(<HeroSection />)

      const menu = screen.getByTestId('theme-dropdown-menu')
      expect(menu).toHaveAttribute('role', 'listbox')

      const options = screen.getAllByRole('option')
      expect(options.length).toBe(5) // system, light, dark, cyberpunk, synthwave
    })
  })
})
