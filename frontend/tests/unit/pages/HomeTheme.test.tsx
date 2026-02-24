/**
 * Homepage Theme Unit Tests
 * Owner: Scenario 8 - Theme Support and Toggle
 *
 * Unit tests for verifying homepage renders correctly with different themes.
 * Test cases 1-4: light, dark, cyberpunk, synthwave themes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Home } from '@/pages/Home'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'

// Test wrapper component with theme provider
function TestWrapper({ children }: { children: React.ReactNode }) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })

  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ThemeProvider>
          <AuthProvider>{children}</AuthProvider>
        </ThemeProvider>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

describe('Homepage Theme Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Reset document attributes
    document.documentElement.removeAttribute('data-theme')
    // Clear localStorage mock returns
    vi.mocked(window.localStorage.getItem).mockReturnValue(null)
    vi.mocked(window.localStorage.setItem).mockClear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Light Theme', () => {
    it('renders homepage with light theme colors', async () => {
      // Setup localStorage to return 'light' theme
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'light'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Wait for component to mount and apply theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify main components are rendered
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        'URL Shortener'
      )
      expect(screen.getByLabelText('Hero section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('applies light theme base classes', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'light'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Check that base-100 background class is used (DaisyUI semantic class)
      const mainContainer = screen.getByRole('main').parentElement
      expect(mainContainer).toHaveClass('bg-base-100')
    })
  })

  describe('Test Case 2: Dark Theme', () => {
    it('renders homepage with dark theme colors', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'dark'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify main components are rendered
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        'URL Shortener'
      )
      expect(screen.getByLabelText('Hero section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('renders feature cards with dark theme styling', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'dark'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Feature section should be visible
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // All feature cards should be rendered
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)
    })
  })

  describe('Test Case 3: Cyberpunk Theme', () => {
    it('renders homepage with cyberpunk theme styling', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'cyberpunk'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // Verify main components are rendered
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        'URL Shortener'
      )
      expect(screen.getByLabelText('Hero section')).toBeInTheDocument()
    })

    it('renders all sections with cyberpunk theme', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'cyberpunk'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // Hero section
      const heroSection = screen.getByLabelText('Hero section')
      expect(heroSection).toBeInTheDocument()

      // Features section
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Footer section
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()
    })

    it('applies gradient text styling for cyberpunk theme', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'cyberpunk'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // Main heading should have gradient classes
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveClass('bg-gradient-to-r')
      expect(heading).toHaveClass('bg-clip-text')
    })
  })

  describe('Test Case 4: Synthwave Theme', () => {
    it('renders homepage with synthwave theme styling', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'synthwave'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // Verify main components are rendered
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
        'URL Shortener'
      )
      expect(screen.getByLabelText('Hero section')).toBeInTheDocument()
    })

    it('renders URL input with synthwave theme', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'synthwave'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // URL input should be present and styled
      const urlInput = screen.getByLabelText('URL to shorten')
      expect(urlInput).toBeInTheDocument()
      expect(urlInput).toHaveClass('input')
      expect(urlInput).toHaveClass('input-bordered')
    })

    it('renders shorten button with synthwave theme', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'synthwave'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // Shorten button should be present with primary styling
      const shortenButton = screen.getByLabelText('Shorten URL')
      expect(shortenButton).toBeInTheDocument()
      expect(shortenButton).toHaveClass('btn')
      expect(shortenButton).toHaveClass('btn-primary')
    })
  })

  describe('Theme Consistency Across Components', () => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave']

    themes.forEach((themeName) => {
      it(`renders all components consistently with ${themeName} theme`, async () => {
        vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
          if (key === 'theme') return themeName
          return null
        })

        render(
          <TestWrapper>
            <Home />
          </TestWrapper>
        )

        // Wait for theme to be applied
        await waitFor(() => {
          expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
        })

        // All main sections should be present
        expect(screen.getByLabelText('Hero section')).toBeInTheDocument()
        expect(screen.getByTestId('features-section')).toBeInTheDocument()
        expect(screen.getByTestId('footer-section')).toBeInTheDocument()

        // Main heading should be rendered
        expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
          'URL Shortener'
        )

        // Feature cards should be visible
        const featureCards = screen.getAllByTestId('feature-card')
        expect(featureCards.length).toBeGreaterThan(0)
      })
    })
  })

  describe('Theme CSS Classes', () => {
    it('uses DaisyUI semantic color classes for theming', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'dark'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Check for DaisyUI semantic classes
      // Main container uses bg-base-100
      const mainContainer = screen.getByRole('main').parentElement
      expect(mainContainer).toHaveClass('bg-base-100')

      // Footer uses bg-base-200
      const footer = screen.getByTestId('footer-section')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('uses text-base-content for text colors', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'dark'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Features section heading uses text-base-content
      const featuresHeading = screen.getByRole('heading', { level: 2 })
      expect(featuresHeading).toHaveClass('text-base-content')
    })
  })

  describe('Theme Persistence', () => {
    it('saves theme to localStorage when theme is applied', async () => {
      vi.mocked(window.localStorage.getItem).mockImplementation((key: string) => {
        if (key === 'theme') return 'synthwave'
        return null
      })

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // localStorage.setItem should have been called with the theme
      expect(window.localStorage.setItem).toHaveBeenCalledWith('theme', 'synthwave')
    })
  })
})
