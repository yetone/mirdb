import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider, useTheme } from '../src/contexts/ThemeContext'
import Home from '../src/pages/Home'
import { AppRoutes } from '../src/App'

// Helper component to verify theme context value
function ThemeDisplay() {
  const { theme } = useTheme()
  return <span data-testid="theme-value">{theme}</span>
}

// Test Case 1: Integration test - Render Homepage with ThemeContext set to 'light'
describe('Light Mode Theme Integration Tests', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('renders Homepage with light theme styles applied when ThemeContext is set to light', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
          <ThemeDisplay />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Verify homepage renders
    const homepage = screen.getByTestId('homepage')
    expect(homepage).toBeInTheDocument()

    // Verify the theme is set to light
    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')

    // Verify data-theme attribute is set on document
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('renders all homepage sections with light theme', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Verify hero section is present
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Verify features section is present
    expect(screen.getByTestId('features-section')).toBeInTheDocument()

    // Verify how it works section is present
    expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

    // Verify footer is present
    expect(screen.getByTestId('footer')).toBeInTheDocument()

    // Verify theme is applied to document
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })
})

// Test Case 2: Unit test - Query text color in light mode
describe('Light Mode Theme Unit Tests - Text Colors', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('text uses base-content class for dark text readability on light background', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Verify theme is light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')

    // Check that the main heading has the text-base-content class
    // This DaisyUI class ensures text color adapts to theme (dark text on light backgrounds)
    const h1Element = screen.getByRole('heading', { level: 1 })
    expect(h1Element).toBeInTheDocument()
    expect(h1Element).toHaveClass('text-base-content')
  })

  it('section headings use base-content class for proper contrast', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Verify theme is light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')

    // Check h2 headings have proper text color class
    const h2Elements = screen.getAllByRole('heading', { level: 2 })
    h2Elements.forEach((h2) => {
      expect(h2).toHaveClass('text-base-content')
    })
  })

  it('subtext uses base-content/70 class for slightly muted text', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // The subheadline paragraph should have the muted text class
    const subheadline = screen.getByText(/transform your long urls into powerful/i)
    expect(subheadline).toHaveClass('text-base-content/70')
  })

  it('feature descriptions use muted text color class', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Feature descriptions should have muted text for hierarchy
    const featureDescriptions = screen.getAllByTestId('feature-description')
    featureDescriptions.forEach((desc) => {
      expect(desc).toHaveClass('text-base-content/70')
    })
  })
})

// Test Case: ThemeContext functionality
describe('ThemeContext Light Mode Functionality', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('sets data-theme attribute to light when light theme is used', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeDisplay />
      </ThemeProvider>
    )

    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('loads light theme from localStorage when previously set', () => {
    // Simulate a previous session where light theme was selected
    localStorage.setItem('theme', 'light')

    render(
      <ThemeProvider>
        <ThemeDisplay />
      </ThemeProvider>
    )

    // Theme should be loaded from localStorage
    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('reads theme from localStorage if available', () => {
    localStorage.setItem('theme', 'light')

    render(
      <ThemeProvider>
        <ThemeDisplay />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })
})

// Test background colors are theme-aware
describe('Light Mode Background Colors', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('hero section uses base-100 to base-200 gradient background', () => {
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

  it('features section uses base-200 background', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toHaveClass('bg-base-200')
  })

  it('footer uses base-200 background', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    const footer = screen.getByTestId('footer')
    expect(footer).toHaveClass('bg-base-200')
  })
})
