/**
 * Theme Context Tests.
 * Owner: Scenario 10 - Dark Mode Theme Toggle
 * Contributor: Scenario 17 - Component Isolation and Props
 */
import React from 'react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ThemeProvider, useThemeContext } from './ThemeContext'
import { THEME_STORAGE_KEY } from '../utils/constants'

// Test component that uses the context
function TestConsumer() {
  const { theme, toggleTheme, setTheme } = useThemeContext()
  return (
    <div>
      <span data-testid="theme-value">{theme}</span>
      <button onClick={toggleTheme} data-testid="toggle-btn">Toggle</button>
      <button onClick={() => setTheme('dark')} data-testid="set-dark-btn">Set Dark</button>
      <button onClick={() => setTheme('light')} data-testid="set-light-btn">Set Light</button>
    </div>
  )
}

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.classList.remove('dark')
  })

  it('provides default theme as light when no preference stored', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
  })

  it('toggleTheme switches from light to dark', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')

    fireEvent.click(screen.getByTestId('toggle-btn'))

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
  })

  it('toggleTheme switches from dark to light', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')

    fireEvent.click(screen.getByTestId('toggle-btn'))

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
  })

  it('persists theme preference to localStorage', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    fireEvent.click(screen.getByTestId('toggle-btn'))

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')
  })

  it('loads theme from localStorage on mount', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
  })

  it('adds dark class to documentElement when theme is dark', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(document.documentElement.classList.contains('dark')).toBe(true)
  })

  it('removes dark class from documentElement when theme is light', () => {
    document.documentElement.classList.add('dark')
    localStorage.setItem(THEME_STORAGE_KEY, 'light')

    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(document.documentElement.classList.contains('dark')).toBe(false)
  })

  it('setTheme allows setting theme directly', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')

    fireEvent.click(screen.getByTestId('set-dark-btn'))

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')
  })

  it('throws error when useThemeContext is used outside provider', () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})

    expect(() => {
      render(<TestConsumer />)
    }).toThrow('useThemeContext must be used within a ThemeProvider')

    consoleError.mockRestore()
  })
})

// ==============================================
// Scenario 17: Component Isolation and Props
// Test Case 4: Access ThemeContext in child component
// ==============================================

describe('ThemeContext Child Access (Test Case 4)', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.classList.remove('dark')
  })

  // Nested child component for testing deep context access
  function DeepNestedChild() {
    const { theme, toggleTheme } = useThemeContext()
    return (
      <div data-testid="nested-child">
        <span data-testid="nested-theme">{theme}</span>
        <button onClick={toggleTheme} data-testid="nested-toggle">Toggle Nested</button>
      </div>
    )
  }

  function MiddleComponent({ children }: { children: React.ReactNode }) {
    return <div data-testid="middle-wrapper">{children}</div>
  }

  it('child component can read theme value from context', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
  })

  it('child component can update theme value via toggleTheme', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')

    fireEvent.click(screen.getByTestId('toggle-btn'))

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
  })

  it('child component can set theme value directly via setTheme', () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    fireEvent.click(screen.getByTestId('set-dark-btn'))
    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')

    fireEvent.click(screen.getByTestId('set-light-btn'))
    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
  })

  it('deeply nested child can access and update theme', () => {
    render(
      <ThemeProvider>
        <MiddleComponent>
          <MiddleComponent>
            <DeepNestedChild />
          </MiddleComponent>
        </MiddleComponent>
      </ThemeProvider>
    )

    expect(screen.getByTestId('nested-theme')).toHaveTextContent('light')

    fireEvent.click(screen.getByTestId('nested-toggle'))

    expect(screen.getByTestId('nested-theme')).toHaveTextContent('dark')
    expect(document.documentElement.classList.contains('dark')).toBe(true)
  })

  it('multiple children share the same theme state', () => {
    function SecondConsumer() {
      const { theme } = useThemeContext()
      return <span data-testid="second-consumer-theme">{theme}</span>
    }

    render(
      <ThemeProvider>
        <TestConsumer />
        <SecondConsumer />
      </ThemeProvider>
    )

    // Both should show the same initial theme
    expect(screen.getByTestId('theme-value')).toHaveTextContent('light')
    expect(screen.getByTestId('second-consumer-theme')).toHaveTextContent('light')

    // Toggle theme via first consumer
    fireEvent.click(screen.getByTestId('toggle-btn'))

    // Both should update to the new theme
    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
    expect(screen.getByTestId('second-consumer-theme')).toHaveTextContent('dark')
  })

  it('child receives initial theme from localStorage', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>
    )

    expect(screen.getByTestId('theme-value')).toHaveTextContent('dark')
  })
})
