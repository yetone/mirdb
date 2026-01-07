import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ErrorBoundary } from '../src/components/ErrorBoundary'
import Home from '../src/pages/Home'
import { ThemeProvider } from '../src/contexts/ThemeContext'

// Component that throws an error when rendered
function ThrowingComponent({ shouldThrow = true }: { shouldThrow?: boolean }) {
  if (shouldThrow) {
    throw new Error('Test error from child component')
  }
  return <div data-testid="normal-content">Normal content</div>
}

// Component that throws an error conditionally
function ConditionalThrowingComponent({ errorMessage }: { errorMessage?: string }) {
  if (errorMessage) {
    throw new Error(errorMessage)
  }
  return <div data-testid="conditional-content">Conditional content rendered</div>
}

describe('Error Boundary Handling - Test Case 1: Integration', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    // Suppress console.error for expected errors during tests
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleErrorSpy.mockRestore()
  })

  it('catches error from child component and displays fallback UI', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          <ThrowingComponent shouldThrow={true} />
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Verify fallback UI is displayed
    const fallbackElement = screen.getByTestId('error-boundary-fallback')
    expect(fallbackElement).toBeInTheDocument()

    // Verify error message is shown
    expect(screen.getByText('Something went wrong')).toBeInTheDocument()
    expect(screen.getByText(/We encountered an unexpected error/i)).toBeInTheDocument()

    // Verify retry button exists
    const retryButton = screen.getByTestId('error-retry-button')
    expect(retryButton).toBeInTheDocument()
    expect(retryButton).toHaveTextContent('Try Again')
  })

  it('renders error fallback with proper accessibility attributes', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      </MemoryRouter>
    )

    const fallbackElement = screen.getByTestId('error-boundary-fallback')
    expect(fallbackElement).toHaveAttribute('role', 'alert')
    expect(fallbackElement).toHaveAttribute('aria-live', 'assertive')
  })

  it('homepage renders with error boundary wrapping child components', () => {
    // First verify homepage renders normally without errors
    render(
      <ThemeProvider>
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // Homepage should render its main content
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()
  })

  it('error boundary catches simulated component error without crashing the app', () => {
    render(
      <ThemeProvider>
        <MemoryRouter>
          <div data-testid="app-wrapper">
            <nav data-testid="navigation">Navigation</nav>
            <main data-testid="main-content">
              <ErrorBoundary>
                <ThrowingComponent shouldThrow={true} />
              </ErrorBoundary>
            </main>
          </div>
        </MemoryRouter>
      </ThemeProvider>
    )

    // The app wrapper and navigation should still be visible
    expect(screen.getByTestId('app-wrapper')).toBeInTheDocument()
    expect(screen.getByTestId('navigation')).toBeInTheDocument()
    expect(screen.getByTestId('main-content')).toBeInTheDocument()

    // Error boundary should show fallback UI
    expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()

    // The throwing component's content should NOT be visible
    expect(screen.queryByTestId('normal-content')).not.toBeInTheDocument()
  })

  it('allows retry after error occurs', () => {
    let shouldThrow = true

    function ToggleThrowingComponent() {
      if (shouldThrow) {
        throw new Error('Initial error')
      }
      return <div data-testid="recovered-content">Content recovered</div>
    }

    const { rerender } = render(
      <MemoryRouter>
        <ErrorBoundary>
          <ToggleThrowingComponent />
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Verify error fallback is shown
    expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()

    // Click retry button
    shouldThrow = false
    const retryButton = screen.getByTestId('error-retry-button')
    fireEvent.click(retryButton)

    // Re-render to trigger the retry
    rerender(
      <MemoryRouter>
        <ErrorBoundary>
          <ToggleThrowingComponent />
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Content should now be visible
    expect(screen.getByTestId('recovered-content')).toBeInTheDocument()
  })

  it('renders custom fallback when provided', () => {
    const customFallback = <div data-testid="custom-fallback">Custom error message</div>

    render(
      <MemoryRouter>
        <ErrorBoundary fallback={customFallback}>
          <ThrowingComponent />
        </ErrorBoundary>
      </MemoryRouter>
    )

    expect(screen.getByTestId('custom-fallback')).toBeInTheDocument()
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })
})

describe('Error Boundary Handling - Test Case 2: Unit Tests for Missing Props', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleErrorSpy.mockRestore()
  })

  it('component renders without crashing when optional data is missing', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          <ConditionalThrowingComponent errorMessage={undefined} />
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Component should render successfully without error fallback
    expect(screen.getByTestId('conditional-content')).toBeInTheDocument()
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })

  it('error boundary handles null children gracefully', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          {null}
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Should not crash and not show error fallback
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })

  it('error boundary handles undefined children gracefully', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          {undefined}
        </ErrorBoundary>
      </MemoryRouter>
    )

    // Should not crash and not show error fallback
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })

  it('homepage components handle missing optional props without crashing', () => {
    // Render the full homepage which has multiple child components
    render(
      <ThemeProvider>
        <MemoryRouter>
          <Home />
        </MemoryRouter>
      </ThemeProvider>
    )

    // All sections should render without error
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // No error boundary fallback should be visible
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })

  it('error boundary renders children normally when no error occurs', () => {
    render(
      <MemoryRouter>
        <ErrorBoundary>
          <div data-testid="child-one">Child One</div>
          <div data-testid="child-two">Child Two</div>
        </ErrorBoundary>
      </MemoryRouter>
    )

    expect(screen.getByTestId('child-one')).toBeInTheDocument()
    expect(screen.getByTestId('child-two')).toBeInTheDocument()
    expect(screen.queryByTestId('error-boundary-fallback')).not.toBeInTheDocument()
  })

  it('error boundary contains error but still allows sibling components to render', () => {
    render(
      <ThemeProvider>
        <MemoryRouter>
          <div data-testid="page-container">
            <header data-testid="page-header">Header</header>
            <ErrorBoundary>
              <ThrowingComponent />
            </ErrorBoundary>
            <footer data-testid="page-footer">Footer</footer>
          </div>
        </MemoryRouter>
      </ThemeProvider>
    )

    // Siblings should still render
    expect(screen.getByTestId('page-container')).toBeInTheDocument()
    expect(screen.getByTestId('page-header')).toBeInTheDocument()
    expect(screen.getByTestId('page-footer')).toBeInTheDocument()

    // Error boundary should show fallback
    expect(screen.getByTestId('error-boundary-fallback')).toBeInTheDocument()
  })
})
