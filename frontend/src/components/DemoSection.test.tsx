import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import DemoSection from './DemoSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <div {...rest}>{children}</div>
    },
  },
}))

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<DemoSection />} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
      </Routes>
    </MemoryRouter>
  )
}

describe('DemoSection Component', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // Test Case 1: Demo section exists with input field and button
  describe('Test Case 1: Demo section structure', () => {
    it('renders demo section with all required elements', () => {
      renderWithRouter()

      // Check demo section exists
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()

      // Check input field exists
      expect(screen.getByTestId('demo-input')).toBeInTheDocument()

      // Check shorten button exists
      expect(screen.getByTestId('demo-shorten-button')).toBeInTheDocument()
    })

    it('has proper heading and description', () => {
      renderWithRouter()

      expect(screen.getByRole('heading', { name: /try it now/i })).toBeInTheDocument()
      expect(screen.getByText(/see how easy it is to shorten your urls/i)).toBeInTheDocument()
    })

    it('input has proper placeholder text', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
    })

    it('button has proper text', () => {
      renderWithRouter()

      const button = screen.getByTestId('demo-shorten-button')
      expect(button).toHaveTextContent('Shorten')
    })
  })

  // Test Case 2: Enter valid URL and click Shorten
  describe('Test Case 2: Valid URL shortening', () => {
    it('shows preview URL when valid URL is entered and Shorten clicked', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/long/url/path' } })
      fireEvent.click(button)

      // Wait for loading to complete (500ms simulated delay)
      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(screen.getByTestId('demo-preview')).toBeInTheDocument()
      })
    })

    it('displays shortened URL preview in correct format', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        const previewUrl = screen.getByTestId('demo-preview-url')
        expect(previewUrl).toBeInTheDocument()
        expect(previewUrl.textContent).toMatch(/^linkshort\.io\/[a-zA-Z0-9]{6}$/)
      })
    })

    it('shows registration prompt after preview is displayed', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(screen.getByTestId('demo-registration-prompt')).toBeInTheDocument()
      })
    })

    it('registration prompt contains link to register page', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        const registerLink = screen.getByTestId('demo-register-link')
        expect(registerLink).toBeInTheDocument()
        expect(registerLink).toHaveAttribute('href', '/register')
      })
    })

    it('navigates to register page when clicking Create Free Account', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(screen.getByTestId('demo-register-link')).toBeInTheDocument()
      })

      fireEvent.click(screen.getByTestId('demo-register-link'))

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('shows loading state while processing', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      // Button should show loading state immediately
      expect(button).toHaveTextContent('Shortening...')
      expect(button).toBeDisabled()

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(button).toHaveTextContent('Shorten')
        expect(button).not.toBeDisabled()
      })
    })

    it('accepts URL with http protocol', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'http://example.com' } })
      fireEvent.click(button)

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(screen.getByTestId('demo-preview')).toBeInTheDocument()
      })

      // Should not show error
      expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument()
    })

    it('triggers shorten on Enter key press', async () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.keyDown(input, { key: 'Enter' })

      await act(async () => {
        vi.advanceTimersByTime(600)
      })

      await waitFor(() => {
        expect(screen.getByTestId('demo-preview')).toBeInTheDocument()
      })
    })
  })

  // Test Case 3: Invalid URL validation
  describe('Test Case 3: Invalid URL validation', () => {
    it('shows error for invalid URL format', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'not-a-valid-url' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
      expect(screen.getByText(/please enter a valid url/i)).toBeInTheDocument()
    })

    it('shows error for URL without protocol', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'example.com/path' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
      expect(screen.getByText(/must start with http:\/\/ or https:\/\//i)).toBeInTheDocument()
    })

    it('shows error for ftp protocol URLs', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'ftp://files.example.com' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
    })

    it('shows error for javascript: protocol', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'javascript:alert(1)' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
    })

    it('does not show preview when URL is invalid', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(button)

      expect(screen.queryByTestId('demo-preview')).not.toBeInTheDocument()
    })

    it('clears error when user starts typing again', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      // First, trigger an error
      fireEvent.change(input, { target: { value: 'invalid' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()

      // Start typing again
      fireEvent.change(input, { target: { value: 'https://' } })

      expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument()
    })

    it('applies error styling to input when error occurs', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: 'invalid' } })
      fireEvent.click(button)

      expect(input).toHaveClass('input-error')
    })
  })

  // Test Case 4: Empty input validation
  describe('Test Case 4: Empty input validation', () => {
    it('shows error when submitting empty input', () => {
      renderWithRouter()

      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
      expect(screen.getByText(/please enter a url/i)).toBeInTheDocument()
    })

    it('shows error when submitting whitespace-only input', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.change(input, { target: { value: '   ' } })
      fireEvent.click(button)

      expect(screen.getByTestId('demo-error')).toBeInTheDocument()
      expect(screen.getByText(/please enter a url/i)).toBeInTheDocument()
    })

    it('does not show preview when input is empty', () => {
      renderWithRouter()

      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.click(button)

      expect(screen.queryByTestId('demo-preview')).not.toBeInTheDocument()
    })

    it('does not show registration prompt when input is empty', () => {
      renderWithRouter()

      const button = screen.getByTestId('demo-shorten-button')

      fireEvent.click(button)

      expect(screen.queryByTestId('demo-registration-prompt')).not.toBeInTheDocument()
    })
  })

  // Additional accessibility tests
  describe('Accessibility', () => {
    it('input has proper aria-label', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      expect(input).toHaveAttribute('aria-label', 'URL to shorten')
    })

    it('error message is associated with input via aria-describedby', () => {
      renderWithRouter()

      const input = screen.getByTestId('demo-input')
      const button = screen.getByTestId('demo-shorten-button')

      // Initially no aria-describedby
      expect(input).not.toHaveAttribute('aria-describedby')

      // Trigger error
      fireEvent.click(button)

      // Now aria-describedby should point to error
      expect(input).toHaveAttribute('aria-describedby', 'demo-error')
    })

    it('has proper section id for anchor navigation', () => {
      renderWithRouter()

      const section = screen.getByTestId('demo-section')
      expect(section).toHaveAttribute('id', 'demo')
    })
  })
})
