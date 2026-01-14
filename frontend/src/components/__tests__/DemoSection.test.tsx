import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import DemoSection from '../DemoSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('DemoSection', () => {
  // Test Case 1: URL input field is displayed with placeholder text
  describe('Test Case 1: URL Input Field Display', () => {
    it('should render URL input field with placeholder text', () => {
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      expect(input).toBeInTheDocument()
      expect(input).toHaveAttribute('placeholder')
      expect(input.getAttribute('placeholder')?.toLowerCase()).toMatch(/url|link|https/)
    })

    it('should have an input with type url or text', () => {
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const inputType = input.getAttribute('type')
      expect(inputType === 'url' || inputType === 'text').toBe(true)
    })
  })

  // Test Case 2: 'Shorten' or 'Try it' submit button is displayed
  describe('Test Case 2: Submit Button Display', () => {
    it("should render 'Shorten' or 'Try it' submit button", () => {
      renderWithRouter(<DemoSection />)

      const submitButton = screen.getByTestId('demo-submit-button')
      expect(submitButton).toBeInTheDocument()

      const buttonText = submitButton.textContent?.toLowerCase() || ''
      expect(buttonText.includes('shorten') || buttonText.includes('try')).toBe(true)
    })

    it('should have submit button styled as primary button', () => {
      renderWithRouter(<DemoSection />)

      const submitButton = screen.getByTestId('demo-submit-button')
      expect(submitButton).toHaveClass('btn')
      expect(submitButton).toHaveClass('btn-primary')
    })
  })

  // Test Case 3: User is prompted to register or shown preview when submitting valid URL
  describe('Test Case 3: Valid URL Submission', () => {
    it("should prompt user to register when entering 'https://example.com' and clicking Shorten", async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<DemoSection />} />
            <Route
              path="/register"
              element={<div data-testid="register-page">Register Page</div>}
            />
          </Routes>
        </MemoryRouter>
      )

      const input = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      await user.type(input, 'https://example.com')
      await user.click(submitButton)

      // Should either navigate to register or show a registration prompt/preview
      await waitFor(() => {
        const registerPage = screen.queryByTestId('register-page')
        const registerPrompt = screen.queryByTestId('demo-register-prompt')
        const shortenedPreview = screen.queryByTestId('demo-shortened-preview')

        expect(
          registerPage !== null || registerPrompt !== null || shortenedPreview !== null
        ).toBe(true)
      })
    })

    it('should show a result or prompt after valid URL submission', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      await user.type(input, 'https://example.com')
      await user.click(submitButton)

      await waitFor(() => {
        // Either a register prompt or a shortened preview should appear
        const registerPrompt = screen.queryByTestId('demo-register-prompt')
        const shortenedPreview = screen.queryByTestId('demo-shortened-preview')

        expect(registerPrompt !== null || shortenedPreview !== null).toBe(true)
      })
    })
  })

  // Test Case 4: Validation error message displayed for invalid URL format
  describe('Test Case 4: Invalid URL Validation', () => {
    it('should display validation error message for invalid URL format', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      // Enter invalid URL
      await user.type(input, 'not-a-valid-url')
      await user.click(submitButton)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('demo-error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage.textContent?.toLowerCase()).toMatch(/invalid|valid|url/)
      })
    })

    it('should show error for URL without protocol', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      await user.type(input, 'example.com')
      await user.click(submitButton)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('demo-error-message')
        expect(errorMessage).toBeInTheDocument()
      })
    })
  })

  // Test Case 5: Form validation prevents submission or shows error for empty URL input
  describe('Test Case 5: Empty URL Validation', () => {
    it('should prevent submission or show error for empty URL input', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const submitButton = screen.getByTestId('demo-submit-button')

      // Click submit without entering any URL
      await user.click(submitButton)

      await waitFor(() => {
        const errorMessage = screen.getByTestId('demo-error-message')
        expect(errorMessage).toBeInTheDocument()
        expect(errorMessage.textContent?.toLowerCase()).toMatch(/required|enter|empty|provide/)
      })
    })

    it('should not show register prompt or preview without URL input', async () => {
      const user = userEvent.setup()
      renderWithRouter(<DemoSection />)

      const submitButton = screen.getByTestId('demo-submit-button')
      await user.click(submitButton)

      // Wait a moment for any state changes
      await waitFor(() => {
        const registerPrompt = screen.queryByTestId('demo-register-prompt')
        const shortenedPreview = screen.queryByTestId('demo-shortened-preview')

        // Neither should appear when URL is empty
        expect(registerPrompt).not.toBeInTheDocument()
        expect(shortenedPreview).not.toBeInTheDocument()
      })
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('should have a proper section with testid', () => {
      renderWithRouter(<DemoSection />)

      const section = screen.getByTestId('demo-section')
      expect(section).toBeInTheDocument()
    })

    it('should have a label or aria-label for the URL input', () => {
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-url-input')
      const hasLabel = input.getAttribute('aria-label') !== null
      const hasLabelledBy = input.getAttribute('aria-labelledby') !== null
      const hasAssociatedLabel = input.closest('form')?.querySelector('label') !== null

      expect(hasLabel || hasLabelledBy || hasAssociatedLabel).toBe(true)
    })

    it('should have a heading for the demo section', () => {
      renderWithRouter(<DemoSection />)

      const section = screen.getByTestId('demo-section')
      const heading = section.querySelector('h2, h3')
      expect(heading).toBeInTheDocument()
    })
  })
})
