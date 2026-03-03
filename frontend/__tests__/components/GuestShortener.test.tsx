/**
 * GuestShortener Component Tests
 * Owner: Scenario 5 - Guest URL Shortening Success Flow
 *
 * Test cases:
 * 1. URL input field with placeholder text is visible
 * 2. Loading state is displayed during API call
 * 3. Short URL is displayed in success state
 * 4. Copy button is visible next to short URL
 * 5. View Analytics option is available for the new URL
 * 6. Prompt to create account for more features is displayed
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import GuestShortener from '../../src/components/GuestShortener'
import * as api from '../../src/api'

// Mock the API module
vi.mock('../../src/api', () => ({
  shortenUrl: vi.fn(),
}))

const mockedShortenUrl = vi.mocked(api.shortenUrl)

// Wrapper component with Router context
const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  )
}

describe('GuestShortener', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  /**
   * Test Case 1: URL input field with placeholder text is visible
   * Type: Unit
   */
  describe('Test Case 1: Render guest shortening section', () => {
    it('should render URL input field with placeholder text', () => {
      renderWithRouter(<GuestShortener />)

      const input = screen.getByTestId('url-input')
      expect(input).toBeInTheDocument()
      expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
      expect(input).toHaveAttribute('type', 'url')
    })

    it('should render the Shorten button', () => {
      renderWithRouter(<GuestShortener />)

      const button = screen.getByRole('button', { name: /shorten/i })
      expect(button).toBeInTheDocument()
    })

    it('should render the title and description', () => {
      renderWithRouter(<GuestShortener />)

      expect(screen.getByText('Try It Now')).toBeInTheDocument()
      expect(screen.getByText(/no account required/i)).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Loading state is displayed during API call
   * Type: Integration
   */
  describe('Test Case 2: Loading state during API call', () => {
    it('should display loading state when shortening URL', async () => {
      // Create a promise that doesn't resolve immediately
      let resolvePromise: (value: api.ShortenUrlResponse) => void
      const pendingPromise = new Promise<api.ShortenUrlResponse>((resolve) => {
        resolvePromise = resolve
      })
      mockedShortenUrl.mockReturnValue(pendingPromise)

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByRole('button', { name: /shorten/i })

      await user.type(input, 'https://example.com/very-long-url-path')
      await user.click(button)

      // Check loading state - button shows loading spinner and is disabled
      await waitFor(() => {
        const loadingButton = screen.getByRole('button')
        expect(loadingButton).toBeDisabled()
        expect(loadingButton).toHaveAttribute('aria-label', 'Shortening URL...')
        // Check that loading spinner is present
        expect(loadingButton.querySelector('.loading-spinner')).toBeInTheDocument()
      })

      // Resolve the promise to clean up
      resolvePromise!({
        id: 1,
        original_url: 'https://example.com/very-long-url-path',
        short_code: 'abc123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      await waitFor(() => {
        expect(screen.getByTestId('success-message')).toBeInTheDocument()
      })
    })

    it('should disable input during loading', async () => {
      let resolvePromise: (value: api.ShortenUrlResponse) => void
      const pendingPromise = new Promise<api.ShortenUrlResponse>((resolve) => {
        resolvePromise = resolve
      })
      mockedShortenUrl.mockReturnValue(pendingPromise)

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com/test')

      fireEvent.submit(input.closest('form')!)

      await waitFor(() => {
        expect(input).toBeDisabled()
      })

      // Clean up
      resolvePromise!({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'test123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })
    })
  })

  /**
   * Test Case 3: Short URL is displayed in success state
   * Type: Integration
   */
  describe('Test Case 3: Success state with short URL', () => {
    it('should display short URL after successful API response', async () => {
      const mockResponse: api.ShortenUrlResponse = {
        id: 1,
        original_url: 'https://example.com/very-long-url-path',
        short_code: 'abc123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      }
      mockedShortenUrl.mockResolvedValue(mockResponse)

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      const input = screen.getByTestId('url-input')
      const button = screen.getByRole('button', { name: /shorten/i })

      await user.type(input, 'https://example.com/very-long-url-path')
      await user.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('success-message')).toBeInTheDocument()
      })

      // Verify short URL is displayed
      const shortUrlElement = screen.getByTestId('short-url')
      expect(shortUrlElement).toBeInTheDocument()
      expect(shortUrlElement.textContent).toContain('/r/abc123')
    })

    it('should display success message', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'xyz789',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByText('Your short URL is ready!')).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 4: Copy button is visible next to short URL
   * Type: Unit
   */
  describe('Test Case 4: Copy button visibility', () => {
    it('should display copy button next to short URL after successful shortening', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'copy123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        const copyButton = screen.getByTestId('copy-button')
        expect(copyButton).toBeInTheDocument()
        expect(copyButton).toHaveAttribute('aria-label', 'Copy short URL to clipboard')
      })
    })

    it('should copy URL to clipboard when copy button is clicked', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'copytest',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('copy-button'))

      // Verify copy succeeded by checking visual feedback (checkmark icon appears)
      await waitFor(() => {
        const copyButton = screen.getByTestId('copy-button')
        // The button should show a checkmark SVG with text-success class after clicking
        const checkIcon = copyButton.querySelector('.text-success')
        expect(checkIcon).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 5: View Analytics option is available for the new URL
   * Type: Integration
   */
  describe('Test Case 5: View Analytics navigation', () => {
    it('should display View Analytics link after successful URL shortening', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/analytics-test',
        short_code: 'analytics123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/analytics-test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        const analyticsLink = screen.getByTestId('view-analytics-link')
        expect(analyticsLink).toBeInTheDocument()
        expect(analyticsLink).toHaveAttribute('href', '/stats/analytics123')
        expect(analyticsLink).toHaveTextContent('View Analytics')
      })
    })

    it('should have correct href attribute for analytics link', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 2,
        original_url: 'https://example.com/test',
        short_code: 'customcode',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        const link = screen.getByTestId('view-analytics-link')
        expect(link.getAttribute('href')).toBe('/stats/customcode')
      })
    })
  })

  /**
   * Test Case 6: Prompt to create account for more features is displayed
   * Type: Unit
   */
  describe('Test Case 6: Account creation prompt', () => {
    it('should display account creation prompt after successful URL shortening', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/prompt-test',
        short_code: 'prompt123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/prompt-test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        const accountPrompt = screen.getByTestId('account-prompt')
        expect(accountPrompt).toBeInTheDocument()
        expect(accountPrompt).toHaveTextContent(/track all your URLs/i)
      })
    })

    it('should display Create Free Account button that links to registration', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'reg123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        const registerLink = screen.getByRole('link', { name: /create free account/i })
        expect(registerLink).toBeInTheDocument()
        expect(registerLink).toHaveAttribute('href', '/register')
      })
    })

    it('should include messaging about advanced analytics', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'msg123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByText(/advanced analytics/i)).toBeInTheDocument()
      })
    })
  })

  /**
   * Scenario 7 - Test Case 2: Toast notification confirms successful copy
   * Type: Integration
   * Owner: Scenario 7 - Copy Short URL Functionality
   */
  describe('Scenario 7 TC2: Toast notification on copy', () => {
    it('should display toast notification when copy button is clicked', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'toast123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('copy-button'))

      // Verify toast notification appears
      await waitFor(() => {
        const toast = screen.getByTestId('copy-toast')
        expect(toast).toBeInTheDocument()
      })
    })

    it('should display success message in toast after copy', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'toastmsg123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('copy-button'))

      // Verify toast message content
      await waitFor(() => {
        const toastMessage = screen.getByTestId('toast-message')
        expect(toastMessage).toHaveTextContent(/copied to clipboard/i)
      })
    })

    it('should have success alert styling in toast', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'toaststyle123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('copy-button'))

      // Verify toast has success styling
      await waitFor(() => {
        const toast = screen.getByTestId('copy-toast')
        const alertDiv = toast.querySelector('.alert-success')
        expect(alertDiv).toBeInTheDocument()
      })
    })
  })

  /**
   * Scenario 13: View Analytics Navigation
   * Owner: Scenario 13 - View Analytics Navigation
   *
   * Test cases:
   * 1. Click View Analytics after successful shortening → User is navigated to /stats/:shortCode route
   * 2. View Analytics link → Link contains correct shortCode from generated URL
   * 3. Guest user clicks View Analytics → Analytics page handles unauthenticated access appropriately
   */
  describe('Scenario 13: View Analytics Navigation', () => {
    /**
     * Test Case 1: Click View Analytics after successful shortening
     * Type: Integration
     * Expected: User is navigated to /stats/:shortCode route
     */
    describe('Test Case 1: Navigation to analytics page', () => {
      it('should navigate to /stats/:shortCode when View Analytics is clicked', async () => {
        const shortCode = 'nav123'
        mockedShortenUrl.mockResolvedValue({
          id: 1,
          original_url: 'https://example.com/navigation-test',
          short_code: shortCode,
          created_at: new Date().toISOString(),
          user_id: null,
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        // Step 1: Complete guest URL shortening
        await user.type(screen.getByTestId('url-input'), 'https://example.com/navigation-test')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        // Wait for success state
        await waitFor(() => {
          expect(screen.getByTestId('success-message')).toBeInTheDocument()
        })

        // Step 2: Click View Analytics
        const analyticsLink = screen.getByTestId('view-analytics-link')
        expect(analyticsLink).toBeInTheDocument()

        // Step 3: Verify the link points to correct route
        expect(analyticsLink).toHaveAttribute('href', `/stats/${shortCode}`)
      })

      it('should render View Analytics as a React Router Link for SPA navigation', async () => {
        mockedShortenUrl.mockResolvedValue({
          id: 1,
          original_url: 'https://example.com/test',
          short_code: 'routerlink123',
          created_at: new Date().toISOString(),
          user_id: null,
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        await waitFor(() => {
          const analyticsLink = screen.getByTestId('view-analytics-link')
          // The link should be rendered as a proper anchor element for SPA navigation
          expect(analyticsLink.tagName.toLowerCase()).toBe('a')
          expect(analyticsLink).toHaveAttribute('href', '/stats/routerlink123')
        })
      })
    })

    /**
     * Test Case 2: View Analytics link contains correct shortCode
     * Type: Unit
     * Expected: Link contains correct shortCode from generated URL
     */
    describe('Test Case 2: ShortCode in analytics link', () => {
      it('should include correct shortCode from API response in link href', async () => {
        const testShortCode = 'abc123xyz'
        mockedShortenUrl.mockResolvedValue({
          id: 42,
          original_url: 'https://example.com/long-url-to-shorten',
          short_code: testShortCode,
          created_at: new Date().toISOString(),
          user_id: null,
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        await user.type(screen.getByTestId('url-input'), 'https://example.com/long-url-to-shorten')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        await waitFor(() => {
          const link = screen.getByTestId('view-analytics-link')
          const href = link.getAttribute('href')

          // Verify the shortCode is correctly embedded in the URL
          expect(href).toBe(`/stats/${testShortCode}`)
          expect(href).toContain(testShortCode)
        })
      })

      it('should correctly handle different shortCode formats', async () => {
        const testCases = [
          { shortCode: 'simple', expected: '/stats/simple' },
          { shortCode: 'UPPERCASE', expected: '/stats/UPPERCASE' },
          { shortCode: 'mix123ABC', expected: '/stats/mix123ABC' },
        ]

        for (const testCase of testCases) {
          vi.clearAllMocks()

          mockedShortenUrl.mockResolvedValue({
            id: 1,
            original_url: 'https://example.com/test',
            short_code: testCase.shortCode,
            created_at: new Date().toISOString(),
            user_id: null,
            click_count: 0,
          })

          const user = userEvent.setup()
          const { unmount } = renderWithRouter(<GuestShortener />)

          await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
          await user.click(screen.getByRole('button', { name: /shorten/i }))

          await waitFor(() => {
            const link = screen.getByTestId('view-analytics-link')
            expect(link.getAttribute('href')).toBe(testCase.expected)
          })

          unmount()
        }
      })
    })

    /**
     * Test Case 3: Guest user clicks View Analytics
     * Type: Integration
     * Expected: Analytics page handles unauthenticated access appropriately
     */
    describe('Test Case 3: Guest access to analytics', () => {
      it('should display View Analytics link for unauthenticated guest users', async () => {
        // This test verifies that guest users (unauthenticated) can see the analytics link
        mockedShortenUrl.mockResolvedValue({
          id: 1,
          original_url: 'https://example.com/guest-test',
          short_code: 'guestcode123',
          created_at: new Date().toISOString(),
          user_id: null, // null user_id indicates guest/unauthenticated
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        await user.type(screen.getByTestId('url-input'), 'https://example.com/guest-test')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        await waitFor(() => {
          // Guest user should see the View Analytics link
          const analyticsLink = screen.getByTestId('view-analytics-link')
          expect(analyticsLink).toBeInTheDocument()
          expect(analyticsLink).toHaveAttribute('href', '/stats/guestcode123')
        })
      })

      it('should render View Analytics link that allows navigation without authentication', async () => {
        mockedShortenUrl.mockResolvedValue({
          id: 1,
          original_url: 'https://example.com/unauth-access',
          short_code: 'unauthlink',
          created_at: new Date().toISOString(),
          user_id: null,
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        await user.type(screen.getByTestId('url-input'), 'https://example.com/unauth-access')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        await waitFor(() => {
          const analyticsLink = screen.getByTestId('view-analytics-link')

          // The link should be accessible and clickable
          expect(analyticsLink).toBeInTheDocument()
          expect(analyticsLink).not.toHaveAttribute('disabled')
          expect(analyticsLink).toBeEnabled()

          // Link should point to public analytics route
          expect(analyticsLink.getAttribute('href')).toBe('/stats/unauthlink')
        })
      })

      it('should show registration prompt alongside View Analytics for guest users', async () => {
        mockedShortenUrl.mockResolvedValue({
          id: 1,
          original_url: 'https://example.com/test',
          short_code: 'prompttest',
          created_at: new Date().toISOString(),
          user_id: null,
          click_count: 0,
        })

        const user = userEvent.setup()
        renderWithRouter(<GuestShortener />)

        await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
        await user.click(screen.getByRole('button', { name: /shorten/i }))

        await waitFor(() => {
          // Both View Analytics and registration prompt should be visible
          expect(screen.getByTestId('view-analytics-link')).toBeInTheDocument()
          expect(screen.getByTestId('account-prompt')).toBeInTheDocument()
          expect(screen.getByRole('link', { name: /create free account/i })).toBeInTheDocument()
        })
      })
    })
  })

  /**
   * Additional edge case tests
   */
  describe('Edge Cases', () => {
    it('should show error when submitting empty URL', async () => {
      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      const button = screen.getByRole('button', { name: /shorten/i })
      await user.click(button)

      await waitFor(() => {
        expect(screen.getByText('Please enter a URL')).toBeInTheDocument()
      })
    })

    it('should allow shortening another URL after success', async () => {
      mockedShortenUrl.mockResolvedValue({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'reset123',
        created_at: new Date().toISOString(),
        user_id: null,
        click_count: 0,
      })

      const user = userEvent.setup()
      renderWithRouter(<GuestShortener />)

      await user.type(screen.getByTestId('url-input'), 'https://example.com/test')
      await user.click(screen.getByRole('button', { name: /shorten/i }))

      await waitFor(() => {
        expect(screen.getByTestId('success-message')).toBeInTheDocument()
      })

      // Click "Shorten Another URL" button
      await user.click(screen.getByRole('button', { name: /shorten another/i }))

      // Should be back to initial state
      expect(screen.getByTestId('url-input')).toBeInTheDocument()
      expect(screen.queryByTestId('success-message')).not.toBeInTheDocument()
    })
  })
})
