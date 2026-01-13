import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../contexts/ThemeContext'
import Home from './Home'
import { AppRoutes } from '../App'

// Helper to render Home with ThemeProvider
function renderHome() {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

// Setup and teardown for animation tests
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
})

afterEach(() => {
  vi.useRealTimers()
})

// Helper to render AppRoutes with ThemeProvider
function renderAppRoutes(initialEntries: string[] = ['/']) {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>
        <AppRoutes />
      </MemoryRouter>
    </ThemeProvider>
  )
}

// Test Case 1: Unit test - Login navigation element is visible and accessible
describe('Test Case 1: Login navigation element visibility', () => {
  it('should render Login link/button on homepage', () => {
    renderHome()

    const loginLink = screen.getByTestId('login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toBeVisible()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('should have accessible Login button in CTA section', () => {
    renderHome()
    vi.advanceTimersByTime(1000) // Allow animations to complete

    const loginBtn = screen.getByTestId('login-btn')
    expect(loginBtn).toBeInTheDocument()
    expect(loginBtn).toHaveTextContent('Login')
  })
})

// Test Case 2: Unit test - Register/Sign Up navigation element is visible and accessible
describe('Test Case 2: Register navigation element visibility', () => {
  it('should render Sign Up link on homepage', () => {
    renderHome()

    const registerLink = screen.getByTestId('register-link')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toBeVisible()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  it('should have accessible Get Started button in CTA section', () => {
    renderHome()
    vi.advanceTimersByTime(1000) // Allow animations to complete

    const getStartedBtn = screen.getByTestId('get-started-btn')
    expect(getStartedBtn).toBeInTheDocument()
    expect(getStartedBtn).toHaveTextContent('Get Started')
  })
})

// Test Case 3: Integration test - Click Login link and check router navigation
describe('Test Case 3: Login navigation integration', () => {
  it('should navigate to /login when Login link is clicked', () => {
    renderAppRoutes()

    const loginLink = screen.getByTestId('login-link')
    fireEvent.click(loginLink)

    expect(screen.getByTestId('login-page')).toBeInTheDocument()
  })

  it('should navigate to /login when Login button is clicked', () => {
    renderAppRoutes()

    const loginBtn = screen.getByTestId('login-btn')
    fireEvent.click(loginBtn)

    expect(screen.getByTestId('login-page')).toBeInTheDocument()
  })
})

// Test Case 4: Integration test - Click Register/Sign Up button and check router navigation
describe('Test Case 4: Register navigation integration', () => {
  it('should navigate to /register when Sign Up link is clicked', () => {
    renderAppRoutes()

    const registerLink = screen.getByTestId('register-link')
    fireEvent.click(registerLink)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  it('should navigate to /register when Get Started button is clicked', () => {
    renderAppRoutes()

    const getStartedBtn = screen.getByTestId('get-started-btn')
    fireEvent.click(getStartedBtn)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })
})

// Test Case 5: E2E test - Navigation response time
describe('Test Case 5: Navigation response time', () => {
  it('should complete Login navigation in less than 1 second', () => {
    const startTime = performance.now()

    renderAppRoutes()

    const loginLink = screen.getByTestId('login-link')
    fireEvent.click(loginLink)

    expect(screen.getByTestId('login-page')).toBeInTheDocument()

    const endTime = performance.now()
    const navigationTime = endTime - startTime

    expect(navigationTime).toBeLessThan(1000)
  })

  it('should complete Register navigation in less than 1 second', () => {
    const startTime = performance.now()

    renderAppRoutes()

    const registerLink = screen.getByTestId('register-link')
    fireEvent.click(registerLink)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()

    const endTime = performance.now()
    const navigationTime = endTime - startTime

    expect(navigationTime).toBeLessThan(1000)
  })
})

/**
 * URL Shortening Demo Section Tests
 * Implements test cases for scenario: URL Shortening Demo (REQ-4, US-4)
 */
describe('URL Shortening Demo Section', () => {
  /**
   * Test Case 1: Demo section with input field and submit button is rendered
   * Input: Render homepage and query for demo section with URL input field
   * Expected: Demo section with input field and submit button is rendered
   */
  it('should render demo section with URL input field and submit button', () => {
    renderHome()

    // Verify demo section exists
    const demoSection = screen.getByTestId('demo-section')
    expect(demoSection).toBeInTheDocument()

    // Verify input field exists
    const urlInput = screen.getByTestId('demo-url-input')
    expect(urlInput).toBeInTheDocument()
    expect(urlInput).toHaveAttribute('placeholder')

    // Verify submit button exists
    const shortenButton = screen.getByTestId('demo-shorten-button')
    expect(shortenButton).toBeInTheDocument()
    expect(shortenButton).toHaveTextContent(/shorten/i)
  })

  /**
   * Test Case 2: Input accepts URL and displays it correctly
   * Input: Enter valid URL 'https://example.com/test' in demo input
   * Expected: Input accepts the URL and displays it correctly
   */
  it('should accept and display valid URL in demo input', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const testUrl = 'https://example.com/test'

    await user.type(urlInput, testUrl)

    expect(urlInput).toHaveValue(testUrl)
  })

  /**
   * Test Case 3: Form submission triggers API call or displays shortened URL
   * Input: Submit demo form with valid URL
   * Expected: Form submission triggers API call or displays shortened URL
   */
  it('should display shortened URL after submitting valid URL', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter a valid URL
    await user.type(urlInput, 'https://example.com/very-long-url-path')

    // Click shorten button
    await user.click(shortenButton)

    // Wait for the shortened URL to appear
    await waitFor(
      () => {
        const result = screen.getByTestId('demo-result')
        expect(result).toBeInTheDocument()
      },
      { timeout: 2000 }
    )

    // Verify shortened URL is displayed
    const shortenedUrl = screen.getByTestId('demo-shortened-url')
    expect(shortenedUrl).toBeInTheDocument()
    expect(shortenedUrl.textContent).toMatch(/\/r\/[A-Za-z0-9]+/)
  })

  /**
   * Test Case 4: CTA prompting user to sign up is displayed after demo result
   * Input: Check for sign-up prompt after demo URL shortening
   * Expected: CTA prompting user to sign up is displayed after demo result
   */
  it('should display sign-up CTA after shortening URL', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter and submit a valid URL
    await user.type(urlInput, 'https://example.com/test-url')
    await user.click(shortenButton)

    // Wait for result and CTA to appear
    await waitFor(
      () => {
        const signupCta = screen.getByTestId('demo-signup-cta')
        expect(signupCta).toBeInTheDocument()
      },
      { timeout: 2000 }
    )

    // Verify CTA content
    const signupCta = screen.getByTestId('demo-signup-cta')
    expect(signupCta).toHaveTextContent(/sign up/i)
    expect(signupCta).toHaveTextContent(/track|save|analytics/i)

    // Verify there's a Sign Up Free button
    const signupButton = screen.getByRole('button', { name: /sign up free/i })
    expect(signupButton).toBeInTheDocument()
  })

  /**
   * Test Case 5: Validation error is displayed for invalid URL
   * Input: Submit demo form with invalid URL 'not-a-url'
   * Expected: Validation error is displayed to the user
   */
  it('should display validation error for invalid URL', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter an invalid URL
    await user.type(urlInput, 'not-a-url')

    // Click shorten button
    await user.click(shortenButton)

    // Verify error message is displayed
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
    expect(errorMessage).toHaveAttribute('role', 'alert')
  })

  /**
   * Test Case 6: Shorten button uses FuturisticButton component
   * Input: Verify FuturisticButton component usage for Shorten button
   * Expected: Shorten button uses FuturisticButton component
   */
  it('should use FuturisticButton component for Shorten button', () => {
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')

    // FuturisticButton renders as a motion.button which should have the characteristic
    // gradient styling and animation-related attributes
    expect(shortenButton).toBeInTheDocument()
    expect(shortenButton.tagName).toBe('BUTTON')

    // Check for FuturisticButton's characteristic gradient styling
    // The button should have classes from FuturisticButton's styling
    expect(shortenButton).toHaveClass('bg-gradient-to-r')
    expect(shortenButton).toHaveClass('from-primary')
    expect(shortenButton).toHaveClass('to-secondary')
  })

  /**
   * Additional test: Empty input validation
   */
  it('should display error when submitting empty input', async () => {
    const user = userEvent.setup()
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Click shorten button without entering URL
    await user.click(shortenButton)

    // Verify error message is displayed
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/enter a url/i)
  })

  /**
   * Additional test: Copy functionality
   */
  it('should allow copying shortened URL to clipboard', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter and submit a valid URL
    await user.type(urlInput, 'https://example.com/copy-test')
    await user.click(shortenButton)

    // Wait for copy button to appear
    await waitFor(
      () => {
        const copyButton = screen.getByTestId('demo-copy-button')
        expect(copyButton).toBeInTheDocument()
      },
      { timeout: 2000 }
    )

    // Click copy button
    const copyButton = screen.getByTestId('demo-copy-button')
    await user.click(copyButton)

    // Verify copy button is present and clickable
    await waitFor(() => {
      expect(copyButton).toBeInTheDocument()
    })
  })
})

/**
 * Error Handling - Invalid Demo Input Tests
 * Scenario ID: 18 - Validate error handling when user enters invalid URL in demo section
 */
describe('Error Handling - Invalid Demo Input', () => {
  /**
   * Test Case 1: Submit demo form with empty input
   * Input: Submit demo form with empty input
   * Expected: Validation error is displayed or button is disabled
   */
  it('should display validation error when submitting empty input', async () => {
    const user = userEvent.setup()
    renderHome()

    const shortenButton = screen.getByTestId('demo-shorten-button')
    const urlInput = screen.getByTestId('demo-url-input')

    // Ensure input is empty
    expect(urlInput).toHaveValue('')

    // Click shorten button without entering URL
    await user.click(shortenButton)

    // Verify error message is displayed
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/enter a url/i)
    expect(errorMessage).toHaveAttribute('role', 'alert')
  })

  /**
   * Test Case 2: Submit demo form with 'not-a-url' as input
   * Input: Submit demo form with 'not-a-url' as input
   * Expected: Error message indicates URL is invalid
   */
  it('should display error message for invalid URL format like "not-a-url"', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter an invalid URL
    await user.type(urlInput, 'not-a-url')

    // Click shorten button
    await user.click(shortenButton)

    // Verify error message is displayed
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
    expect(errorMessage).toHaveAttribute('role', 'alert')

    // Should not display shortened URL result
    expect(screen.queryByTestId('demo-result')).not.toBeInTheDocument()
  })

  /**
   * Test Case 3: Submit demo form with 'ftp://example.com' (non-http URL)
   * Input: Submit demo form with 'ftp://example.com' (non-http URL)
   * Expected: Appropriate handling for non-HTTP URLs (error or accept)
   */
  it('should display error for non-HTTP URLs like ftp://example.com', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Enter a non-HTTP URL
    await user.type(urlInput, 'ftp://example.com')

    // Click shorten button
    await user.click(shortenButton)

    // Verify error message is displayed (only http/https are valid)
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
    expect(errorMessage).toHaveAttribute('role', 'alert')

    // Should not display shortened URL result
    expect(screen.queryByTestId('demo-result')).not.toBeInTheDocument()
  })

  /**
   * Test Case 4: Submit demo form with extremely long URL (>2048 chars)
   * Input: Submit demo form with extremely long URL (>2048 chars)
   * Expected: Appropriate handling for very long URLs
   */
  it('should display error for extremely long URLs (>2048 chars)', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input') as HTMLInputElement
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // Create a URL that exceeds 2048 characters
    const baseUrl = 'https://example.com/?param='
    const longParam = 'a'.repeat(2049 - baseUrl.length + 1) // Ensure total > 2048
    const longUrl = baseUrl + longParam

    // Verify the URL is longer than 2048 characters
    expect(longUrl.length).toBeGreaterThan(2048)

    // Use fireEvent to set value directly (userEvent.type is too slow for 2000+ chars)
    fireEvent.change(urlInput, { target: { value: longUrl } })
    expect(urlInput.value).toBe(longUrl)

    // Click shorten button
    await user.click(shortenButton)

    // Verify error message about URL length is displayed
    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/too long|maximum length|2048/i)
    expect(errorMessage).toHaveAttribute('role', 'alert')

    // Should not display shortened URL result
    expect(screen.queryByTestId('demo-result')).not.toBeInTheDocument()
  })

  /**
   * Additional test: Error message clears when entering new valid input
   */
  it('should clear error message when successfully shortening a URL after an error', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    // First, trigger an error with invalid input
    await user.type(urlInput, 'invalid-url')
    await user.click(shortenButton)

    // Verify error is shown
    expect(screen.getByTestId('demo-error')).toBeInTheDocument()

    // Clear input and enter valid URL
    await user.clear(urlInput)
    await user.type(urlInput, 'https://example.com')
    await user.click(shortenButton)

    // Wait for result to appear
    await waitFor(
      () => {
        expect(screen.getByTestId('demo-result')).toBeInTheDocument()
      },
      { timeout: 2000 }
    )

    // Error message should no longer be visible
    expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument()
  })

  /**
   * Additional test: javascript: protocol should be rejected
   */
  it('should reject javascript: URLs', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    await user.type(urlInput, 'javascript:alert(1)')
    await user.click(shortenButton)

    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
  })

  /**
   * Additional test: mailto: protocol should be rejected
   */
  it('should reject mailto: URLs', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    await user.type(urlInput, 'mailto:test@example.com')
    await user.click(shortenButton)

    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
  })

  /**
   * Additional test: file: protocol should be rejected
   */
  it('should reject file: URLs', async () => {
    const user = userEvent.setup()
    renderHome()

    const urlInput = screen.getByTestId('demo-url-input')
    const shortenButton = screen.getByTestId('demo-shorten-button')

    await user.type(urlInput, 'file:///etc/passwd')
    await user.click(shortenButton)

    const errorMessage = screen.getByTestId('demo-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent(/valid url/i)
  })
})

/**
 * Footer Section Tests
 * Implements test cases for scenario: Footer Section Display (REQ-8)
 */
describe('Footer Section Display', () => {
  /**
   * Test Case 1: Footer section is present at the bottom of the page
   * Input: Render homepage and query for footer element
   * Expected: Footer section is present at the bottom of the page
   */
  it('should render footer section on homepage', () => {
    renderHome()

    // Verify footer element exists
    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  /**
   * Test Case 2: Footer contains navigation links
   * Input: Query for navigation links in footer
   * Expected: Footer contains navigation links
   */
  it('should contain navigation links in footer', () => {
    renderHome()

    // Verify footer has navigation links
    const footerNav = screen.getByTestId('footer-nav')
    expect(footerNav).toBeInTheDocument()

    // Check for specific navigation links
    const loginLink = screen.getByTestId('footer-login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')

    const registerLink = screen.getByTestId('footer-register-link')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  /**
   * Test Case 3: Footer contains copyright information
   * Input: Query for copyright text in footer
   * Expected: Copyright information is displayed
   */
  it('should display copyright information in footer', () => {
    renderHome()

    // Verify copyright text exists
    const copyrightText = screen.getByTestId('footer-copyright')
    expect(copyrightText).toBeInTheDocument()
    expect(copyrightText).toHaveTextContent(/©/)
    expect(copyrightText).toHaveTextContent(/URL Shortener/i)
    expect(copyrightText).toHaveTextContent(/all rights reserved/i)
  })

  /**
   * Additional test: Footer links are accessible
   */
  it('should have accessible footer navigation links', () => {
    renderHome()

    const footerNav = screen.getByTestId('footer-nav')

    // All links should be visible and accessible
    const links = footerNav.querySelectorAll('a')
    expect(links.length).toBeGreaterThan(0)

    links.forEach((link) => {
      expect(link).toBeVisible()
      expect(link).toHaveAttribute('href')
    })
  })
})
