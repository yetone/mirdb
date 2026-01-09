import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import UrlDemoSection from '../src/components/UrlDemoSection'

// Wrapper component for router context
const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Mock clipboard API
const mockWriteText = vi.fn().mockResolvedValue(undefined)

describe('UrlDemoSection', () => {
  beforeEach(() => {
    // Mock clipboard API using vi.stubGlobal for proper jsdom support
    vi.stubGlobal('navigator', {
      ...navigator,
      clipboard: {
        writeText: mockWriteText,
      },
    })
    mockWriteText.mockClear()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  // Test Case 1: Check for URL demo section on homepage
  it('renders the URL demo section with all required elements', () => {
    renderWithRouter(<UrlDemoSection />)

    // Check section is visible
    const section = screen.getByTestId('url-demo-section')
    expect(section).toBeInTheDocument()

    // Check heading is visible
    const heading = screen.getByTestId('demo-heading')
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('Try It Out')

    // Check description is visible
    const description = screen.getByTestId('demo-description')
    expect(description).toBeInTheDocument()
    expect(description).toHaveTextContent(/shorten/i)

    // Check input field is present
    const input = screen.getByTestId('demo-url-input')
    expect(input).toBeInTheDocument()

    // Check output container is present
    const outputContainer = screen.getByTestId('demo-output-container')
    expect(outputContainer).toBeInTheDocument()

    // Check copy button is present
    const copyButton = screen.getByTestId('demo-copy-button')
    expect(copyButton).toBeInTheDocument()
  })

  // Test Case 2: Input field accepts text entry
  it('accepts text entry in the URL input field', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    expect(input).toHaveValue('')

    await user.type(input, 'https://example.com/test')
    expect(input).toHaveValue('https://example.com/test')
  })

  // Test Case 3: Demo shows example shortened URL format
  it('displays a shortened URL preview when user enters a URL', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    await user.type(input, 'https://example.com/long-url')

    // Check that shortened URL appears
    const shortenedUrl = screen.getByTestId('demo-shortened-url')
    expect(shortenedUrl).toBeInTheDocument()
    expect(shortenedUrl.textContent).toMatch(/https:\/\/short\.url\/[a-z0-9]+/)
  })

  // Additional tests for comprehensive coverage
  it('shows placeholder text when no URL is entered', () => {
    renderWithRouter(<UrlDemoSection />)

    const placeholder = screen.getByTestId('demo-output-placeholder')
    expect(placeholder).toBeInTheDocument()
    expect(placeholder).toHaveTextContent('Your short URL will appear here')
  })

  it('removes placeholder when URL is entered', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    await user.type(input, 'https://test.com')

    // Placeholder should no longer be visible
    expect(screen.queryByTestId('demo-output-placeholder')).not.toBeInTheDocument()
    // Shortened URL should be visible
    expect(screen.getByTestId('demo-shortened-url')).toBeInTheDocument()
  })

  it('has a copy button that is disabled when no URL is generated', () => {
    renderWithRouter(<UrlDemoSection />)

    const copyButton = screen.getByTestId('demo-copy-button')
    expect(copyButton).toBeDisabled()
  })

  it('has an enabled copy button when a shortened URL is generated', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    await user.type(input, 'https://example.com')

    const copyButton = screen.getByTestId('demo-copy-button')
    expect(copyButton).not.toBeDisabled()
  })

  it('copy button changes state when clicked indicating copy action occurred', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    await user.type(input, 'https://example.com')

    const copyButton = screen.getByTestId('demo-copy-button')

    // Before click - should say "Copy shortened URL"
    expect(copyButton).toHaveAttribute('aria-label', 'Copy shortened URL')

    await user.click(copyButton)

    // After click - should say "Copied to clipboard"
    await waitFor(() => {
      expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard')
    })
  })

  it('shows "Copied!" feedback after clicking copy', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')
    await user.type(input, 'https://example.com')

    const copyButton = screen.getByTestId('demo-copy-button')
    await user.click(copyButton)

    // Check for copied state
    await waitFor(() => {
      expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard')
    })
  })

  it('includes a disclaimer about demo mode', () => {
    renderWithRouter(<UrlDemoSection />)

    const disclaimer = screen.getByTestId('demo-disclaimer')
    expect(disclaimer).toBeInTheDocument()
    expect(disclaimer).toHaveTextContent(/demo preview/i)
    expect(disclaimer).toHaveTextContent(/sign up/i)
  })

  it('includes a call-to-action to sign up', () => {
    renderWithRouter(<UrlDemoSection />)

    const ctaSection = screen.getByTestId('demo-cta-section')
    expect(ctaSection).toBeInTheDocument()

    const signupCta = screen.getByTestId('demo-signup-cta')
    expect(signupCta).toBeInTheDocument()
    expect(signupCta).toHaveAttribute('href', '/register')
    expect(signupCta).toHaveTextContent(/sign up/i)
  })

  it('has proper accessibility attributes', () => {
    renderWithRouter(<UrlDemoSection />)

    // Check section has aria-labelledby
    const section = screen.getByTestId('url-demo-section')
    expect(section).toHaveAttribute('aria-labelledby', 'demo-heading')

    // Check input has associated label
    const input = screen.getByTestId('demo-url-input')
    expect(input).toHaveAttribute('aria-describedby', 'demo-input-hint')

    // Check heading is properly associated
    const heading = screen.getByTestId('demo-heading')
    expect(heading).toHaveAttribute('id', 'demo-heading')
  })

  it('generates different shortened URLs for different inputs', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')

    // Enter first URL
    await user.type(input, 'https://first.com')
    const firstUrl = screen.getByTestId('demo-shortened-url').textContent

    // Clear and enter second URL
    await user.clear(input)
    await user.type(input, 'https://second.com')
    const secondUrl = screen.getByTestId('demo-shortened-url').textContent

    expect(firstUrl).not.toBe(secondUrl)
  })

  it('updates shortened URL as user types', async () => {
    const user = userEvent.setup()
    renderWithRouter(<UrlDemoSection />)

    const input = screen.getByTestId('demo-url-input')

    await user.type(input, 'a')
    const url1 = screen.getByTestId('demo-shortened-url').textContent

    await user.type(input, 'b')
    const url2 = screen.getByTestId('demo-shortened-url').textContent

    // URLs should be different after typing more
    expect(url1).not.toBe(url2)
  })

  it('section has correct id for anchor navigation', () => {
    renderWithRouter(<UrlDemoSection />)

    const section = screen.getByTestId('url-demo-section')
    expect(section).toHaveAttribute('id', 'demo')
  })
})
