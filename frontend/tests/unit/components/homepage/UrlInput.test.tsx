/**
 * Tests for UrlInput component.
 * Owner: Scenario 2 - Hero Section URL Input and Shortening CTA
 *
 * Test cases:
 * - URL input field exists with correct type and placeholder
 * - URL input field receives focus on mount
 * - Shorten URL button exists
 * - Enter key triggers form submission
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, waitFor } from '../../../test-utils'
import userEvent from '@testing-library/user-event'
import UrlInput from '@/components/homepage/UrlInput'

describe('UrlInput', () => {
  it('renders URL input field with correct type and placeholder', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    expect(input).toBeInTheDocument()
    expect(input).toHaveAttribute('type', 'url')
    expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
  })

  it('auto-focuses URL input field on mount', async () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')

    await waitFor(() => {
      expect(document.activeElement).toBe(input)
    })
  })

  it('renders Shorten URL button', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const button = screen.getByTestId('shorten-button')
    expect(button).toBeInTheDocument()
    expect(button).toHaveTextContent('Shorten URL')
  })

  it('triggers submission when Enter key is pressed with valid URL', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    await user.type(input, 'https://example.com')
    await user.keyboard('{Enter}')

    expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
  })

  it('triggers submission when Shorten URL button is clicked', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    const button = screen.getByTestId('shorten-button')

    await user.type(input, 'https://example.com')
    await user.click(button)

    expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
  })

  it('does not submit when input is empty', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const button = screen.getByTestId('shorten-button')
    await user.click(button)

    expect(handleSubmit).not.toHaveBeenCalled()
  })

  it('does not submit empty string when Enter is pressed', async () => {
    const handleSubmit = vi.fn()
    const user = userEvent.setup()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    await user.click(input)
    await user.keyboard('{Enter}')

    expect(handleSubmit).not.toHaveBeenCalled()
  })

  it('displays error message when error prop is provided', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} error="Invalid URL format" />)

    const errorMessage = screen.getByTestId('url-error')
    expect(errorMessage).toBeInTheDocument()
    expect(errorMessage).toHaveTextContent('Invalid URL format')
  })

  it('shows loading spinner when isLoading is true', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

    const button = screen.getByTestId('shorten-button')
    expect(button).toContainElement(button.querySelector('.loading'))
    expect(button).not.toHaveTextContent('Shorten URL')
  })

  it('disables input and button when isLoading is true', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} isLoading={true} />)

    const input = screen.getByTestId('url-input')
    const button = screen.getByTestId('shorten-button')

    expect(input).toBeDisabled()
    expect(button).toBeDisabled()
  })

  it('has proper accessibility attributes', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} />)

    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-label', 'URL to shorten')
  })

  it('associates error with input via aria-describedby when error exists', () => {
    const handleSubmit = vi.fn()
    render(<UrlInput onSubmit={handleSubmit} error="Invalid URL" />)

    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-describedby', 'url-error')
  })
})
