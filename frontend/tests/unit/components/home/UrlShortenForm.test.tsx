/**
 * Unit tests for UrlShortenForm component.
 * Owner: Scenario 2 - Guest URL Creation Flow
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { UrlShortenForm } from '../../../../src/components/home/UrlShortenForm';

describe('UrlShortenForm', () => {
  const mockFetch = vi.fn();
  const originalFetch = global.fetch;

  beforeEach(() => {
    global.fetch = mockFetch;
    mockFetch.mockReset();
  });

  afterEach(() => {
    global.fetch = originalFetch;
    vi.clearAllMocks();
  });

  it('should render the form with input and button', () => {
    render(<UrlShortenForm />);

    expect(screen.getByTestId('url-shorten-form')).toBeInTheDocument();
    expect(screen.getByTestId('url-input')).toBeInTheDocument();
    expect(screen.getByTestId('shorten-button')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Paste your long URL here...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /shorten/i })).toBeInTheDocument();
  });

  it('should have accessible labels', () => {
    render(<UrlShortenForm />);

    expect(screen.getByLabelText('URL to shorten')).toBeInTheDocument();
    expect(screen.getByLabelText('Shorten URL')).toBeInTheDocument();
  });

  it('should show error for empty URL submission', async () => {
    const user = userEvent.setup();
    render(<UrlShortenForm />);

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Please enter a URL')).toBeInTheDocument();
    });
  });

  it('should show error for invalid URL format', async () => {
    const user = userEvent.setup();
    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'not-a-valid-url');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Please enter a valid URL')).toBeInTheDocument();
    });
  });

  it('should show loading state during submission', async () => {
    const user = userEvent.setup();

    // Create a promise that we can control
    let resolvePromise: (value: unknown) => void;
    const pendingPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });
    mockFetch.mockReturnValueOnce(pendingPromise);

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    // Check loading state
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /shortening/i })).toBeInTheDocument();
      expect(submitButton).toBeDisabled();
    });

    // Resolve the fetch
    resolvePromise!({
      ok: true,
      json: () => Promise.resolve({ short_code: 'abc', original_url: 'https://example.com' }),
    });

    await waitFor(() => {
      expect(submitButton).not.toBeDisabled();
    });
  });

  it('should disable button during loading', async () => {
    const user = userEvent.setup();

    let resolvePromise: (value: unknown) => void;
    const pendingPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });
    mockFetch.mockReturnValueOnce(pendingPromise);

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(submitButton).toBeDisabled();
    });

    // Cleanup
    resolvePromise!({
      ok: true,
      json: () => Promise.resolve({ short_code: 'abc', original_url: 'https://example.com' }),
    });
  });

  it('should display shortened URL after successful submission', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com/long-path',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com/long-path');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
      expect(screen.getByTestId('short-url')).toBeInTheDocument();
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    // Check that the short URL link is correct
    const shortUrlLink = screen.getByTestId('short-url');
    expect(shortUrlLink).toHaveAttribute('href', expect.stringContaining('/r/abc123'));
  });

  it('should show copy button after successful URL creation', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });
  });

  it('should show "New URL" button after successful creation', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('new-url-button')).toBeInTheDocument();
    });
  });

  it('should reset form when "New URL" button is clicked', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
    });

    const newUrlButton = screen.getByTestId('new-url-button');
    await user.click(newUrlButton);

    await waitFor(() => {
      expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
      expect(screen.getByTestId('url-input')).toHaveValue('');
    });
  });

  it('should mark input as invalid when there is an error', async () => {
    const user = userEvent.setup();
    render(<UrlShortenForm />);

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      const input = screen.getByTestId('url-input');
      expect(input).toHaveAttribute('aria-invalid', 'true');
    });
  });

  it('should show guest URL message', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText(/create an account to save your links/i)).toBeInTheDocument();
    });
  });

  it('should handle API error', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ detail: 'Server error' }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Server error')).toBeInTheDocument();
    });
  });

  it('should clear error when typing new URL', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    // First, create a successful result
    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
    });

    // Type a new URL - should clear the result
    await user.clear(input);
    await user.type(input, 'https://new-url.com');

    await waitFor(() => {
      expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
    });
  });

  it('should submit form on Enter key', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'abc123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com{enter}');

    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalled();
    });
  });
});
