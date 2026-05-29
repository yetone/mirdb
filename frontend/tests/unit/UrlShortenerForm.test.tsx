/**
 * UrlShortenerForm Unit Tests
 * Owner: Scenario 2 - Anonymous URL Shortening Flow
 *        Extended by Scenario 3 - URL Form Validation
 *
 * Tests for form submission, validation, and result display.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import UrlShortenerForm, { validateUrl } from '../../src/components/Home/UrlShortenerForm';
import { ThemeProvider } from '../../src/contexts/ThemeContext';

describe('UrlShortenerForm component', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    fetchSpy = vi.spyOn(globalThis, 'fetch');
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  it('renders input field and submit button', () => {
    render(<UrlShortenerForm />);
    expect(screen.getByTestId('url-input')).toBeInTheDocument();
    expect(screen.getByTestId('shorten-button')).toBeInTheDocument();
  });

  it('shows "Please enter a URL" when submitting empty form (TC-1)', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const button = screen.getByTestId('shorten-button');

    await user.click(button);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Please enter a URL');
  });

  it('shows validation error for invalid URL format (TC-2)', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'not-a-url-at-all');
    await user.click(button);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Please enter a valid URL');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  it('clears error when user starts typing after an error', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'not-a-url');
    await user.click(button);

    await screen.findByTestId('url-error');

    await user.clear(input);
    await user.type(input, 'example.com');

    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument();
  });

  it('accepts URL without protocol and auto-prefixes with https:// (TC-3)', async () => {
    const user = userEvent.setup();
    const onSuccess = vi.fn();

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://short.link/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(<UrlShortenerForm onSuccess={onSuccess} />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'example.com/path');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });
    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument();
  });

  it('rejects javascript: URL and shows security error (TC-4)', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'javascript:alert(1)');
    await user.click(button);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Invalid URL: unsafe protocol');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  it('rejects URL longer than 2048 characters (TC-5)', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    const longUrl = 'https://example.com/' + 'a'.repeat(2040);
    await user.type(input, longUrl);
    await user.click(button);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('URL is too long (max 2048 characters)');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  it('has correct ARIA attributes for accessibility', () => {
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    expect(input).toHaveAttribute('type', 'url');
    expect(input).toHaveAttribute('aria-invalid', 'false');
  });

  it('sets aria-invalid to true when there is an error', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.click(button);

    await screen.findByTestId('url-error');
    expect(input).toHaveAttribute('aria-invalid', 'true');
  });
});

describe('validateUrl', () => {
  it('rejects empty string with "Please enter a URL" error (TC-1)', () => {
    const result = validateUrl('');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Please enter a URL');
  });

  it('rejects whitespace-only string as empty (TC-1)', () => {
    const result = validateUrl('   ');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Please enter a URL');
  });

  it('rejects invalid URL format "not-a-url-at-all" (TC-2)', () => {
    const result = validateUrl('not-a-url-at-all');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Please enter a valid URL');
  });

  it('auto-prefixes URL without protocol with https:// (TC-3)', () => {
    const result = validateUrl('example.com/path');
    expect(result.isValid).toBe(true);
    expect(result.normalizedUrl).toBe('https://example.com/path');
  });

  it('rejects javascript: protocol for security (TC-4)', () => {
    const result = validateUrl('javascript:alert(1)');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Invalid URL: unsafe protocol');
  });

  it('rejects data: protocol for security', () => {
    const result = validateUrl('data:text/html,<script>alert(1)</script>');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Invalid URL: unsafe protocol');
  });

  it('rejects vbscript: protocol for security', () => {
    const result = validateUrl('vbscript:msgbox("test")');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Invalid URL: unsafe protocol');
  });

  it('rejects file: protocol for security', () => {
    const result = validateUrl('file:///etc/passwd');
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('Invalid URL: unsafe protocol');
  });

  it('accepts valid http URL', () => {
    const result = validateUrl('http://example.com');
    expect(result.isValid).toBe(true);
    expect(result.normalizedUrl).toBe('http://example.com');
  });

  it('accepts valid https URL', () => {
    const result = validateUrl('https://example.com/path?query=1');
    expect(result.isValid).toBe(true);
    expect(result.normalizedUrl).toBe('https://example.com/path?query=1');
  });

  it('accepts valid URL with port', () => {
    const result = validateUrl('https://localhost:8080/api');
    expect(result.isValid).toBe(true);
    expect(result.normalizedUrl).toBe('https://localhost:8080/api');
  });

  it('rejects URL longer than 2048 characters (TC-5)', () => {
    const longUrl = 'https://example.com/' + 'a'.repeat(2040);
    expect(longUrl.length).toBeGreaterThan(2048);
    const result = validateUrl(longUrl);
    expect(result.isValid).toBe(false);
    expect(result.error).toBe('URL is too long (max 2048 characters)');
  });

  it('accepts URL at exactly 2048 characters', () => {
    const exactUrl = 'https://x.com/' + 'a'.repeat(2034);
    expect(exactUrl.length).toBe(2048);
    const result = validateUrl(exactUrl);
    expect(result.isValid).toBe(true);
  });
});

describe('UrlShortenerForm - Result Display (Scenario 13)', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;
  let clipboardWriteText: ReturnType<typeof vi.fn>;
  let originalIsSecureContext: boolean | undefined;

  beforeEach(() => {
    fetchSpy = vi.spyOn(globalThis, 'fetch');
    clipboardWriteText = vi.fn();
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: clipboardWriteText },
      writable: true,
      configurable: true,
    });
    originalIsSecureContext = window.isSecureContext;
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    Object.defineProperty(window, 'isSecureContext', {
      value: originalIsSecureContext,
      writable: true,
      configurable: true,
    });
  });

  it('TC-1: displays shortened URL prominently after successful submission', async () => {
    const user = userEvent.setup();
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'https://example.com/long-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });

    const result = screen.getByTestId('result');
    expect(result).toHaveTextContent('Your shortened URL');
    expect(result).toHaveTextContent('https://example.com/r/abc123');

    const urlLink = screen.getByTestId('result-url');
    expect(urlLink).toHaveAttribute('href', 'https://example.com/r/abc123');
    expect(urlLink).toHaveClass('font-semibold');
    expect(urlLink).toHaveClass('text-base');
  });

  it('TC-2: clicking copy button writes URL to clipboard and shows success feedback', async () => {
    const user = userEvent.setup();

    // @ts-expect-error - jsdom doesn't implement isSecureContext
    window.isSecureContext = true;

    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: writeTextMock },
      writable: true,
      configurable: true,
    });

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'https://example.com/long-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toHaveTextContent('Copy');
    expect(screen.getByTestId('copy-icon')).toBeInTheDocument();

    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(copyButton).toHaveTextContent('Copied!');
    });
    await waitFor(() => {
      expect(screen.getByTestId('copy-check-icon')).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(writeTextMock).toHaveBeenCalledWith('https://example.com/r/abc123');
    });
    expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard');
  });

  it('TC-3: shows fallback message when clipboard is unavailable (insecure context)', async () => {
    const user = userEvent.setup();

    Object.defineProperty(window, 'isSecureContext', {
      value: false,
      writable: true,
      configurable: true,
    });

    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    });

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'https://example.com/long-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });

    const copyButton = screen.getByTestId('copy-button');
    await user.click(copyButton);

    await waitFor(() => {
      expect(screen.getByTestId('copy-fallback-message')).toBeInTheDocument();
    });

    expect(screen.getByTestId('copy-fallback-message')).toHaveTextContent(
      'URL selected for manual copying. Press Ctrl+C to copy.'
    );
    expect(screen.getByTestId('copy-fallback-message')).toHaveAttribute('role', 'status');
  });

  it('TC-4: submitting another URL replaces previous result', async () => {
    const user = userEvent.setup();

    fetchSpy
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ short_url: 'https://example.com/r/xyz789' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'https://example.com/first-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result-url')).toHaveTextContent('https://example.com/r/abc123');
    });

    await user.clear(input);
    await user.type(input, 'https://example.com/second-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result-url')).toHaveTextContent('https://example.com/r/xyz789');
    });

    expect(screen.queryByText('https://example.com/r/abc123')).not.toBeInTheDocument();
    expect(screen.queryAllByTestId('result')).toHaveLength(1);
  });

  it('TC-5: result area adapts styling to light theme', async () => {
    const user = userEvent.setup();
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    await user.type(input, 'https://example.com/long-url');
    await user.click(button);

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });

    const result = screen.getByTestId('result');
    expect(result).toHaveClass('bg-green-50');
    expect(result).toHaveClass('border-green-200');

    const label = screen.getByTestId('result-label');
    expect(label).toHaveClass('text-green-800');
  });

  it('TC-5: result area adapts styling to dark theme', async () => {
    const user = userEvent.setup();
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    await user.type(screen.getByTestId('url-input'), 'https://example.com/long-url');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });

    const result = screen.getByTestId('result');
    expect(result).toHaveAttribute('data-theme');
  });

  it('TC-5: copy button adapts styling to cyberpunk theme', async () => {
    const user = userEvent.setup();
    clipboardWriteText.mockResolvedValueOnce(undefined);

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    await user.type(screen.getByTestId('url-input'), 'https://example.com/long-url');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toHaveTextContent('Copy');
  });

  it('copy feedback reverts after timeout', async () => {
    clipboardWriteText.mockResolvedValueOnce(undefined);

    // @ts-expect-error - jsdom doesn't implement isSecureContext
    window.isSecureContext = true;

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://example.com/r/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(
      <ThemeProvider>
        <UrlShortenerForm />
      </ThemeProvider>
    );

    const input = screen.getByTestId('url-input');
    const submitButton = screen.getByTestId('shorten-button');

    await userEvent.type(input, 'https://example.com/long-url');
    await userEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    const copyButton = screen.getByTestId('copy-button');

    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(copyButton).toHaveTextContent('Copied!');
    });

    // Wait for the 2000ms timeout to expire
    await new Promise((resolve) => setTimeout(resolve, 2200));

    await waitFor(() => {
      expect(copyButton).toHaveTextContent('Copy');
    });
  });
});
