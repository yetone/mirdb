/**
 * UrlShortenerForm Unit Tests
 * Owner: Scenario 2 - Anonymous URL Shortening Flow
 *        Extended by Scenario 3 - URL Form Validation
 *
 * Tests for form submission, validation, and result display.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import UrlShortenerForm, { validateUrl } from '../../src/components/Home/UrlShortenerForm';

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

