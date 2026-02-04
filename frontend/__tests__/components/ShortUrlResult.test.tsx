import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ShortUrlResult } from '../../src/components/ShortUrlResult';

describe('ShortUrlResult', () => {
  const mockOnCopy = vi.fn();
  const testShortUrl = 'http://localhost:3000/r/abc123';

  beforeEach(() => {
    mockOnCopy.mockClear();
  });

  // Test Case 3: ShortUrlResult component displays the full shortened URL
  it('displays the shortened URL', () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    const link = screen.getByRole('link', { name: testShortUrl });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', testShortUrl);
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test Case 5: Click copy button - navigator.clipboard.writeText called, 'Copied!' feedback shown
  it('calls onCopy when copy button is clicked', async () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    const copyButton = screen.getByRole('button', { name: /copy shortened url/i });
    copyButton.click();

    expect(mockOnCopy).toHaveBeenCalledTimes(1);
  });

  it('shows "Copied!" feedback when copied is true', () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={true} />
    );

    expect(screen.getByText(/copied!/i)).toBeInTheDocument();
    const button = screen.getByRole('button', { name: /url copied to clipboard/i });
    expect(button).toHaveClass('btn-success');
  });

  it('shows "Copy" text when copied is false', () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    expect(screen.getByText('Copy')).toBeInTheDocument();
    const button = screen.getByRole('button', { name: /copy shortened url/i });
    expect(button).toHaveClass('btn-secondary');
  });

  it('has proper accessibility attributes', () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    const region = screen.getByRole('region', { name: /shortened url result/i });
    expect(region).toBeInTheDocument();

    // Check for ARIA live region
    const liveRegion = document.querySelector('[aria-live="polite"]');
    expect(liveRegion).toBeInTheDocument();
  });

  it('announces copy status to screen readers', () => {
    const { rerender } = render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    // Screen reader announcement should be empty initially
    const liveRegion = document.querySelector('[aria-live="polite"]');
    expect(liveRegion?.textContent).toBe('');

    // After copied, should announce
    rerender(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={true} />
    );

    expect(liveRegion?.textContent).toBe('URL copied to clipboard');
  });

  it('displays helper text', () => {
    render(
      <ShortUrlResult shortUrl={testShortUrl} onCopy={mockOnCopy} copied={false} />
    );

    expect(screen.getByText(/your shortened url/i)).toBeInTheDocument();
  });
});
