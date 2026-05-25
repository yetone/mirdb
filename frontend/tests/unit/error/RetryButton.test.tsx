/**
 * Unit tests for Retry Button component.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import RetryButton from '../../../src/components/error/RetryButton';

describe('RetryButton', () => {
  it('renders with default label', () => {
    render(<RetryButton onRetry={vi.fn()} />);

    const button = screen.getByTestId('retry-button');
    expect(button).toBeInTheDocument();
    expect(button).toHaveTextContent('Retry');
    expect(button).toHaveAttribute('type', 'button');
  });

  it('renders with custom label', () => {
    render(<RetryButton onRetry={vi.fn()} label="Try Again" />);

    expect(screen.getByTestId('retry-button')).toHaveTextContent('Try Again');
  });

  it('calls onRetry when clicked', () => {
    const onRetry = vi.fn();
    render(<RetryButton onRetry={onRetry} />);

    fireEvent.click(screen.getByTestId('retry-button'));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it('shows loading state during retry', async () => {
    const onRetry = vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 50)));
    render(<RetryButton onRetry={onRetry} />);

    fireEvent.click(screen.getByTestId('retry-button'));

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toHaveTextContent('Retrying...');
    });

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toHaveTextContent('Retry');
    });
  });

  it('disables button while retrying', async () => {
    const onRetry = vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 50)));
    render(<RetryButton onRetry={onRetry} />);

    fireEvent.click(screen.getByTestId('retry-button'));

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toBeDisabled();
    });

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).not.toBeDisabled();
    });
  });

  it('respects disabled prop', () => {
    render(<RetryButton onRetry={vi.fn()} disabled />);

    expect(screen.getByTestId('retry-button')).toBeDisabled();
  });

  it('does not call onRetry when disabled', () => {
    const onRetry = vi.fn();
    render(<RetryButton onRetry={onRetry} disabled />);

    fireEvent.click(screen.getByTestId('retry-button'));
    expect(onRetry).not.toHaveBeenCalled();
  });

  it('sets aria-busy during retry', async () => {
    const onRetry = vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 50)));
    render(<RetryButton onRetry={onRetry} />);

    fireEvent.click(screen.getByTestId('retry-button'));

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toHaveAttribute('aria-busy', 'true');
    });

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toHaveAttribute('aria-busy', 'false');
    });
  });

  it('shows spinner during retry', async () => {
    const onRetry = vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 50)));
    render(<RetryButton onRetry={onRetry} />);

    fireEvent.click(screen.getByTestId('retry-button'));

    await waitFor(() => {
      expect(screen.getByTestId('retry-button-spinner')).toBeInTheDocument();
    });
  });

  it('uses custom loading label', async () => {
    const onRetry = vi.fn().mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 50)));
    render(<RetryButton onRetry={onRetry} loadingLabel="Please wait..." />);

    fireEvent.click(screen.getByTestId('retry-button'));

    await waitFor(() => {
      expect(screen.getByTestId('retry-button')).toHaveTextContent('Please wait...');
    });
  });
});
