/**
 * Unit tests for KVExplorer GET operations.
 * Covers REQ-7 (read operations), Scenarios 8.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import KVExplorer from '../../../src/components/kv-explorer/KVExplorer';
import type { KVOperationResponse } from '../../../src/types';

describe('KVExplorer - GET Operations', () => {
  it('renders the KVExplorer section container', () => {
    render(<KVExplorer />);
    expect(screen.getByTestId('kv-explorer')).toBeInTheDocument();
  });

  it('renders the section heading', () => {
    render(<KVExplorer />);
    expect(screen.getByTestId('kv-explorer-heading')).toHaveTextContent('Key-Value Explorer');
  });

  it('renders the key input field', () => {
    render(<KVExplorer />);
    const input = screen.getByTestId('kv-key-input');
    expect(input).toBeInTheDocument();
    expect(input).toHaveAttribute('type', 'text');
    expect(input).toHaveAttribute('placeholder', 'Enter key...');
  });

  it('renders the Get button', () => {
    render(<KVExplorer />);
    const button = screen.getByTestId('kv-get-button');
    expect(button).toBeInTheDocument();
    expect(button).toHaveTextContent('Get');
  });

  it('displays validation error when Get is clicked with empty key', async () => {
    const user = userEvent.setup();
    render(<KVExplorer />);

    await user.click(screen.getByTestId('kv-get-button'));

    expect(screen.getByTestId('kv-key-error')).toHaveTextContent('Key is required');
    expect(screen.getByTestId('kv-key-input')).toHaveAttribute('aria-invalid', 'true');
  });

  it('clears validation error when user starts typing', async () => {
    const user = userEvent.setup();
    render(<KVExplorer />);

    await user.click(screen.getByTestId('kv-get-button'));
    expect(screen.getByTestId('kv-key-error')).toBeInTheDocument();

    await user.type(screen.getByTestId('kv-key-input'), 'a');
    expect(screen.queryByTestId('kv-key-error')).not.toBeInTheDocument();
  });

  it('does not call onGet when key is empty', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn();
    render(<KVExplorer onGet={onGet} />);

    await user.click(screen.getByTestId('kv-get-button'));

    expect(onGet).not.toHaveBeenCalled();
  });

  it('calls onGet with the entered key when Get is clicked', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'ok' as const, value: 'world' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'hello');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(onGet).toHaveBeenCalledTimes(1);
    });
    expect(onGet).toHaveBeenCalledWith('hello');
  });

  it('shows loading state during GET request', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockImplementation(() => new Promise<KVOperationResponse>((resolve) => {
      setTimeout(() => resolve({ status: 'ok', value: 'world' }), 100);
    }));
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'hello');
    await user.click(screen.getByTestId('kv-get-button'));

    expect(screen.getByTestId('kv-loading')).toHaveTextContent('Loading...');
    expect(screen.getByTestId('kv-get-button')).toBeDisabled();
    expect(screen.getByTestId('kv-key-input')).toBeDisabled();

    await waitFor(() => {
      expect(screen.queryByTestId('kv-loading')).not.toBeInTheDocument();
    });
  });

  it('displays the value when GET returns existing key', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'ok' as const, value: 'world' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'hello');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-result-value')).toBeInTheDocument();
    });
    expect(screen.getByTestId('kv-result-text')).toHaveTextContent('world');
  });

  it('displays Key not found when GET returns non-existing key', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'not_found' as const, message: 'Key not found' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'nonexistent');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-not-found')).toBeInTheDocument();
    });
    expect(screen.getByTestId('kv-not-found')).toHaveTextContent('Key not found');
  });

  it('displays default Key not found message when message is missing', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'not_found' as const });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'nonexistent');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-not-found')).toHaveTextContent('Key not found');
    });
  });

  it('displays error message when GET returns error status', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'error' as const, message: 'Server error' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'testkey');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-error-message')).toBeInTheDocument();
    });
    expect(screen.getByTestId('kv-error-message')).toHaveTextContent('Server error');
  });

  it('displays default error message when error response has no message', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'error' as const });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'testkey');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-error-message')).toHaveTextContent('An error occurred');
    });
  });

  it('displays fallback error message when onGet throws', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockRejectedValue(new Error('Network failure'));
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'testkey');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-error-message')).toHaveTextContent('Failed to fetch value');
    });
  });

  it('triggers Get on Enter key press', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'ok' as const, value: 'world' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), 'hello');
    await user.keyboard('{Enter}');

    await waitFor(() => {
      expect(onGet).toHaveBeenCalledTimes(1);
    });
    expect(onGet).toHaveBeenCalledWith('hello');
  });

  it('trims whitespace from key before calling onGet', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'ok' as const, value: 'world' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), '  hello  ');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(onGet).toHaveBeenCalledTimes(1);
    });
    expect(onGet).toHaveBeenCalledWith('hello');
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(<KVExplorer />);
    expect(screen.getByTestId('kv-explorer')).toHaveAttribute('aria-label', 'Key-Value Explorer');
  });

  it('input field has accessible label', () => {
    render(<KVExplorer />);
    const input = screen.getByTestId('kv-key-input');
    const label = document.querySelector('label[for="kv-key"]');
    expect(label).toBeInTheDocument();
    expect(label).toHaveTextContent('Key');
  });

  it('does not show validation error for whitespace-only key after trim', async () => {
    const user = userEvent.setup();
    const onGet = vi.fn().mockResolvedValue({ status: 'ok' as const, value: 'world' });
    render(<KVExplorer onGet={onGet} />);

    await user.type(screen.getByTestId('kv-key-input'), '   ');
    await user.click(screen.getByTestId('kv-get-button'));

    expect(screen.getByTestId('kv-key-error')).toHaveTextContent('Key is required');
    expect(onGet).not.toHaveBeenCalled();
  });
});

describe('KVExplorer - API Client Integration', () => {
  it('posts correct payload for get operation', async () => {
    const user = userEvent.setup();
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'ok', value: 'world' }),
    } as Response);
    global.fetch = mockFetch;

    render(<KVExplorer />);
    await user.type(screen.getByTestId('kv-key-input'), 'hello');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    const [, init] = mockFetch.mock.calls[0];
    const body = JSON.parse(init.body);
    expect(body).toEqual({ op: 'get', key: 'hello' });
    expect(init.method).toBe('POST');
  });

  it('handles not_found response from API', async () => {
    const user = userEvent.setup();
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'not_found', message: 'Key not found' }),
    } as Response);
    global.fetch = mockFetch;

    render(<KVExplorer />);
    await user.type(screen.getByTestId('kv-key-input'), 'nonexistent');
    await user.click(screen.getByTestId('kv-get-button'));

    await waitFor(() => {
      expect(screen.getByTestId('kv-not-found')).toHaveTextContent('Key not found');
    });
  });

  it('does not make API call when key is empty', async () => {
    const user = userEvent.setup();
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'ok' }),
    } as Response);
    global.fetch = mockFetch;

    render(<KVExplorer />);
    await user.click(screen.getByTestId('kv-get-button'));

    expect(mockFetch).not.toHaveBeenCalled();
  });
});
