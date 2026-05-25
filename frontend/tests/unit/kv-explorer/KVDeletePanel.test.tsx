/**
 * Unit/Integration tests for KVDeletePanel component.
 * Covers REQ-7 (delete operations) and Appendix A (flush_all), Scenario 10.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import KVDeletePanel, {
  validateDeleteKey,
} from '../../../src/components/kv-explorer/KVDeletePanel';
import * as client from '../../../src/api/client';

describe('KVDeletePanel', () => {
  let executeOperationSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    executeOperationSpy = vi.spyOn(client, 'executeOperation');
  });

  afterEach(() => {
    executeOperationSpy.mockRestore();
    vi.clearAllMocks();
  });

  describe('rendering', () => {
    it('renders the DELETE panel container', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-panel')).toBeInTheDocument();
    });

    it('renders the section heading', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-heading')).toHaveTextContent(
        'Delete Key-Value Pair'
      );
    });

    it('renders the key input field', () => {
      render(<KVDeletePanel />);
      const input = screen.getByTestId('kv-delete-key-input');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('type', 'text');
      expect(input).toHaveAttribute('placeholder', 'Enter key to delete...');
    });

    it('renders the Delete button', () => {
      render(<KVDeletePanel />);
      const button = screen.getByTestId('kv-delete-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Delete');
    });

    it('renders the Flush All button', () => {
      render(<KVDeletePanel />);
      const button = screen.getByTestId('kv-flush-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Flush All');
    });

    it('renders the danger zone description', () => {
      render(<KVDeletePanel />);
      expect(screen.getByText('Danger Zone')).toBeInTheDocument();
      expect(
        screen.getByText(/Remove all key-value pairs from the store/)
      ).toBeInTheDocument();
    });

    it('has proper ARIA label on section', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-panel')).toHaveAttribute(
        'aria-label',
        'Key-Value DELETE Operation'
      );
    });
  });

  describe('delete validation', () => {
    it('shows validation error when Delete is clicked with empty key', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-key-error')).toHaveTextContent(
        'Key is required'
      );
      expect(screen.getByTestId('kv-delete-key-input')).toHaveAttribute(
        'aria-invalid',
        'true'
      );
    });

    it('clears validation error when user starts typing', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));
      expect(screen.getByTestId('kv-delete-key-error')).toBeInTheDocument();

      await user.type(screen.getByTestId('kv-delete-key-input'), 'a');
      expect(
        screen.queryByTestId('kv-delete-key-error')
      ).not.toBeInTheDocument();
    });

    it('does not call executeOperation when key is empty', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      expect(executeOperationSpy).not.toHaveBeenCalled();
    });

    it('shows validation error for whitespace-only key', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), '   ');
      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-key-error')).toHaveTextContent(
        'Key is required'
      );
      expect(executeOperationSpy).not.toHaveBeenCalled();
    });
  });

  describe('delete existing key', () => {
    it('calls executeOperation with correct params for delete', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'existing_key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });

      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'delete',
        key: 'existing_key',
      });
    });

    it('displays success message after deleting existing key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
        message: 'Key deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'existing_key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-success-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-success-message')).toHaveTextContent(
        'Key deleted'
      );
    });

    it('displays default success message when no message in response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'mykey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-success-message')).toBeInTheDocument();
      });
      expect(
        screen.getByTestId('kv-delete-success-message')
      ).toHaveTextContent('DELETED');
    });

    it('calls onDeleteSuccess callback with key', async () => {
      const user = userEvent.setup();
      const onDeleteSuccess = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel onDeleteSuccess={onDeleteSuccess} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'mykey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteSuccess).toHaveBeenCalledWith('mykey');
      });
    });

    it('triggers delete on Enter key press', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.keyboard('{Enter}');

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });
      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'delete',
        key: 'testkey',
      });
    });

    it('trims whitespace from key before deleting', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), '  hello  ');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });
      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'delete',
        key: 'hello',
      });
    });
  });

  describe('delete non-existing key', () => {
    it('displays not found message when deleting non-existing key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
        message: 'Key not found',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-not-found-message')).toHaveTextContent(
        'Key not found'
      );
    });

    it('displays default not found message when message is missing', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-not-found-message')).toHaveTextContent(
        'Key not found'
      );
    });

    it('calls onDeleteNotFound callback with key', async () => {
      const user = userEvent.setup();
      const onDeleteNotFound = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
      });

      render(<KVDeletePanel onDeleteNotFound={onDeleteNotFound} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'missing');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteNotFound).toHaveBeenCalledWith('missing');
      });
    });

    it('does not show error for not_found - it is a graceful response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toBeInTheDocument();
      });

      expect(
        screen.queryByTestId('kv-delete-error-message')
      ).not.toBeInTheDocument();
    });
  });

  describe('delete error handling', () => {
    it('displays API error message when delete fails', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error: internal failure',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-error-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-error-message')).toHaveTextContent(
        'Server error: internal failure'
      );
    });

    it('calls onDeleteError callback on API failure', async () => {
      const user = userEvent.setup();
      const onDeleteError = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error',
      });

      render(<KVDeletePanel onDeleteError={onDeleteError} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteError).toHaveBeenCalledWith('Server error');
      });
    });

    it('displays network error message on fetch exception', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('Connection refused'));

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-error-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-error-message')).toHaveTextContent(
        'Connection refused'
      );
    });

    it('calls onDeleteError with generic message for unknown error', async () => {
      const user = userEvent.setup();
      const onDeleteError = vi.fn();
      executeOperationSpy.mockRejectedValue('some string error');

      render(<KVDeletePanel onDeleteError={onDeleteError} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteError).toHaveBeenCalledWith('Network error');
      });
    });
  });

  describe('delete loading state', () => {
    it('shows loading text on Delete button during request', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'deleted' }) => void;
      const promise = new Promise<{ status: 'deleted' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-button')).toHaveTextContent(
          'Deleting...'
        );
      });

      resolveOp!({ status: 'deleted' });

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-button')).toHaveTextContent('Delete');
      });
    });

    it('disables delete input during loading', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'deleted' }) => void;
      const promise = new Promise<{ status: 'deleted' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-key-input')).toBeDisabled();
      });

      resolveOp!({ status: 'deleted' });
    });
  });

  describe('flush_all operation', () => {
    it('shows confirmation dialog when Flush All is clicked', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.getByTestId('kv-flush-dialog-overlay')).toBeInTheDocument();
      expect(screen.getByTestId('kv-flush-dialog-title')).toHaveTextContent(
        'Confirm Flush All'
      );
      expect(screen.getByTestId('kv-flush-dialog-text')).toHaveTextContent(
        /Are you sure you want to delete all key-value pairs/
      );
    });

    it('dialog has proper ARIA attributes', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      const dialog = screen.getByTestId('kv-flush-dialog-overlay');
      expect(dialog).toHaveAttribute('role', 'dialog');
      expect(dialog).toHaveAttribute('aria-modal', 'true');
    });

    it('shows confirm and cancel buttons in dialog', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.getByTestId('kv-flush-confirm-button')).toHaveTextContent(
        'Yes, Flush All'
      );
      expect(screen.getByTestId('kv-flush-cancel-button')).toHaveTextContent(
        'Cancel'
      );
    });

    it('closes dialog when Cancel is clicked', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      expect(screen.getByTestId('kv-flush-dialog-overlay')).toBeInTheDocument();

      await user.click(screen.getByTestId('kv-flush-cancel-button'));

      expect(
        screen.queryByTestId('kv-flush-dialog-overlay')
      ).not.toBeInTheDocument();
    });

    it('calls executeOperation with flush_all on confirm', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });
      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'flush_all',
      });
    });

    it('displays success message after flush_all', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
        message: 'All data flushed',
      });

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-success-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-flush-success-message')).toHaveTextContent(
        'All data flushed'
      );
    });

    it('displays default success message when no message in response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-success-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-flush-success-message')).toHaveTextContent(
        'All data flushed successfully'
      );
    });

    it('calls onFlushSuccess callback after flush', async () => {
      const user = userEvent.setup();
      const onFlushSuccess = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVDeletePanel onFlushSuccess={onFlushSuccess} />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(onFlushSuccess).toHaveBeenCalled();
      });
    });

    it('displays error message when flush_all fails', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Flush failed: permission denied',
      });

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-error-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-flush-error-message')).toHaveTextContent(
        'Flush failed: permission denied'
      );
    });

    it('calls onFlushError callback on flush failure', async () => {
      const user = userEvent.setup();
      const onFlushError = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error',
      });

      render(<KVDeletePanel onFlushError={onFlushError} />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(onFlushError).toHaveBeenCalledWith('Server error');
      });
    });

    it('displays network error on flush exception', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('Connection timeout'));

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-error-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-flush-error-message')).toHaveTextContent(
        'Connection timeout'
      );
    });

    it('shows loading state during flush operation', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      // Dialog should close and loading state should be active
      await waitFor(() => {
        expect(
          screen.queryByTestId('kv-flush-dialog-overlay')
        ).not.toBeInTheDocument();
      });

      resolveOp!({ status: 'ok' });

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-success-message')).toBeInTheDocument();
      });
    });
  });

  describe('cross-operation loading state', () => {
    it('disables flush button while delete is in progress', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'deleted' }) => void;
      const promise = new Promise<{ status: 'deleted' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-button')).toBeDisabled();
      });

      resolveOp!({ status: 'deleted' });
    });

    it('disables delete button while flush is in progress', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-button')).toBeDisabled();
      });

      resolveOp!({ status: 'ok' });
    });
  });

  describe('accessibility', () => {
    it('associates key error with input via aria-describedby', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      const keyInput = screen.getByTestId('kv-delete-key-input');
      const errorId = keyInput.getAttribute('aria-describedby');
      expect(errorId).toBe('kv-delete-key-error');
      expect(document.getElementById(errorId!)).toHaveTextContent(
        'Key is required'
      );
    });

    it('delete success message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-delete-success-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('delete not_found message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'missing');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-delete-not-found-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('delete error message has role="alert"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error',
      });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-delete-error-message');
        expect(msg).toHaveAttribute('role', 'alert');
      });
    });

    it('flush success message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-flush-success-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('flush error message has role="alert"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('fail'));

      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-flush-error-message');
        expect(msg).toHaveAttribute('role', 'alert');
      });
    });
  });

  describe('validateDeleteKey utility', () => {
    it('returns undefined for valid key', () => {
      expect(validateDeleteKey('hello')).toBeUndefined();
    });

    it('returns error for empty key', () => {
      expect(validateDeleteKey('')).toBe('Key is required');
    });

    it('returns error for whitespace-only key', () => {
      expect(validateDeleteKey('   ')).toBe('Key is required');
    });

    it('returns undefined for key with leading/trailing spaces', () => {
      expect(validateDeleteKey('  hello  ')).toBeUndefined();
    });
  });

  describe('API client integration', () => {
    beforeEach(() => {
      // Restore spy so executeOperation calls through to the real implementation
      executeOperationSpy.mockRestore();
    });

    it('posts correct payload for delete operation', async () => {
      const user = userEvent.setup();
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ status: 'deleted' }),
      } as Response);
      global.fetch = mockFetch;

      render(<KVDeletePanel />);
      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledTimes(1);
      });

      const [, init] = mockFetch.mock.calls[0];
      const body = JSON.parse(init.body);
      expect(body).toEqual({ op: 'delete', key: 'testkey' });
      expect(init.method).toBe('POST');
    });

    it('posts correct payload for flush_all operation', async () => {
      const user = userEvent.setup();
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ status: 'ok' }),
      } as Response);
      global.fetch = mockFetch;

      render(<KVDeletePanel />);
      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledTimes(1);
      });

      const [, init] = mockFetch.mock.calls[0];
      const body = JSON.parse(init.body);
      expect(body).toEqual({ op: 'flush_all' });
      expect(init.method).toBe('POST');
    });

    it('does not make API call when delete key is empty', async () => {
      const user = userEvent.setup();
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ status: 'deleted' }),
      } as Response);
      global.fetch = mockFetch;

      render(<KVDeletePanel />);
      await user.click(screen.getByTestId('kv-delete-button'));

      expect(mockFetch).not.toHaveBeenCalled();
    });
  });
});
