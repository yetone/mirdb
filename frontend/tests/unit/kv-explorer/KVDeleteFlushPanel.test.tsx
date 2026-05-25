/**
 * Unit/Integration tests for KVDeletePanel and KVFlushPanel components.
 * Covers REQ-7 (delete operations) and Appendix A (flush_all).
 * Owner: Scenario 10 - KV Explorer - DELETE and FLUSH Operations
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { KVDeletePanel, KVFlushPanel } from '../../../src/components/kv-explorer';
import * as client from '../../../src/api/client';

describe('KVDeletePanel', () => {
  const executeOperationSpy = vi.spyOn(client, 'executeOperation');

  beforeEach(() => {
    executeOperationSpy.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('rendering', () => {
    it('renders the DELETE panel container', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-panel')).toBeInTheDocument();
    });

    it('renders the heading', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-heading')).toHaveTextContent('Delete Key');
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

    it('has proper ARIA label on section', () => {
      render(<KVDeletePanel />);
      expect(screen.getByTestId('kv-delete-panel')).toHaveAttribute(
        'aria-label',
        'Key-Value DELETE Operation'
      );
    });

    it('associates key input with accessible label', () => {
      render(<KVDeletePanel />);
      const input = screen.getByTestId('kv-delete-key-input');
      const label = document.querySelector('label[for="kv-delete-key"]');
      expect(label).toBeInTheDocument();
      expect(label).toHaveTextContent('Key');
      expect(input).toHaveAttribute('id', 'kv-delete-key');
    });
  });

  describe('validation', () => {
    it('shows validation error when Delete is clicked with empty key', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-key-error')).toHaveTextContent('Key is required');
      expect(screen.getByTestId('kv-delete-key-input')).toHaveAttribute('aria-invalid', 'true');
    });

    it('clears validation error when user starts typing', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));
      expect(screen.getByTestId('kv-delete-key-error')).toBeInTheDocument();

      await user.type(screen.getByTestId('kv-delete-key-input'), 'a');
      expect(screen.queryByTestId('kv-delete-key-error')).not.toBeInTheDocument();
    });

    it('does not call executeOperation when key is empty', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      expect(executeOperationSpy).not.toHaveBeenCalled();
    });

    it('shows validation error for whitespace-only key after trim', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), '   ');
      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-key-error')).toHaveTextContent('Key is required');
      expect(executeOperationSpy).not.toHaveBeenCalled();
    });
  });

  describe('DELETE operation - existing key', () => {
    it('calls executeOperation with correct params for delete', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

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

    it('displays DELETED success message after deleting existing key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'existing_key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-success-message')).toHaveTextContent('DELETED');
      });
    });

    it('calls onDeleteSuccess callback with key after successful delete', async () => {
      const user = userEvent.setup();
      const onDeleteSuccess = vi.fn();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

      render(<KVDeletePanel onDeleteSuccess={onDeleteSuccess} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'existing_key');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteSuccess).toHaveBeenCalledWith('existing_key');
      });
    });

    it('triggers Delete on Enter key press', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'existing_key');
      await user.keyboard('{Enter}');

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });
      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'delete',
        key: 'existing_key',
      });
    });

    it('trims whitespace from key before calling executeOperation', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), '  existing_key  ');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });
      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'delete',
        key: 'existing_key',
      });
    });
  });

  describe('DELETE operation - non-existing key', () => {
    it('displays not found message when deleting non-existing key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'not_found', message: 'Key not found' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toBeInTheDocument();
      });
      expect(screen.getByTestId('kv-delete-not-found-message')).toHaveTextContent('Key not found');
    });

    it('displays default not found message when message is missing', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'not_found' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toHaveTextContent('Key not found');
      });
    });

    it('does not show server error for not_found response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'not_found', message: 'Key not found' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-not-found-message')).toBeInTheDocument();
      });
      expect(screen.queryByTestId('kv-delete-error-message')).not.toBeInTheDocument();
    });

    it('calls onDeleteError callback on not_found response', async () => {
      const user = userEvent.setup();
      const onDeleteError = vi.fn();
      executeOperationSpy.mockResolvedValue({ status: 'not_found', message: 'Key not found' });

      render(<KVDeletePanel onDeleteError={onDeleteError} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'nonexistent');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteError).toHaveBeenCalledWith('Key not found');
      });
    });
  });

  describe('DELETE error handling', () => {
    it('displays API error message when operation returns error status', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Server error' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-error-message')).toHaveTextContent('Server error');
      });
    });

    it('displays default error message when error response has no message', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-error-message')).toHaveTextContent('Delete operation failed');
      });
    });

    it('calls onDeleteError callback on API failure', async () => {
      const user = userEvent.setup();
      const onDeleteError = vi.fn();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Server error' });

      render(<KVDeletePanel onDeleteError={onDeleteError} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteError).toHaveBeenCalledWith('Server error');
      });
    });

    it('displays network error message on fetch exception', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('Connection refused'));

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-error-message')).toHaveTextContent('Connection refused');
      });
    });

    it('calls onDeleteError with generic message for unknown error', async () => {
      const user = userEvent.setup();
      const onDeleteError = vi.fn();
      executeOperationSpy.mockRejectedValue('some string error');

      render(<KVDeletePanel onDeleteError={onDeleteError} />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        expect(onDeleteError).toHaveBeenCalledWith('Network error');
      });
    });
  });

  describe('DELETE loading state', () => {
    it('shows loading text on Delete button during submission', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'deleted' }) => void;
      const promise = new Promise<{ status: 'deleted' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-button')).toHaveTextContent('Deleting...');

      resolveOp!({ status: 'deleted' });

      await waitFor(() => {
        expect(screen.getByTestId('kv-delete-button')).toHaveTextContent('Delete');
      });
    });

    it('disables input during loading', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'deleted' }) => void;
      const promise = new Promise<{ status: 'deleted' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      expect(screen.getByTestId('kv-delete-key-input')).toBeDisabled();
      expect(screen.getByTestId('kv-delete-button')).toBeDisabled();

      resolveOp!({ status: 'deleted' });
    });
  });

  describe('DELETE accessibility', () => {
    it('success message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'deleted' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-delete-success-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('error message has role="alert"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Server error' });

      render(<KVDeletePanel />);

      await user.type(screen.getByTestId('kv-delete-key-input'), 'testkey');
      await user.click(screen.getByTestId('kv-delete-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-delete-error-message');
        expect(msg).toHaveAttribute('role', 'alert');
      });
    });

    it('associates validation error with input via aria-describedby', async () => {
      const user = userEvent.setup();
      render(<KVDeletePanel />);

      await user.click(screen.getByTestId('kv-delete-button'));

      const input = screen.getByTestId('kv-delete-key-input');
      const errorId = input.getAttribute('aria-describedby');
      expect(errorId).toBe('kv-delete-key-error');
      expect(document.getElementById(errorId!)).toHaveTextContent('Key is required');
    });
  });
});

describe('KVFlushPanel', () => {
  const executeOperationSpy = vi.spyOn(client, 'executeOperation');

  beforeEach(() => {
    executeOperationSpy.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('rendering', () => {
    it('renders the FLUSH panel container', () => {
      render(<KVFlushPanel />);
      expect(screen.getByTestId('kv-flush-panel')).toBeInTheDocument();
    });

    it('renders the heading', () => {
      render(<KVFlushPanel />);
      expect(screen.getByTestId('kv-flush-heading')).toHaveTextContent('Flush All Data');
    });

    it('renders the warning text', () => {
      render(<KVFlushPanel />);
      expect(screen.getByTestId('kv-flush-warning')).toHaveTextContent(
        'This will permanently remove all keys'
      );
    });

    it('renders the Flush All button', () => {
      render(<KVFlushPanel />);
      const button = screen.getByTestId('kv-flush-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Flush All');
    });

    it('has proper ARIA label on section', () => {
      render(<KVFlushPanel />);
      expect(screen.getByTestId('kv-flush-panel')).toHaveAttribute(
        'aria-label',
        'Key-Value FLUSH Operation'
      );
    });
  });

  describe('confirmation dialog', () => {
    it('shows confirmation dialog when Flush All is clicked', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.getByTestId('kv-flush-confirmation-dialog')).toBeInTheDocument();
      expect(screen.getByTestId('kv-flush-dialog-title')).toHaveTextContent('Confirm Flush');
    });

    it('shows confirmation message in dialog', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.getByTestId('kv-flush-dialog-message')).toHaveTextContent(
        'Are you sure you want to delete all keys?'
      );
    });

    it('shows Confirm and Cancel buttons in dialog', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.getByTestId('kv-flush-confirm-button')).toHaveTextContent('Yes, Flush All');
      expect(screen.getByTestId('kv-flush-cancel-button')).toHaveTextContent('Cancel');
    });

    it('hides dialog and returns to idle when Cancel is clicked', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      expect(screen.getByTestId('kv-flush-confirmation-dialog')).toBeInTheDocument();

      await user.click(screen.getByTestId('kv-flush-cancel-button'));

      expect(screen.queryByTestId('kv-flush-confirmation-dialog')).not.toBeInTheDocument();
      expect(screen.getByTestId('kv-flush-button')).toBeInTheDocument();
    });

    it('does not call executeOperation when dialog is cancelled', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-cancel-button'));

      expect(executeOperationSpy).not.toHaveBeenCalled();
    });
  });

  describe('FLUSH operation - success', () => {
    it('calls executeOperation with flush_all when confirmed', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'ok' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });

      expect(executeOperationSpy).toHaveBeenCalledWith({ op: 'flush_all' });
    });

    it('displays success message after successful flush', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'ok' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-success-message')).toHaveTextContent(
          'All data has been flushed.'
        );
      });
    });

    it('calls onFlushSuccess callback after successful flush', async () => {
      const user = userEvent.setup();
      const onFlushSuccess = vi.fn();
      executeOperationSpy.mockResolvedValue({ status: 'ok' });

      render(<KVFlushPanel onFlushSuccess={onFlushSuccess} />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(onFlushSuccess).toHaveBeenCalled();
      });
    });

    it('hides confirmation dialog after successful flush', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'ok' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.queryByTestId('kv-flush-confirmation-dialog')).not.toBeInTheDocument();
      });
    });
  });

  describe('FLUSH error handling', () => {
    it('displays API error message when flush returns error status', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Flush failed' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-error-message')).toHaveTextContent('Flush failed');
      });
    });

    it('displays default error message when error response has no message', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-error-message')).toHaveTextContent(
          'Flush operation failed'
        );
      });
    });

    it('calls onFlushError callback on API failure', async () => {
      const user = userEvent.setup();
      const onFlushError = vi.fn();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Server error' });

      render(<KVFlushPanel onFlushError={onFlushError} />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(onFlushError).toHaveBeenCalledWith('Server error');
      });
    });

    it('displays network error message on fetch exception', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('Connection refused'));

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-flush-error-message')).toHaveTextContent('Connection refused');
      });
    });

    it('calls onFlushError with generic message for unknown error', async () => {
      const user = userEvent.setup();
      const onFlushError = vi.fn();
      executeOperationSpy.mockRejectedValue('some string error');

      render(<KVFlushPanel onFlushError={onFlushError} />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        expect(onFlushError).toHaveBeenCalledWith('Network error');
      });
    });
  });

  describe('FLUSH loading state', () => {
    it('shows loading text during flush operation', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      const button = screen.getByTestId('kv-flush-button');
      expect(button).toHaveTextContent('Flushing...');
      expect(button).toBeDisabled();

      resolveOp!({ status: 'ok' });
    });

    it('does not show Flush All button while confirming', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      expect(screen.queryByTestId('kv-flush-button')).not.toBeInTheDocument();
    });
  });

  describe('FLUSH accessibility', () => {
    it('confirmation dialog has role alertdialog', async () => {
      const user = userEvent.setup();
      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));

      const dialog = screen.getByTestId('kv-flush-confirmation-dialog');
      expect(dialog).toHaveAttribute('role', 'alertdialog');
      expect(dialog).toHaveAttribute('aria-modal', 'true');
    });

    it('success message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'ok' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-flush-success-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('error message has role="alert"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({ status: 'error', message: 'Server error' });

      render(<KVFlushPanel />);

      await user.click(screen.getByTestId('kv-flush-button'));
      await user.click(screen.getByTestId('kv-flush-confirm-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-flush-error-message');
        expect(msg).toHaveAttribute('role', 'alert');
      });
    });
  });
});
