/**
 * Unit/Integration tests for KVSetPanel component.
 * Covers REQ-7 (write operations) - SET functionality.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import KVSetPanel, {
  validateSetForm,
  isFormValid,
} from '../../../src/components/kv-explorer/KVSetPanel';
import * as client from '../../../src/api/client';

describe('KVSetPanel', () => {
  const executeOperationSpy = vi.spyOn(client, 'executeOperation');

  beforeEach(() => {
    executeOperationSpy.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('rendering', () => {
    it('renders the SET panel container', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-panel')).toBeInTheDocument();
    });

    it('renders the heading', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-heading')).toHaveTextContent(
        'Set Key-Value Pair'
      );
    });

    it('renders the key input field', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-key-input')).toBeInTheDocument();
      expect(screen.getByLabelText(/^Key$/)).toBeInTheDocument();
    });

    it('renders the value input field', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-value-input')).toBeInTheDocument();
      expect(screen.getByLabelText(/^Value$/)).toBeInTheDocument();
    });

    it('renders the flags input field with default 0', () => {
      render(<KVSetPanel />);
      const flagsInput = screen.getByTestId('kv-set-flags-input');
      expect(flagsInput).toBeInTheDocument();
      expect(flagsInput).toHaveValue(0);
    });

    it('renders the expiration input field with default 0', () => {
      render(<KVSetPanel />);
      const exptimeInput = screen.getByTestId('kv-set-exptime-input');
      expect(exptimeInput).toBeInTheDocument();
      expect(exptimeInput).toHaveValue(0);
    });

    it('renders the Set button', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-submit-button')).toHaveTextContent(
        'Set'
      );
    });

    it('renders the Reset button', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-reset-button')).toHaveTextContent(
        'Reset'
      );
    });
  });

  describe('form validation', () => {
    it('shows error when key is empty', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-key-error')).toHaveTextContent(
        'Key is required'
      );
      expect(screen.getByTestId('kv-set-key-input')).toHaveAttribute(
        'aria-invalid',
        'true'
      );
    });

    it('shows error when value is empty', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-value-error')).toHaveTextContent(
        'Value is required'
      );
      expect(screen.getByTestId('kv-set-value-input')).toHaveAttribute(
        'aria-invalid',
        'true'
      );
    });

    it('shows error when key is whitespace-only', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), '   ');
      await user.type(screen.getByTestId('kv-set-value-input'), 'test');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-key-error')).toHaveTextContent(
        'Key is required'
      );
    });

    it('shows error when value is whitespace-only', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'test');
      await user.type(screen.getByTestId('kv-set-value-input'), '   ');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-value-error')).toHaveTextContent(
        'Value is required'
      );
    });

    it('shows both errors when both fields are empty', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-key-error')).toBeInTheDocument();
      expect(screen.getByTestId('kv-set-value-error')).toBeInTheDocument();
      expect(screen.getByTestId('kv-set-error-message')).toHaveTextContent(
        'Please fix the errors below.'
      );
    });

    it('clears key error when user starts typing', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));
      expect(screen.getByTestId('kv-set-key-error')).toBeInTheDocument();

      await user.type(screen.getByTestId('kv-set-key-input'), 'a');
      expect(
        screen.queryByTestId('kv-set-key-error')
      ).not.toBeInTheDocument();
    });

    it('clears value error when user starts typing', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));
      expect(screen.getByTestId('kv-set-value-error')).toBeInTheDocument();

      await user.type(screen.getByTestId('kv-set-value-input'), 'a');
      expect(
        screen.queryByTestId('kv-set-value-error')
      ).not.toBeInTheDocument();
    });

    it('does not submit when validation fails', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(executeOperationSpy).not.toHaveBeenCalled();
    });
  });

  describe('SET operation - new key', () => {
    it('calls executeOperation with correct params for new key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'hello');
      await user.type(screen.getByTestId('kv-set-value-input'), 'world');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledTimes(1);
      });

      expect(executeOperationSpy).toHaveBeenCalledWith({
        op: 'set',
        key: 'hello',
        value: 'world',
        flags: 0,
        exptime: 0,
      });
    });

    it('displays STORED success message after successful SET', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'mykey');
      await user.type(screen.getByTestId('kv-set-value-input'), 'myvalue');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-success-message')).toHaveTextContent(
          'STORED'
        );
      });
    });

    it('calls onSetSuccess callback with key and value', async () => {
      const user = userEvent.setup();
      const onSetSuccess = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel onSetSuccess={onSetSuccess} />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'mykey');
      await user.type(screen.getByTestId('kv-set-value-input'), 'myvalue');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(onSetSuccess).toHaveBeenCalledWith('mykey', 'myvalue');
      });
    });
  });

  describe('SET operation - update existing key', () => {
    it('calls executeOperation to overwrite existing key with new value', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'hello');
      await user.type(
        screen.getByTestId('kv-set-value-input'),
        'updated'
      );
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith({
          op: 'set',
          key: 'hello',
          value: 'updated',
          flags: 0,
          exptime: 0,
        });
      });
    });

    it('shows STORED message after updating existing key', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'hello');
      await user.type(
        screen.getByTestId('kv-set-value-input'),
        'updated'
      );
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-success-message')).toHaveTextContent(
          'STORED'
        );
      });
    });
  });

  describe('SET operation with expiration', () => {
    it('submits with non-zero exptime', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'tempkey');
      await user.type(screen.getByTestId('kv-set-value-input'), 'tempvalue');

      const exptimeInput = screen.getByTestId('kv-set-exptime-input');
      await user.clear(exptimeInput);
      await user.type(exptimeInput, '60');

      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith({
          op: 'set',
          key: 'tempkey',
          value: 'tempvalue',
          flags: 0,
          exptime: 60,
        });
      });
    });

    it('submits with custom flags', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'flagged');
      await user.type(screen.getByTestId('kv-set-value-input'), 'data');

      const flagsInput = screen.getByTestId('kv-set-flags-input');
      await user.clear(flagsInput);
      await user.type(flagsInput, '42');

      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith({
          op: 'set',
          key: 'flagged',
          value: 'data',
          flags: 42,
          exptime: 0,
        });
      });
    });
  });

  describe('error handling', () => {
    it('displays API error message when operation fails', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error: key too large',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'bigkey');
      await user.type(screen.getByTestId('kv-set-value-input'), 'bigvalue');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-error-message')).toHaveTextContent(
          'Server error: key too large'
        );
      });
    });

    it('calls onSetError callback on API failure', async () => {
      const user = userEvent.setup();
      const onSetError = vi.fn();
      executeOperationSpy.mockResolvedValue({
        status: 'error',
        message: 'Server error',
      });

      render(<KVSetPanel onSetError={onSetError} />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(onSetError).toHaveBeenCalledWith('Server error');
      });
    });

    it('displays network error message on fetch exception', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(new Error('Connection refused'));

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-error-message')).toHaveTextContent(
          'Connection refused'
        );
      });
    });

    it('calls onSetError with generic message for unknown error', async () => {
      const user = userEvent.setup();
      const onSetError = vi.fn();
      executeOperationSpy.mockRejectedValue('some string error');

      render(<KVSetPanel onSetError={onSetError} />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(onSetError).toHaveBeenCalledWith('Network error');
      });
    });
  });

  describe('loading state', () => {
    it('shows loading text on Set button during submission', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>(
        (resolve) => {
          resolveOp = resolve;
        }
      );
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-submit-button')).toHaveTextContent(
        'Setting...'
      );

      resolveOp!({ status: 'ok' });

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-submit-button')).toHaveTextContent(
          'Set'
        );
      });
    });

    it('disables inputs during loading', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>(
        (resolve) => {
          resolveOp = resolve;
        }
      );
      executeOperationSpy.mockReturnValue(promise as Promise<any>);

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      expect(screen.getByTestId('kv-set-key-input')).toBeDisabled();
      expect(screen.getByTestId('kv-set-value-input')).toBeDisabled();
      expect(screen.getByTestId('kv-set-flags-input')).toBeDisabled();
      expect(screen.getByTestId('kv-set-exptime-input')).toBeDisabled();

      resolveOp!({ status: 'ok' });
    });
  });

  describe('reset functionality', () => {
    it('clears form fields when Reset is clicked', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.clear(screen.getByTestId('kv-set-flags-input'));
      await user.type(screen.getByTestId('kv-set-flags-input'), '5');

      await user.click(screen.getByTestId('kv-set-reset-button'));

      expect(screen.getByTestId('kv-set-key-input')).toHaveValue('');
      expect(screen.getByTestId('kv-set-value-input')).toHaveValue('');
      expect(screen.getByTestId('kv-set-flags-input')).toHaveValue(0);
      expect(screen.getByTestId('kv-set-exptime-input')).toHaveValue(0);
    });

    it('clears validation errors when Reset is clicked', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));
      expect(screen.getByTestId('kv-set-key-error')).toBeInTheDocument();

      await user.click(screen.getByTestId('kv-set-reset-button'));

      expect(
        screen.queryByTestId('kv-set-key-error')
      ).not.toBeInTheDocument();
      expect(
        screen.queryByTestId('kv-set-error-message')
      ).not.toBeInTheDocument();
    });

    it('clears success message when Reset is clicked', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        expect(screen.getByTestId('kv-set-success-message')).toBeInTheDocument();
      });

      await user.click(screen.getByTestId('kv-set-reset-button'));

      expect(
        screen.queryByTestId('kv-set-success-message')
      ).not.toBeInTheDocument();
    });
  });

  describe('accessibility', () => {
    it('has proper ARIA label on section', () => {
      render(<KVSetPanel />);
      expect(screen.getByTestId('kv-set-panel')).toHaveAttribute(
        'aria-label',
        'Key-Value SET Operation'
      );
    });

    it('associates key error with input via aria-describedby', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      const keyInput = screen.getByTestId('kv-set-key-input');
      const errorId = keyInput.getAttribute('aria-describedby');
      expect(errorId).toBe('kv-set-key-error');
      expect(document.getElementById(errorId!)).toHaveTextContent(
        'Key is required'
      );
    });

    it('associates value error with input via aria-describedby', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      const valueInput = screen.getByTestId('kv-set-value-input');
      const errorId = valueInput.getAttribute('aria-describedby');
      expect(errorId).toBe('kv-set-value-error');
      expect(document.getElementById(errorId!)).toHaveTextContent(
        'Value is required'
      );
    });

    it('success message has role="status"', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      });

      render(<KVSetPanel />);

      await user.type(screen.getByTestId('kv-set-key-input'), 'key');
      await user.type(screen.getByTestId('kv-set-value-input'), 'val');
      await user.click(screen.getByTestId('kv-set-submit-button'));

      await waitFor(() => {
        const msg = screen.getByTestId('kv-set-success-message');
        expect(msg).toHaveAttribute('role', 'status');
      });
    });

    it('error message has role="alert"', async () => {
      const user = userEvent.setup();
      render(<KVSetPanel />);

      await user.click(screen.getByTestId('kv-set-submit-button'));

      const msg = screen.getByTestId('kv-set-error-message');
      expect(msg).toHaveAttribute('role', 'alert');
    });
  });

  describe('validateSetForm utility', () => {
    it('returns empty errors for valid form data', () => {
      const data = { key: 'hello', value: 'world', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({});
    });

    it('returns key error for empty key', () => {
      const data = { key: '', value: 'world', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({ key: 'Key is required' });
    });

    it('returns value error for empty value', () => {
      const data = { key: 'hello', value: '', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({ value: 'Value is required' });
    });

    it('returns both errors when both are empty', () => {
      const data = { key: '', value: '', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({
        key: 'Key is required',
        value: 'Value is required',
      });
    });

    it('returns key error for whitespace-only key', () => {
      const data = { key: '   ', value: 'world', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({ key: 'Key is required' });
    });

    it('returns value error for whitespace-only value', () => {
      const data = { key: 'hello', value: '   ', flags: '0', exptime: '0' };
      expect(validateSetForm(data)).toEqual({ value: 'Value is required' });
    });
  });

  describe('isFormValid utility', () => {
    it('returns true for empty errors object', () => {
      expect(isFormValid({})).toBe(true);
    });

    it('returns false when errors has any key', () => {
      expect(isFormValid({ key: 'error' })).toBe(false);
      expect(isFormValid({ value: 'error' })).toBe(false);
    });
  });
});
