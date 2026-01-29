/**
 * Integration tests for copy-to-clipboard functionality.
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Tests:
 * - Copy button copies correct text to clipboard
 * - Visual feedback is shown after successful copy
 * - Copy button handles clipboard errors gracefully
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CopyButton } from '../../src/components/Terminal/CopyButton';
import { InteractiveTerminal } from '../../src/components/Terminal/InteractiveTerminal';

describe('Copy to Clipboard Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('copies code to clipboard when copy button is clicked', async () => {
    const user = userEvent.setup();
    const testCode = 'console.log("hello");';
    const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<CopyButton text={testCode} />);

    const copyButton = screen.getByTestId('copy-button');
    await user.click(copyButton);

    expect(writeTextSpy).toHaveBeenCalledWith(testCode);
  });

  it('shows visual feedback after successful copy', async () => {
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<CopyButton text="test code" />);

    const copyButton = screen.getByTestId('copy-button');
    await user.click(copyButton);

    // Should show "Copied!" text
    await waitFor(() => {
      expect(screen.getByTestId('copy-success')).toBeInTheDocument();
      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });

    // Aria label should update
    expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
  });

  it('reverts to original state after timeout', async () => {
    vi.useFakeTimers();
    vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<CopyButton text="test code" />);

    const copyButton = screen.getByTestId('copy-button');

    // Use fireEvent instead of userEvent for fake timers compatibility
    await act(async () => {
      fireEvent.click(copyButton);
      // Allow promise to resolve
      await Promise.resolve();
    });

    // Should show "Copied!"
    expect(screen.getByText('Copied!')).toBeInTheDocument();

    // Advance time by 2 seconds
    await act(async () => {
      vi.advanceTimersByTime(2000);
    });

    // Should revert to "Copy"
    expect(screen.getByText('Copy')).toBeInTheDocument();

    vi.useRealTimers();
  });

  it('handles clipboard write errors gracefully', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(navigator.clipboard, 'writeText').mockRejectedValue(new Error('Clipboard error'));

    render(<CopyButton text="test code" />);

    const copyButton = screen.getByTestId('copy-button');

    // Use fireEvent for error handling test
    await act(async () => {
      fireEvent.click(copyButton);
      // Allow promise rejection to process
      await new Promise(resolve => setTimeout(resolve, 0));
    });

    // Button should not crash and should remain clickable
    expect(copyButton).toBeInTheDocument();
    expect(consoleError).toHaveBeenCalled();

    consoleError.mockRestore();
  });
});

describe('Copy Button in Terminal Context', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('copies the current tab code when copy button is clicked', async () => {
    const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<InteractiveTerminal />);

    const copyButton = screen.getByTestId('copy-button');

    await act(async () => {
      fireEvent.click(copyButton);
      await Promise.resolve();
    });

    // Should have copied the Basic Usage code
    expect(writeTextSpy).toHaveBeenCalled();
    const copiedText = writeTextSpy.mock.calls[0][0];
    expect(copiedText).toContain('set mykey');
    expect(copiedText).toContain('get mykey');
  });

  it('copies different code when tab is changed', async () => {
    const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

    render(<InteractiveTerminal />);

    // Switch to Configuration tab
    await act(async () => {
      fireEvent.click(screen.getByTestId('tab-config'));
    });

    // Click copy button
    const copyButton = screen.getByTestId('copy-button');

    await act(async () => {
      fireEvent.click(copyButton);
      await Promise.resolve();
    });

    // Should have copied the Configuration code
    const copiedText = writeTextSpy.mock.calls[0][0];
    expect(copiedText).toContain('[server]');
    expect(copiedText).toContain('mirdb.toml');
  });
});

describe('CopyButton Graceful Degradation', () => {
  it('does not render when clipboard API is unavailable', () => {
    // Store original clipboard
    const originalClipboard = navigator.clipboard;

    // Remove clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    });

    render(<CopyButton text="test code" />);

    // Button should not be in the document (graceful degradation)
    expect(screen.queryByTestId('copy-button')).not.toBeInTheDocument();

    // Restore clipboard
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    });
  });
});
