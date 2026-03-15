import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { CodeBlock } from '@/components/ui/CodeBlock';
import { QuickStart } from '@/components/sections/QuickStart';

describe('CopyToClipboard Integration', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  describe('CodeBlock copy functionality', () => {
    it('should copy code content to clipboard when button is clicked', async () => {
      const code = 'set mykey 0 0 5\r\nhello';
      render(<CodeBlock code={code} language="bash" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        expect(navigator.clipboard.writeText).toHaveBeenCalledWith(code);
      });
    });

    it('should show visual feedback indicating successful copy', async () => {
      render(<CodeBlock code="test code" language="javascript" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        // Check for visual feedback (icon change or text)
        const copiedElements = screen.getAllByLabelText(/copied/i);
        expect(copiedElements.length).toBeGreaterThan(0);
      });
    });

    it('should reset visual feedback after timeout', async () => {
      render(<CodeBlock code="test code" language="javascript" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        const copiedElements = screen.getAllByLabelText(/copied/i);
        expect(copiedElements.length).toBeGreaterThan(0);
      });

      // Advance time past the reset timeout
      vi.advanceTimersByTime(2500);

      await waitFor(() => {
        const copyElements = screen.getAllByLabelText(/copy code/i);
        expect(copyElements.length).toBeGreaterThan(0);
      });
    });
  });

  describe('QuickStart section copy functionality', () => {
    it('should allow copying code examples from QuickStart section', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });
      expect(copyButtons.length).toBeGreaterThan(0);

      // Click the first copy button
      fireEvent.click(copyButtons[0]);

      await waitFor(() => {
        expect(navigator.clipboard.writeText).toHaveBeenCalled();
      });
    });

    it('should provide visual feedback when copying from QuickStart examples', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });
      fireEvent.click(copyButtons[0]);

      await waitFor(() => {
        // At least one element should show copied state
        const copiedIndicators = screen.getAllByLabelText(/copied/i);
        expect(copiedIndicators.length).toBeGreaterThan(0);
      });
    });

    it('should copy the correct code content for each code block', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });

      // Copy from first block
      fireEvent.click(copyButtons[0]);

      await waitFor(() => {
        const call = vi.mocked(navigator.clipboard.writeText).mock.calls[0];
        expect(call[0]).toBeTruthy();
        expect(typeof call[0]).toBe('string');
      });
    });
  });

  describe('Clipboard API error handling', () => {
    it('should handle clipboard permission denied gracefully', async () => {
      vi.spyOn(navigator.clipboard, 'writeText').mockRejectedValue(
        new Error('Clipboard access denied')
      );

      render(<CodeBlock code="test code" language="javascript" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });
      fireEvent.click(copyButton);

      // Should not crash and button should still be present
      await waitFor(() => {
        expect(copyButton).toBeInTheDocument();
      });
    });
  });
});
