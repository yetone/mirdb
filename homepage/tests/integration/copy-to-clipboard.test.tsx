import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { CodeBlock } from '../../src/components/common/CodeBlock';
import { UsageExample } from '../../src/components/sections/UsageExample';
import { QuickStart } from '../../src/components/sections/QuickStart';

// Mock prismjs
vi.mock('prismjs', () => ({
  default: {
    highlightElement: vi.fn(),
  },
}));

vi.mock('prismjs/components/prism-bash', () => ({}));

describe('Copy to Clipboard Integration', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
    mockWriteText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('CodeBlock copy functionality', () => {
    it('copies text to clipboard when copy button is clicked', async () => {
      const testCode = 'echo "test"';
      render(<CodeBlock code={testCode} language="bash" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(mockWriteText).toHaveBeenCalledWith(testCode);
    });

    it('shows "Copied!" feedback after clicking', async () => {
      render(<CodeBlock code="test" language="bash" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });

    it('copy button returns to original state after 2 seconds', async () => {
      render(<CodeBlock code="test" language="bash" />);

      const copyButton = screen.getByRole('button', { name: /copy/i });

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(screen.getByText('Copied!')).toBeInTheDocument();

      await act(async () => {
        vi.advanceTimersByTime(2000);
      });

      expect(screen.getByText('Copy')).toBeInTheDocument();
      expect(screen.queryByText('Copied!')).not.toBeInTheDocument();
    });

    it('updates aria-label when copied', async () => {
      render(<CodeBlock code="test" language="bash" />);

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i });
      expect(copyButton).toBeInTheDocument();

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument();
    });
  });

  describe('UsageExample component copy', () => {
    it('can copy usage example code', async () => {
      render(<UsageExample />);

      const copyButton = screen.getByRole('button', { name: /copy/i });

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(mockWriteText).toHaveBeenCalled();
      // Verify it contains the memcached commands
      const copiedText = mockWriteText.mock.calls[0][0];
      expect(copiedText).toContain('set mykey');
      expect(copiedText).toContain('get mykey');
      expect(copiedText).toContain('telnet localhost 12333');
    });
  });

  describe('QuickStart component copy', () => {
    it('can copy installation command from QuickStart', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });
      // First copy button should be for installation
      const installCopyButton = copyButtons[0];

      await act(async () => {
        fireEvent.click(installCopyButton);
      });

      expect(mockWriteText).toHaveBeenCalled();
      const copiedText = mockWriteText.mock.calls[0][0];
      expect(copiedText).toContain('git clone');
    });

    it('can copy connection command from QuickStart', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });
      // Second copy button should be for connection
      const connectCopyButton = copyButtons[1];

      await act(async () => {
        fireEvent.click(connectCopyButton);
      });

      expect(mockWriteText).toHaveBeenCalled();
      const copiedText = mockWriteText.mock.calls[0][0];
      expect(copiedText).toContain('telnet localhost 12333');
    });
  });

  describe('Multiple copy actions', () => {
    it('resets previous copy state when copying different code blocks', async () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByRole('button', { name: /copy/i });

      // Click first copy button
      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      // First button should show "Copied!"
      expect(copyButtons[0]).toHaveTextContent('Copied!');

      // Click second copy button
      await act(async () => {
        fireEvent.click(copyButtons[1]);
      });

      // Second button should now show "Copied!"
      expect(copyButtons[1]).toHaveTextContent('Copied!');

      // Advance time to reset
      await act(async () => {
        vi.advanceTimersByTime(2000);
      });

      // Both should be back to "Copy"
      copyButtons.forEach((button) => {
        expect(button).toHaveTextContent('Copy');
      });
    });
  });
});
