import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CodeBlock, type CodeBlockProps } from './CodeBlock';

// Global clipboard mock
const mockWriteText = vi.fn().mockResolvedValue(undefined);

describe('CodeBlock Component - Unit Tests', () => {
  const defaultProps: CodeBlockProps = {
    code: 'cargo install mirdb',
    language: 'bash',
  };

  beforeEach(() => {
    // Mock clipboard API using Object.defineProperty
    mockWriteText.mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    mockWriteText.mockClear();
  });

  describe('Test Case 6: Render code block component with copy functionality', () => {
    it('renders without crashing', () => {
      render(<CodeBlock {...defaultProps} />);
      expect(screen.getByTestId('code-block')).toBeInTheDocument();
    });

    it('renders code content correctly', () => {
      render(<CodeBlock {...defaultProps} />);
      expect(screen.getByText('cargo install mirdb')).toBeInTheDocument();
    });

    it('renders copy button', () => {
      render(<CodeBlock {...defaultProps} />);
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    it('copy button has proper accessibility label', () => {
      render(<CodeBlock {...defaultProps} />);
      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toHaveAttribute('aria-label', 'Copy code');
    });

    it('renders code block with correct language class', () => {
      render(<CodeBlock code="echo hello" language="bash" />);
      const codePre = screen.getByTestId('code-pre');
      expect(codePre).toHaveClass('language-bash');
    });
  });

  describe('Copy button functionality', () => {
    it('clicking copy button copies code to clipboard', async () => {
      render(<CodeBlock {...defaultProps} />);

      const copyButton = screen.getByTestId('copy-button');
      await userEvent.click(copyButton);

      expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb');
    });

    it('shows checkmark after successful copy', async () => {
      render(<CodeBlock {...defaultProps} />);

      const copyButton = screen.getByTestId('copy-button');
      await userEvent.click(copyButton);

      await waitFor(() => {
        expect(screen.getByTestId('copied-icon')).toBeInTheDocument();
      });
    });

    it('updates aria-label to Copied! after successful copy', async () => {
      render(<CodeBlock {...defaultProps} />);

      const copyButton = screen.getByTestId('copy-button');
      await userEvent.click(copyButton);

      await waitFor(() => {
        expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
      });
    });

    it('reverts to copy icon after 2 seconds', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      render(<CodeBlock {...defaultProps} />);

      const copyButton = screen.getByTestId('copy-button');
      await user.click(copyButton);

      await waitFor(() => {
        expect(screen.getByTestId('copied-icon')).toBeInTheDocument();
      });

      await vi.advanceTimersByTimeAsync(2000);

      await waitFor(() => {
        expect(screen.getByTestId('copy-icon')).toBeInTheDocument();
      });

      vi.useRealTimers();
    });
  });

  describe('Header rendering', () => {
    it('renders header with title when provided', () => {
      render(<CodeBlock {...defaultProps} title="Installation" />);
      expect(screen.getByTestId('code-block-header')).toBeInTheDocument();
      expect(screen.getByText('Installation')).toBeInTheDocument();
    });

    it('does not render header when title is not provided', () => {
      render(<CodeBlock {...defaultProps} />);
      expect(screen.queryByTestId('code-block-header')).not.toBeInTheDocument();
    });

    it('renders language indicator in header', () => {
      render(<CodeBlock {...defaultProps} title="Installation" />);
      expect(screen.getByText('bash')).toBeInTheDocument();
    });
  });

  describe('Line numbers', () => {
    it('does not show line numbers by default', () => {
      const multilineCode = `lineA
lineB
lineC`;
      render(<CodeBlock code={multilineCode} language="bash" />);
      // When no line numbers, there should be no line-number class elements
      const codeBlock = screen.getByTestId('code-block');
      expect(codeBlock.querySelector('.line-number')).not.toBeInTheDocument();
    });

    it('shows line numbers when showLineNumbers is true', () => {
      const multilineCode = `lineA
lineB
lineC`;
      render(<CodeBlock code={multilineCode} language="bash" showLineNumbers />);
      const codeBlock = screen.getByTestId('code-block');
      const lineNumbers = codeBlock.querySelectorAll('.line-number');
      expect(lineNumbers.length).toBe(3);
      expect(lineNumbers[0]).toHaveTextContent('1');
      expect(lineNumbers[1]).toHaveTextContent('2');
      expect(lineNumbers[2]).toHaveTextContent('3');
    });
  });

  describe('Different languages', () => {
    it('handles bash language', () => {
      render(<CodeBlock code="echo hello" language="bash" />);
      const codePre = screen.getByTestId('code-pre');
      expect(codePre).toHaveClass('language-bash');
    });

    it('handles toml language', () => {
      render(<CodeBlock code='addr = "0.0.0.0:12333"' language="toml" />);
      const codePre = screen.getByTestId('code-pre');
      expect(codePre).toHaveClass('language-toml');
    });

    it('defaults to bash language when not specified', () => {
      render(<CodeBlock code="echo hello" />);
      const codePre = screen.getByTestId('code-pre');
      expect(codePre).toHaveClass('language-bash');
    });
  });

  describe('Multiline code', () => {
    it('renders multiline code correctly', () => {
      const multilineCode = `cargo install mirdb
mirdb -c mirdb.toml`;
      render(<CodeBlock code={multilineCode} language="bash" />);
      expect(screen.getByText(/cargo install mirdb/)).toBeInTheDocument();
      expect(screen.getByText(/mirdb -c mirdb.toml/)).toBeInTheDocument();
    });

    it('copies multiline code correctly', async () => {
      const multilineCode = `cargo install mirdb
mirdb -c mirdb.toml`;
      render(<CodeBlock code={multilineCode} language="bash" />);

      const copyButton = screen.getByTestId('copy-button');
      await userEvent.click(copyButton);

      expect(mockWriteText).toHaveBeenCalledWith(multilineCode);
    });
  });

  describe('Accessibility', () => {
    it('copy button is focusable', () => {
      render(<CodeBlock {...defaultProps} />);
      const copyButton = screen.getByTestId('copy-button');
      copyButton.focus();
      expect(document.activeElement).toBe(copyButton);
    });

    it('copy button has type button', () => {
      render(<CodeBlock {...defaultProps} />);
      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toHaveAttribute('type', 'button');
    });
  });
});

describe('CodeBlock Component - Edge Cases', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('handles empty code string', () => {
    render(<CodeBlock code="" language="bash" />);
    expect(screen.getByTestId('code-block')).toBeInTheDocument();
  });

  it('handles code with special characters', () => {
    const codeWithSpecialChars = 'echo "Hello <world> & friends"';
    render(<CodeBlock code={codeWithSpecialChars} language="bash" />);
    expect(screen.getByText(/Hello <world> & friends/)).toBeInTheDocument();
  });

  it('handles clipboard API failure gracefully', async () => {
    // Mock clipboard to reject
    const mockFailingWriteText = vi.fn().mockRejectedValue(new Error('Clipboard error'));
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockFailingWriteText,
      },
      writable: true,
      configurable: true,
    });

    // Mock document.execCommand for fallback
    const execCommandMock = vi.fn().mockReturnValue(true);
    document.execCommand = execCommandMock;

    render(<CodeBlock code="test code" language="bash" />);
    const copyButton = screen.getByTestId('copy-button');

    // Should not throw
    await userEvent.click(copyButton);

    // Fallback should be attempted
    expect(execCommandMock).toHaveBeenCalledWith('copy');
  });
});
