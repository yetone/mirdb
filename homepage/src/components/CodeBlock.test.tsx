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
      // Text is split across syntax highlighting spans, so check textContent
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement?.textContent).toContain('cargo install mirdb');
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
      // Text is split across syntax highlighting spans, so check textContent
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement?.textContent).toContain('cargo install mirdb');
      expect(codeElement?.textContent).toContain('mirdb -c mirdb.toml');
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

describe('CodeBlock Component - Syntax Highlighting (Test Case 4)', () => {
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

  describe('Bash/Shell syntax highlighting', () => {
    it('applies syntax highlighting to bash code', () => {
      render(<CodeBlock code="cargo install mirdb" language="bash" />);
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'true');
    });

    it('highlights bash builtin commands', () => {
      render(<CodeBlock code="cargo install mirdb" language="bash" />);
      const builtinTokens = screen.getAllByTestId('token-builtin');
      expect(builtinTokens.length).toBeGreaterThan(0);
      expect(builtinTokens[0]).toHaveClass('syntax-builtin');
    });

    it('highlights bash comments', () => {
      render(<CodeBlock code="# This is a comment" language="bash" />);
      const commentToken = screen.getByTestId('token-comment');
      expect(commentToken).toHaveClass('syntax-comment');
      expect(commentToken.textContent).toBe('# This is a comment');
    });

    it('highlights bash strings', () => {
      render(<CodeBlock code='echo "hello world"' language="bash" />);
      const stringToken = screen.getByTestId('token-string');
      expect(stringToken).toHaveClass('syntax-string');
    });

    it('highlights bash variables', () => {
      render(<CodeBlock code="echo $HOME" language="bash" />);
      const varToken = screen.getByTestId('token-variable');
      expect(varToken).toHaveClass('syntax-variable');
    });

    it('highlights shell language the same as bash', () => {
      render(<CodeBlock code="cargo install mirdb" language="shell" />);
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'true');
    });
  });

  describe('TOML syntax highlighting', () => {
    it('applies syntax highlighting to TOML code', () => {
      render(<CodeBlock code='addr = "0.0.0.0:12333"' language="toml" />);
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'true');
    });

    it('highlights TOML property keys', () => {
      render(<CodeBlock code='addr = "0.0.0.0:12333"' language="toml" />);
      const propToken = screen.getByTestId('token-property');
      expect(propToken).toHaveClass('syntax-property');
      expect(propToken.textContent).toBe('addr');
    });

    it('highlights TOML strings', () => {
      render(<CodeBlock code='addr = "0.0.0.0:12333"' language="toml" />);
      const stringToken = screen.getByTestId('token-string');
      expect(stringToken).toHaveClass('syntax-string');
    });

    it('highlights TOML numbers', () => {
      render(<CodeBlock code="max_level = 7" language="toml" />);
      const numToken = screen.getByTestId('token-number');
      expect(numToken).toHaveClass('syntax-number');
      expect(numToken.textContent).toBe('7');
    });

    it('highlights TOML section headers', () => {
      render(<CodeBlock code="[server]" language="toml" />);
      const keywordToken = screen.getByTestId('token-keyword');
      expect(keywordToken).toHaveClass('syntax-keyword');
    });

    it('highlights complete TOML config', () => {
      const config = `addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"`;
      render(<CodeBlock code={config} language="toml" />);

      // Should have property tokens
      const propTokens = screen.getAllByTestId('token-property');
      expect(propTokens.length).toBe(3);

      // Should have string tokens
      const stringTokens = screen.getAllByTestId('token-string');
      expect(stringTokens.length).toBe(2);

      // Should have number token
      const numToken = screen.getByTestId('token-number');
      expect(numToken).toBeInTheDocument();
    });
  });

  describe('Rust syntax highlighting', () => {
    it('applies syntax highlighting to Rust code', () => {
      render(<CodeBlock code="fn main() {}" language="rust" />);
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'true');
    });

    it('highlights Rust keywords', () => {
      render(<CodeBlock code="let mut x = 5;" language="rust" />);
      const keywordTokens = screen.getAllByTestId('token-keyword');
      expect(keywordTokens.length).toBeGreaterThanOrEqual(2);
    });

    it('highlights Rust macros', () => {
      render(<CodeBlock code='println!("Hello")' language="rust" />);
      const funcToken = screen.getByTestId('token-function');
      expect(funcToken).toHaveClass('syntax-function');
      expect(funcToken.textContent).toBe('println!');
    });

    it('highlights Rust strings', () => {
      render(<CodeBlock code='"hello world"' language="rust" />);
      const stringToken = screen.getByTestId('token-string');
      expect(stringToken).toHaveClass('syntax-string');
    });

    it('highlights Rust comments', () => {
      render(<CodeBlock code="// This is a comment" language="rust" />);
      const commentToken = screen.getByTestId('token-comment');
      expect(commentToken).toHaveClass('syntax-comment');
    });

    it('highlights Rust builtin types', () => {
      render(<CodeBlock code="let s: String" language="rust" />);
      const builtinToken = screen.getByTestId('token-builtin');
      expect(builtinToken).toHaveClass('syntax-builtin');
    });
  });

  describe('Unsupported languages', () => {
    it('does not apply syntax highlighting for unsupported languages', () => {
      render(<CodeBlock code="some code" language="python" />);
      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'false');
    });
  });

  describe('Syntax highlighting with line numbers', () => {
    it('applies syntax highlighting with line numbers enabled', () => {
      const code = `cargo install mirdb
mirdb -c mirdb.toml`;
      render(<CodeBlock code={code} language="bash" showLineNumbers />);

      const codeElement = screen.getByTestId('code-pre').querySelector('code');
      expect(codeElement).toHaveAttribute('data-highlighted', 'true');

      // Verify line numbers are present
      const lineNumbers = screen.getByTestId('code-block').querySelectorAll('.line-number');
      expect(lineNumbers.length).toBe(2);
    });
  });
});
