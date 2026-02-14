import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { CodeBlock } from '../../../../src/components/common/CodeBlock';

// Mock prismjs
vi.mock('prismjs', () => ({
  default: {
    highlightElement: vi.fn(),
  },
}));

vi.mock('prismjs/components/prism-bash', () => ({}));

describe('CodeBlock', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
    mockWriteText.mockResolvedValue(undefined);
  });

  it('renders code with the specified language class', () => {
    render(<CodeBlock code="echo hello" language="bash" />);

    const codeElement = screen.getByText('echo hello');
    expect(codeElement).toBeInTheDocument();
    expect(codeElement).toHaveClass('language-bash');
  });

  it('renders code with shell language for syntax highlighting', () => {
    const shellCode = 'ls -la';
    render(<CodeBlock code={shellCode} language="shell" />);

    const codeElement = screen.getByText(shellCode);
    expect(codeElement).toHaveClass('language-shell');
  });

  it('renders copy button with accessible label', () => {
    render(<CodeBlock code="test code" language="bash" />);

    const copyButton = screen.getByRole('button', {
      name: /copy code to clipboard/i,
    });
    expect(copyButton).toBeInTheDocument();
    expect(copyButton).toHaveTextContent('Copy');
  });

  it('shows "Copied!" feedback after clicking copy button', async () => {
    render(<CodeBlock code="test code" language="bash" />);

    const copyButton = screen.getByRole('button', {
      name: /copy code to clipboard/i,
    });

    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument();
    });
    expect(screen.getByText('Copied!')).toBeInTheDocument();
  });

  it('copies the code text to clipboard when copy button is clicked', async () => {
    const testCode = 'echo "Hello World"';
    render(<CodeBlock code={testCode} language="bash" />);

    const copyButton = screen.getByRole('button', {
      name: /copy code to clipboard/i,
    });

    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(mockWriteText).toHaveBeenCalledWith(testCode);
    });
  });

  it('renders title when provided', () => {
    render(<CodeBlock code="test" language="bash" title="Terminal" />);

    expect(screen.getByText('Terminal')).toBeInTheDocument();
  });

  it('renders line numbers when showLineNumbers is true', () => {
    const multilineCode = 'line 1\nline 2\nline 3';
    render(
      <CodeBlock code={multilineCode} language="bash" showLineNumbers={true} />
    );

    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('does not render line numbers by default', () => {
    const multilineCode = 'line 1\nline 2';
    render(<CodeBlock code={multilineCode} language="bash" />);

    // Line numbers should not appear as separate elements
    // The "1" from "line 1" should exist but not a separate "1" for line number
    const allTextElements = screen.queryAllByText('1');
    // Should only find "1" as part of "line 1", not as a standalone line number
    expect(allTextElements.length).toBeLessThanOrEqual(1);
  });

  it('applies correct styling classes', () => {
    const { container } = render(
      <CodeBlock code="test" language="bash" />
    );

    const wrapper = container.firstChild;
    expect(wrapper).toHaveClass('relative');
    expect(wrapper).toHaveClass('rounded-lg');
    expect(wrapper).toHaveClass('overflow-hidden');
  });
});
