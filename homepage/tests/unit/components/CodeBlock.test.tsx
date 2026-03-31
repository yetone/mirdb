import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { CodeBlock } from '../../../src/components/ui/CodeBlock';

describe('CodeBlock', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('should render code content', () => {
    render(<CodeBlock code="echo hello" language="bash" />);

    expect(screen.getByText('echo hello')).toBeInTheDocument();
  });

  it('should display with syntax highlighting class for bash', () => {
    render(<CodeBlock code="npm install" language="bash" />);

    const codeElement = screen.getByText('npm install');
    expect(codeElement).toHaveClass('text-green-400');
  });

  it('should display with syntax highlighting class for rust', () => {
    render(<CodeBlock code="fn main()" language="rust" />);

    const codeElement = screen.getByText('fn main()');
    expect(codeElement).toHaveClass('text-orange-400');
  });

  it('should show copy button by default', () => {
    render(<CodeBlock code="test code" />);

    expect(screen.getByTestId('copy-button')).toBeInTheDocument();
  });

  it('should hide copy button when showCopyButton is false', () => {
    render(<CodeBlock code="test code" showCopyButton={false} />);

    expect(screen.queryByTestId('copy-button')).not.toBeInTheDocument();
  });

  it('should render title when provided', () => {
    render(<CodeBlock code="test" title="Terminal" />);

    expect(screen.getByText('Terminal')).toBeInTheDocument();
  });

  it('should copy code to clipboard when copy button is clicked', async () => {
    const testCode = 'cargo build --release';
    render(<CodeBlock code={testCode} />);

    const copyButton = screen.getByTestId('copy-button');
    fireEvent.click(copyButton);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testCode);
  });

  it('should show check icon after successful copy', async () => {
    render(<CodeBlock code="test" />);

    const copyButton = screen.getByTestId('copy-button');
    fireEvent.click(copyButton);

    // Wait for state update
    await vi.waitFor(() => {
      expect(screen.getByTestId('check-icon')).toBeInTheDocument();
    });
  });

  it('should have copy icon initially', () => {
    render(<CodeBlock code="test" />);

    expect(screen.getByTestId('copy-icon')).toBeInTheDocument();
  });

  it('should have accessible aria-label on copy button', () => {
    render(<CodeBlock code="test" />);

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  it('should render code block container with correct testid', () => {
    render(<CodeBlock code="test" />);

    expect(screen.getByTestId('code-block')).toBeInTheDocument();
  });
});
