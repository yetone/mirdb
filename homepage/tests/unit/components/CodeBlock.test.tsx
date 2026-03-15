import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { CodeBlock } from '@/components/ui/CodeBlock';

describe('CodeBlock', () => {
  const defaultProps = {
    code: 'const hello = "world";',
    language: 'javascript',
  };

  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('should render the code content', () => {
    render(<CodeBlock {...defaultProps} />);

    // Prism.js tokenizes the code, so we check the pre element's textContent
    const preElement = document.querySelector('pre');
    expect(preElement?.textContent).toContain('const');
    expect(preElement?.textContent).toContain('hello');
  });

  it('should render with proper language class for syntax highlighting', () => {
    render(<CodeBlock {...defaultProps} />);

    const codeElement = screen.getByRole('code');
    expect(codeElement).toHaveClass('language-javascript');
  });

  it('should render a copy button', () => {
    render(<CodeBlock {...defaultProps} />);

    const copyButton = screen.getByRole('button', { name: /copy/i });
    expect(copyButton).toBeInTheDocument();
  });

  it('should render title when provided', () => {
    render(<CodeBlock {...defaultProps} title="Example Code" />);

    expect(screen.getByText('Example Code')).toBeInTheDocument();
  });

  it('should not render title when not provided', () => {
    render(<CodeBlock {...defaultProps} />);

    expect(screen.queryByText('Example Code')).not.toBeInTheDocument();
  });

  it('should copy code to clipboard when copy button is clicked', async () => {
    render(<CodeBlock {...defaultProps} />);

    const copyButton = screen.getByRole('button', { name: /copy/i });
    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(defaultProps.code);
    });
  });

  it('should show visual feedback after successful copy', async () => {
    render(<CodeBlock {...defaultProps} />);

    const copyButton = screen.getByRole('button', { name: /copy/i });
    fireEvent.click(copyButton);

    await waitFor(() => {
      // Button and/or icon should show "Copied" state
      const copiedElements = screen.getAllByLabelText(/copied/i);
      expect(copiedElements.length).toBeGreaterThan(0);
    });
  });

  it('should apply syntax highlighting CSS classes', () => {
    render(<CodeBlock {...defaultProps} />);

    const preElement = screen.getByRole('code').closest('pre');
    expect(preElement).toHaveClass('language-javascript');
  });

  it('should render code in a pre element for proper formatting', () => {
    render(<CodeBlock {...defaultProps} />);

    const preElement = screen.getByRole('code').closest('pre');
    expect(preElement).toBeInTheDocument();
  });

  it('should support bash language for shell commands', () => {
    render(<CodeBlock code="echo hello" language="bash" />);

    const codeElement = screen.getByRole('code');
    expect(codeElement).toHaveClass('language-bash');
  });

  it('should be accessible with proper ARIA attributes', () => {
    render(<CodeBlock {...defaultProps} title="Test Code" />);

    const codeElement = screen.getByRole('code');
    expect(codeElement).toBeInTheDocument();

    const copyButton = screen.getByRole('button', { name: /copy/i });
    expect(copyButton).toHaveAttribute('aria-label');
  });
});
