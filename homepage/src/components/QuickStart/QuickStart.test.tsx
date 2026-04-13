import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { CodeBlock } from './CodeBlock';
import { QuickStart } from './QuickStart';

describe('CodeBlock', () => {
  it('renders code content with proper pre/code tags', () => {
    const testCode = 'console.log("hello")';
    render(<CodeBlock code={testCode} />);

    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toBeInTheDocument();

    const preElement = codeBlock.querySelector('pre');
    expect(preElement).toBeInTheDocument();
    expect(preElement).toHaveClass('code-block-pre');

    const codeElement = codeBlock.querySelector('code');
    expect(codeElement).toBeInTheDocument();
    expect(codeElement).toHaveClass('code-block-code');
    expect(codeElement).toHaveTextContent(testCode);
  });

  it('renders with default bash language', () => {
    render(<CodeBlock code="npm install" />);

    const codeElement = screen.getByTestId('code-block').querySelector('code');
    expect(codeElement).toHaveClass('language-bash');
    expect(codeElement).toHaveAttribute('data-language', 'bash');
  });

  it('renders with custom language', () => {
    render(<CodeBlock code="const x = 1" language="javascript" />);

    const codeElement = screen.getByTestId('code-block').querySelector('code');
    expect(codeElement).toHaveClass('language-javascript');
    expect(codeElement).toHaveAttribute('data-language', 'javascript');
  });

  it('renders title when provided', () => {
    render(<CodeBlock code="test" title="Terminal" />);

    const title = screen.getByTestId('code-block-title');
    expect(title).toBeInTheDocument();
    expect(title).toHaveTextContent('Terminal');
  });

  it('does not render title when not provided', () => {
    render(<CodeBlock code="test" />);

    expect(screen.queryByTestId('code-block-title')).not.toBeInTheDocument();
  });

  it('applies monospace font styling', () => {
    render(<CodeBlock code="code" />);

    const codeElement = screen.getByTestId('code-block').querySelector('code');
    expect(codeElement).toHaveClass('code-block-code');
  });

  it('preserves whitespace in code', () => {
    const multilineCode = `line1
  indented
    more indented`;
    render(<CodeBlock code={multilineCode} />);

    const codeElement = screen.getByTestId('code-block').querySelector('code');
    expect(codeElement?.textContent).toBe(multilineCode);
  });
});

describe('QuickStart', () => {
  it('renders Quick Start heading', () => {
    render(<QuickStart />);

    const heading = screen.getByRole('heading', { name: /quick start/i, level: 2 });
    expect(heading).toBeInTheDocument();
  });

  it('renders installation code block', () => {
    render(<QuickStart />);

    expect(screen.getByText(/cargo build/i)).toBeInTheDocument();
  });

  it('renders usage examples with set and get operations', () => {
    render(<QuickStart />);

    expect(screen.getByText(/set mykey/i)).toBeInTheDocument();
    expect(screen.getByText(/get mykey/i)).toBeInTheDocument();
  });

  it('has proper section structure with aria-labelledby', () => {
    render(<QuickStart />);

    const section = document.querySelector('#quick-start');
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('aria-labelledby', 'quick-start-heading');
  });
});
