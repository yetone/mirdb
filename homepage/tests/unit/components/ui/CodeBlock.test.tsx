/**
 * Unit tests for CodeBlock component
 * Owner: Scenario 4 - Getting Started Section
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { CodeBlock } from '../../../../src/components/ui/CodeBlock';

describe('CodeBlock', () => {
  const defaultProps = {
    code: 'git clone https://github.com/yetone/mirdb.git',
    language: 'bash',
    label: 'Clone Repository',
    onCopy: vi.fn(),
    copyState: 'idle' as const,
  };

  it('should render the code content', () => {
    render(<CodeBlock {...defaultProps} />);

    const codeContent = screen.getByTestId('code-content');
    expect(codeContent).toHaveTextContent('git clone https://github.com/yetone/mirdb.git');
  });

  it('should render the label', () => {
    render(<CodeBlock {...defaultProps} />);

    const label = screen.getByTestId('code-block-label');
    expect(label).toHaveTextContent('Clone Repository');
  });

  it('should render language as label when no label is provided', () => {
    const { rerender } = render(<CodeBlock {...defaultProps} label={undefined} />);

    const label = screen.getByTestId('code-block-label');
    expect(label).toHaveTextContent('bash');
  });

  it('should render copy button', () => {
    render(<CodeBlock {...defaultProps} />);

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toBeInTheDocument();
    expect(copyButton).toHaveTextContent('Copy');
  });

  it('should call onCopy when copy button is clicked', () => {
    const onCopy = vi.fn();
    render(<CodeBlock {...defaultProps} onCopy={onCopy} />);

    const copyButton = screen.getByTestId('copy-button');
    fireEvent.click(copyButton);

    expect(onCopy).toHaveBeenCalledWith('git clone https://github.com/yetone/mirdb.git');
  });

  it('should show "Copied!" text when copyState is success', () => {
    render(<CodeBlock {...defaultProps} copyState="success" />);

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toHaveTextContent('Copied!');
  });

  it('should show "Failed" text when copyState is error', () => {
    render(<CodeBlock {...defaultProps} copyState="error" />);

    const copyButton = screen.getByTestId('copy-button');
    expect(copyButton).toHaveTextContent('Failed');
  });

  it('should have accessible label on copy button', () => {
    render(<CodeBlock {...defaultProps} />);

    const copyButton = screen.getByRole('button', { name: /copy clone repository to clipboard/i });
    expect(copyButton).toBeInTheDocument();
  });

  it('should render code block container with test id', () => {
    render(<CodeBlock {...defaultProps} />);

    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toBeInTheDocument();
  });

  it('should render different commands correctly', () => {
    const commands = [
      { code: 'cargo build --release', label: 'Build Project' },
      { code: 'cargo run --release', label: 'Run Server' },
    ];

    commands.forEach(({ code, label }) => {
      const { unmount } = render(
        <CodeBlock {...defaultProps} code={code} label={label} />
      );

      expect(screen.getByTestId('code-content')).toHaveTextContent(code);
      expect(screen.getByTestId('code-block-label')).toHaveTextContent(label);

      unmount();
    });
  });
});
