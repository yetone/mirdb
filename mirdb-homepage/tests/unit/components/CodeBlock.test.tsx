/**
 * CodeBlock Component Tests.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { CodeBlock } from '../../../src/components/ui/CodeBlock';

// Mock Prism.js
vi.mock('prismjs', () => ({
  default: {
    highlightElement: vi.fn(),
  },
}));

vi.mock('prismjs/components/prism-bash', () => ({}));

describe('CodeBlock', () => {
  const originalNavigator = global.navigator;

  beforeEach(() => {
    vi.resetAllMocks();
    // Mock clipboard API
    Object.defineProperty(global, 'navigator', {
      value: {
        clipboard: {
          writeText: vi.fn().mockResolvedValue(undefined),
        },
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    Object.defineProperty(global, 'navigator', {
      value: originalNavigator,
      writable: true,
      configurable: true,
    });
  });

  it('should render code content', () => {
    render(<CodeBlock code="const x = 1;" language="javascript" />);

    expect(screen.getByTestId('code-content')).toHaveTextContent('const x = 1;');
  });

  it('should apply correct language class', () => {
    render(<CodeBlock code="echo hello" language="bash" />);

    const codeElement = screen.getByTestId('code-content');
    expect(codeElement).toHaveClass('language-bash');
  });

  it('should render filename when provided', () => {
    render(<CodeBlock code="test" language="bash" filename="test.sh" />);

    expect(screen.getByText('test.sh')).toBeInTheDocument();
  });

  it('should render copy button by default', () => {
    render(<CodeBlock code="test" language="bash" />);

    expect(screen.getByRole('button', { name: /copy/i })).toBeInTheDocument();
  });

  it('should not render copy button when showCopyButton is false', () => {
    render(<CodeBlock code="test" language="bash" showCopyButton={false} />);

    expect(screen.queryByRole('button', { name: /copy/i })).not.toBeInTheDocument();
  });

  it('should use bash as default language', () => {
    render(<CodeBlock code="echo test" />);

    const codeElement = screen.getByTestId('code-content');
    expect(codeElement).toHaveClass('language-bash');
  });

  it('should apply Prism syntax highlighting classes', () => {
    render(<CodeBlock code="SET key value" language="bash" />);

    const codeElement = screen.getByTestId('code-content');
    expect(codeElement.className).toContain('language-');
  });
});
