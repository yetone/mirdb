/**
 * Quick Start Section Component Tests
 * Owner: Scenario 5 - Quick Start Section
 *
 * Tests for REQ-4: Quick Start section with code example and copy functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { QuickStart } from '../../../../src/components/sections/QuickStart';
import { CodeBlock } from '../../../../src/components/ui/CodeBlock';

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
};

beforeEach(() => {
  vi.useFakeTimers();
  Object.assign(navigator, {
    clipboard: mockClipboard,
  });
  mockClipboard.writeText.mockClear();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('QuickStart Section', () => {
  // Test Case 1: Code block element is rendered
  describe('Test Case 1: Code block element is rendered', () => {
    it('renders the QuickStart section with code block', () => {
      render(<QuickStart />);

      const section = screen.getByTestId('quickstart');
      expect(section).toBeInTheDocument();

      const codeBlock = screen.getByTestId('code-block');
      expect(codeBlock).toBeInTheDocument();
    });

    it('renders with proper section structure', () => {
      render(<QuickStart />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent('Quick Start');
    });
  });

  // Test Case 2: Code example contains MirDB commands
  describe('Test Case 2: Code example contains MirDB commands', () => {
    it('displays mirdb-server command', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent.textContent).toContain('mirdb-server');
    });

    it('displays port flag and value', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent.textContent).toContain('--port');
      expect(codeContent.textContent).toContain('11211');
    });

    it('displays set and get commands', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent.textContent).toContain('set');
      expect(codeContent.textContent).toContain('get');
    });
  });

  // Test Case 3: Syntax highlighting is applied (code has colored tokens)
  describe('Test Case 3: Syntax highlighting is applied', () => {
    it('renders code with syntax highlighting tokens', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');
      const tokens = codeContent.querySelectorAll('[data-token-type]');

      expect(tokens.length).toBeGreaterThan(0);
    });

    it('applies correct token types for shell syntax', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');

      // Check for prompt tokens
      const promptTokens = codeContent.querySelectorAll('[data-token-type="prompt"]');
      expect(promptTokens.length).toBeGreaterThan(0);

      // Check for command tokens
      const commandTokens = codeContent.querySelectorAll('[data-token-type="command"]');
      expect(commandTokens.length).toBeGreaterThan(0);

      // Check for flag tokens
      const flagTokens = codeContent.querySelectorAll('[data-token-type="flag"]');
      expect(flagTokens.length).toBeGreaterThan(0);

      // Check for value tokens
      const valueTokens = codeContent.querySelectorAll('[data-token-type="value"]');
      expect(valueTokens.length).toBeGreaterThan(0);
    });

    it('tokens have different color classes', () => {
      render(<QuickStart />);

      const codeContent = screen.getByTestId('code-content');

      // Check that tokens have different CSS classes for colors
      const promptToken = codeContent.querySelector('[data-token-type="prompt"]');
      const commandToken = codeContent.querySelector('[data-token-type="command"]');
      const flagToken = codeContent.querySelector('[data-token-type="flag"]');

      expect(promptToken?.className).toContain('text-gray-400');
      expect(commandToken?.className).toContain('text-cyan-400');
      expect(flagToken?.className).toContain('text-yellow-400');
    });
  });

  // Test Case 4: Copy button is rendered
  describe('Test Case 4: Copy button is rendered', () => {
    it('renders copy button', () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toBeInTheDocument();
    });

    it('copy button has proper aria-label', () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toHaveAttribute('aria-label', 'Copy code');
    });

    it('copy button displays "Copy" text initially', () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton.textContent).toContain('Copy');
    });
  });

  // Test Case 5: Code content is copied to clipboard (integration)
  describe('Test Case 5: Code content is copied to clipboard', () => {
    it('copies code to clipboard when copy button is clicked', async () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(mockClipboard.writeText).toHaveBeenCalledTimes(1);
      // The copied text should not include the $ prompt
      const copiedText = mockClipboard.writeText.mock.calls[0][0];
      expect(copiedText).toContain('mirdb-server');
      expect(copiedText).not.toMatch(/^\$/m); // Should not start lines with $
    });
  });

  // Test Case 6: Visual feedback shows copy was successful
  describe('Test Case 6: Visual feedback shows copy was successful', () => {
    it('shows "Copied!" feedback after clicking copy button', async () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      await act(async () => {
        fireEvent.click(copyButton);
      });

      const feedback = screen.getByTestId('copy-feedback');
      expect(feedback).toHaveTextContent('Copied!');
    });

    it('shows success icon after copying', async () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      await act(async () => {
        fireEvent.click(copyButton);
      });

      const successIcon = screen.getByTestId('copy-success-icon');
      expect(successIcon).toBeInTheDocument();
    });

    it('copy button aria-label changes to "Copied!" after successful copy', async () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
    });

    it('resets to "Copy" state after timeout', async () => {
      render(<QuickStart />);

      const copyButton = screen.getByTestId('copy-button');
      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(screen.getByTestId('copy-feedback')).toBeInTheDocument();

      await act(async () => {
        vi.advanceTimersByTime(2000);
      });

      expect(copyButton).toHaveAttribute('aria-label', 'Copy code');
      expect(copyButton.textContent).toContain('Copy');
    });
  });
});

describe('CodeBlock Component', () => {
  it('renders code content correctly', () => {
    const testCode = '$ echo hello';
    render(<CodeBlock code={testCode} language="shell" />);

    const codeContent = screen.getByTestId('code-content');
    expect(codeContent.textContent).toContain('echo');
    expect(codeContent.textContent).toContain('hello');
  });

  it('renders without copy button when showCopyButton is false', () => {
    render(<CodeBlock code="$ test" language="shell" showCopyButton={false} />);

    const copyButton = screen.queryByTestId('copy-button');
    expect(copyButton).not.toBeInTheDocument();
  });

  it('handles non-shell language gracefully', () => {
    const jsCode = 'const x = 1;';
    render(<CodeBlock code={jsCode} language="javascript" />);

    const codeContent = screen.getByTestId('code-content');
    expect(codeContent.textContent).toBe(jsCode);
  });

  it('has correct data-language attribute', () => {
    render(<CodeBlock code="$ test" language="shell" />);

    const codeContent = screen.getByTestId('code-content');
    expect(codeContent).toHaveAttribute('data-language', 'shell');
  });
});
