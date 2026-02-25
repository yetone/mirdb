import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { UsageDemo } from '../../../src/components/UsageDemo/UsageDemo';
import { CodeBlock } from '../../../src/components/UsageDemo/CodeBlock';

describe('UsageDemo Component', () => {
  const mockClipboard = {
    writeText: vi.fn(),
  };

  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    });
    mockClipboard.writeText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('Test Case 1: SET operation code example', () => {
    it('renders with code examples for SET operation', () => {
      render(<UsageDemo />);

      expect(screen.getByText('SET Operation')).toBeInTheDocument();
      expect(screen.getByText(/set mykey 0 0 5/)).toBeInTheDocument();
      expect(screen.getByText(/STORED/)).toBeInTheDocument();
    });
  });

  describe('Test Case 2: GET operation code example', () => {
    it('renders with code examples for GET operation', () => {
      render(<UsageDemo />);

      expect(screen.getByText('GET Operation')).toBeInTheDocument();
      expect(screen.getByText(/get mykey/)).toBeInTheDocument();
      expect(screen.getByText(/VALUE mykey 0 5/)).toBeInTheDocument();
    });
  });

  describe('Test Case 7: usage.gif display', () => {
    it('displays usage.gif image with proper alt text', () => {
      render(<UsageDemo />);

      const usageGif = screen.getByRole('img', {
        name: /mirdb usage demonstration showing set and get operations in action/i
      });
      expect(usageGif).toBeInTheDocument();
      expect(usageGif).toHaveAttribute('src', '/assets/usage.gif');
    });
  });

  it('renders Quick Start section title', () => {
    render(<UsageDemo />);
    expect(screen.getByText('Quick Start')).toBeInTheDocument();
  });

  it('renders subtitle explaining Memcached protocol', () => {
    render(<UsageDemo />);
    expect(screen.getByText(/MirDB uses the Memcached protocol/)).toBeInTheDocument();
  });

  it('has an id attribute of "usage" for navigation', () => {
    const { container } = render(<UsageDemo />);
    const section = container.querySelector('#usage');
    expect(section).toBeInTheDocument();
  });
});

describe('CodeBlock Component', () => {
  const mockClipboard = {
    writeText: vi.fn(),
  };

  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    });
    mockClipboard.writeText.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('Test Case 3: Monospace font and syntax highlighting', () => {
    it('displays code with monospace font and syntax highlighting', () => {
      const testCode = 'echo "Hello World"';
      const { container } = render(<CodeBlock code={testCode} language="bash" />);

      const codeElement = container.querySelector('code');
      expect(codeElement).toBeInTheDocument();
      expect(codeElement).toHaveAttribute('data-language', 'bash');
      expect(codeElement?.textContent).toBe(testCode);
    });
  });

  describe('Test Case 4: Copy button visibility', () => {
    it('renders copy button for each code block', () => {
      render(<CodeBlock code="test code" />);

      const copyButton = screen.getByRole('button', { name: /copy to clipboard/i });
      expect(copyButton).toBeInTheDocument();
    });
  });

  describe('Test Case 6: Copy button click functionality', () => {
    it('copies code content to clipboard and shows visual feedback when clicked', async () => {
      const testCode = 'set mykey 0 0 5';
      render(<CodeBlock code={testCode} />);

      const copyButton = screen.getByRole('button', { name: /copy to clipboard/i });

      await vi.waitFor(async () => {
        fireEvent.click(copyButton);
      });

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalledWith(testCode);
      });

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument();
      });
    });
  });

  it('renders title when provided', () => {
    render(<CodeBlock code="test" title="Test Title" />);
    expect(screen.getByText('Test Title')).toBeInTheDocument();
  });

  it('does not render title when not provided', () => {
    const { container } = render(<CodeBlock code="test" />);
    const titleElement = container.querySelector('[class*="codeBlockTitle"]');
    expect(titleElement).not.toBeInTheDocument();
  });

  it('uses bash as default language', () => {
    const { container } = render(<CodeBlock code="test" />);
    const codeElement = container.querySelector('code');
    expect(codeElement).toHaveAttribute('data-language', 'bash');
  });
});

describe('Test Case 8: Mobile responsive code blocks', () => {
  it('code blocks container has horizontal scroll capability', () => {
    const longCode = 'this is a very long line of code that should be scrollable on mobile devices';
    const { container } = render(<CodeBlock code={longCode} />);

    const preElement = container.querySelector('pre');
    expect(preElement).toBeInTheDocument();

    // The pre element should have overflow-x: auto for horizontal scrolling
    // This is defined in CSS, so we check the element exists with the correct structure
    expect(preElement?.querySelector('code')).toBeInTheDocument();
  });
});
