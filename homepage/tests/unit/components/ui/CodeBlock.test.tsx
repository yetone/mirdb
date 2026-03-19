/**
 * Unit tests for CodeBlock component.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 */

import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { CodeBlock } from '@/components/ui/CodeBlock';

// Mock the clipboard API
const mockWriteText = jest.fn();
Object.assign(navigator, {
  clipboard: {
    writeText: mockWriteText,
  },
});

describe('CodeBlock Component', () => {
  beforeEach(() => {
    mockWriteText.mockReset();
    mockWriteText.mockResolvedValue(undefined);
  });

  // Test Case 2: Render CodeBlock with git clone command
  describe('TC2: Render CodeBlock with git clone command', () => {
    const gitCloneCommand = 'git clone https://github.com/yetone/mirdb.git';

    it('should display the git clone command', () => {
      render(<CodeBlock code={gitCloneCommand} language="bash" />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent).toHaveTextContent(gitCloneCommand);
    });

    it('should display the command with syntax highlighting', () => {
      render(<CodeBlock code={gitCloneCommand} language="bash" />);

      // Check that 'git' command is highlighted (has a specific class)
      const codeElement = screen.getByTestId('code-content');
      expect(codeElement.querySelector('.text-green-400')).toBeInTheDocument();
    });

    it('should display the URL with highlighting', () => {
      render(<CodeBlock code={gitCloneCommand} language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      // URL should be highlighted with blue color
      expect(codeElement.querySelector('.text-blue-400')).toBeInTheDocument();
    });

    it('should display language indicator', () => {
      render(<CodeBlock code={gitCloneCommand} language="bash" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toHaveTextContent('bash');
    });
  });

  // Test Case 3: Render CodeBlock with cargo command
  describe('TC3: Render CodeBlock with cargo command', () => {
    const cargoCommand = 'cargo run --release';

    it('should display the cargo run command', () => {
      render(<CodeBlock code={cargoCommand} language="bash" />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent).toHaveTextContent(cargoCommand);
    });

    it('should display with bash syntax highlighting', () => {
      render(<CodeBlock code={cargoCommand} language="bash" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toHaveTextContent('bash');
    });

    it('should highlight the cargo command', () => {
      render(<CodeBlock code={cargoCommand} language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      // 'cargo' should be highlighted
      expect(codeElement.querySelector('.text-green-400')).toBeInTheDocument();
    });

    it('should highlight the --release flag', () => {
      render(<CodeBlock code={cargoCommand} language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      // Flag should be highlighted with yellow
      expect(codeElement.querySelector('.text-yellow-400')).toBeInTheDocument();
    });
  });

  // Test for copy button functionality
  describe('Copy functionality', () => {
    it('should render copy button', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toBeInTheDocument();
    });

    it('should have accessible label for copy button', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const copyButton = screen.getByTestId('copy-button');
      expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    it('should copy code to clipboard when button is clicked', async () => {
      const testCode = 'git clone https://github.com/yetone/mirdb.git';
      render(<CodeBlock code={testCode} language="bash" />);

      const copyButton = screen.getByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButton);
      });

      expect(mockWriteText).toHaveBeenCalledWith(testCode);
    });

    it('should show success state after copying', async () => {
      render(<CodeBlock code="test code" language="bash" />);

      const copyButton = screen.getByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButton);
      });

      await waitFor(() => {
        expect(screen.getByText('Copied!')).toBeInTheDocument();
      });
    });

    it('should update aria-label when copied', async () => {
      render(<CodeBlock code="test code" language="bash" />);

      const copyButton = screen.getByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButton);
      });

      await waitFor(() => {
        expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
      });
    });
  });

  // Test for line numbers
  describe('Line numbers', () => {
    it('should not show line numbers by default', () => {
      render(<CodeBlock code="line1\nline2" language="bash" />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent.textContent).not.toMatch(/^\s*1\s/);
    });

    it('should show line numbers when showLineNumbers is true', () => {
      render(<CodeBlock code="line1\nline2" language="bash" showLineNumbers />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent).toHaveTextContent('1');
      expect(codeContent).toHaveTextContent('2');
    });
  });

  // Test for code block structure
  describe('Code block structure', () => {
    it('should render with correct container structure', () => {
      render(<CodeBlock code="test" language="bash" />);

      const codeBlock = screen.getByTestId('code-block');
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock).toHaveClass('relative', 'group', 'rounded-lg');
    });

    it('should have a pre element for code content', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const preElement = screen.getByTestId('code-content');
      expect(preElement.tagName.toLowerCase()).toBe('pre');
    });

    it('should contain a code element inside pre', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const codeElement = screen.getByTestId('code-content').querySelector('code');
      expect(codeElement).toBeInTheDocument();
    });

    it('should apply language class to code element', () => {
      render(<CodeBlock code="test code" language="rust" />);

      const codeElement = screen.getByTestId('code-content').querySelector('code');
      expect(codeElement).toHaveClass('language-rust');
    });
  });

  // Test for different languages
  describe('Language support', () => {
    it('should render bash code correctly', () => {
      render(<CodeBlock code="echo hello" language="bash" />);

      expect(screen.getByTestId('code-language')).toHaveTextContent('bash');
    });

    it('should render shell code with highlighting', () => {
      render(<CodeBlock code="cd project" language="shell" />);

      expect(screen.getByTestId('code-language')).toHaveTextContent('shell');
    });

    it('should render with default language bash when not specified', () => {
      render(<CodeBlock code="test code" />);

      expect(screen.getByTestId('code-language')).toHaveTextContent('bash');
    });
  });
});
