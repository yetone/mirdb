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

    it('should display rust language indicator', () => {
      render(<CodeBlock code="fn main() {}" language="rust" />);

      expect(screen.getByTestId('code-language')).toHaveTextContent('rust');
    });

    it('should display sh language indicator', () => {
      render(<CodeBlock code="#!/bin/sh" language="sh" />);

      expect(screen.getByTestId('code-language')).toHaveTextContent('sh');
    });
  });

  // Scenario 16: Code Syntax Highlighting - Test Case 1
  describe('Syntax Highlighting - Bash Commands', () => {
    it('should apply syntax highlighting to bash commands', () => {
      const bashCode = 'git clone https://github.com/yetone/mirdb.git';
      render(<CodeBlock code={bashCode} language="bash" />);

      const codeElement = screen.getByTestId('code-content');

      // Command (git) should be highlighted with green
      const commandSpan = codeElement.querySelector('.text-green-400');
      expect(commandSpan).toBeInTheDocument();
      expect(commandSpan?.textContent).toBe('git');
    });

    it('should highlight multiple bash commands', () => {
      const bashCode = 'cd mirdb';
      render(<CodeBlock code={bashCode} language="bash" />);

      const codeElement = screen.getByTestId('code-content');

      // cd command should be highlighted
      const commandSpan = codeElement.querySelector('.text-green-400');
      expect(commandSpan).toBeInTheDocument();
      expect(commandSpan?.textContent).toBe('cd');
    });

    it('should highlight echo command', () => {
      render(<CodeBlock code='echo "Hello World"' language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const commandSpan = codeElement.querySelector('.text-green-400');
      expect(commandSpan).toBeInTheDocument();
      expect(commandSpan?.textContent).toBe('echo');
    });

    it('should highlight telnet command', () => {
      render(<CodeBlock code="telnet localhost 11211" language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const commandSpan = codeElement.querySelector('.text-green-400');
      expect(commandSpan).toBeInTheDocument();
      expect(commandSpan?.textContent).toBe('telnet');
    });
  });

  // Scenario 16: Code Syntax Highlighting - Test Case 1 (continued)
  describe('Syntax Highlighting - URLs', () => {
    it('should highlight URLs with blue color', () => {
      render(<CodeBlock code="git clone https://github.com/yetone/mirdb.git" language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const urlSpan = codeElement.querySelector('.text-blue-400');
      expect(urlSpan).toBeInTheDocument();
      expect(urlSpan?.textContent).toContain('https://github.com/yetone/mirdb.git');
    });
  });

  // Scenario 16: Code Syntax Highlighting - Test Case 1 (continued)
  describe('Syntax Highlighting - Flags', () => {
    it('should highlight command flags with yellow color', () => {
      render(<CodeBlock code="cargo run --release" language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const flagSpan = codeElement.querySelector('.text-yellow-400');
      expect(flagSpan).toBeInTheDocument();
      expect(flagSpan?.textContent).toBe('--release');
    });

    it('should highlight short flags', () => {
      render(<CodeBlock code="ls -la" language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const flagSpan = codeElement.querySelector('.text-yellow-400');
      expect(flagSpan).toBeInTheDocument();
      expect(flagSpan?.textContent).toBe('-la');
    });
  });

  // Scenario 16: Code Syntax Highlighting - Test Case 1 (continued)
  describe('Syntax Highlighting - Strings', () => {
    it('should highlight double-quoted strings with orange color', () => {
      render(<CodeBlock code='echo "Hello World"' language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const stringSpan = codeElement.querySelector('.text-orange-400');
      expect(stringSpan).toBeInTheDocument();
      expect(stringSpan?.textContent).toBe('"Hello World"');
    });

    it('should highlight single-quoted strings', () => {
      render(<CodeBlock code="echo 'Hello World'" language="bash" />);

      const codeElement = screen.getByTestId('code-content');
      const stringSpan = codeElement.querySelector('.text-orange-400');
      expect(stringSpan).toBeInTheDocument();
      expect(stringSpan?.textContent).toBe("'Hello World'");
    });
  });

  // Scenario 16: Code Syntax Highlighting - Test Case 2
  describe('Language Indicator Display', () => {
    it('should display bash language label prominently', () => {
      render(<CodeBlock code="npm install" language="bash" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toBeInTheDocument();
      expect(languageLabel).toHaveTextContent('bash');
      expect(languageLabel).toHaveClass('uppercase');
    });

    it('should display rust language label', () => {
      render(<CodeBlock code="fn main() {}" language="rust" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toBeInTheDocument();
      expect(languageLabel).toHaveTextContent('rust');
    });

    it('should display javascript language label', () => {
      render(<CodeBlock code="const x = 1;" language="javascript" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toBeInTheDocument();
      expect(languageLabel).toHaveTextContent('javascript');
    });

    it('should display python language label', () => {
      render(<CodeBlock code="print('hello')" language="python" />);

      const languageLabel = screen.getByTestId('code-language');
      expect(languageLabel).toBeInTheDocument();
      expect(languageLabel).toHaveTextContent('python');
    });
  });

  // Theme styling tests (for dark mode code block appearance)
  describe('Theme Styling', () => {
    it('should have dark background styling for code blocks', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const codeBlock = screen.getByTestId('code-block');
      expect(codeBlock).toHaveClass('bg-slate-900');
      expect(codeBlock).toHaveClass('dark:bg-slate-950');
    });

    it('should have appropriate header styling', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const codeBlock = screen.getByTestId('code-block');
      const header = codeBlock.querySelector('.bg-slate-800');
      expect(header).toBeInTheDocument();
    });

    it('should have light text color for code content', () => {
      render(<CodeBlock code="test code" language="bash" />);

      const codeContent = screen.getByTestId('code-content');
      expect(codeContent).toHaveClass('text-slate-200');
    });
  });
});
