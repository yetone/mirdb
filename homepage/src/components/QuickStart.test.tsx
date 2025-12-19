import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  QuickStart,
  installCommand,
  runCommand,
  usageExample,
  configExample,
} from './QuickStart';

describe('QuickStart Section - E2E Tests', () => {
  beforeEach(() => {
    // Mock clipboard API for copy button tests
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Installation command present', () => {
    it('displays cargo install mirdb command in a code block', () => {
      render(<QuickStart />);

      // Check the quick start section exists
      const quickStartSection = screen.getByTestId('quick-start-section');
      expect(quickStartSection).toBeInTheDocument();

      // Check for the installation command (text split across syntax highlighting spans)
      const installStep = screen.getByTestId('install-step');
      const codeBlock = within(installStep).getByTestId('code-block');
      const codeElement = codeBlock.querySelector('code');
      expect(codeElement?.textContent).toContain('cargo install mirdb');
    });

    it('has installation step with proper title', () => {
      render(<QuickStart />);

      const installStep = screen.getByTestId('install-step');
      expect(installStep).toBeInTheDocument();
      expect(within(installStep).getByText('Install MirDB')).toBeInTheDocument();
    });

    it('installation command is in a code block', () => {
      render(<QuickStart />);

      const installStep = screen.getByTestId('install-step');
      const codeBlock = within(installStep).getByTestId('code-block');
      expect(codeBlock).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Run command example present', () => {
    it('displays command showing how to start MirDB with config file', () => {
      render(<QuickStart />);

      // Check for the run command (text split across syntax highlighting spans)
      const runStep = screen.getByTestId('run-step');
      const codeBlock = within(runStep).getByTestId('code-block');
      const codeElement = codeBlock.querySelector('code');
      expect(codeElement?.textContent).toContain('mirdb -c mirdb.toml');
    });

    it('has run step with proper title', () => {
      render(<QuickStart />);

      const runStep = screen.getByTestId('run-step');
      expect(runStep).toBeInTheDocument();
      expect(within(runStep).getByText('Run MirDB')).toBeInTheDocument();
    });

    it('run command is in a code block', () => {
      render(<QuickStart />);

      const runStep = screen.getByTestId('run-step');
      const codeBlock = within(runStep).getByTestId('code-block');
      expect(codeBlock).toBeInTheDocument();
    });
  });

  describe('Test Case 3: SET/GET usage examples present', () => {
    it('displays SET command example with STORED response', () => {
      render(<QuickStart />);

      // Check for SET command elements in the usage step code block (text split across spans)
      const usageStep = screen.getByTestId('usage-step');
      const codeBlock = within(usageStep).getByTestId('code-block');
      const codeElement = codeBlock.querySelector('code');
      expect(codeElement?.textContent).toContain('set mykey 0 0 5');
      // STORED appears in code block
      expect(usageExample).toContain('STORED');
    });

    it('displays GET command example with VALUE response', () => {
      render(<QuickStart />);

      // Check for GET command elements (text split across syntax highlighting spans)
      const usageStep = screen.getByTestId('usage-step');
      const codeBlock = within(usageStep).getByTestId('code-block');
      const codeElement = codeBlock.querySelector('code');
      expect(codeElement?.textContent).toContain('get mykey');
      expect(codeElement?.textContent).toContain('VALUE mykey 0 5');
    });

    it('displays END response for GET command', () => {
      render(<QuickStart />);

      // Check that END appears in the usage example
      expect(usageExample).toContain('END');
      // Also verify the code is rendered in the component
      const usageStep = screen.getByTestId('usage-step');
      const codeBlock = within(usageStep).getByTestId('code-block');
      expect(codeBlock).toBeInTheDocument();
    });

    it('shows complete usage example with telnet', () => {
      render(<QuickStart />);

      // Text split across syntax highlighting spans
      const usageStep = screen.getByTestId('usage-step');
      const codeBlock = within(usageStep).getByTestId('code-block');
      const codeElement = codeBlock.querySelector('code');
      expect(codeElement?.textContent).toContain('telnet localhost 12333');
    });

    it('has usage step with proper title', () => {
      render(<QuickStart />);

      const usageStep = screen.getByTestId('usage-step');
      expect(usageStep).toBeInTheDocument();
      expect(within(usageStep).getByText('Basic Usage')).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Code blocks have syntax highlighting', () => {
    it('code blocks render with bash language class', () => {
      render(<QuickStart />);

      const installStep = screen.getByTestId('install-step');
      const codePre = within(installStep).getByTestId('code-pre');
      expect(codePre).toHaveClass('language-bash');
    });

    it('config code block renders with toml language class', () => {
      render(<QuickStart />);

      const configStep = screen.getByTestId('config-step');
      const codePre = within(configStep).getByTestId('code-pre');
      expect(codePre).toHaveClass('language-toml');
    });

    it('all code blocks have proper styling classes', () => {
      render(<QuickStart />);

      const codeBlocks = screen.getAllByTestId('code-block');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(3);

      codeBlocks.forEach((block) => {
        expect(block).toHaveClass('code-block');
      });
    });
  });

  describe('Test Case 5: Copy-to-clipboard button on code blocks', () => {
    it('each code block has a copy button', () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByTestId('copy-button');
      // Should have at least 4 code blocks (install, config, run, usage)
      expect(copyButtons.length).toBeGreaterThanOrEqual(4);
    });

    it('clicking copy button on install command copies the code', async () => {
      render(<QuickStart />);

      const installStep = screen.getByTestId('install-step');
      const copyButton = within(installStep).getByTestId('copy-button');

      await userEvent.click(copyButton);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(installCommand);
    });

    it('clicking copy button on run command copies the code', async () => {
      render(<QuickStart />);

      const runStep = screen.getByTestId('run-step');
      const copyButton = within(runStep).getByTestId('copy-button');

      await userEvent.click(copyButton);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(runCommand);
    });

    it('clicking copy button on config copies the code', async () => {
      render(<QuickStart />);

      const configStep = screen.getByTestId('config-step');
      const copyButton = within(configStep).getByTestId('copy-button');

      await userEvent.click(copyButton);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(configExample);
    });

    it('clicking copy button on usage example copies the code', async () => {
      render(<QuickStart />);

      const usageStep = screen.getByTestId('usage-step');
      const copyButton = within(usageStep).getByTestId('copy-button');

      await userEvent.click(copyButton);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(usageExample);
    });
  });

  describe('Quick Start Section Structure', () => {
    it('has section title Quick Start', () => {
      render(<QuickStart />);

      expect(screen.getByText('Quick Start')).toBeInTheDocument();
    });

    it('has proper id for navigation', () => {
      render(<QuickStart />);

      const quickStartSection = screen.getByTestId('quick-start-section');
      expect(quickStartSection).toHaveAttribute('id', 'quick-start');
    });

    it('has section description about 5 minute setup', () => {
      render(<QuickStart />);

      expect(screen.getByText(/5 minutes/)).toBeInTheDocument();
    });

    it('displays steps in correct order (1, 2, 3, 4)', () => {
      render(<QuickStart />);

      const stepNumbers = screen.getAllByText(/^[1-4]$/);
      expect(stepNumbers.length).toBe(4);
      expect(stepNumbers[0]).toHaveTextContent('1');
      expect(stepNumbers[1]).toHaveTextContent('2');
      expect(stepNumbers[2]).toHaveTextContent('3');
      expect(stepNumbers[3]).toHaveTextContent('4');
    });
  });

  describe('Exported Constants', () => {
    it('installCommand matches expected value', () => {
      expect(installCommand).toBe('cargo install mirdb');
    });

    it('runCommand matches expected value', () => {
      expect(runCommand).toBe('mirdb -c mirdb.toml');
    });

    it('usageExample contains SET command', () => {
      expect(usageExample).toContain('set mykey');
    });

    it('usageExample contains GET command', () => {
      expect(usageExample).toContain('get mykey');
    });

    it('usageExample contains STORED response', () => {
      expect(usageExample).toContain('STORED');
    });

    it('usageExample contains VALUE response', () => {
      expect(usageExample).toContain('VALUE');
    });

    it('usageExample contains END response', () => {
      expect(usageExample).toContain('END');
    });

    it('configExample contains address configuration', () => {
      expect(configExample).toContain('addr');
      expect(configExample).toContain('0.0.0.0:12333');
    });
  });
});

describe('QuickStart Component - Unit Tests', () => {
  it('renders without crashing', () => {
    render(<QuickStart />);
    expect(screen.getByTestId('quick-start-section')).toBeInTheDocument();
  });

  it('contains all required steps', () => {
    render(<QuickStart />);

    expect(screen.getByTestId('install-step')).toBeInTheDocument();
    expect(screen.getByTestId('config-step')).toBeInTheDocument();
    expect(screen.getByTestId('run-step')).toBeInTheDocument();
    expect(screen.getByTestId('usage-step')).toBeInTheDocument();
  });

  it('renders usage notes section', () => {
    render(<QuickStart />);

    // Check for usage notes explaining SET and GET commands
    expect(screen.getByText(/SET command:/)).toBeInTheDocument();
    expect(screen.getByText(/GET command:/)).toBeInTheDocument();
    expect(screen.getAllByText(/Response:/).length).toBeGreaterThanOrEqual(2);
  });
});
