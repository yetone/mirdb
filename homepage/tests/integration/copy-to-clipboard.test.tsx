/**
 * Integration tests for copy-to-clipboard functionality
 * Owner: Scenario 4 - Getting Started Section
 *
 * Tests the integration between GettingStarted, CodeBlock,
 * useCopyToClipboard hook, and Toast components.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { GettingStarted } from '../../src/components/sections/GettingStarted';

describe('Copy-to-Clipboard Integration', () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Copy functionality flow', () => {
    it('should copy git clone command and show success toast', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      expect(mockWriteText).toHaveBeenCalledWith('git clone https://github.com/yetone/mirdb.git');

      await waitFor(() => {
        expect(screen.getByTestId('toast')).toBeInTheDocument();
      });

      expect(screen.getByTestId('toast')).toHaveTextContent('Copied to clipboard!');
    });

    it('should copy cargo build command when Build Project copy button is clicked', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[1]);
      });

      expect(mockWriteText).toHaveBeenCalledWith('cargo build --release');
    });

    it('should copy cargo run command when Run Server copy button is clicked', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[2]);
      });

      expect(mockWriteText).toHaveBeenCalledWith('cargo run --release');
    });
  });

  describe('Copy button state changes', () => {
    it('should change copy button text to "Copied!" after successful copy', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');
      expect(copyButtons[0]).toHaveTextContent('Copy');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(copyButtons[0]).toHaveTextContent('Copied!');
      });
    });

    it('should change copy button text to "Failed" when copy fails', async () => {
      mockWriteText.mockRejectedValue(new Error('Clipboard access denied'));

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(copyButtons[0]).toHaveTextContent('Failed');
      });
    });
  });

  describe('Error handling', () => {
    it('should show error toast when clipboard API fails', async () => {
      mockWriteText.mockRejectedValue(new Error('Clipboard access denied'));

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(screen.getByTestId('toast')).toBeInTheDocument();
      });

      expect(screen.getByTestId('toast')).toHaveTextContent('Failed to copy');
    });
  });

  describe('Toast notification behavior', () => {
    it('should show toast when copying a command', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(screen.getByTestId('toast')).toBeInTheDocument();
      });
    });

    it('should show toast when copying different commands', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      // Copy first command
      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      expect(mockWriteText).toHaveBeenCalledWith('git clone https://github.com/yetone/mirdb.git');

      // Copy second command
      await act(async () => {
        fireEvent.click(copyButtons[1]);
      });

      expect(mockWriteText).toHaveBeenCalledWith('cargo build --release');

      await waitFor(() => {
        expect(screen.getByTestId('toast')).toHaveTextContent('Copied to clipboard!');
      });
    });
  });

  describe('Multiple code blocks interaction', () => {
    it('should handle copying multiple commands in sequence', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      // Copy all three commands in sequence
      for (let i = 0; i < 3; i++) {
        await act(async () => {
          fireEvent.click(copyButtons[i]);
        });
      }

      expect(mockWriteText).toHaveBeenCalledTimes(3);
      expect(mockWriteText).toHaveBeenNthCalledWith(1, 'git clone https://github.com/yetone/mirdb.git');
      expect(mockWriteText).toHaveBeenNthCalledWith(2, 'cargo build --release');
      expect(mockWriteText).toHaveBeenNthCalledWith(3, 'cargo run --release');
    });

    it('should update only the clicked button state', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      // Click first button
      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(copyButtons[0]).toHaveTextContent('Copied!');
      });

      // Other buttons should still show "Copy"
      expect(copyButtons[1]).toHaveTextContent('Copy');
      expect(copyButtons[2]).toHaveTextContent('Copy');
    });
  });

  describe('Accessibility', () => {
    it('should have accessible toast with role="alert"', async () => {
      mockWriteText.mockResolvedValue(undefined);

      render(<GettingStarted />);

      const copyButtons = screen.getAllByTestId('copy-button');

      await act(async () => {
        fireEvent.click(copyButtons[0]);
      });

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });
    });

    it('should have accessible copy buttons with aria-label', () => {
      render(<GettingStarted />);

      const cloneButton = screen.getByRole('button', { name: /copy clone repository to clipboard/i });
      const buildButton = screen.getByRole('button', { name: /copy build project to clipboard/i });
      const runButton = screen.getByRole('button', { name: /copy run server to clipboard/i });

      expect(cloneButton).toBeInTheDocument();
      expect(buildButton).toBeInTheDocument();
      expect(runButton).toBeInTheDocument();
    });
  });
});
