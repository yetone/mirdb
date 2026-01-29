/**
 * Unit tests for Installation section components.
 * Owner: Scenario 6 - Installation Instructions Section
 *
 * Tests cover:
 * - InstallationSection component rendering
 * - Prerequisites section with Rust toolchain link
 * - Build from source instructions with copy buttons
 * - Run with config instructions with copy buttons
 * - Copy button functionality for all code blocks
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { InstallationSection } from '../../../src/components/Installation/InstallationSection';

describe('InstallationSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('rendering', () => {
    it('renders the installation section container', () => {
      render(<InstallationSection />);
      expect(screen.getByTestId('installation-section')).toBeInTheDocument();
    });

    it('renders all three main sections', () => {
      render(<InstallationSection />);
      expect(screen.getByTestId('prerequisites-section')).toBeInTheDocument();
      expect(screen.getByTestId('build-section')).toBeInTheDocument();
      expect(screen.getByTestId('run-section')).toBeInTheDocument();
    });
  });

  describe('prerequisites section', () => {
    it('displays the prerequisites heading', () => {
      render(<InstallationSection />);
      expect(screen.getByRole('heading', { name: 'Prerequisites' })).toBeInTheDocument();
    });

    it('lists Rust toolchain as a requirement', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/requires the Rust toolchain/i)).toBeInTheDocument();
    });

    it('provides a link to rustup.rs', () => {
      render(<InstallationSection />);
      const link = screen.getByTestId('rustup-link');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', 'https://rustup.rs');
    });

    it('opens rustup link in a new tab with security attributes', () => {
      render(<InstallationSection />);
      const link = screen.getByTestId('rustup-link');
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('mentions minimum Rust version', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/Rust version 1.70/i)).toBeInTheDocument();
    });
  });

  describe('build from source section', () => {
    it('displays the build from source heading', () => {
      render(<InstallationSection />);
      expect(screen.getByRole('heading', { name: 'Build from Source' })).toBeInTheDocument();
    });

    it('displays clone repository instruction', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/clone the repository/i)).toBeInTheDocument();
    });

    it('displays git clone command in code block', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-clone');
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock).toHaveTextContent('git clone');
      expect(codeBlock).toHaveTextContent('mirdb');
    });

    it('displays cargo build command with copy button', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-build');
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock).toHaveTextContent('cargo build --release');
    });

    it('displays build instruction step numbers', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/1\. Clone the repository/i)).toBeInTheDocument();
      expect(screen.getByText(/2\. Build in release mode/i)).toBeInTheDocument();
    });
  });

  describe('run with config section', () => {
    it('displays the run with configuration heading', () => {
      render(<InstallationSection />);
      expect(screen.getByRole('heading', { name: 'Run with Configuration' })).toBeInTheDocument();
    });

    it('displays configuration example in TOML format', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-config');
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock).toHaveTextContent('mirdb.toml');
      expect(codeBlock).toHaveTextContent('[server]');
      expect(codeBlock).toHaveTextContent('[storage]');
    });

    it('shows run command with config file', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-run');
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock).toHaveTextContent('./target/release/mirdb');
      expect(codeBlock).toHaveTextContent('--config');
    });
  });

  describe('copy button functionality', () => {
    it('has copy button for clone command', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-clone');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]');
      expect(copyButton).toBeInTheDocument();
    });

    it('has copy button for build command', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-build');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]');
      expect(copyButton).toBeInTheDocument();
    });

    it('has copy button for config example', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-config');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]');
      expect(copyButton).toBeInTheDocument();
    });

    it('has copy button for run command', () => {
      render(<InstallationSection />);
      const codeBlock = screen.getByTestId('code-block-run');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]');
      expect(copyButton).toBeInTheDocument();
    });

    it('copies clone command to clipboard when clicked', async () => {
      const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);
      render(<InstallationSection />);

      const codeBlock = screen.getByTestId('code-block-clone');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]') as HTMLElement;

      await act(async () => {
        fireEvent.click(copyButton);
        await Promise.resolve();
      });

      expect(writeTextSpy).toHaveBeenCalledWith(expect.stringContaining('git clone'));
    });

    it('copies build command to clipboard when clicked', async () => {
      const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);
      render(<InstallationSection />);

      const codeBlock = screen.getByTestId('code-block-build');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]') as HTMLElement;

      await act(async () => {
        fireEvent.click(copyButton);
        await Promise.resolve();
      });

      expect(writeTextSpy).toHaveBeenCalledWith('cargo build --release');
    });

    it('copies config example to clipboard when clicked', async () => {
      const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);
      render(<InstallationSection />);

      const codeBlock = screen.getByTestId('code-block-config');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]') as HTMLElement;

      await act(async () => {
        fireEvent.click(copyButton);
        await Promise.resolve();
      });

      expect(writeTextSpy).toHaveBeenCalledWith(expect.stringContaining('[server]'));
      expect(writeTextSpy).toHaveBeenCalledWith(expect.stringContaining('[storage]'));
    });

    it('copies run command to clipboard when clicked', async () => {
      const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);
      render(<InstallationSection />);

      const codeBlock = screen.getByTestId('code-block-run');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]') as HTMLElement;

      await act(async () => {
        fireEvent.click(copyButton);
        await Promise.resolve();
      });

      expect(writeTextSpy).toHaveBeenCalledWith(expect.stringContaining('./target/release/mirdb'));
    });

    it('shows visual feedback after copying', async () => {
      vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);
      render(<InstallationSection />);

      const codeBlock = screen.getByTestId('code-block-build');
      const copyButton = codeBlock.querySelector('[data-testid="copy-button"]') as HTMLElement;

      await act(async () => {
        fireEvent.click(copyButton);
        await Promise.resolve();
      });

      await waitFor(() => {
        expect(codeBlock.querySelector('[data-testid="copy-success"]')).toBeInTheDocument();
      });
    });
  });

  describe('code block language indicators', () => {
    it('shows bash language for shell commands', () => {
      render(<InstallationSection />);
      const cloneBlock = screen.getByTestId('code-block-clone');
      expect(cloneBlock).toHaveTextContent('bash');
    });

    it('shows toml language for config example', () => {
      render(<InstallationSection />);
      const configBlock = screen.getByTestId('code-block-config');
      expect(configBlock).toHaveTextContent('toml');
    });
  });

  describe('quick start note', () => {
    it('displays the quick start success note', () => {
      render(<InstallationSection />);
      const note = screen.getByTestId('quick-start-note');
      expect(note).toBeInTheDocument();
    });

    it('mentions the default port', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/port 11211/i)).toBeInTheDocument();
    });

    it('mentions Memcached-compatible client', () => {
      render(<InstallationSection />);
      expect(screen.getByText(/Memcached-compatible client/i)).toBeInTheDocument();
    });
  });

  describe('accessibility', () => {
    it('has proper heading hierarchy', () => {
      render(<InstallationSection />);
      const headings = screen.getAllByRole('heading', { level: 3 });
      expect(headings.length).toBeGreaterThanOrEqual(3);
    });

    it('external link has accessible icon with aria-hidden', () => {
      render(<InstallationSection />);
      const link = screen.getByTestId('rustup-link');
      const svg = link.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });
});
