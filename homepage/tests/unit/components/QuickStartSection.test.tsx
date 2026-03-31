import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QuickStartSection } from '../../../src/components/sections/QuickStartSection';

describe('QuickStartSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('should render the quick start section', () => {
    render(<QuickStartSection />);

    expect(screen.getByTestId('quick-start-section')).toBeInTheDocument();
  });

  it('should display Quick Start heading', () => {
    render(<QuickStartSection />);

    expect(screen.getByRole('heading', { name: /quick start/i })).toBeInTheDocument();
  });

  it('should display installation code blocks', () => {
    render(<QuickStartSection />);

    const codeBlocks = screen.getAllByTestId('code-block');
    expect(codeBlocks.length).toBeGreaterThan(0);
  });

  it('should include cargo/rust installation instructions', () => {
    render(<QuickStartSection />);

    expect(screen.getByText(/cargo build/i)).toBeInTheDocument();
  });

  it('should include basic memcached commands', () => {
    render(<QuickStartSection />);

    expect(screen.getByText(/set mykey/i)).toBeInTheDocument();
    expect(screen.getByText(/get mykey/i)).toBeInTheDocument();
  });

  it('should include link to full documentation', () => {
    render(<QuickStartSection />);

    const docsLink = screen.getByTestId('docs-link');
    expect(docsLink).toBeInTheDocument();
    expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
  });

  it('should have documentation link open in new tab', () => {
    render(<QuickStartSection />);

    const docsLink = screen.getByTestId('docs-link');
    expect(docsLink).toHaveAttribute('target', '_blank');
    expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('should display Installation section heading', () => {
    render(<QuickStartSection />);

    expect(screen.getByRole('heading', { name: /installation/i })).toBeInTheDocument();
  });

  it('should display Basic Usage section heading', () => {
    render(<QuickStartSection />);

    expect(screen.getByRole('heading', { name: /basic usage/i })).toBeInTheDocument();
  });

  it('should display Rust Client Example section heading', () => {
    render(<QuickStartSection />);

    expect(screen.getByRole('heading', { name: /rust client example/i })).toBeInTheDocument();
  });

  it('should have proper section id for navigation', () => {
    render(<QuickStartSection />);

    const section = screen.getByTestId('quick-start-section');
    expect(section).toHaveAttribute('id', 'quick-start');
  });

  it('should include git clone command', () => {
    render(<QuickStartSection />);

    expect(screen.getByText(/git clone/i)).toBeInTheDocument();
  });

  it('should include MirDB repository URL in clone command', () => {
    render(<QuickStartSection />);

    expect(screen.getByText(/github.com\/yetone\/mirdb/i)).toBeInTheDocument();
  });

  it('should render copy buttons on all code blocks', () => {
    render(<QuickStartSection />);

    const copyButtons = screen.getAllByTestId('copy-button');
    const codeBlocks = screen.getAllByTestId('code-block');

    expect(copyButtons.length).toBe(codeBlocks.length);
  });
});
