import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QuickStart } from '@/components/sections/QuickStart';

describe('QuickStart', () => {
  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('should render the Quick Start section', () => {
    render(<QuickStart />);

    expect(screen.getByRole('region', { name: /quick start/i })).toBeInTheDocument();
  });

  it('should render with code block elements', () => {
    render(<QuickStart />);

    const codeBlocks = screen.getAllByRole('code');
    expect(codeBlocks.length).toBeGreaterThan(0);
  });

  it('should include set operation code example', () => {
    render(<QuickStart />);

    // Prism.js tokenizes the code, so we check the pre element's textContent
    const preElements = document.querySelectorAll('pre');
    const hasSetCommand = Array.from(preElements).some(pre => {
      const text = pre.textContent || '';
      return /\bset\b/i.test(text);
    });
    expect(hasSetCommand).toBe(true);
  });

  it('should include get operation code example', () => {
    render(<QuickStart />);

    // Prism.js tokenizes the code, so we check the pre element's textContent
    const preElements = document.querySelectorAll('pre');
    const hasGetCommand = Array.from(preElements).some(pre => {
      const text = pre.textContent || '';
      return /\bget\b/i.test(text);
    });
    expect(hasGetCommand).toBe(true);
  });

  it('should have syntax highlighting CSS classes applied to code blocks', () => {
    render(<QuickStart />);

    const codeElements = screen.getAllByRole('code');
    codeElements.forEach(codeEl => {
      expect(codeEl.className).toMatch(/language-/);
    });
  });

  it('should have copy button on each code block', () => {
    render(<QuickStart />);

    const copyButtons = screen.getAllByRole('button', { name: /copy/i });
    const codeBlocks = screen.getAllByRole('code');

    expect(copyButtons.length).toBeGreaterThanOrEqual(codeBlocks.length);
  });

  it('should have accessible section with proper heading', () => {
    render(<QuickStart />);

    const heading = screen.getByRole('heading', { name: /quick start/i });
    expect(heading).toBeInTheDocument();
  });

  it('should have an id for navigation', () => {
    render(<QuickStart />);

    const section = screen.getByRole('region', { name: /quick start/i });
    expect(section).toHaveAttribute('id', 'quick-start');
  });

  it('should display memcached command syntax examples', () => {
    render(<QuickStart />);

    // Check for memcached-style commands
    const preElements = document.querySelectorAll('pre');
    const hasMemcachedSyntax = Array.from(preElements).some(pre => {
      const text = pre.textContent || '';
      return text.includes('set') && text.includes('get');
    });

    expect(hasMemcachedSyntax).toBe(true);
  });

  it('should provide context about connecting to MirDB', () => {
    render(<QuickStart />);

    // Should have some instructional text about getting started
    const elements = screen.getAllByText(/connect|start|telnet|client/i);
    expect(elements.length).toBeGreaterThan(0);
  });
});
