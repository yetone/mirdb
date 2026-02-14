import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QuickStart } from '../../../../src/components/sections/QuickStart';

// Mock prismjs
vi.mock('prismjs', () => ({
  default: {
    highlightElement: vi.fn(),
  },
}));

vi.mock('prismjs/components/prism-bash', () => ({}));

describe('QuickStart', () => {
  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('renders Quick Start heading', () => {
    render(<QuickStart />);

    expect(screen.getByRole('heading', { name: /quick start/i, level: 2 })).toBeInTheDocument();
  });

  it('renders section with correct id for navigation', () => {
    const { container } = render(<QuickStart />);

    const section = container.querySelector('#quickstart');
    expect(section).toBeInTheDocument();
  });

  it('displays installation command with copy button', () => {
    render(<QuickStart />);

    // Check for installation-related text
    expect(screen.getByText(/git clone/)).toBeInTheDocument();
    expect(screen.getByText(/cargo build/)).toBeInTheDocument();

    // Should have at least one copy button
    const copyButtons = screen.getAllByRole('button', { name: /copy/i });
    expect(copyButtons.length).toBeGreaterThan(0);
  });

  it('displays system requirements section', () => {
    render(<QuickStart />);

    expect(screen.getByText(/system requirements/i)).toBeInTheDocument();
    expect(screen.getByText(/rust 1\.70/i)).toBeInTheDocument();
  });

  it('shows connection instructions', () => {
    render(<QuickStart />);

    expect(screen.getByText(/telnet localhost 12333/)).toBeInTheDocument();
  });

  it('has multiple code blocks with copy buttons', () => {
    render(<QuickStart />);

    const copyButtons = screen.getAllByRole('button', { name: /copy/i });
    expect(copyButtons.length).toBeGreaterThanOrEqual(2);
  });

  it('includes link to GitHub documentation', () => {
    render(<QuickStart />);

    const docLink = screen.getByRole('link', { name: /view full documentation/i });
    expect(docLink).toBeInTheDocument();
    expect(docLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    expect(docLink).toHaveAttribute('target', '_blank');
  });

  it('displays Step 1 and Step 2 headings', () => {
    render(<QuickStart />);

    expect(screen.getByText(/step 1: installation/i)).toBeInTheDocument();
    expect(screen.getByText(/step 2: connect/i)).toBeInTheDocument();
  });

  it('shows supported platforms', () => {
    render(<QuickStart />);

    expect(screen.getByText(/linux, macos, or windows/i)).toBeInTheDocument();
  });

  it('displays What\'s Next section', () => {
    render(<QuickStart />);

    expect(screen.getByText(/what's next/i)).toBeInTheDocument();
  });
});
