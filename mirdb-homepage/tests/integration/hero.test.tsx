import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import Hero from '../../src/components/Hero';

describe('Hero Section Integration', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('navigates to Quick Start section hash when "Get Started" is clicked', async () => {
    const user = userEvent.setup();

    const quickStart = document.createElement('section');
    quickStart.id = 'quick-start';
    document.body.appendChild(quickStart);

    render(<Hero />);

    const getStartedLink = screen.getByRole('link', { name: /get started/i });
    expect(getStartedLink).toHaveAttribute('href', '#quick-start');
    await user.click(getStartedLink);

    expect(window.location.hash).toBe('#quick-start');

    document.body.removeChild(quickStart);
  });

  it('uses anchor navigation for "Get Started" without JavaScript', () => {
    render(<Hero />);
    const getStartedLink = screen.getByRole('link', { name: /get started/i });
    expect(getStartedLink).toHaveAttribute('href', '#quick-start');
    expect(getStartedLink.tagName).toBe('A');
  });

  it('renders "View on GitHub" as an anchor element for no-JS compatibility', () => {
    render(<Hero />);
    const ghLink = screen.getByRole('link', { name: /view on github/i });
    expect(ghLink.tagName).toBe('A');
    expect(ghLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  it('renders all hero content as static HTML for no-JS compatibility', () => {
    render(<Hero />);

    expect(screen.getByRole('heading', { name: 'MirDB' })).toBeInTheDocument();
    expect(
      screen.getByText('A Persistent Key-Value Store with Memcached Protocol')
    ).toBeInTheDocument();
    expect(screen.getByAltText('MirDB logo')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /view on github/i })).toBeInTheDocument();
  });
});
