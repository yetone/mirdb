import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import Hero from '../../src/components/Hero';

describe('Hero Section', () => {
  it('renders the product name "MirDB"', () => {
    render(<Hero />);
    expect(screen.getByRole('heading', { name: 'MirDB', level: 1 })).toBeInTheDocument();
  });

  it('renders the tagline text', () => {
    render(<Hero />);
    expect(
      screen.getByText('A Persistent Key-Value Store with Memcached Protocol')
    ).toBeInTheDocument();
  });

  it('renders the logo image with alt text', () => {
    render(<Hero />);
    const logo = screen.getByAltText('MirDB logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('src', '/assets/logo.gif');
  });

  it('renders the logo with appropriate dimensions', () => {
    render(<Hero />);
    const logo = screen.getByAltText('MirDB logo');
    expect(logo).toHaveAttribute('width', '112');
    expect(logo).toHaveAttribute('height', '112');
  });

  it('renders the hero section as a landmark region', () => {
    render(<Hero />);
    const section = screen.getByRole('region', { name: 'Hero section' });
    expect(section).toBeInTheDocument();
  });

  it('renders the primary CTA "Get Started" linking to Quick Start', () => {
    render(<Hero />);
    const cta = screen.getByRole('link', { name: /get started/i });
    expect(cta).toBeInTheDocument();
    expect(cta).toHaveAttribute('href', '#quick-start');
  });

  it('renders the secondary CTA "View on GitHub" with correct link', () => {
    render(<Hero />);
    const cta = screen.getByRole('link', { name: /view on github/i });
    expect(cta).toBeInTheDocument();
    expect(cta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  it('has the "View on GitHub" link open in a new tab securely', () => {
    render(<Hero />);
    const cta = screen.getByRole('link', { name: /view on github/i });
    expect(cta).toHaveAttribute('target', '_blank');
    expect(cta).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
