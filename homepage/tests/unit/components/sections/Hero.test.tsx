/**
 * Hero Section Component Tests
 * Owner: Scenario 3 - Hero Section Display
 *
 * Tests for REQ-2: Hero section with headline and CTA buttons
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Hero } from '../../../../src/components/sections/Hero';

describe('Hero Section', () => {
  it('renders H1 heading with "Persistent Key-Value Store"', () => {
    render(<Hero />);

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Persistent Key-Value Store');
  });

  it('renders subtitle mentioning "Memcached Protocol"', () => {
    render(<Hero />);

    const subtitle = screen.getByText(/memcached protocol/i);
    expect(subtitle).toBeInTheDocument();
  });

  it('renders "Get Started" CTA button', () => {
    render(<Hero />);

    const getStartedButton = screen.getByRole('link', { name: /get started/i });
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveAttribute('href', '#documentation');
  });

  it('renders "GitHub" CTA button with link to repository', () => {
    render(<Hero />);

    const githubButton = screen.getByRole('link', { name: /github/i });
    expect(githubButton).toBeInTheDocument();
    expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    expect(githubButton).toHaveAttribute('target', '_blank');
    expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('has proper accessibility attributes', () => {
    render(<Hero />);

    const section = screen.getByRole('region', { hidden: true }) ||
                   document.querySelector('section[aria-labelledby]');
    expect(section).toHaveAttribute('aria-labelledby', 'hero-heading');

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toHaveAttribute('id', 'hero-heading');
  });
});
