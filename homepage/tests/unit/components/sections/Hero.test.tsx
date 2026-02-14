import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Hero } from '../../../../src/components/sections/Hero';
import { GITHUB_URL, TAGLINE } from '../../../../src/utils/constants';

describe('Hero Component', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    const { container } = render(<Hero />);
    expect(container).toBeDefined();
    expect(container.querySelector('section')).toBeInTheDocument();
  });

  // Test Case 2: Logo image is present with alt text 'MirDB Logo'
  it('displays the MirDB logo with correct alt text', () => {
    render(<Hero />);
    const logo = screen.getByAltText('MirDB Logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('src', '/logo.gif');
  });

  // Test Case 3: Tagline text is visible
  it('displays the tagline "Persistent Key-Value Storage with Memcached Protocol"', () => {
    render(<Hero />);
    const tagline = screen.getByText(TAGLINE);
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('Persistent Key-Value Storage with Memcached Protocol');
  });

  // Test Case 4: 'Get Started' button is present with correct href
  it('displays "Get Started" button linking to Quick Start section', () => {
    render(<Hero />);
    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveAttribute('href', '#quickstart');
  });

  // Test Case 5: 'View on GitHub' button is present with correct href
  it('displays "View on GitHub" button linking to GitHub repository', () => {
    render(<Hero />);
    const githubButton = screen.getByRole('button', { name: /view on github/i });
    expect(githubButton).toBeInTheDocument();
    expect(githubButton).toHaveAttribute('href', GITHUB_URL);
  });

  // Test Case 6: H1 element contains project name for SEO
  it('has an H1 element containing the project name "MirDB"', () => {
    render(<Hero />);
    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('MirDB');
  });

  // Additional test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<Hero />);
    const section = screen.getByRole('region', { name: /mirdb/i });
    expect(section).toBeInTheDocument();
  });
});
