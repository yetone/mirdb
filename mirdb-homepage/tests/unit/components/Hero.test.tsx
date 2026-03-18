/**
 * Hero Component Unit Tests.
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Tests:
 * - Semantic HTML structure verification
 * - Component renders correctly
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Hero } from '../../../src/components/sections/Hero';

describe('Hero Component', () => {
  it('TC4: renders with proper semantic HTML elements (header, h1, p)', () => {
    const { container } = render(<Hero />);

    // Verify header element is used
    const header = container.querySelector('header');
    expect(header).toBeTruthy();
    expect(header?.id).toBe('hero');

    // Verify h1 element exists
    const h1 = container.querySelector('h1');
    expect(h1).toBeTruthy();
    expect(h1?.textContent).toContain('MirDB');

    // Verify paragraph elements exist for description
    const paragraphs = container.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(1);
  });

  it('renders the MirDB logo with appropriate alt text', () => {
    render(<Hero />);

    const logo = screen.getByAltText('MirDB Logo');
    expect(logo).toBeTruthy();
    expect(logo.getAttribute('src')).toBe('/assets/logo.svg');
  });

  it('displays the MirDB product name in h1', () => {
    render(<Hero />);

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading.textContent).toBe('MirDB');
  });

  it('includes tagline mentioning Persistent Key-Value Store and Memcached Protocol', () => {
    const { container } = render(<Hero />);

    const textContent = container.textContent;
    expect(textContent).toContain('Persistent Key-Value Store');
    expect(textContent).toContain('Memcached Protocol');
  });

  it('contains CTA buttons', () => {
    render(<Hero />);

    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    const githubButton = screen.getByRole('button', { name: /view on github/i });

    expect(getStartedButton).toBeTruthy();
    expect(githubButton).toBeTruthy();
  });
});
