/**
 * Footer Component Unit Tests.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Tests:
 * - Footer component accepts and displays author, license props
 * - Default values are rendered correctly
 * - GitHub link is present and functional
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Footer } from '../../../src/components/layout/Footer';

describe('Footer', () => {
  it('renders footer element', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer.tagName).toBe('FOOTER');
  });

  it('displays default author name', () => {
    render(<Footer />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent('yetone');
  });

  it('displays custom author name when provided', () => {
    render(<Footer author="Custom Author" />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent('Custom Author');
  });

  it('displays default MIT license', () => {
    render(<Footer />);

    const license = screen.getByTestId('footer-license');
    expect(license).toHaveTextContent('MIT');
  });

  it('displays custom license when provided', () => {
    render(<Footer license="Apache 2.0" />);

    const license = screen.getByTestId('footer-license');
    expect(license).toHaveTextContent('Apache 2.0');
  });

  it('displays copyright notice with year', () => {
    render(<Footer year={2024} />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent('2024');
    expect(copyright).toHaveTextContent('©');
  });

  it('displays current year by default', () => {
    const currentYear = new Date().getFullYear();
    render(<Footer />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent(currentYear.toString());
  });

  it('displays Open Source reference', () => {
    render(<Footer />);

    const openSource = screen.getByTestId('footer-open-source');
    expect(openSource).toHaveTextContent('Open Source');
  });

  it('renders GitHub link with correct attributes', () => {
    render(<Footer />);

    const githubLink = screen.getByTestId('footer-github-link');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('GitHub link has accessible aria-label', () => {
    render(<Footer />);

    const githubLink = screen.getByTestId('footer-github-link');
    expect(githubLink).toHaveAttribute('aria-label', 'View MirDB on GitHub');
  });

  it('accepts all custom props together', () => {
    render(<Footer author="Test Author" license="GPL-3.0" year={2023} />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent('Test Author');
    expect(copyright).toHaveTextContent('2023');

    const license = screen.getByTestId('footer-license');
    expect(license).toHaveTextContent('GPL-3.0');
  });

  it('contains proper semantic HTML structure', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();

    // Verify there are text elements for copyright and license
    expect(screen.getByTestId('footer-copyright').tagName).toBe('P');
    expect(screen.getByTestId('footer-license').tagName).toBe('P');
  });
});
