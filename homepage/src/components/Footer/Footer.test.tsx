/**
 * Footer Component Unit Tests
 * Owner: Scenario 17 - Footer and Supplementary Content
 *
 * Tests:
 * - Footer renders GitHub repository link
 * - Footer renders license information
 * - Footer renders copyright/attribution
 * - All links have proper security attributes
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Footer } from './Footer';
import { GITHUB_URL, PRODUCT_NAME } from '../../utils/constants';

describe('Footer', () => {
  it('renders the footer element', () => {
    render(<Footer />);
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('id', 'footer');
  });

  it('renders GitHub repository link', () => {
    render(<Footer />);
    const githubLink = screen.getByRole('link', { name: /github repository/i });
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', GITHUB_URL);
  });

  it('renders license information link', () => {
    render(<Footer />);
    const licenseLink = screen.getByRole('link', { name: /mit license/i });
    expect(licenseLink).toBeInTheDocument();
    expect(licenseLink).toHaveAttribute('href', `${GITHUB_URL}/blob/master/LICENSE`);
  });

  it('renders copyright attribution with product name', () => {
    render(<Footer />);
    const currentYear = new Date().getFullYear();
    const attribution = screen.getByText(new RegExp(`© ${currentYear} ${PRODUCT_NAME}`));
    expect(attribution).toBeInTheDocument();
  });

  it('GitHub link has proper security attributes', () => {
    render(<Footer />);
    const githubLink = screen.getByRole('link', { name: /github repository/i });
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('license link has proper security attributes', () => {
    render(<Footer />);
    const licenseLink = screen.getByRole('link', { name: /mit license/i });
    expect(licenseLink).toHaveAttribute('target', '_blank');
    expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('renders all required footer elements together', () => {
    render(<Footer />);

    // Check GitHub link
    expect(screen.getByRole('link', { name: /github repository/i })).toBeInTheDocument();

    // Check license link
    expect(screen.getByRole('link', { name: /mit license/i })).toBeInTheDocument();

    // Check attribution
    const currentYear = new Date().getFullYear();
    expect(screen.getByText(new RegExp(`© ${currentYear} ${PRODUCT_NAME}`))).toBeInTheDocument();
  });

  it('footer has proper accessibility role', () => {
    render(<Footer />);
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });
});
