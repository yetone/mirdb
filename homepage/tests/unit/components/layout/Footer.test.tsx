/**
 * Footer Component Unit Tests
 * Owner: Scenario 7 - Footer Display
 *
 * Tests for:
 * - Copyright text with current year
 * - MirDB name/logo presence
 * - GitHub repository link
 * - Proper security attributes on external links
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Footer } from '../../../../src/components/layout/Footer';
import { GITHUB_URL } from '../../../../src/utils/constants';

describe('Footer Component', () => {
  const currentYear = new Date().getFullYear();

  // Test Case 1: Copyright text is displayed with current year
  it('displays copyright text with current year', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveTextContent('©');
    expect(footer).toHaveTextContent(currentYear.toString());
    expect(footer).toHaveTextContent('MirDB');
  });

  // Test Case 2: MirDB name or logo is present in footer
  it('displays MirDB name in footer', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toHaveTextContent('MirDB');
  });

  // Test Case 3: GitHub repository link is present
  it('renders GitHub repository link', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', GITHUB_URL);
  });

  // Test Case 4: All footer links have proper attributes for external links
  it('has proper security attributes on external links', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Additional tests for completeness
  it('has footer semantic element', () => {
    render(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('displays all rights reserved text', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toHaveTextContent('All rights reserved');
  });

  it('renders with correct data-testid attribute', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  it('GitHub link text is visible', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toHaveTextContent('GitHub');
  });
});
