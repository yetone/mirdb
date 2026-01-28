/**
 * Footer Unit Tests
 * Scenario 5 - Footer Section
 *
 * Tests for the Footer component verifying:
 * - Component renders without errors
 * - Copyright text is displayed
 * - Terms of Service link exists
 * - Privacy Policy link exists
 * - Footer is positioned at bottom with proper semantic role
 *
 * Requirements: REQ-9
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { Footer } from '../../../src/components/homepage/Footer';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    footer: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <footer {...props}>{children}</footer>
    ),
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
  },
}));

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('Footer', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    const { container } = renderWithRouter(<Footer />);
    expect(container).toBeInTheDocument();
    expect(screen.getByTestId('footer-section')).toBeInTheDocument();
  });

  // Test Case 2: Check for copyright text
  it('displays copyright text with copyright symbol or Copyright word', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer-section');
    // Check for copyright symbol (©) or the word 'Copyright'
    expect(footer.textContent).toMatch(/©|Copyright/i);
  });

  // Test Case 3: Check for Terms of Service link
  it('displays Terms of Service link', () => {
    renderWithRouter(<Footer />);

    // Find link with text containing 'Terms' or 'Terms of Service'
    const termsLink = screen.getByRole('link', { name: /Terms/i });
    expect(termsLink).toBeInTheDocument();
    expect(termsLink).toHaveAttribute('href');
  });

  // Test Case 4: Check for Privacy Policy link
  it('displays Privacy Policy link', () => {
    renderWithRouter(<Footer />);

    // Find link with text containing 'Privacy' or 'Privacy Policy'
    const privacyLink = screen.getByRole('link', { name: /Privacy/i });
    expect(privacyLink).toBeInTheDocument();
    expect(privacyLink).toHaveAttribute('href');
  });

  // Test Case 5: Check footer is positioned at bottom with appropriate semantic role
  it('has appropriate semantic role and positioning for footer', () => {
    renderWithRouter(<Footer />);

    // Footer should use the <footer> HTML element which has implicit role of contentinfo
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('data-testid', 'footer-section');
  });

  // Additional test: Footer contains current year in copyright
  it('displays current year in copyright notice', () => {
    renderWithRouter(<Footer />);

    const currentYear = new Date().getFullYear().toString();
    const footer = screen.getByTestId('footer-section');
    expect(footer.textContent).toContain(currentYear);
  });

  // Additional test: Custom links can be passed as props
  it('renders with custom links when provided', () => {
    const customLinks = [
      { label: 'Custom Terms', href: '/custom-terms' },
      { label: 'Custom Privacy', href: '/custom-privacy' },
    ];
    const customCopyright = 'Custom Copyright Text';

    renderWithRouter(<Footer links={customLinks} copyright={customCopyright} />);

    expect(screen.getByRole('link', { name: 'Custom Terms' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Custom Privacy' })).toBeInTheDocument();
    expect(screen.getByTestId('footer-section').textContent).toContain(customCopyright);
  });

  // Additional test: Footer has proper accessibility structure
  it('has proper accessibility structure', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer-section');
    // Footer should have semantic footer element
    expect(footer.tagName).toBe('FOOTER');

    // Links should be accessible
    const links = screen.getAllByRole('link');
    expect(links.length).toBeGreaterThanOrEqual(2); // At least Terms and Privacy
  });

  // Additional test: Default links include Terms and Privacy
  it('includes default Terms and Privacy links when no props provided', () => {
    renderWithRouter(<Footer />);

    const termsLink = screen.getByRole('link', { name: /Terms/i });
    const privacyLink = screen.getByRole('link', { name: /Privacy/i });

    expect(termsLink).toHaveAttribute('href', expect.stringContaining('terms'));
    expect(privacyLink).toHaveAttribute('href', expect.stringContaining('privacy'));
  });
});
