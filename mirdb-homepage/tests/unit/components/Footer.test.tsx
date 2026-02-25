import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Footer } from '../../../src/components/Footer';
import { GITHUB_URL } from '../../../src/utils/constants';

describe('Footer Component', () => {
  let originalDate: typeof Date;
  const mockYear = 2026;

  beforeEach(() => {
    // Mock Date to return consistent year
    originalDate = global.Date;
    const MockDate = vi.fn(() => ({
      getFullYear: () => mockYear,
    })) as unknown as typeof Date;
    MockDate.now = Date.now;
    global.Date = MockDate;
  });

  afterEach(() => {
    global.Date = originalDate;
  });

  // Test Case 1: Component renders with footer content
  it('renders with footer content', () => {
    render(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();

    // Check that Resources section is present
    expect(screen.getByText('Resources')).toBeInTheDocument();
  });

  it('renders footer with proper role', () => {
    render(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('id', 'resources');
  });

  // Test Case 2: GitHub repository link is present with correct href
  it('renders GitHub repository link with correct href', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', GITHUB_URL);
  });

  it('GitHub link opens in new tab', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test Case 3: Documentation link is present
  it('renders Documentation link', () => {
    render(<Footer />);

    const docLink = screen.getByRole('link', { name: /documentation/i });
    expect(docLink).toBeInTheDocument();
    expect(docLink).toHaveAttribute('href', `${GITHUB_URL}#readme`);
  });

  it('Documentation link opens in new tab', () => {
    render(<Footer />);

    const docLink = screen.getByRole('link', { name: /documentation/i });
    expect(docLink).toHaveAttribute('target', '_blank');
    expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test Case 4: License information is displayed
  it('displays license information', () => {
    render(<Footer />);

    expect(screen.getByText('License:')).toBeInTheDocument();
    const licenseLink = screen.getByRole('link', { name: /mit license/i });
    expect(licenseLink).toBeInTheDocument();
  });

  it('license link points to correct URL', () => {
    render(<Footer />);

    const licenseLink = screen.getByRole('link', { name: /mit license/i });
    expect(licenseLink).toHaveAttribute(
      'href',
      `${GITHUB_URL}/blob/master/LICENSE`
    );
    expect(licenseLink).toHaveAttribute('target', '_blank');
    expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test Case 5: Copyright notice with current year is displayed
  it('displays copyright notice with current year', () => {
    render(<Footer />);

    const copyright = screen.getByText(new RegExp(`${mockYear}.*MirDB`, 'i'));
    expect(copyright).toBeInTheDocument();
    expect(copyright.textContent).toContain('All rights reserved');
  });

  it('includes the copyright symbol', () => {
    render(<Footer />);

    const copyright = screen.getByText(/©/);
    expect(copyright).toBeInTheDocument();
  });

  // Additional tests for complete coverage
  it('renders Quick Start link', () => {
    render(<Footer />);

    const quickStartLink = screen.getByRole('link', { name: /quick start/i });
    expect(quickStartLink).toBeInTheDocument();
    expect(quickStartLink).toHaveAttribute('href', `${GITHUB_URL}#usage`);
    expect(quickStartLink).toHaveAttribute('target', '_blank');
  });

  it('renders tagline', () => {
    render(<Footer />);

    expect(
      screen.getByText(/Persistent Key-Value Store with Memcached Protocol/i)
    ).toBeInTheDocument();
  });

  it('renders footer navigation with proper aria-label', () => {
    render(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('renders all resource links in a list', () => {
    render(<Footer />);

    const list = screen.getByRole('list');
    expect(list).toBeInTheDocument();

    const listItems = within(list).getAllByRole('listitem');
    expect(listItems.length).toBe(3); // GitHub, Documentation, Quick Start
  });

  it('all external links have external icon', () => {
    render(<Footer />);

    const links = screen.getAllByRole('link', {
      name: /\(opens in new tab\)/i,
    });
    // All footer links should indicate they open in new tab
    expect(links.length).toBeGreaterThanOrEqual(3);
  });
});

// Test Case 6: Integration test - Click GitHub link opens in new tab
describe('Footer Link Interactions', () => {
  it('GitHub link has correct attributes for opening in new tab', () => {
    render(<Footer />);

    const githubLink = screen.getByRole('link', { name: /github/i });

    // Verify target and rel attributes are set correctly
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    // The href should be the GitHub URL
    expect(githubLink).toHaveAttribute('href', GITHUB_URL);
  });

  it('all external links have proper security attributes', () => {
    render(<Footer />);

    const nav = screen.getByRole('navigation', { name: /footer navigation/i });
    const links = within(nav).getAllByRole('link');

    links.forEach((link) => {
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});

// Test Case 7: Integration test - Responsive footer on mobile
describe('Footer Responsive Behavior', () => {
  it('renders footer with all content on mobile viewport', () => {
    // Set viewport to mobile size
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    window.dispatchEvent(new Event('resize'));

    render(<Footer />);

    // All elements should still be present on mobile
    expect(screen.getByText('Resources')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument();
    expect(
      screen.getByRole('link', { name: /documentation/i })
    ).toBeInTheDocument();
    expect(screen.getByText('License:')).toBeInTheDocument();
    expect(screen.getByText(/©/)).toBeInTheDocument();
  });

  it('footer maintains accessibility on mobile viewport', () => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    window.dispatchEvent(new Event('resize'));

    render(<Footer />);

    // Check that footer has proper contentinfo role
    expect(screen.getByRole('contentinfo')).toBeInTheDocument();

    // Check that navigation is still accessible
    expect(
      screen.getByRole('navigation', { name: /footer navigation/i })
    ).toBeInTheDocument();

    // All links should still be accessible
    const links = screen.getAllByRole('link');
    expect(links.length).toBeGreaterThanOrEqual(4); // GitHub, Documentation, Quick Start, MIT License
  });

  it('footer content is readable with proper semantic structure', () => {
    render(<Footer />);

    // Check heading hierarchy
    const heading = screen.getByRole('heading', { level: 3, name: 'Resources' });
    expect(heading).toBeInTheDocument();

    // Check list structure
    const list = screen.getByRole('list');
    expect(list).toBeInTheDocument();
    expect(within(list).getAllByRole('listitem')).toHaveLength(3);
  });
});

describe('Footer Accessibility', () => {
  it('has proper landmark role', () => {
    render(<Footer />);

    expect(screen.getByRole('contentinfo')).toBeInTheDocument();
  });

  it('all links have accessible names', () => {
    render(<Footer />);

    const links = screen.getAllByRole('link');
    links.forEach((link) => {
      expect(link).toHaveAccessibleName();
    });
  });

  it('external links indicate they open in new tab', () => {
    render(<Footer />);

    const externalLinks = screen.getAllByRole('link', {
      name: /\(opens in new tab\)/,
    });
    expect(externalLinks.length).toBeGreaterThanOrEqual(3);
  });
});
