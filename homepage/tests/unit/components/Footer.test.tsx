import React from 'react';
import { render, screen } from '@testing-library/react';
import { Footer } from '@/components/layout/Footer';

// Mock CSS modules
jest.mock('@/components/layout/Footer.module.css', () => ({
  footer: 'footer',
  container: 'container',
  content: 'content',
  brand: 'brand',
  logo: 'logo',
  copyright: 'copyright',
  links: 'links',
  link: 'link',
  externalIcon: 'externalIcon',
}));

jest.mock('@/components/common/Container.module.css', () => ({
  container: 'container',
}));

describe('Footer Component', () => {
  it('renders footer section', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  it('renders MirDB branding', () => {
    render(<Footer />);

    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('renders copyright notice with current year', () => {
    render(<Footer />);

    const currentYear = new Date().getFullYear();
    const copyright = screen.getByText(new RegExp(`${currentYear}.*MirDB.*All rights reserved`));
    expect(copyright).toBeInTheDocument();
  });

  it('renders GitHub link with correct href and attributes', () => {
    render(<Footer />);

    const githubLink = screen.getByTestId('footer-github-link');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(githubLink).toHaveTextContent('GitHub');
  });

  it('renders License link with correct href and attributes', () => {
    render(<Footer />);

    const licenseLink = screen.getByTestId('footer-license-link');
    expect(licenseLink).toBeInTheDocument();
    expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');
    expect(licenseLink).toHaveAttribute('target', '_blank');
    expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(licenseLink).toHaveTextContent('License');
  });

  it('renders Contact/Issues link with correct href and attributes', () => {
    render(<Footer />);

    const issuesLink = screen.getByTestId('footer-issues-link');
    expect(issuesLink).toBeInTheDocument();
    expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    expect(issuesLink).toHaveAttribute('target', '_blank');
    expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(issuesLink).toHaveTextContent('Contact / Issues');
  });

  it('has proper accessibility attributes', () => {
    render(<Footer />);

    // Footer role
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();

    // Navigation with aria-label
    const nav = screen.getByRole('navigation', { name: 'Footer navigation' });
    expect(nav).toBeInTheDocument();
  });

  it('all external links open in new tab', () => {
    render(<Footer />);

    const githubLink = screen.getByTestId('footer-github-link');
    const licenseLink = screen.getByTestId('footer-license-link');
    const issuesLink = screen.getByTestId('footer-issues-link');

    [githubLink, licenseLink, issuesLink].forEach((link) => {
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  it('renders all required links', () => {
    render(<Footer />);

    // Check all three required links are present
    expect(screen.getByTestId('footer-github-link')).toBeInTheDocument();
    expect(screen.getByTestId('footer-license-link')).toBeInTheDocument();
    expect(screen.getByTestId('footer-issues-link')).toBeInTheDocument();
  });
});
