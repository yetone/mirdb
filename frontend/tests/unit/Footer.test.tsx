/**
 * Footer Unit Tests
 * Owner: Scenario 12 - Footer and Additional Content
 *
 * Tests for footer rendering and content.
 */
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import Footer from '../../src/components/Home/Footer';

describe('Footer', () => {
  it('renders footer element with semantic footer tag', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  it('renders footer with accessible landmark role', () => {
    render(<Footer />);
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('renders copyright text containing current year', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    const currentYear = new Date().getFullYear().toString();
    expect(footer.textContent).toContain(currentYear);
  });

  it('renders copyright text containing product name', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer.textContent).toMatch(/URL Shortener/i);
  });

  it('renders copyright text with All rights reserved', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer.textContent).toMatch(/All rights reserved/i);
  });

  it('renders Terms of Service link', () => {
    render(<Footer />);
    const termsLink = screen.getByText('Terms of Service');
    expect(termsLink).toBeInTheDocument();
    expect(termsLink.tagName.toLowerCase()).toBe('a');
  });

  it('renders Privacy Policy link', () => {
    render(<Footer />);
    const privacyLink = screen.getByText('Privacy Policy');
    expect(privacyLink).toBeInTheDocument();
    expect(privacyLink.tagName.toLowerCase()).toBe('a');
  });

  it('footer links have placeholder or valid href attributes', () => {
    render(<Footer />);
    const termsLink = screen.getByText('Terms of Service').closest('a');
    const privacyLink = screen.getByText('Privacy Policy').closest('a');
    expect(termsLink).toHaveAttribute('href');
    expect(privacyLink).toHaveAttribute('href');
    const termsHref = termsLink?.getAttribute('href') ?? '';
    const privacyHref = privacyLink?.getAttribute('href') ?? '';
    expect(termsHref.length).toBeGreaterThan(0);
    expect(privacyHref.length).toBeGreaterThan(0);
  });

  it('footer contains accessible navigation with aria-label', () => {
    render(<Footer />);
    const nav = screen.getByLabelText('Footer navigation');
    expect(nav).toBeInTheDocument();
    expect(nav.tagName.toLowerCase()).toBe('nav');
  });

  it('renders footer links inside the navigation landmark', () => {
    render(<Footer />);
    const nav = screen.getByLabelText('Footer navigation');
    const termsLink = within(nav).getByText('Terms of Service');
    const privacyLink = within(nav).getByText('Privacy Policy');
    expect(termsLink).toBeInTheDocument();
    expect(privacyLink).toBeInTheDocument();
  });

  it('footer uses responsive layout classes', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer.className).toMatch(/px-4/);
    expect(footer.className).toMatch(/sm:px-6/);
    expect(footer.className).toMatch(/lg:px-8/);
  });

  it('footer links have minimum touch target size', () => {
    render(<Footer />);
    const links = screen.getAllByRole('link', { hidden: true });
    links.forEach((link) => {
      expect(link.className).toMatch(/min-h-\[44px\]/);
    });
  });

  it('footer applies theme-aware border styling with dark mode support', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer.className).toMatch(/border-gray-200/);
    expect(footer.className).toMatch(/dark:border-gray-700/);
  });

  it('footer uses opacity utilities for consistent text styling', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    const copyright = footer.querySelector('p');
    expect(copyright).not.toBeNull();
    expect(copyright!.className).toMatch(/opacity-70/);
  });

  it('footer links have hover transition effects', () => {
    render(<Footer />);
    const links = screen.getAllByRole('link', { hidden: true });
    links.forEach((link) => {
      expect(link.className).toMatch(/hover:opacity-100/);
      expect(link.className).toMatch(/transition-opacity/);
    });
  });

  it('footer maintains consistent max-width container', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    const innerContainer = footer.querySelector('.max-w-7xl');
    expect(innerContainer).not.toBeNull();
    expect(innerContainer!.className).toMatch(/max-w-7xl/);
  });

  it('footer content is organized in a flex layout', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    const innerContainer = footer.querySelector('.max-w-7xl');
    expect(innerContainer).not.toBeNull();
    expect(innerContainer!.className).toMatch(/flex/);
  });

  it('footer copyright text uses small font size', () => {
    render(<Footer />);
    const footer = screen.getByTestId('footer');
    const copyright = footer.querySelector('p');
    expect(copyright).not.toBeNull();
    expect(copyright!.className).toMatch(/text-sm/);
  });
});
