/**
 * Navbar Component Unit Tests
 * Owner: Scenario 2 - Navbar & Logo Display
 *
 * Tests for:
 * - Logo display with proper alt text
 * - Product name display
 * - Navigation links (GitHub, Documentation, API Reference)
 * - Theme toggle button
 * - External link security attributes
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { Navbar } from '../../../../src/components/layout/Navbar';
import { GITHUB_URL, NAV_LINKS } from '../../../../src/utils/constants';

describe('Navbar Component', () => {
  // Test Case 1: MirDB logo image is rendered with proper alt text
  it('renders MirDB logo image with proper alt text', () => {
    render(<Navbar />);

    const logo = screen.getByTestId('navbar-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('alt', 'MirDB Logo');
    expect(logo).toHaveAttribute('src', '/logo.gif');
  });

  // Test Case 2: Product name 'MirDB' text is displayed
  it('displays MirDB product name text', () => {
    render(<Navbar />);

    const brandName = screen.getByTestId('navbar-brand');
    expect(brandName).toBeInTheDocument();
    expect(brandName).toHaveTextContent('MirDB');
  });

  // Test Case 3: GitHub link is present with href to repository
  it('renders GitHub link with correct href to repository', () => {
    render(<Navbar />);

    const githubLink = screen.getByTestId('nav-link-github');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', GITHUB_URL);
    expect(githubLink).toHaveTextContent('GitHub');
  });

  // Test Case 4: Documentation link is present
  it('renders Documentation link', () => {
    render(<Navbar />);

    const docsLink = screen.getByTestId('nav-link-documentation');
    expect(docsLink).toBeInTheDocument();
    expect(docsLink).toHaveTextContent('Documentation');
    expect(docsLink).toHaveAttribute('href', '#documentation');
  });

  // Test Case 5: Theme toggle button is rendered
  it('renders theme toggle button', () => {
    render(<Navbar />);

    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();
    expect(themeToggle).toHaveAttribute('aria-label');
  });

  // Test Case 6: GitHub link has target='_blank' and rel='noopener noreferrer' attributes
  it('GitHub link has secure external link attributes', () => {
    render(<Navbar />);

    const githubLink = screen.getByTestId('nav-link-github');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Additional tests for completeness
  it('renders API Reference link', () => {
    render(<Navbar />);

    const apiLink = screen.getByTestId('nav-link-api-reference');
    expect(apiLink).toBeInTheDocument();
    expect(apiLink).toHaveTextContent('API Reference');
  });

  it('renders all navigation links from constants', () => {
    render(<Navbar />);

    NAV_LINKS.forEach((link) => {
      const testId = `nav-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`;
      const navLink = screen.getByTestId(testId);
      expect(navLink).toBeInTheDocument();
      expect(navLink).toHaveTextContent(link.label);
    });
  });

  it('calls onThemeToggle when theme toggle button is clicked', () => {
    const mockToggle = vi.fn();
    render(<Navbar onThemeToggle={mockToggle} />);

    const themeToggle = screen.getByTestId('theme-toggle');
    fireEvent.click(themeToggle);

    expect(mockToggle).toHaveBeenCalledTimes(1);
  });

  it('shows correct aria-label based on isDarkMode prop', () => {
    const { rerender } = render(<Navbar isDarkMode={false} />);

    let themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark mode');

    rerender(<Navbar isDarkMode={true} />);
    themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light mode');
  });

  it('has proper navigation role and aria-label', () => {
    render(<Navbar />);

    const nav = screen.getByRole('navigation');
    expect(nav).toBeInTheDocument();
    expect(nav).toHaveAttribute('aria-label', 'Main navigation');
  });

  it('renders mobile menu button on small screens', () => {
    render(<Navbar />);

    const mobileMenuButton = screen.getByTestId('mobile-menu-button');
    expect(mobileMenuButton).toBeInTheDocument();
    expect(mobileMenuButton).toHaveAttribute('aria-label', 'Toggle mobile menu');
  });

  it('toggles mobile menu when mobile menu button is clicked', () => {
    render(<Navbar />);

    const mobileMenuButton = screen.getByTestId('mobile-menu-button');

    // Initially, mobile menu should not be visible
    expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();

    // Click to open
    fireEvent.click(mobileMenuButton);
    expect(screen.getByTestId('mobile-menu')).toBeInTheDocument();

    // Click to close
    fireEvent.click(mobileMenuButton);
    expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument();
  });

  it('home link has proper aria-label', () => {
    render(<Navbar />);

    const homeLink = screen.getByLabelText('MirDB Home');
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });
});
