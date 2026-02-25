import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Header } from '../../../src/components/Header';
import { NAVIGATION_LINKS } from '../../../src/utils/constants';

describe('Header Component', () => {
  // Test Case 1: Component renders with MirDB logo
  it('renders with MirDB logo', () => {
    render(<Header />);

    const logo = screen.getByRole('img', { name: /mirdb logo/i });
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('src', '/assets/logo.gif');
  });

  it('renders MirDB text in logo', () => {
    render(<Header />);

    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('logo links to home page', () => {
    render(<Header />);

    const logoLink = screen.getByRole('link', { name: /mirdb home/i });
    expect(logoLink).toHaveAttribute('href', '/');
  });

  // Test Case 2: Navigation links for Features, Usage, Architecture, Resources are present
  it('renders all navigation links', () => {
    render(<Header />);

    NAVIGATION_LINKS.forEach((link) => {
      const navLink = screen.getByRole('link', { name: link.label });
      expect(navLink).toBeInTheDocument();
      expect(navLink).toHaveAttribute('href', link.href);
    });
  });

  it('renders Features navigation link', () => {
    render(<Header />);

    const link = screen.getByRole('link', { name: 'Features' });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '#features');
  });

  it('renders Usage navigation link', () => {
    render(<Header />);

    const link = screen.getByRole('link', { name: 'Usage' });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '#usage');
  });

  it('renders Architecture navigation link', () => {
    render(<Header />);

    const link = screen.getByRole('link', { name: 'Architecture' });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '#architecture');
  });

  it('renders Resources navigation link', () => {
    render(<Header />);

    const link = screen.getByRole('link', { name: 'Resources' });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '#resources');
  });

  // Test Case 3: Theme toggle component slot is available
  it('renders theme toggle slot when themeToggle prop is provided', () => {
    const mockThemeToggle = <button data-testid="mock-theme-toggle">Toggle Theme</button>;

    render(<Header themeToggle={mockThemeToggle} />);

    expect(screen.getByTestId('mock-theme-toggle')).toBeInTheDocument();
    expect(screen.getByTestId('theme-toggle-slot')).toBeInTheDocument();
  });

  it('does not render theme toggle slot when themeToggle prop is not provided', () => {
    render(<Header />);

    expect(screen.queryByTestId('theme-toggle-slot')).not.toBeInTheDocument();
  });

  // Header structure and accessibility tests
  it('has banner role for the header', () => {
    render(<Header />);

    expect(screen.getByRole('banner')).toBeInTheDocument();
  });

  it('has navigation landmark with proper label', () => {
    render(<Header />);

    const nav = screen.getByRole('navigation', { name: /main navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('renders GitHub link with proper attributes', () => {
    render(<Header />);

    const githubLink = screen.getByRole('link', { name: /view mirdb on github/i });
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Mobile menu tests
  it('renders mobile menu button', () => {
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /navigation menu/i });
    expect(menuButton).toBeInTheDocument();
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
  });

  it('toggles mobile menu on button click', async () => {
    const user = userEvent.setup();
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /open navigation menu/i });

    // Initially closed
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');

    // Open menu
    await user.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'true');

    // Close menu
    await user.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
  });
});

describe('Header Navigation Links', () => {
  it('all navigation links are within a list', () => {
    render(<Header />);

    const list = screen.getByRole('list');
    expect(list).toBeInTheDocument();

    const listItems = screen.getAllByRole('listitem');
    expect(listItems).toHaveLength(NAVIGATION_LINKS.length);
  });

  it('navigation links have focus-visible styles available', () => {
    render(<Header />);

    const links = screen.getAllByRole('link').filter(
      (link) => link.getAttribute('href')?.startsWith('#')
    );

    // All anchor navigation links should be present
    expect(links.length).toBe(NAVIGATION_LINKS.length);
  });
});
