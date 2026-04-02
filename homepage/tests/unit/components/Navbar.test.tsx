import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { Navbar } from '@/components/layout/Navbar';

// Mock CSS modules
jest.mock('@/components/layout/Navbar.module.css', () => ({
  header: 'header',
  nav: 'nav',
  container: 'container',
  logo: 'logo',
  logoText: 'logoText',
  navList: 'navList',
  navLink: 'navLink',
  externalIcon: 'externalIcon',
  menuButton: 'menuButton',
  hamburger: 'hamburger',
  open: 'open',
}));

jest.mock('@/components/layout/MobileMenu.module.css', () => ({
  backdrop: 'backdrop',
  visible: 'visible',
  menu: 'menu',
  open: 'open',
  navList: 'navList',
  navLink: 'navLink',
  externalIcon: 'externalIcon',
}));

jest.mock('@/components/common/Container.module.css', () => ({
  container: 'container',
}));

describe('Navbar Component', () => {
  beforeEach(() => {
    // Reset scroll position
    window.scrollTo = jest.fn();
  });

  it('renders navigation bar with logo and links', () => {
    render(<Navbar />);

    // Check logo is present
    expect(screen.getByLabelText('MirDB Home')).toBeInTheDocument();
    expect(screen.getByText('MirDB')).toBeInTheDocument();

    // Check navigation links are present
    expect(screen.getByTestId('nav-link-home')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-features')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-docs')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-github')).toBeInTheDocument();
  });

  it('renders all required navigation items', () => {
    render(<Navbar />);

    const navLinks = ['Home', 'Features', 'Docs', 'GitHub'];
    navLinks.forEach((linkText) => {
      const link = screen.getByTestId(`nav-link-${linkText.toLowerCase()}`);
      expect(link).toHaveTextContent(linkText);
    });
  });

  it('GitHub link has target="_blank" and rel="noopener noreferrer"', () => {
    render(<Navbar />);

    const githubLink = screen.getByTestId('nav-link-github');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  it('internal links do not have target="_blank"', () => {
    render(<Navbar />);

    const homeLink = screen.getByTestId('nav-link-home');
    const featuresLink = screen.getByTestId('nav-link-features');
    const docsLink = screen.getByTestId('nav-link-docs');

    expect(homeLink).not.toHaveAttribute('target');
    expect(featuresLink).not.toHaveAttribute('target');
    expect(docsLink).not.toHaveAttribute('target');
  });

  it('renders mobile menu button', () => {
    render(<Navbar />);

    const menuButton = screen.getByTestId('mobile-menu-button');
    expect(menuButton).toBeInTheDocument();
    expect(menuButton).toHaveAttribute('aria-label', 'Open menu');
  });

  it('toggles mobile menu when button is clicked', () => {
    render(<Navbar />);

    const menuButton = screen.getByTestId('mobile-menu-button');

    // Initially closed
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');

    // Click to open
    fireEvent.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'true');
    expect(menuButton).toHaveAttribute('aria-label', 'Close menu');

    // Click to close
    fireEvent.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
    expect(menuButton).toHaveAttribute('aria-label', 'Open menu');
  });

  it('has proper accessibility attributes', () => {
    render(<Navbar />);

    const header = screen.getByRole('banner');
    expect(header).toBeInTheDocument();

    const nav = screen.getByRole('navigation', { name: 'Main navigation' });
    expect(nav).toBeInTheDocument();

    const menubar = screen.getByRole('menubar');
    expect(menubar).toBeInTheDocument();
  });

  it('Home link scrolls to top when clicked', () => {
    render(<Navbar />);

    const homeLink = screen.getByTestId('nav-link-home');
    fireEvent.click(homeLink);

    expect(window.scrollTo).toHaveBeenCalledWith({ top: 0, behavior: 'smooth' });
  });

  it('Features link scrolls to features section', () => {
    // Create a mock element for the features section
    const mockElement = document.createElement('div');
    mockElement.id = 'features';
    mockElement.scrollIntoView = jest.fn();
    document.body.appendChild(mockElement);

    render(<Navbar />);

    const featuresLink = screen.getByTestId('nav-link-features');
    fireEvent.click(featuresLink);

    expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

    // Cleanup
    document.body.removeChild(mockElement);
  });
});
