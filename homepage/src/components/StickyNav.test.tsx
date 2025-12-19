import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { StickyNav, navItems } from './StickyNav';

describe('StickyNav', () => {
  beforeEach(() => {
    // Mock scrollIntoView
    Element.prototype.scrollIntoView = vi.fn();
    // Mock window.scrollTo
    window.scrollTo = vi.fn();
  });

  it('renders the navigation with logo', () => {
    render(<StickyNav />);
    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('renders all navigation links', () => {
    render(<StickyNav />);
    navItems.forEach((item) => {
      expect(screen.getByText(item.label)).toBeInTheDocument();
    });
  });

  it('renders navigation links with correct href attributes', () => {
    render(<StickyNav />);
    navItems.forEach((item) => {
      const link = screen.getByTestId(`nav-${item.id}`);
      expect(link).toHaveAttribute('href', item.href);
    });
  });

  it('renders GitHub link', () => {
    render(<StickyNav />);
    const githubLink = screen.getByRole('link', { name: /github/i });
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', expect.stringContaining('github.com'));
  });

  it('has proper test ID for the navigation', () => {
    render(<StickyNav />);
    expect(screen.getByTestId('sticky-nav')).toBeInTheDocument();
  });

  it('calls scrollIntoView when navigation link is clicked', () => {
    // Create a mock element with the target id
    const mockElement = document.createElement('div');
    mockElement.id = 'features';
    document.body.appendChild(mockElement);

    render(<StickyNav />);
    const featuresLink = screen.getByTestId('nav-features');
    fireEvent.click(featuresLink);

    expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

    // Cleanup
    document.body.removeChild(mockElement);
  });

  it('scrolls to top when logo is clicked', () => {
    render(<StickyNav />);
    const logo = screen.getByText('MirDB');
    fireEvent.click(logo);

    expect(window.scrollTo).toHaveBeenCalledWith({ top: 0, behavior: 'smooth' });
  });

  it('prevents default behavior when clicking navigation links', () => {
    // Create a mock element with the target id
    const mockElement = document.createElement('div');
    mockElement.id = 'quick-start';
    document.body.appendChild(mockElement);

    render(<StickyNav />);
    const quickStartLink = screen.getByTestId('nav-quick-start');

    const clickEvent = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
    });
    const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');

    quickStartLink.dispatchEvent(clickEvent);
    expect(preventDefaultSpy).toHaveBeenCalled();

    // Cleanup
    document.body.removeChild(mockElement);
  });

  it('renders Features link that targets #features', () => {
    render(<StickyNav />);
    const featuresLink = screen.getByText('Features');
    expect(featuresLink).toHaveAttribute('href', '#features');
  });

  it('renders Quick Start link that targets #quick-start', () => {
    render(<StickyNav />);
    const quickStartLink = screen.getByText('Quick Start');
    expect(quickStartLink).toHaveAttribute('href', '#quick-start');
  });

  it('renders Commands link that targets #commands', () => {
    render(<StickyNav />);
    const commandsLink = screen.getByText('Commands');
    expect(commandsLink).toHaveAttribute('href', '#commands');
  });

  it('renders Configuration link that targets #configuration', () => {
    render(<StickyNav />);
    const configLink = screen.getByText('Configuration');
    expect(configLink).toHaveAttribute('href', '#configuration');
  });
});
