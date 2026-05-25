/**
 * Unit tests for Layout components.
 * Owner: Scenario 13 - Responsive Layout
 */

import { describe, it, expect, vi, beforeAll } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Layout, Header, Container } from '../../../src/components/layout';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

beforeAll(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });
});

function renderWithTheme(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('Container', () => {
  it('renders with default div element', () => {
    render(<Container>Test content</Container>);
    expect(screen.getByTestId('layout-container')).toBeInTheDocument();
    expect(screen.getByText('Test content')).toBeInTheDocument();
  });

  it('renders with custom element via as prop', () => {
    render(<Container as="main">Main content</Container>);
    const container = screen.getByTestId('layout-container');
    expect(container.tagName.toLowerCase()).toBe('main');
  });

  it('applies custom className', () => {
    render(<Container className="custom-class">Content</Container>);
    const container = screen.getByTestId('layout-container');
    expect(container).toHaveClass('container', 'custom-class');
  });
});

describe('Header', () => {
  it('renders the header', () => {
    renderWithTheme(<Header />);
    expect(screen.getByTestId('layout-header')).toBeInTheDocument();
  });

  it('displays the brand with logo and title', () => {
    renderWithTheme(<Header />);
    expect(screen.getByTestId('header-brand')).toBeInTheDocument();
    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('has a hamburger menu button', () => {
    renderWithTheme(<Header />);
    expect(screen.getByTestId('header-menu-button')).toBeInTheDocument();
  });

  it('toggles mobile nav when menu button is clicked', async () => {
    const user = userEvent.setup();
    renderWithTheme(<Header />);

    const menuButton = screen.getByTestId('header-menu-button');

    // Initially closed
    expect(screen.queryByTestId('header-mobile-nav')).not.toBeInTheDocument();

    // Open menu
    await user.click(menuButton);
    expect(screen.getByTestId('header-mobile-nav')).toBeInTheDocument();
    expect(screen.getByTestId('header-mobile-nav-github')).toBeInTheDocument();
    expect(screen.getByTestId('header-mobile-nav-docs')).toBeInTheDocument();

    // Close menu
    await user.click(menuButton);
    expect(screen.queryByTestId('header-mobile-nav')).not.toBeInTheDocument();
  });

  it('calls onMenuToggle callback when menu is toggled', async () => {
    const user = userEvent.setup();
    const onMenuToggle = vi.fn();
    renderWithTheme(<Header onMenuToggle={onMenuToggle} />);

    const menuButton = screen.getByTestId('header-menu-button');
    await user.click(menuButton);
    expect(onMenuToggle).toHaveBeenCalledWith(true);

    await user.click(menuButton);
    expect(onMenuToggle).toHaveBeenCalledWith(false);
  });

  it('closes mobile nav when a link is clicked', async () => {
    const user = userEvent.setup();
    renderWithTheme(<Header />);

    const menuButton = screen.getByTestId('header-menu-button');
    await user.click(menuButton);
    expect(screen.getByTestId('header-mobile-nav')).toBeInTheDocument();

    const githubLink = screen.getByTestId('header-mobile-nav-github');
    await user.click(githubLink);
    expect(screen.queryByTestId('header-mobile-nav')).not.toBeInTheDocument();
  });

  it('menu button has correct ARIA attributes', () => {
    renderWithTheme(<Header />);
    const menuButton = screen.getByTestId('header-menu-button');
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
    expect(menuButton).toHaveAttribute('aria-controls', 'mobile-nav');
  });

  it('menu button updates aria-expanded when menu is open', async () => {
    const user = userEvent.setup();
    renderWithTheme(<Header />);
    const menuButton = screen.getByTestId('header-menu-button');

    await user.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'true');

    await user.click(menuButton);
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
  });
});

describe('Layout', () => {
  it('renders the layout wrapper', () => {
    renderWithTheme(
      <Layout>
        <div data-testid="test-content">Test Content</div>
      </Layout>
    );
    expect(screen.getByTestId('layout-root')).toBeInTheDocument();
  });

  it('renders the header', () => {
    renderWithTheme(
      <Layout>
        <div>Test Content</div>
      </Layout>
    );
    expect(screen.getByTestId('layout-header')).toBeInTheDocument();
  });

  it('renders children content', () => {
    renderWithTheme(
      <Layout>
        <div data-testid="test-content">Test Content</div>
      </Layout>
    );
    expect(screen.getByTestId('test-content')).toBeInTheDocument();
  });

  it('renders the footer', () => {
    renderWithTheme(
      <Layout>
        <div>Test Content</div>
      </Layout>
    );
    expect(screen.getByTestId('layout-footer')).toBeInTheDocument();
  });

  it('footer contains MirDB branding text', () => {
    renderWithTheme(
      <Layout>
        <div>Test Content</div>
      </Layout>
    );
    const footer = screen.getByTestId('layout-footer');
    expect(footer).toHaveTextContent(/MirDB/);
    expect(footer).toHaveTextContent(/key-value store/);
  });
});
