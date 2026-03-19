/**
 * Unit tests for Header component.
 * Owner: Scenario 8 - Navigation Header
 */

import { render, screen } from '@testing-library/react';
import { Header } from '@/components/layout/Header';

// Mock the Navigation component to isolate Header tests
jest.mock('@/components/layout/Navigation', () => ({
  Navigation: () => <nav data-testid="navigation">Mocked Navigation</nav>,
}));

// Mock the ThemeToggle component to isolate Header tests
jest.mock('@/components/ui/ThemeToggle', () => ({
  ThemeToggle: () => <button data-testid="theme-toggle">Mocked ThemeToggle</button>,
}));

describe('Header', () => {
  it('renders the header element', () => {
    render(<Header />);
    const header = screen.getByRole('banner');
    expect(header).toBeInTheDocument();
  });

  it('renders with fixed positioning classes', () => {
    render(<Header />);
    const header = screen.getByRole('banner');
    expect(header).toHaveClass('fixed');
    expect(header).toHaveClass('top-0');
    expect(header).toHaveClass('left-0');
    expect(header).toHaveClass('right-0');
    expect(header).toHaveClass('z-50');
  });

  it('renders the MirDB logo with text', () => {
    render(<Header />);
    const logoLink = screen.getByRole('link', { name: /mirdb home/i });
    expect(logoLink).toBeInTheDocument();
    expect(logoLink).toHaveAttribute('href', '/');
    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('renders the Navigation component', () => {
    render(<Header />);
    expect(screen.getByTestId('navigation')).toBeInTheDocument();
  });

  it('has correct height for header offset', () => {
    render(<Header />);
    const header = screen.getByRole('banner');
    expect(header).toHaveClass('h-20');
  });

  it('applies custom className when provided', () => {
    render(<Header className="custom-class" />);
    const header = screen.getByRole('banner');
    expect(header).toHaveClass('custom-class');
  });
});
