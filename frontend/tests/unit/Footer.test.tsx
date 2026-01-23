/**
 * Footer Section Unit Tests
 * Owner: Scenario 15
 *
 * Test coverage:
 * - Footer rendering
 * - Navigation links
 * - Product description
 *
 * Test suites:
 * - describe('Footer Section')
 * - describe('Footer Links')
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import Footer from '../../src/components/homepage/Footer';

describe('Footer Section', () => {
  it('should render the footer element at bottom of page', () => {
    renderWithProviders(<Footer />);

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('should have the footer HTML element', () => {
    const { container } = renderWithProviders(<Footer />);

    const footerElement = container.querySelector('footer');
    expect(footerElement).toBeInTheDocument();
  });
});

describe('Footer Links', () => {
  it('should contain a link to /login', () => {
    renderWithProviders(<Footer />, { useMemoryRouter: true, initialEntries: ['/'] });

    const footer = screen.getByRole('contentinfo');
    const loginLink = within(footer).getByRole('link', { name: /login/i });

    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('should contain a link to /register', () => {
    renderWithProviders(<Footer />, { useMemoryRouter: true, initialEntries: ['/'] });

    const footer = screen.getByRole('contentinfo');
    const registerLink = within(footer).getByRole('link', { name: /register/i });

    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');
  });
});

describe('Footer Product Description', () => {
  it('should contain text describing the URL shortening service', () => {
    renderWithProviders(<Footer />);

    const footer = screen.getByRole('contentinfo');

    // Check for URL shortening service description
    expect(within(footer).getByText(/url short/i)).toBeInTheDocument();
  });

  it('should describe analytics and link management capabilities', () => {
    renderWithProviders(<Footer />);

    const footer = screen.getByRole('contentinfo');
    const descriptionText = within(footer).getByText(/analytics/i);

    expect(descriptionText).toBeInTheDocument();
  });

  it('should include copyright information', () => {
    renderWithProviders(<Footer />);

    const footer = screen.getByRole('contentinfo');
    const copyrightText = within(footer).getByText(/copyright/i);

    expect(copyrightText).toBeInTheDocument();
  });
});
