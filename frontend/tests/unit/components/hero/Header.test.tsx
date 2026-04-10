/**
 * Unit tests for Header component
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Tests:
 * - Renders MirDB logo and home link
 * - Displays navigation links
 * - Navigation links route correctly
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import Header from '@/components/layout/Header';

const renderHeader = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Header />
    </MemoryRouter>
  );
};

describe('Header Component', () => {
  describe('Logo and Branding', () => {
    it('renders the MirDB logo link', () => {
      renderHeader();
      const logoLink = screen.getByRole('link', { name: /mirdb home/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });

    it('displays the MirDB text', () => {
      renderHeader();
      expect(screen.getByText('MirDB')).toBeInTheDocument();
    });
  });

  describe('Navigation Links', () => {
    it('renders Dashboard navigation link', () => {
      renderHeader();
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toBeInTheDocument();
      expect(dashboardLink).toHaveAttribute('href', '/dashboard');
    });

    it('renders Browser navigation link', () => {
      renderHeader();
      const browserLink = screen.getByRole('link', { name: /browser/i });
      expect(browserLink).toBeInTheDocument();
      expect(browserLink).toHaveAttribute('href', '/browser');
    });

    it('renders Config navigation link', () => {
      renderHeader();
      const configLink = screen.getByRole('link', { name: /config/i });
      expect(configLink).toBeInTheDocument();
      expect(configLink).toHaveAttribute('href', '/config');
    });

    it('renders Docs navigation link', () => {
      renderHeader();
      const docsLink = screen.getByRole('link', { name: /docs/i });
      expect(docsLink).toBeInTheDocument();
      expect(docsLink).toHaveAttribute('href', '/docs');
    });

    it('highlights the active navigation link', () => {
      renderHeader('/dashboard');
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toHaveClass('header-nav-link--active');
    });
  });

  describe('Accessibility', () => {
    it('has proper banner role', () => {
      renderHeader();
      expect(screen.getByRole('banner')).toBeInTheDocument();
    });

    it('has proper navigation role and label', () => {
      renderHeader();
      expect(screen.getByRole('navigation', { name: /main navigation/i })).toBeInTheDocument();
    });

    it('renders mobile menu toggle button', () => {
      renderHeader();
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      expect(menuButton).toBeInTheDocument();
    });
  });
});
