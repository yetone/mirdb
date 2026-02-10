/**
 * Header Component Unit Tests
 * Owner: Scenario 5 - Navigation and GitHub Links
 *
 * Tests for:
 * - Sticky positioning
 * - Contains Navigation component
 * - Proper semantic structure
 * - Accessibility attributes
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Header } from '@/components/layout/Header';
import { ThemeProvider } from '@/context/ThemeContext';

// Helper to render Header with required providers
const renderHeader = () => {
  return render(
    <ThemeProvider defaultTheme="light">
      <Header />
    </ThemeProvider>
  );
};

describe('Header Component', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 4: Sticky header', () => {
    it('renders as a header element', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
    });

    it('has sticky positioning styles', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ position: 'sticky' });
    });

    it('is positioned at top of viewport', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ top: '0' });
    });

    it('has a high z-index for stacking context', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ zIndex: '1000' });
    });
  });

  describe('Structure', () => {
    it('contains Navigation component', () => {
      renderHeader();
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();
    });

    it('contains logo/brand name through Navigation', () => {
      renderHeader();
      const logo = screen.getByText('MirDB');
      expect(logo).toBeInTheDocument();
    });

    it('contains navigation links through Navigation', () => {
      renderHeader();
      expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Getting Started' })).toBeInTheDocument();
    });

    it('contains GitHub link through Navigation', () => {
      renderHeader();
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toBeInTheDocument();
      expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });
  });

  describe('Accessibility', () => {
    it('has proper role="banner"', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
    });

    it('has aria-label for the header', () => {
      renderHeader();
      const header = screen.getByRole('banner', { name: /site header/i });
      expect(header).toBeInTheDocument();
    });
  });

  describe('Visual appearance', () => {
    it('has background color from CSS variables', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ backgroundColor: 'var(--bg-primary)' });
    });

    it('has bottom border for visual separation', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ borderBottom: '1px solid var(--border-color)' });
    });

    it('has shadow for depth', () => {
      renderHeader();
      const header = screen.getByRole('banner');
      expect(header).toHaveStyle({ boxShadow: 'var(--shadow-sm)' });
    });
  });
});
