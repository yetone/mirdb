/**
 * Unit tests for Hero component
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Tests:
 * - Renders MirDB branding and logo
 * - Displays tagline text
 * - Shows CTA buttons (Quick Start Guide, GitHub Repository)
 * - Verifies navigation link functionality
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Hero from '@/components/hero/Hero';

const renderHero = () => {
  return render(
    <BrowserRouter>
      <Hero />
    </BrowserRouter>
  );
};

describe('Hero Component', () => {
  describe('Branding and Logo', () => {
    it('renders the MirDB title', () => {
      renderHero();
      const title = screen.getByRole('heading', { level: 1, name: /mirdb/i });
      expect(title).toBeInTheDocument();
    });

    it('renders the hero section with proper aria label', () => {
      renderHero();
      const section = screen.getByRole('region', { name: /mirdb/i });
      expect(section).toBeInTheDocument();
    });
  });

  describe('Tagline and Value Proposition', () => {
    it('displays the tagline text', () => {
      renderHero();
      const tagline = screen.getByText(/persistent key-value store with memcached protocol/i);
      expect(tagline).toBeInTheDocument();
    });

    it('displays the value proposition description', () => {
      renderHero();
      const description = screen.getByText(/combines the simplicity and speed of memcached/i);
      expect(description).toBeInTheDocument();
    });

    it('displays feature highlights', () => {
      renderHero();
      // Use exact match for feature text elements to avoid matching description text
      const features = screen.getAllByText(/Lightning Fast|Persistent Storage|Drop-in Replacement/);
      expect(features).toHaveLength(3);

      // Verify each feature is within the hero-feature class
      features.forEach(feature => {
        expect(feature).toHaveClass('hero-feature-text');
      });
    });
  });

  describe('CTA Buttons', () => {
    it('renders Quick Start Guide button as internal link', () => {
      renderHero();
      const quickStartButton = screen.getByRole('link', { name: /quick start guide/i });
      expect(quickStartButton).toBeInTheDocument();
      expect(quickStartButton).toHaveAttribute('href', '/docs');
    });

    it('renders GitHub Repository button as external link', () => {
      renderHero();
      const githubButton = screen.getByRole('link', { name: /github repository/i });
      expect(githubButton).toBeInTheDocument();
      expect(githubButton).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');
      expect(githubButton).toHaveAttribute('target', '_blank');
      expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});
