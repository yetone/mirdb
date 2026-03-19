/**
 * Unit tests for Features component.
 * Owner: Scenario 2 - Value Proposition Features Section
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Features } from '@/components/sections/Features';

describe('Features Component', () => {
  // Test Case 1: Three feature cards are rendered with correct titles
  describe('TC1: Renders three feature cards with correct titles', () => {
    it('should render three feature cards with titles: Memcached Compatible, Persistent Storage, Rust Performance', () => {
      render(<Features />);

      // Check that all three feature titles are present
      expect(screen.getByText('Memcached Compatible')).toBeInTheDocument();
      expect(screen.getByText('Persistent Storage')).toBeInTheDocument();
      expect(screen.getByText('Rust Performance')).toBeInTheDocument();

      // Check that exactly three feature cards are rendered
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(3);
    });

    it('should render feature cards with correct test IDs', () => {
      render(<Features />);

      expect(screen.getByTestId('feature-card-memcached-compatible')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-persistent-storage')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-rust-performance')).toBeInTheDocument();
    });
  });

  // Test Case 2: Each card contains description text
  describe('TC2: Each card contains description text', () => {
    it('should display description for Memcached Compatible feature', () => {
      render(<Features />);

      expect(
        screen.getByText(/Drop-in replacement for memcached clients/)
      ).toBeInTheDocument();
    });

    it('should display description for Persistent Storage feature', () => {
      render(<Features />);

      expect(
        screen.getByText(/Data persists across restarts using LSM tree architecture/)
      ).toBeInTheDocument();
    });

    it('should display description for Rust Performance feature', () => {
      render(<Features />);

      expect(
        screen.getByText(/Built in Rust for memory safety and blazing-fast performance/)
      ).toBeInTheDocument();
    });
  });

  // Test Case 3: Each feature card displays a relevant icon with proper alt text
  describe('TC3: Each feature card displays an icon with proper alt text', () => {
    it('should render icons for each feature card', () => {
      render(<Features />);

      // Check for icon containers (the divs that wrap the icons)
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        // Each card should have an icon container with the appropriate styling
        const iconContainer = card.querySelector('.w-16.h-16');
        expect(iconContainer).toBeInTheDocument();

        // Each card should have an SVG icon inside
        const svgIcon = card.querySelector('svg');
        expect(svgIcon).toBeInTheDocument();
      });
    });

    it('should have screen reader text for icon accessibility', () => {
      render(<Features />);

      // Check for screen reader only text
      expect(screen.getByText('Memcached Compatible icon')).toBeInTheDocument();
      expect(screen.getByText('Persistent Storage icon')).toBeInTheDocument();
      expect(screen.getByText('Rust Performance icon')).toBeInTheDocument();
    });

    it('should have icons marked as aria-hidden', () => {
      render(<Features />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        const svgIcon = card.querySelector('svg');
        expect(svgIcon).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  // Additional tests for section structure
  describe('Section structure and accessibility', () => {
    it('should render a section with id="features"', () => {
      render(<Features />);

      const section = document.getElementById('features');
      expect(section).toBeInTheDocument();
    });

    it('should have proper heading structure', () => {
      render(<Features />);

      expect(
        screen.getByRole('heading', { name: 'Why Choose MirDB?', level: 2 })
      ).toBeInTheDocument();
    });

    it('should have proper aria-labelledby on the section', () => {
      render(<Features />);

      const section = document.getElementById('features');
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
    });
  });
});
