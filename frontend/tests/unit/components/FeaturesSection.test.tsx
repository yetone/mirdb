/**
 * Unit tests for FeaturesSection component
 * Owner: Scenario 4 - Features Section Display
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { FeaturesSection } from '../../../src/components/landing/FeaturesSection';
import { FEATURES_CONTENT } from '../../../src/constants/landingContent';

describe('FeaturesSection', () => {
  describe('Initial render', () => {
    it('should render section with proper accessibility attributes', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toBeInTheDocument();
      expect(section).toHaveAttribute('aria-labelledby', 'features-title');
    });

    it('should render features title', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('features-title');
      expect(title).toBeInTheDocument();
      expect(title).toHaveTextContent(FEATURES_CONTENT.title);
    });

    it('should display 4 feature cards with correct content for each feature', () => {
      render(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');
      expect(grid).toBeInTheDocument();

      // Verify all 4 feature cards are rendered
      FEATURES_CONTENT.items.forEach((item) => {
        const card = screen.getByTestId(`feature-card-${item.id}`);
        expect(card).toBeInTheDocument();
      });

      // Verify we have exactly 4 cards
      const cards = screen.getAllByTestId(/^feature-card-/);
      expect(cards).toHaveLength(4);
    });
  });

  describe('Feature card: URL Shortening', () => {
    it('should show icon, title "URL Shortening", and description', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-shorten');
      expect(card).toBeInTheDocument();

      // Check icon
      const icon = screen.getByTestId('feature-icon-shorten');
      expect(icon).toBeInTheDocument();

      // Check title
      const title = screen.getByTestId('feature-title-shorten');
      expect(title).toHaveTextContent('URL Shortening');

      // Check description
      const description = screen.getByTestId('feature-description-shorten');
      expect(description).toHaveTextContent(
        FEATURES_CONTENT.items.find((i) => i.id === 'shorten')?.description || ''
      );
    });
  });

  describe('Feature card: Detailed Analytics', () => {
    it('should show icon, title "Detailed Analytics", and description about tracking', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-analytics');
      expect(card).toBeInTheDocument();

      // Check icon
      const icon = screen.getByTestId('feature-icon-analytics');
      expect(icon).toBeInTheDocument();

      // Check title
      const title = screen.getByTestId('feature-title-analytics');
      expect(title).toHaveTextContent('Detailed Analytics');

      // Check description mentions tracking
      const description = screen.getByTestId('feature-description-analytics');
      const analyticsItem = FEATURES_CONTENT.items.find((i) => i.id === 'analytics');
      expect(description).toHaveTextContent(analyticsItem?.description || '');
      expect(description.textContent?.toLowerCase()).toContain('track');
    });
  });

  describe('Feature card: Shareable Stats', () => {
    it('should show icon, title "Shareable Stats", and description about sharing', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-share');
      expect(card).toBeInTheDocument();

      // Check icon
      const icon = screen.getByTestId('feature-icon-share');
      expect(icon).toBeInTheDocument();

      // Check title
      const title = screen.getByTestId('feature-title-share');
      expect(title).toHaveTextContent('Shareable Stats');

      // Check description mentions sharing
      const description = screen.getByTestId('feature-description-share');
      const shareItem = FEATURES_CONTENT.items.find((i) => i.id === 'share');
      expect(description).toHaveTextContent(shareItem?.description || '');
      expect(description.textContent?.toLowerCase()).toContain('share');
    });
  });

  describe('Feature card: Secure Authentication', () => {
    it('should show icon, title "Secure Authentication", and description about security', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-secure');
      expect(card).toBeInTheDocument();

      // Check icon
      const icon = screen.getByTestId('feature-icon-secure');
      expect(icon).toBeInTheDocument();

      // Check title
      const title = screen.getByTestId('feature-title-secure');
      expect(title).toHaveTextContent('Secure Authentication');

      // Check description mentions security
      const description = screen.getByTestId('feature-description-secure');
      const secureItem = FEATURES_CONTENT.items.find((i) => i.id === 'secure');
      expect(description).toHaveTextContent(secureItem?.description || '');
      expect(description.textContent?.toLowerCase()).toMatch(/protect|secure|jwt/);
    });
  });

  describe('Grid layout classes', () => {
    it('should have responsive grid classes for 1/2/4 columns', () => {
      render(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');

      // Verify grid has responsive column classes
      expect(grid).toHaveClass('grid');
      expect(grid).toHaveClass('grid-cols-1');
      expect(grid).toHaveClass('md:grid-cols-2');
      expect(grid).toHaveClass('lg:grid-cols-4');
    });
  });

  describe('Accessibility', () => {
    it('should have proper heading hierarchy', () => {
      render(<FeaturesSection />);

      // Section title should be h2
      const sectionTitle = screen.getByRole('heading', { level: 2 });
      expect(sectionTitle).toHaveTextContent(FEATURES_CONTENT.title);

      // Feature card titles should be h3
      const cardTitles = screen.getAllByRole('heading', { level: 3 });
      expect(cardTitles).toHaveLength(4);
    });

    it('should use article elements for feature cards', () => {
      render(<FeaturesSection />);

      const articles = screen.getAllByRole('article');
      expect(articles).toHaveLength(4);
    });
  });
});
