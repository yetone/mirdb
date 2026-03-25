/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests features section rendering including:
 * - Section heading visibility
 * - Three feature cards with GlassMorphismCard
 * - URL shortening feature content
 * - Analytics dashboard feature content
 * - Click tracking feature content
 * - Lucide icons on each card
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';
import { renderWithProviders } from './test-utils';

describe('FeaturesSection', () => {
  describe('Test Case 1: Features section exists with section heading', () => {
    it('renders features section with proper structure', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
      expect(featuresSection).toBeVisible();
    });

    it('features section has proper aria-labelledby for accessibility', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');
    });

    it('displays section heading with "Features" text', () => {
      renderWithProviders(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent(/features/i);
    });

    it('heading has correct id for aria-labelledby', () => {
      renderWithProviders(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'features-heading');
    });
  });

  describe('Test Case 2: Three GlassMorphismCard components render', () => {
    it('renders exactly three feature cards', () => {
      renderWithProviders(<FeaturesSection />);

      const card0 = screen.getByTestId('feature-card-0');
      const card1 = screen.getByTestId('feature-card-1');
      const card2 = screen.getByTestId('feature-card-2');

      expect(card0).toBeInTheDocument();
      expect(card1).toBeInTheDocument();
      expect(card2).toBeInTheDocument();
    });

    it('feature cards are visible', () => {
      renderWithProviders(<FeaturesSection />);

      const card0 = screen.getByTestId('feature-card-0');
      const card1 = screen.getByTestId('feature-card-1');
      const card2 = screen.getByTestId('feature-card-2');

      expect(card0).toBeVisible();
      expect(card1).toBeVisible();
      expect(card2).toBeVisible();
    });

    it('feature cards use GlassMorphismCard styling', () => {
      renderWithProviders(<FeaturesSection />);

      const card0 = screen.getByTestId('feature-card-0');
      // GlassMorphismCard applies backdrop-blur-md class
      expect(card0).toHaveClass('backdrop-blur-md');
    });
  });

  describe('Test Case 3: URL shortening feature card', () => {
    it('displays URL Shortening title', () => {
      renderWithProviders(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-0');
      expect(title).toHaveTextContent('URL Shortening');
    });

    it('displays URL shortening description', () => {
      renderWithProviders(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-0');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/short.*links|urls|transform/i);
    });

    it('URL shortening card is properly structured', () => {
      renderWithProviders(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-0');
      const title = within(card).getByTestId('feature-title-0');
      const description = within(card).getByTestId('feature-description-0');

      expect(title).toBeInTheDocument();
      expect(description).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Analytics dashboard feature card', () => {
    it('displays Analytics Dashboard title', () => {
      renderWithProviders(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-1');
      expect(title).toHaveTextContent('Analytics Dashboard');
    });

    it('displays analytics description with insights keywords', () => {
      renderWithProviders(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-1');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/analytics|insights|performance/i);
    });

    it('analytics card has title and description within the card', () => {
      renderWithProviders(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-1');
      const title = within(card).getByTestId('feature-title-1');
      const description = within(card).getByTestId('feature-description-1');

      expect(title).toBeInTheDocument();
      expect(description).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Click tracking feature card', () => {
    it('displays Click Tracking title', () => {
      renderWithProviders(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-2');
      expect(title).toHaveTextContent('Click Tracking');
    });

    it('displays click tracking description', () => {
      renderWithProviders(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-2');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/click|track|monitor/i);
    });

    it('click tracking card is the third feature card', () => {
      renderWithProviders(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-2');
      const title = within(card).getByTestId('feature-title-2');

      expect(title).toHaveTextContent('Click Tracking');
    });
  });

  describe('Test Case 6: Icons on feature cards', () => {
    it('each feature card has an icon container', () => {
      renderWithProviders(<FeaturesSection />);

      const icon0 = screen.getByTestId('feature-icon-0');
      const icon1 = screen.getByTestId('feature-icon-1');
      const icon2 = screen.getByTestId('feature-icon-2');

      expect(icon0).toBeInTheDocument();
      expect(icon1).toBeInTheDocument();
      expect(icon2).toBeInTheDocument();
    });

    it('icons are visible within their containers', () => {
      renderWithProviders(<FeaturesSection />);

      const icon0 = screen.getByTestId('feature-icon-0');
      const icon1 = screen.getByTestId('feature-icon-1');
      const icon2 = screen.getByTestId('feature-icon-2');

      expect(icon0).toBeVisible();
      expect(icon1).toBeVisible();
      expect(icon2).toBeVisible();
    });

    it('icon containers contain SVG elements (Lucide icons)', () => {
      renderWithProviders(<FeaturesSection />);

      const icon0 = screen.getByTestId('feature-icon-0');
      const icon1 = screen.getByTestId('feature-icon-1');
      const icon2 = screen.getByTestId('feature-icon-2');

      // Lucide icons render as SVG elements
      expect(icon0.querySelector('svg')).toBeInTheDocument();
      expect(icon1.querySelector('svg')).toBeInTheDocument();
      expect(icon2.querySelector('svg')).toBeInTheDocument();
    });

    it('icons have aria-hidden for accessibility', () => {
      renderWithProviders(<FeaturesSection />);

      const icon0 = screen.getByTestId('feature-icon-0');
      const svg = icon0.querySelector('svg');

      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Accessibility', () => {
    it('features section uses semantic section element', () => {
      renderWithProviders(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section.tagName.toLowerCase()).toBe('section');
    });

    it('feature titles use heading level 3', () => {
      renderWithProviders(<FeaturesSection />);

      const title0 = screen.getByTestId('feature-title-0');
      const title1 = screen.getByTestId('feature-title-1');
      const title2 = screen.getByTestId('feature-title-2');

      expect(title0.tagName.toLowerCase()).toBe('h3');
      expect(title1.tagName.toLowerCase()).toBe('h3');
      expect(title2.tagName.toLowerCase()).toBe('h3');
    });
  });

  describe('Styling', () => {
    it('features grid is responsive', () => {
      renderWithProviders(<FeaturesSection />);

      // Find the grid container (parent of feature cards)
      const featuresSection = screen.getByTestId('features-section');
      const gridContainer = featuresSection.querySelector('.grid');

      expect(gridContainer).toHaveClass('grid-cols-1');
      expect(gridContainer).toHaveClass('md:grid-cols-3');
    });

    it('section has proper padding', () => {
      renderWithProviders(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toHaveClass('py-20');
      expect(section).toHaveClass('px-4');
    });
  });
});
