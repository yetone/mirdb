/**
 * Features Section Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test Cases:
 * 1. Features section exists with accessible landmark or id='features'
 * 2. At least 3 feature cards are rendered
 * 3. Each card contains an icon, heading (title), and paragraph (description)
 * 4. One feature card mentions 'short', 'shorten', or 'link' in title or description
 * 5. One feature card mentions 'analytics', 'track', 'click', or 'statistics'
 * 6. One feature card mentions 'share', 'sharing', or 'shareable'
 * 7. Feature cards use GlassMorphismCard component or equivalent styling
 * 8. Feature cards display in 3-column grid layout at 1024px
 * 9. Feature cards stack vertically in single column at 375px
 */

import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './test-utils';
import { FeaturesSection } from '../../src/components/homepage/FeaturesSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({
      children,
      ...props
    }: {
      children: React.ReactNode;
      [key: string]: unknown;
    }) => {
      const {
        variants,
        initial,
        whileInView,
        viewport,
        animate,
        exit,
        ...domProps
      } = props;
      return <div {...domProps}>{children}</div>;
    },
  },
}));

describe('FeaturesSection', () => {
  // Test Case 1: Features section exists with accessible landmark or id='features'
  describe('TC-1: Features Section Container', () => {
    it('should have a features section with id="features"', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
    });

    it('should have accessible landmark with aria-labelledby', () => {
      renderWithProviders(<FeaturesSection />);

      const section = document.getElementById('features');
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
    });

    it('should be a semantic section element', () => {
      renderWithProviders(<FeaturesSection />);

      const section = document.getElementById('features');
      expect(section?.tagName).toBe('SECTION');
    });
  });

  // Test Case 2: At least 3 feature cards are rendered
  describe('TC-2: Feature Cards Count', () => {
    it('should render at least 3 feature cards', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });
  });

  // Test Case 3: Each card contains an icon, heading (title), and paragraph (description)
  describe('TC-3: Feature Card Structure', () => {
    it('each card should contain an icon element', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        const icon = within(card).getByTestId('feature-icon');
        expect(icon).toBeInTheDocument();
      });
    });

    it('each card should contain a heading (h3 title)', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        const heading = within(card).getByRole('heading', { level: 3 });
        expect(heading).toBeInTheDocument();
      });
    });

    it('each card should contain a paragraph (description)', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        const paragraphs = card.querySelectorAll('p');
        expect(paragraphs.length).toBeGreaterThanOrEqual(1);
      });
    });
  });

  // Test Case 4: URL shortening feature content
  describe('TC-4: URL Shortening Feature', () => {
    it('should have a feature card mentioning short/shorten/link', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      const hasUrlShorteningFeature = featureCards.some((card) => {
        const text = card.textContent?.toLowerCase() || '';
        return (
          text.includes('short') ||
          text.includes('shorten') ||
          text.includes('link')
        );
      });

      expect(hasUrlShorteningFeature).toBe(true);
    });
  });

  // Test Case 5: Analytics feature content
  describe('TC-5: Analytics Feature', () => {
    it('should have a feature card mentioning analytics/track/click/statistics', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      const hasAnalyticsFeature = featureCards.some((card) => {
        const text = card.textContent?.toLowerCase() || '';
        return (
          text.includes('analytics') ||
          text.includes('track') ||
          text.includes('click') ||
          text.includes('statistics')
        );
      });

      expect(hasAnalyticsFeature).toBe(true);
    });
  });

  // Test Case 6: Sharing feature content
  describe('TC-6: Sharing Feature', () => {
    it('should have a feature card mentioning share/sharing/shareable', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      const hasSharingFeature = featureCards.some((card) => {
        const text = card.textContent?.toLowerCase() || '';
        return (
          text.includes('share') ||
          text.includes('sharing') ||
          text.includes('shareable')
        );
      });

      expect(hasSharingFeature).toBe(true);
    });
  });

  // Test Case 7: GlassMorphismCard styling
  describe('TC-7: GlassMorphismCard Styling', () => {
    it('feature cards should use GlassMorphismCard styling (backdrop-blur)', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        // GlassMorphismCard applies backdrop-blur-md class
        expect(card).toHaveClass('backdrop-blur-md');
      });
    });

    it('feature cards should have rounded corners', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        expect(card).toHaveClass('rounded-xl');
      });
    });
  });

  // Test Case 8: 3-column grid layout at 1024px (desktop)
  describe('TC-8: Desktop Grid Layout', () => {
    it('should have md:grid-cols-3 class for 3-column layout on desktop', () => {
      renderWithProviders(<FeaturesSection />);

      const section = document.getElementById('features');
      const grid = section?.querySelector('.grid');

      expect(grid).toHaveClass('md:grid-cols-3');
    });
  });

  // Test Case 9: Single column layout at 375px (mobile)
  describe('TC-9: Mobile Single Column Layout', () => {
    it('should have grid-cols-1 class for single column on mobile', () => {
      renderWithProviders(<FeaturesSection />);

      const section = document.getElementById('features');
      const grid = section?.querySelector('.grid');

      expect(grid).toHaveClass('grid-cols-1');
    });
  });

  // Additional accessibility tests
  describe('Accessibility', () => {
    it('should have a section heading', () => {
      renderWithProviders(<FeaturesSection />);

      const heading = screen.getByRole('heading', { name: /powerful features/i });
      expect(heading).toBeInTheDocument();
    });

    it('icons should have aria-hidden attribute', () => {
      renderWithProviders(<FeaturesSection />);

      const icons = screen.getAllByTestId('feature-icon');
      icons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg');
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });
});
