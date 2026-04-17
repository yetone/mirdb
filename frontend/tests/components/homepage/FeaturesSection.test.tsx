/**
 * FeaturesSection Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests for the Features Section component:
 * - Section container rendering
 * - Feature card count (3-5 cards)
 * - Feature card structure (icon, title, description)
 * - Core features representation
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { FeaturesSection } from '@/components/homepage/FeaturesSection';

describe('FeaturesSection', () => {
  // Test Case 1: Features section container element exists
  describe('Features section container', () => {
    it('renders the features section container element', () => {
      render(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });

    it('has proper accessibility attributes', () => {
      render(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'features-heading');
    });
  });

  // Test Case 2: Between 3 and 5 feature cards are rendered
  describe('Feature card count', () => {
    it('renders between 3 and 5 feature cards', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
      expect(featureCards.length).toBeLessThanOrEqual(5);
    });

    it('renders exactly 5 feature cards as specified in requirements', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards).toHaveLength(5);
    });
  });

  // Test Case 3: All feature cards contain an icon
  describe('Feature card icons', () => {
    it('all feature cards contain an icon element', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('feature-icon');
        expect(iconContainer).toBeInTheDocument();

        // Check that icon container has an SVG child (from lucide-react)
        const svg = iconContainer.querySelector('svg');
        expect(svg).toBeInTheDocument();
      });
    });

    it('icons are properly hidden from screen readers', () => {
      render(<FeaturesSection />);

      const iconContainers = screen.getAllByTestId('feature-icon');

      iconContainers.forEach((iconContainer) => {
        expect(iconContainer).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  // Test Case 4: All feature cards have a heading element with title text
  describe('Feature card titles', () => {
    it('all feature cards have a title element', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const title = within(card).getByTestId('feature-title');
        expect(title).toBeInTheDocument();
        expect(title.tagName).toBe('H3');
      });
    });

    it('feature titles are not empty', () => {
      render(<FeaturesSection />);

      const titles = screen.getAllByTestId('feature-title');

      titles.forEach((title) => {
        expect(title.textContent).not.toBe('');
        expect(title.textContent!.length).toBeGreaterThan(0);
      });
    });
  });

  // Test Case 5: All feature cards have description text
  describe('Feature card descriptions', () => {
    it('all feature cards have a description element', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const description = within(card).getByTestId('feature-description');
        expect(description).toBeInTheDocument();
      });
    });

    it('descriptions contain meaningful text', () => {
      render(<FeaturesSection />);

      const descriptions = screen.getAllByTestId('feature-description');

      descriptions.forEach((description) => {
        expect(description.textContent).not.toBe('');
        // Description should have at least 20 characters for meaningful content
        expect(description.textContent!.length).toBeGreaterThan(20);
      });
    });
  });

  // Test Case 6: Core features are represented
  describe('Core features representation', () => {
    it('includes instant URL shortening feature', () => {
      render(<FeaturesSection />);

      const shorteningFeature = screen.getByText(/instant url shortening/i);
      expect(shorteningFeature).toBeInTheDocument();
    });

    it('includes analytics feature', () => {
      render(<FeaturesSection />);

      const analyticsFeature = screen.getByText(/click analytics/i);
      expect(analyticsFeature).toBeInTheDocument();
    });

    it('includes QR code feature', () => {
      render(<FeaturesSection />);

      // Use getAllByText since "QR code" appears in both title and description
      const qrCodeFeatures = screen.getAllByText(/qr code/i);
      expect(qrCodeFeatures.length).toBeGreaterThanOrEqual(1);
    });

    it('includes custom short codes feature', () => {
      render(<FeaturesSection />);

      const customCodesFeature = screen.getByText(/custom short codes/i);
      expect(customCodesFeature).toBeInTheDocument();
    });

    it('includes geographic tracking feature', () => {
      render(<FeaturesSection />);

      const geoTrackingFeature = screen.getByText(/geographic tracking/i);
      expect(geoTrackingFeature).toBeInTheDocument();
    });

    it('all core features (shortening, analytics, QR codes) are present', () => {
      render(<FeaturesSection />);

      // Verify the three core features mentioned in test case 6
      expect(screen.getByText(/instant url shortening/i)).toBeInTheDocument();
      expect(screen.getByText(/click analytics/i)).toBeInTheDocument();
      // Use getAllByText since "QR code" appears in both title and description
      expect(screen.getAllByText(/qr code/i).length).toBeGreaterThanOrEqual(1);
    });
  });

  // Additional integration tests
  describe('FeaturesSection integration', () => {
    it('renders with proper grid layout classes', () => {
      render(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      const gridContainer = featuresSection.querySelector('.grid');

      expect(gridContainer).toBeInTheDocument();
      expect(gridContainer).toHaveClass('grid-cols-1', 'md:grid-cols-2', 'lg:grid-cols-3');
    });

    it('has a section heading', () => {
      render(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent(/powerful features/i);
    });

    it('has a section description', () => {
      render(<FeaturesSection />);

      const description = screen.getByText(/everything you need to shorten/i);
      expect(description).toBeInTheDocument();
    });
  });
});
