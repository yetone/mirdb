/**
 * FeaturesSection Component Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test cases:
 * 1. Features section renders with exactly 3 feature cards
 * 2. Feature card for 'Instant URL Shortening' is present with icon, title, and description
 * 3. Feature card for 'Detailed Analytics' is present with icon, title, and description
 * 4. Feature card for 'Share Statistics' is present with icon, title, and description
 * 5. Feature cards use GlassMorphismCard component
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '../../test-utils';
import { FeaturesSection } from '../../../src/components/landing/FeaturesSection';

describe('FeaturesSection', () => {
  describe('Test Case 1: Features section renders with exactly 3 feature cards', () => {
    it('should render the features section with exactly 3 feature cards', () => {
      render(<FeaturesSection />);

      // Check that the features section exists
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Check that exactly 3 feature cards are rendered
      const featureCards = screen.getAllByTestId(/^feature-card-\d$/);
      expect(featureCards).toHaveLength(3);
    });

    it('should render the features grid container', () => {
      render(<FeaturesSection />);

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });
  });

  describe("Test Case 2: Feature card for 'Instant URL Shortening' is present with icon, title, and description", () => {
    it('should render the Instant URL Shortening feature card', () => {
      render(<FeaturesSection />);

      // Find the feature title
      const title = screen.getByText('Instant URL Shortening');
      expect(title).toBeInTheDocument();
    });

    it('should have an icon for Instant URL Shortening', () => {
      render(<FeaturesSection />);

      // The first feature card (index 0) should be URL Shortening
      const featureIcon = screen.getByTestId('feature-icon-0');
      expect(featureIcon).toBeInTheDocument();

      // Check that the icon container has content (SVG)
      const svgElement = featureIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });

    it('should have a description for Instant URL Shortening', () => {
      render(<FeaturesSection />);

      // Use the specific testid to find the description
      const description = screen.getByTestId('feature-description-0');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/Create short, memorable links in seconds/i);
    });
  });

  describe("Test Case 3: Feature card for 'Detailed Analytics' is present with icon, title, and description", () => {
    it('should render the Detailed Analytics feature card', () => {
      render(<FeaturesSection />);

      const title = screen.getByText('Detailed Analytics');
      expect(title).toBeInTheDocument();
    });

    it('should have an icon for Detailed Analytics', () => {
      render(<FeaturesSection />);

      // The second feature card (index 1) should be Analytics
      const featureIcon = screen.getByTestId('feature-icon-1');
      expect(featureIcon).toBeInTheDocument();

      const svgElement = featureIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });

    it('should have a description for Detailed Analytics', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-1');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/Track clicks, referrers, browsers, and locations/i);
    });
  });

  describe("Test Case 4: Feature card for 'Share Statistics' is present with icon, title, and description", () => {
    it('should render the Share Statistics feature card', () => {
      render(<FeaturesSection />);

      const title = screen.getByText('Share Statistics');
      expect(title).toBeInTheDocument();
    });

    it('should have an icon for Share Statistics', () => {
      render(<FeaturesSection />);

      // The third feature card (index 2) should be Share Statistics
      const featureIcon = screen.getByTestId('feature-icon-2');
      expect(featureIcon).toBeInTheDocument();

      const svgElement = featureIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });

    it('should have a description for Share Statistics', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-2');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toMatch(/Generate public share links for your analytics/i);
    });
  });

  describe('Test Case 5: Feature cards use GlassMorphismCard component', () => {
    it('should render feature cards using GlassMorphismCard component', () => {
      render(<FeaturesSection />);

      // GlassMorphismCard components have the data-testid="glassmorphism-card"
      const glassMorphismCards = screen.getAllByTestId('glassmorphism-card');

      // Should have exactly 3 GlassMorphismCard components for the feature cards
      expect(glassMorphismCards).toHaveLength(3);
    });

    it('should wrap each feature card content in a GlassMorphismCard', () => {
      render(<FeaturesSection />);

      // Each feature card should be wrapped in a GlassMorphismCard
      const cards = screen.getAllByTestId('glassmorphism-card');
      expect(cards).toHaveLength(3);

      // Verify that each card contains a feature card
      cards.forEach((card, index) => {
        const featureCard = within(card).getByTestId(`feature-card-${index}`);
        expect(featureCard).toBeInTheDocument();
      });
    });
  });

  describe('Accessibility', () => {
    it('should have proper semantic structure', () => {
      render(<FeaturesSection />);

      // Should have a section element
      const section = screen.getByTestId('features-section');
      expect(section.tagName).toBe('SECTION');

      // Should have a heading
      const heading = screen.getByRole('heading', { name: /Powerful Features/i });
      expect(heading).toBeInTheDocument();
    });

    it('should have aria-labelledby on the section', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
    });
  });

  describe('Responsive Layout', () => {
    it('should have grid layout classes for responsive design', () => {
      render(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');
      expect(grid.className).toContain('grid');
      expect(grid.className).toContain('grid-cols-1');
      expect(grid.className).toContain('md:grid-cols-3');
    });
  });
});
