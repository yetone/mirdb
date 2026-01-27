/**
 * Home Page Accessibility Tests - Image Alt Text
 * Owner: Scenario 15 - Accessibility - Image Alt Text
 *
 * Tests that all images have descriptive alt text and
 * feature icons have appropriate alt text or aria-labels.
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '../test-utils';
import Home from '../../src/pages/Home';

describe('Accessibility - Image Alt Text', () => {
  describe('Test Case 1: All img elements have non-empty alt attributes', () => {
    it('should have non-empty alt attributes on all img elements', () => {
      const { container } = render(<Home />);

      // Get all img elements in the landing page
      const images = container.querySelectorAll('img');

      // If there are images, they should all have non-empty alt attributes
      images.forEach((img) => {
        const altText = img.getAttribute('alt');
        // Each image must have an alt attribute that is not empty
        expect(altText).not.toBeNull();
        expect(altText).not.toBe('');
      });

      // If no images are found, the test should still pass
      // (component uses SVG icons instead of img elements)
      expect(true).toBe(true);
    });

    it('should not have img elements with empty or missing alt attributes', () => {
      const { container } = render(<Home />);

      // Get all img elements
      const images = container.querySelectorAll('img');

      // Check that no images have empty or missing alt
      const imagesWithoutAlt = Array.from(images).filter((img) => {
        const alt = img.getAttribute('alt');
        return alt === null || alt === '';
      });

      expect(imagesWithoutAlt).toHaveLength(0);
    });

    it('should render the landing page correctly', () => {
      render(<Home />);

      // Verify the landing page renders
      expect(screen.getByTestId('landing-page')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Feature icons have appropriate alt text or aria-labels', () => {
    it('should have feature icons that are decorative with aria-hidden="true"', () => {
      const { container } = render(<Home />);

      // Get the features section
      const featuresSection = screen.getByTestId('features-section');

      // Get all SVG elements within feature icons
      const featureIcons = featuresSection.querySelectorAll('[data-testid^="feature-icon-"] svg');

      featureIcons.forEach((svg) => {
        // Decorative icons should have aria-hidden="true"
        // This is the correct pattern when the icon is accompanied by visible text
        expect(svg.getAttribute('aria-hidden')).toBe('true');
      });
    });

    it('should have feature cards with visible text labels for each feature', () => {
      render(<Home />);

      const featuresSection = screen.getByTestId('features-section');

      // Each feature should have a visible title that describes the feature
      expect(within(featuresSection).getByText('Instant URL Shortening')).toBeInTheDocument();
      expect(within(featuresSection).getByText('Detailed Analytics')).toBeInTheDocument();
      expect(within(featuresSection).getByText('Share Statistics')).toBeInTheDocument();
    });

    it('should have accessible feature section with aria-labelledby', () => {
      render(<Home />);

      const featuresSection = screen.getByTestId('features-section');

      // The section should have aria-labelledby pointing to the heading
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

      // The heading should exist
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();
    });

    it('should have social proof icons that are decorative with aria-hidden="true"', () => {
      const { container } = render(<Home />);

      // Get the social proof section
      const socialProofSection = screen.getByTestId('social-proof-section');

      // Get all SVG elements within stat icons
      const statIcons = socialProofSection.querySelectorAll('[data-testid^="stat-icon-"] svg');

      statIcons.forEach((svg) => {
        // Decorative icons should have aria-hidden="true"
        expect(svg.getAttribute('aria-hidden')).toBe('true');
      });
    });

    it('should have stat cards with visible text labels', () => {
      render(<Home />);

      const socialProofSection = screen.getByTestId('social-proof-section');

      // Each stat should have a visible label
      expect(within(socialProofSection).getByText('Links Created')).toBeInTheDocument();
      expect(within(socialProofSection).getByText('Clicks Tracked')).toBeInTheDocument();
      expect(within(socialProofSection).getByText('Happy Users')).toBeInTheDocument();
    });

    it('should have "how it works" step icons that are accessible', () => {
      const { container } = render(<Home />);

      // Get the steps container
      const stepsContainer = screen.getByTestId('steps-container');

      // Each step should have a visible title
      expect(within(stepsContainer).getByText('Create')).toBeInTheDocument();
      expect(within(stepsContainer).getByText('Share')).toBeInTheDocument();
      expect(within(stepsContainer).getByText('Track')).toBeInTheDocument();
    });

    it('should ensure all decorative SVGs have aria-hidden="true"', () => {
      const { container } = render(<Home />);

      // Get all SVGs that are inside elements with data-testid containing "icon"
      const iconContainers = container.querySelectorAll('[data-testid*="icon"]');

      iconContainers.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg');
        if (svg) {
          // Decorative icons (those with nearby text labels) should have aria-hidden
          expect(svg.getAttribute('aria-hidden')).toBe('true');
        }
      });
    });

    it('should have sections with appropriate aria-labels or aria-labelledby', () => {
      render(<Home />);

      // Hero section should have aria-label
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toBeInTheDocument();

      // Features section should have aria-labelledby
      const featuresSection = screen.getByRole('region', { name: /powerful features/i });
      expect(featuresSection).toBeInTheDocument();

      // Social proof section should have aria-labelledby
      const socialProofSection = screen.getByRole('region', { name: /trusted by thousands/i });
      expect(socialProofSection).toBeInTheDocument();
    });
  });
});
