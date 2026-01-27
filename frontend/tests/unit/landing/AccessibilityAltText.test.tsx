/**
 * Accessibility - Alt Text Tests
 * Owner: Scenario 14 - Accessibility - Alt Text
 *
 * Tests to verify all images and icons have appropriate alt text:
 * 1. All img elements have alt attributes defined
 * 2. Hero visual has descriptive alt text or is marked decorative
 * 3. Feature icons have appropriate aria-labels or alt text
 *
 * WCAG 2.1 Requirements:
 * - All meaningful images should have descriptive alt text
 * - Decorative images should have empty alt="" or aria-hidden="true"
 * - SVG icons should use aria-hidden="true" when decorative
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './setup.tsx';
import Home from '../../../src/pages/Home';
import HeroSection from '../../../src/components/landing/HeroSection';
import FeaturesSection from '../../../src/components/landing/FeaturesSection';
import HowItWorksSection from '../../../src/components/landing/HowItWorksSection';
import CTASection from '../../../src/components/landing/CTASection';
import Footer from '../../../src/components/landing/Footer';

describe('Accessibility - Alt Text (Scenario 14)', () => {
  /**
   * Test Case 1: Query all img elements
   * Expected: All images have alt attributes defined
   */
  describe('Test Case 1: All img elements have alt attributes', () => {
    it('all img elements in the landing page have alt attributes', () => {
      renderWithProviders(<Home />);

      // Query all img elements on the page
      const images = document.querySelectorAll('img');

      // If there are images, each must have an alt attribute
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('HeroSection has no img elements without alt attributes', () => {
      renderWithProviders(<HeroSection />);

      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('FeaturesSection has no img elements without alt attributes', () => {
      renderWithProviders(<FeaturesSection />);

      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('HowItWorksSection has no img elements without alt attributes', () => {
      renderWithProviders(<HowItWorksSection />);

      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('CTASection has no img elements without alt attributes', () => {
      renderWithProviders(<CTASection />);

      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('Footer has no img elements without alt attributes', () => {
      renderWithProviders(<Footer />);

      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });
  });

  /**
   * Test Case 2: Check hero image alt text
   * Expected: Hero visual has descriptive alt text or is marked decorative
   *
   * The HeroSection uses a mockup window (CSS-based) rather than an actual image,
   * which is an acceptable approach for decorative visuals.
   */
  describe('Test Case 2: Hero visual accessibility', () => {
    it('hero section uses CSS-based mockup window instead of image (decorative)', () => {
      renderWithProviders(<HeroSection />);

      // The hero section uses a div-based mockup-window class
      // which is a CSS/DaisyUI component, not an actual image
      const mockupWindow = document.querySelector('.mockup-window');
      expect(mockupWindow).toBeInTheDocument();

      // This is an acceptable pattern: decorative visuals are implemented
      // as CSS components rather than images, so no alt text is needed
    });

    it('hero visual elements are contained within semantic structure', () => {
      renderWithProviders(<HeroSection />);

      // Check that the visual preview is within the hero section
      const section = document.querySelector('section[aria-labelledby="hero-heading"]');
      expect(section).toBeInTheDocument();

      // The mockup window should be inside the labeled section
      const mockupWindow = section?.querySelector('.mockup-window');
      expect(mockupWindow).toBeInTheDocument();
    });

    it('hero section has no images requiring alt text (uses CSS for visuals)', () => {
      renderWithProviders(<HeroSection />);

      // Query for any img elements in the hero
      const images = document.querySelectorAll('section[aria-labelledby="hero-heading"] img');

      // If there are images, they should have alt attributes
      // Currently, hero uses CSS mockup instead of images
      if (images.length > 0) {
        images.forEach((img) => {
          const alt = img.getAttribute('alt');
          // Either has descriptive alt text OR is marked decorative with empty alt
          expect(alt !== null).toBe(true);
        });
      } else {
        // No images present - visual is CSS-based (acceptable for decorative content)
        expect(images.length).toBe(0);
      }
    });
  });

  /**
   * Test Case 3: Check feature icons
   * Expected: Feature icons have appropriate aria-labels or alt text
   *
   * The FeaturesSection uses Heroicon SVG components with aria-hidden="true"
   * because the icons are decorative - the feature title and description
   * provide the meaning.
   */
  describe('Test Case 3: Feature icons accessibility', () => {
    it('feature icons are SVGs with aria-hidden="true" (decorative icons)', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('feature-icon');
        const svg = iconContainer.querySelector('svg');

        // SVG icons should be present
        expect(svg).toBeInTheDocument();

        // Decorative icons should have aria-hidden="true"
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });

    it('feature cards provide meaning through text, not just icons', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        // Each card should have a title
        const title = within(card).getByTestId('feature-title');
        expect(title).toBeInTheDocument();
        expect(title.textContent).not.toBe('');

        // Each card should have a description
        const description = within(card).getByTestId('feature-description');
        expect(description).toBeInTheDocument();
        expect(description.textContent).not.toBe('');
      });
    });

    it('all feature icons use Heroicons with proper aria-hidden attribute', () => {
      renderWithProviders(<FeaturesSection />);

      // Get all SVGs within feature icons
      const featureIcons = document.querySelectorAll('[data-testid="feature-icon"] svg');

      expect(featureIcons.length).toBeGreaterThan(0);

      featureIcons.forEach((svg) => {
        // Each SVG icon should be marked as decorative
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  /**
   * Additional tests: HowItWorksSection icons accessibility
   */
  describe('HowItWorksSection icons accessibility', () => {
    it('step icons are marked as decorative with aria-hidden', () => {
      renderWithProviders(<HowItWorksSection />);

      // Icons in HowItWorksSection are inside divs with aria-hidden="true"
      const iconContainers = document.querySelectorAll('[aria-hidden="true"]');

      // The section contains decorative elements marked with aria-hidden
      const svgIcons = document.querySelectorAll('svg');

      svgIcons.forEach((svg) => {
        // SVG icons in HowItWorksSection should either:
        // 1. Have aria-hidden="true" on the SVG itself, OR
        // 2. Be contained within a parent with aria-hidden="true"

        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const parentHasAriaHidden = svg.closest('[aria-hidden="true"]') !== null;

        expect(hasAriaHidden || parentHasAriaHidden).toBe(true);
      });
    });

    it('step numbers have appropriate aria-label', () => {
      renderWithProviders(<HowItWorksSection />);

      // Step number indicators should have aria-label
      const stepNumbers = document.querySelectorAll('[data-testid^="step-number-"]');

      stepNumbers.forEach((stepNumber, index) => {
        expect(stepNumber).toHaveAttribute('aria-label', `Step ${index + 1}`);
      });
    });
  });

  /**
   * Full page accessibility check
   */
  describe('Full landing page image accessibility', () => {
    it('landing page has no images without alt attributes', () => {
      renderWithProviders(<Home />);

      const allImages = document.querySelectorAll('img');

      // Every image must have an alt attribute (can be empty for decorative)
      allImages.forEach((img, index) => {
        expect(img).toHaveAttribute(
          'alt',
          undefined, // any value is acceptable
        );
      });
    });

    it('all SVG icons on the landing page are properly marked as decorative', () => {
      renderWithProviders(<Home />);

      // Get all SVG elements
      const allSvgs = document.querySelectorAll('svg');

      allSvgs.forEach((svg) => {
        // Each SVG should either:
        // 1. Have aria-hidden="true" (decorative)
        // 2. Be inside a button/link with accessible name (interactive)
        // 3. Have role="img" with aria-label (meaningful image)

        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const isInInteractiveElement = svg.closest('button, a') !== null;
        const hasRoleImg = svg.getAttribute('role') === 'img';
        const parentHasAriaHidden = svg.closest('[aria-hidden="true"]') !== null;

        const isAccessible = hasAriaHidden || isInInteractiveElement || hasRoleImg || parentHasAriaHidden;
        expect(isAccessible).toBe(true);
      });
    });

    it('decorative images/icons do not convey essential information', () => {
      renderWithProviders(<Home />);

      // For each feature card, verify that the icon is supplementary
      // (the title and description convey the meaning)
      const featureCards = screen.queryAllByTestId('feature-card');

      featureCards.forEach((card) => {
        // Card must have visible text content
        const textContent = card.textContent;
        expect(textContent).not.toBe('');

        // Icon is decorative, text provides meaning
        const title = within(card).queryByTestId('feature-title');
        const description = within(card).queryByTestId('feature-description');

        if (title) {
          expect(title.textContent).not.toBe('');
        }
        if (description) {
          expect(description.textContent).not.toBe('');
        }
      });
    });
  });
});
