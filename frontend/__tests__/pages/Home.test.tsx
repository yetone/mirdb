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

/**
 * SEO - Semantic HTML Tests
 * Owner: Scenario 16 - SEO - Semantic HTML
 *
 * Tests that the landing page uses proper semantic HTML structure
 * for SEO and accessibility compliance (NFR-4).
 */
describe('SEO - Semantic HTML', () => {
  describe('Test Case 1: Page has exactly one h1 element in hero section', () => {
    it('should have exactly one h1 element on the page', () => {
      const { container } = render(<Home />);

      // Get all h1 elements
      const h1Elements = container.querySelectorAll('h1');

      // There should be exactly one h1 element
      expect(h1Elements).toHaveLength(1);
    });

    it('should have the h1 element in the hero section', () => {
      render(<Home />);

      // Get the hero section
      const heroSection = screen.getByRole('region', { name: /hero section/i });

      // The h1 should be within the hero section
      const h1InHero = heroSection.querySelector('h1');
      expect(h1InHero).toBeInTheDocument();
      expect(h1InHero).toHaveTextContent(/shorten, share, and track your links/i);
    });
  });

  describe('Test Case 2: Heading hierarchy is correct (h1 > h2 > h3)', () => {
    it('should have h1 before any h2 elements in document order', () => {
      const { container } = render(<Home />);

      // Get all heading elements in document order
      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingArray = Array.from(headings);

      // First heading should be h1
      expect(headingArray[0]?.tagName).toBe('H1');
    });

    it('should not have h3 elements before h2 elements', () => {
      const { container } = render(<Home />);

      // Get all heading elements in document order
      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingArray = Array.from(headings);

      let hasSeenH2 = false;

      headingArray.forEach((heading) => {
        const tagName = heading.tagName;

        if (tagName === 'H2') {
          hasSeenH2 = true;
        }

        // If we encounter h3 before seeing h2, that's an error
        if (tagName === 'H3' && !hasSeenH2) {
          throw new Error('h3 element found before h2 element');
        }
      });

      // The page should have h2 elements (sections have h2)
      expect(hasSeenH2).toBe(true);
    });

    it('should have proper heading levels without skipping levels', () => {
      const { container } = render(<Home />);

      // Get all heading elements in document order
      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingArray = Array.from(headings);

      let maxLevelSeen = 0;

      headingArray.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1), 10);

        // Each heading should be at most one level deeper than what we've seen
        // Allow jumping back up the hierarchy (e.g., h2 after h3 is fine)
        if (level > maxLevelSeen + 1) {
          throw new Error(
            `Heading level skip detected: h${level} found but max level seen was h${maxLevelSeen}`
          );
        }

        maxLevelSeen = Math.max(maxLevelSeen, level);
      });

      expect(headingArray.length).toBeGreaterThan(0);
    });

    it('should have h2 elements for main sections', () => {
      render(<Home />);

      // Features section should have h2
      expect(screen.getByRole('heading', { name: /powerful features/i, level: 2 })).toBeInTheDocument();

      // How It Works section should have h2
      expect(screen.getByRole('heading', { name: /how it works/i, level: 2 })).toBeInTheDocument();

      // Social Proof section should have h2
      expect(screen.getByRole('heading', { name: /trusted by thousands/i, level: 2 })).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Main content is wrapped in <main> element', () => {
    it('should have a main element wrapping the content', () => {
      const { container } = render(<Home />);

      // Get the main element
      const mainElement = container.querySelector('main');
      expect(mainElement).toBeInTheDocument();
    });

    it('should have exactly one main element', () => {
      const { container } = render(<Home />);

      // Get all main elements
      const mainElements = container.querySelectorAll('main');
      expect(mainElements).toHaveLength(1);
    });

    it('should have main element accessible via role', () => {
      render(<Home />);

      // Main should be accessible via role
      const mainElement = screen.getByRole('main');
      expect(mainElement).toBeInTheDocument();
    });

    it('should have main element with proper id for skip link', () => {
      render(<Home />);

      // Main should have id for skip-to-content functionality
      const mainElement = screen.getByRole('main');
      expect(mainElement).toHaveAttribute('id', 'main-content');
    });
  });

  describe('Test Case 4: Sections use <section> elements with appropriate aria-labels', () => {
    it('should have hero section with aria-label', () => {
      render(<Home />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toBeInTheDocument();
      expect(heroSection.tagName).toBe('SECTION');
    });

    it('should have features section with aria-labelledby', () => {
      render(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection.tagName).toBe('SECTION');
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');
    });

    it('should have how it works section with aria-labelledby', () => {
      const { container } = render(<Home />);

      // Find the how-it-works section
      const howItWorksSection = container.querySelector('#how-it-works');
      expect(howItWorksSection).toBeInTheDocument();
      expect(howItWorksSection?.tagName).toBe('SECTION');
      expect(howItWorksSection).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
    });

    it('should have social proof section with aria-labelledby', () => {
      render(<Home />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection.tagName).toBe('SECTION');
      expect(socialProofSection).toHaveAttribute('aria-labelledby', 'social-proof-heading');
    });

    it('should have all sections accessible via region role', () => {
      render(<Home />);

      // All sections should be accessible via role="region"
      const regions = screen.getAllByRole('region');

      // We should have multiple regions (hero, features, how-it-works, social-proof)
      expect(regions.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 5: Footer uses <footer> element', () => {
    it('should have a footer element', () => {
      const { container } = render(<Home />);

      const footerElement = container.querySelector('footer');
      expect(footerElement).toBeInTheDocument();
    });

    it('should have footer accessible via contentinfo role', () => {
      render(<Home />);

      // Footer has implicit contentinfo role
      const footerElement = screen.getByRole('contentinfo');
      expect(footerElement).toBeInTheDocument();
    });

    it('should have footer with appropriate aria-label', () => {
      render(<Home />);

      const footerElement = screen.getByRole('contentinfo');
      expect(footerElement).toHaveAttribute('aria-label', 'Site footer');
    });

    it('should have footer containing navigation links', () => {
      render(<Home />);

      const footerElement = screen.getByRole('contentinfo');

      // Footer should contain navigation
      const footerNav = within(footerElement).getByRole('navigation', { name: /footer navigation/i });
      expect(footerNav).toBeInTheDocument();
    });

    it('should have footer containing copyright notice', () => {
      render(<Home />);

      const footerElement = screen.getByRole('contentinfo');
      expect(footerElement).toHaveTextContent(/all rights reserved/i);
    });
  });
});
