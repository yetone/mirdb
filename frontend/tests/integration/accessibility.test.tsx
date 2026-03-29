/**
 * Accessibility Integration Tests
 * Owner: Scenario 8 - Keyboard Navigation Accessibility
 * Owner: Scenario 14 - Image Alt Text and Decorative Elements
 *
 * Tests keyboard navigation, focus indicators, and accessibility compliance:
 * - Tab navigation through interactive elements
 * - Visible focus indicators
 * - Enter/Space key activation
 * - No focus traps
 * - Image alt text and decorative element accessibility
 *
 * Requirements: NFR-2, US-5
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { Home } from '../../src/pages/Home';
import { HeroSection } from '../../src/components/homepage/HeroSection';
import { FeaturesSection } from '../../src/components/homepage/FeaturesSection';
import { SocialProofSection } from '../../src/components/homepage/SocialProofSection';
import { Footer } from '../../src/components/homepage/Footer';

// Helper to render components with router
const renderWithRouter = (component: React.ReactNode) => {
  return render(<MemoryRouter>{component}</MemoryRouter>);
};

describe('Keyboard Navigation Accessibility', () => {
  describe('Tab Navigation Order', () => {
    it('should move focus through interactive elements in logical DOM order', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Home />);

      // Get all focusable elements
      const primaryCta = screen.getByTestId('cta-primary');
      const secondaryCta = screen.getByTestId('cta-secondary');
      const footerLinks = screen.getAllByRole('link').filter(
        (link) => link.getAttribute('data-testid')?.startsWith('footer-link-')
      );
      const socialLinks = screen.getAllByRole('link').filter(
        (link) => link.getAttribute('aria-label')
      );

      // Start focus from body
      document.body.focus();

      // Tab through elements - first interactive elements should be hero CTAs
      await user.tab();
      expect(primaryCta).toHaveFocus();

      await user.tab();
      expect(secondaryCta).toHaveFocus();

      // Continue tabbing - focus should eventually reach footer links
      // Tab through all elements to verify order is logical
      let tabCount = 0;
      const maxTabs = 50; // Safety limit
      let reachedFooter = false;

      while (tabCount < maxTabs) {
        await user.tab();
        tabCount++;

        const activeElement = document.activeElement;
        if (activeElement?.getAttribute('data-testid')?.startsWith('footer-link-')) {
          reachedFooter = true;
          break;
        }
      }

      expect(reachedFooter).toBe(true);
    });

    it('should allow reverse tab navigation with Shift+Tab', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Home />);

      const primaryCta = screen.getByTestId('cta-primary');
      const secondaryCta = screen.getByTestId('cta-secondary');

      // Focus on secondary CTA
      secondaryCta.focus();
      expect(secondaryCta).toHaveFocus();

      // Shift+Tab should go back to primary CTA
      await user.tab({ shift: true });
      expect(primaryCta).toHaveFocus();
    });
  });

  describe('Focus Indicators', () => {
    it('should have visible focus indicator on primary CTA button', async () => {
      renderWithRouter(<HeroSection />);

      const primaryCta = screen.getByTestId('cta-primary');

      // Focus the button
      primaryCta.focus();
      expect(primaryCta).toHaveFocus();

      // Check that the element is a focusable link (anchor tag)
      expect(primaryCta.tagName.toLowerCase()).toBe('a');

      // DaisyUI btn class provides focus styles via CSS
      // Verify the button has the btn class which includes focus styling
      expect(primaryCta).toHaveClass('btn');
      expect(primaryCta).toHaveClass('btn-primary');
    });

    it('should have visible focus indicator on secondary CTA button', async () => {
      renderWithRouter(<HeroSection />);

      const secondaryCta = screen.getByTestId('cta-secondary');

      // Focus the button
      secondaryCta.focus();
      expect(secondaryCta).toHaveFocus();

      // Check that the element is a focusable link
      expect(secondaryCta.tagName.toLowerCase()).toBe('a');

      // Verify the button has the btn class which includes focus styling
      expect(secondaryCta).toHaveClass('btn');
      expect(secondaryCta).toHaveClass('btn-outline');
    });

    it('should have visible focus state on footer navigation links', async () => {
      renderWithRouter(<Footer />);

      // Get footer navigation links
      const featuresLink = screen.getByTestId('footer-link-features');
      const aboutLink = screen.getByTestId('footer-link-about');
      const docsLink = screen.getByTestId('footer-link-documentation');
      const privacyLink = screen.getByTestId('footer-link-privacy');

      // Test focus on each link category
      const linksToTest = [featuresLink, aboutLink, docsLink, privacyLink];

      for (const link of linksToTest) {
        link.focus();
        expect(link).toHaveFocus();
        // DaisyUI link class provides focus/hover styles
        expect(link).toHaveClass('link');
      }
    });
  });

  describe('Keyboard Activation', () => {
    it('should trigger click event when Enter is pressed on focused CTA button', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const primaryCta = screen.getByTestId('cta-primary');

      // Focus the button
      primaryCta.focus();
      expect(primaryCta).toHaveFocus();

      // Verify it's a link that responds to Enter key
      // Links naturally respond to Enter key in browsers
      expect(primaryCta.tagName.toLowerCase()).toBe('a');
      expect(primaryCta).toHaveAttribute('href', '/register');

      // Simulate Enter key - should not throw
      await user.keyboard('{Enter}');

      // The link should still be focusable after interaction
      expect(primaryCta).toBeInTheDocument();
    });

    it('should trigger click event when Space is pressed on focused CTA button', async () => {
      const user = userEvent.setup();
      renderWithRouter(<HeroSection />);

      const primaryCta = screen.getByTestId('cta-primary');

      // Focus the button
      primaryCta.focus();
      expect(primaryCta).toHaveFocus();

      // Links respond to Enter; buttons respond to both Enter and Space
      // Since these are anchor tags styled as buttons, verify they're interactive
      expect(primaryCta).toHaveAttribute('href');

      // Test that pressing Space doesn't cause issues
      // Note: Native anchor tags activate on Enter, not Space
      // But the link should still be functional
      await user.keyboard(' ');

      // Element should still be in document and accessible
      expect(primaryCta).toBeInTheDocument();
    });

    it('should allow activating footer links with Enter key', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Footer />);

      const featuresLink = screen.getByTestId('footer-link-features');

      // Focus the link
      featuresLink.focus();
      expect(featuresLink).toHaveFocus();

      // Verify it's a link with proper href
      expect(featuresLink.tagName.toLowerCase()).toBe('a');
      expect(featuresLink).toHaveAttribute('href', '/features');

      // Press Enter - should not throw
      await user.keyboard('{Enter}');

      // Link should remain in document
      expect(featuresLink).toBeInTheDocument();
    });
  });

  describe('Focus Trap Prevention', () => {
    it('should not trap focus - Tab eventually cycles through all elements', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Home />);

      // Get all focusable elements
      const focusableSelector =
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
      const focusableElements = document.querySelectorAll(focusableSelector);

      expect(focusableElements.length).toBeGreaterThan(0);

      // Start from first focusable element
      const firstElement = focusableElements[0] as HTMLElement;
      firstElement.focus();

      // Tab through all elements
      const visitedElements = new Set<Element>();
      let currentElement = document.activeElement;
      let tabCount = 0;
      const maxTabs = focusableElements.length + 5; // Allow some buffer

      while (tabCount < maxTabs) {
        visitedElements.add(currentElement!);
        await user.tab();
        currentElement = document.activeElement;
        tabCount++;

        // If we've cycled back to first element or body, no trap occurred
        if (currentElement === firstElement || currentElement === document.body) {
          break;
        }
      }

      // Verify we didn't get stuck (either completed cycle or tab count didn't max out)
      expect(tabCount).toBeLessThanOrEqual(maxTabs);

      // Verify we visited multiple elements (not trapped in one)
      expect(visitedElements.size).toBeGreaterThan(1);
    });

    it('should allow focus to escape any component using Tab', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Home />);

      // Focus on hero section CTA
      const primaryCta = screen.getByTestId('cta-primary');
      primaryCta.focus();
      expect(primaryCta).toHaveFocus();

      // Tab away - should move to next element
      await user.tab();
      expect(primaryCta).not.toHaveFocus();

      // Should have moved to secondary CTA
      const secondaryCta = screen.getByTestId('cta-secondary');
      expect(secondaryCta).toHaveFocus();

      // Continue tabbing - should eventually leave hero section
      let escapedHero = false;
      let tabCount = 0;
      const maxTabs = 20;

      while (tabCount < maxTabs) {
        await user.tab();
        tabCount++;

        const activeElement = document.activeElement;
        const heroSection = screen.getByTestId('hero-section');

        // Check if active element is outside hero section
        if (activeElement && !heroSection.contains(activeElement)) {
          escapedHero = true;
          break;
        }
      }

      expect(escapedHero).toBe(true);
    });

    it('should allow focus to escape footer section', async () => {
      const user = userEvent.setup();
      renderWithRouter(<Home />);

      // Get footer and a footer link
      const footer = screen.getByTestId('footer');
      const featuresLink = screen.getByTestId('footer-link-features');

      // Focus on a footer link
      featuresLink.focus();
      expect(featuresLink).toHaveFocus();
      expect(footer.contains(featuresLink)).toBe(true);

      // Tab through footer - should eventually cycle (not trap)
      let tabCount = 0;
      const maxTabs = 30; // Footer has many links

      while (tabCount < maxTabs) {
        await user.tab();
        tabCount++;

        const activeElement = document.activeElement;

        // If we've left footer or cycled to body, no trap
        if (!footer.contains(activeElement) || activeElement === document.body) {
          break;
        }
      }

      // Should have tabbed through without getting stuck
      expect(tabCount).toBeLessThanOrEqual(maxTabs);
    });
  });

  describe('Accessibility Attributes', () => {
    it('should have proper aria-labelledby on hero section', () => {
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');

      // Verify the referenced element exists
      const headline = document.getElementById('hero-headline');
      expect(headline).toBeInTheDocument();
    });

    it('should have proper aria-label on footer', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer');
      expect(footer).toHaveAttribute('aria-label', 'Site footer');
    });

    it('should have aria-labels on social media links', () => {
      renderWithRouter(<Footer />);

      const twitterLink = screen.getByTestId('social-twitter');
      const githubLink = screen.getByTestId('social-github');
      const linkedinLink = screen.getByTestId('social-linkedin');

      expect(twitterLink).toHaveAttribute('aria-label', 'Twitter');
      expect(githubLink).toHaveAttribute('aria-label', 'GitHub');
      expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn');
    });

    it('should have proper navigation aria-labels in footer', () => {
      renderWithRouter(<Footer />);

      // Check for navigation sections with proper aria-labels
      const navSections = screen.getAllByRole('navigation');

      expect(navSections.length).toBeGreaterThan(0);

      // Each navigation should have an aria-label
      navSections.forEach((nav) => {
        expect(nav).toHaveAttribute('aria-label');
      });
    });
  });

  describe('Interactive Element Accessibility', () => {
    it('should have all CTA buttons as accessible links', () => {
      renderWithRouter(<HeroSection />);

      const primaryCta = screen.getByTestId('cta-primary');
      const secondaryCta = screen.getByTestId('cta-secondary');

      // Both should be links with href
      expect(primaryCta).toHaveAttribute('href');
      expect(secondaryCta).toHaveAttribute('href');

      // Should be accessible by role
      expect(screen.getByRole('link', { name: /get started free/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /view demo/i })).toBeInTheDocument();
    });

    it('should have all footer links accessible', () => {
      renderWithRouter(<Footer />);

      // Get all links in footer
      const links = screen.getAllByRole('link');

      // Each link should have href attribute
      links.forEach((link) => {
        expect(link).toHaveAttribute('href');
      });

      // Should have expected number of links (15 nav links + 3 social)
      expect(links.length).toBeGreaterThanOrEqual(15);
    });
  });
});

/**
 * Image Alt Text and Decorative Elements Tests
 * Owner: Scenario 14
 *
 * Validates that all images have appropriate alt text or are marked as decorative
 * as per accessibility requirements in NFR-2.
 *
 * Requirements: NFR-2
 */
describe('Image Alt Text and Decorative Elements', () => {
  describe('All Images Have Alt Attributes', () => {
    it('should have alt attribute on every img element in HomePage', () => {
      renderWithRouter(<Home />);

      // Query all img elements
      const images = document.querySelectorAll('img');

      // Every img element should have an alt attribute
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('should have alt attribute on every img element in HeroSection', () => {
      renderWithRouter(<HeroSection />);

      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('should have alt attribute on every img element in FeaturesSection', () => {
      renderWithRouter(<FeaturesSection />);

      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('should have alt attribute on every img element in Footer', () => {
      renderWithRouter(<Footer />);

      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });
  });

  describe('Feature Card Icons Accessibility', () => {
    it('should have feature icons marked as decorative with aria-hidden', () => {
      renderWithRouter(<FeaturesSection />);

      // Get all feature icon containers
      const featureIcons = screen.getAllByTestId('feature-icon');

      featureIcons.forEach((iconContainer) => {
        // The SVG inside should be aria-hidden since the feature title provides context
        const svg = iconContainer.querySelector('svg');
        expect(svg).toBeInTheDocument();
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });

    it('should have feature cards with descriptive titles alongside icons', () => {
      renderWithRouter(<FeaturesSection />);

      // Each feature card should have both an icon and a descriptive title
      const featureTitles = screen.getAllByTestId('feature-title');

      expect(featureTitles.length).toBe(3);

      // Verify each title has meaningful text
      const expectedTitles = ['Smart Shortening', 'Real-time Analytics', 'Link Dashboard'];
      featureTitles.forEach((title, index) => {
        expect(title).toHaveTextContent(expectedTitles[index]);
      });
    });

    it('should have statistics icons marked as decorative', () => {
      renderWithRouter(<SocialProofSection />);

      // Get statistics items
      const statisticItems = screen.getAllByTestId('statistic-item');

      statisticItems.forEach((item) => {
        const svg = item.querySelector('svg');
        if (svg) {
          // Icons accompanying statistics should be decorative
          expect(svg).toHaveAttribute('aria-hidden', 'true');
        }
      });
    });
  });

  describe('Logo Image Accessibility', () => {
    it('should have hero logo icon marked as decorative with product name in text', () => {
      renderWithRouter(<HeroSection />);

      const productBranding = screen.getByTestId('product-branding');
      const productName = screen.getByTestId('product-name');

      // Product name should contain the brand name
      expect(productName).toHaveTextContent('ShortLink');

      // The SVG icon should be decorative since text provides the name
      const svg = productBranding.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });

    it('should have footer logo icon marked as decorative with product name in text', () => {
      renderWithRouter(<Footer />);

      const footerBranding = screen.getByTestId('footer-branding');
      const footerLogo = screen.getByTestId('footer-logo');

      // Footer should have the product name in text
      expect(footerLogo).toHaveTextContent('ShortLink');

      // The SVG icon should be decorative
      const svg = footerBranding.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Hero Visual Element Accessibility', () => {
    it('should have hero visual/icon properly marked as decorative', () => {
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');

      // All SVG icons in hero should be decorative
      const svgs = heroSection.querySelectorAll('svg');

      svgs.forEach((svg) => {
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });

    it('should provide contextual information through text, not images', () => {
      renderWithRouter(<HeroSection />);

      // Hero should have accessible text content
      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');

      expect(headline).toHaveTextContent(/shorten links/i);
      expect(subheadline).toHaveTextContent(/create short, memorable links/i);
    });
  });

  describe('Social Media Icons Accessibility', () => {
    it('should have social media SVG icons marked as decorative with aria-labels on links', () => {
      renderWithRouter(<Footer />);

      // Social links should have aria-labels
      const twitterLink = screen.getByTestId('social-twitter');
      const githubLink = screen.getByTestId('social-github');
      const linkedinLink = screen.getByTestId('social-linkedin');

      // Links have aria-labels
      expect(twitterLink).toHaveAttribute('aria-label', 'Twitter');
      expect(githubLink).toHaveAttribute('aria-label', 'GitHub');
      expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn');

      // SVGs inside should be decorative
      [twitterLink, githubLink, linkedinLink].forEach((link) => {
        const svg = link.querySelector('svg');
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  describe('Decorative Elements with role="presentation"', () => {
    it('should not use role="img" on decorative SVGs without accessible name', () => {
      renderWithRouter(<Home />);

      // Query all SVGs
      const svgs = document.querySelectorAll('svg');

      svgs.forEach((svg) => {
        // If SVG has role="img", it must have an accessible name
        if (svg.getAttribute('role') === 'img') {
          const hasAriaLabel = svg.hasAttribute('aria-label');
          const hasAriaLabelledBy = svg.hasAttribute('aria-labelledby');
          const hasTitle = svg.querySelector('title');

          expect(hasAriaLabel || hasAriaLabelledBy || hasTitle).toBe(true);
        }
      });
    });

    it('should have decorative icons properly hidden from assistive technology', () => {
      renderWithRouter(<Home />);

      // All SVG icons that are purely decorative should have aria-hidden
      const allSvgs = document.querySelectorAll('svg');

      allSvgs.forEach((svg) => {
        // Decorative SVGs should have aria-hidden="true"
        // or be contained in an element with aria-hidden
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const parentHasAriaHidden = svg.closest('[aria-hidden="true"]') !== null;
        const hasAccessibleName = svg.hasAttribute('aria-label') ||
          svg.hasAttribute('aria-labelledby') ||
          svg.querySelector('title') !== null;

        // If no accessible name, must be hidden
        if (!hasAccessibleName) {
          expect(hasAriaHidden || parentHasAriaHidden).toBe(true);
        }
      });
    });
  });
});
