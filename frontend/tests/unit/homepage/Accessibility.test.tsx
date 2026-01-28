/**
 * Homepage Accessibility Unit Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Unit tests verifying accessibility requirements for the homepage components.
 * Tests WCAG 2.1 AA compliance including:
 * - Landmark regions (main, nav, footer)
 * - Alt text for images
 * - Heading hierarchy
 * - ARIA attributes
 *
 * Test Cases:
 * TC5: Check for proper landmark regions
 * TC6: Verify images have alt attributes
 * TC7: Check heading hierarchy
 *
 * Requirements: NFR-2 (WCAG 2.1 AA compliance)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import Home from '../../../src/pages/Home';
import { HeroSection } from '../../../src/components/homepage';
import { FeaturesSection } from '../../../src/components/homepage';
import { CTASection } from '../../../src/components/homepage';
import { Footer } from '../../../src/components/homepage';

// Mock the API module
vi.mock('../../../src/api', () => ({
  getCurrentUser: vi.fn().mockRejectedValue(new Error('Not authenticated')),
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
  default: {
    interceptors: {
      request: { use: vi.fn() },
      response: { use: vi.fn() },
    },
  },
}));

// Test wrapper with all providers
interface TestWrapperProps {
  children: React.ReactNode;
}

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

const TestWrapper = ({ children }: TestWrapperProps) => {
  const testQueryClient = createTestQueryClient();

  return (
    <QueryClientProvider client={testQueryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter>{children}</MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
};

describe('Accessibility Compliance - Landmark Regions (TC5)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * TC5: Check for proper landmark regions
   * Expected: Page has main, nav, and footer landmarks
   */
  it('should have a main landmark region', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify main landmark exists
    const mainLandmark = screen.getByRole('main');
    expect(mainLandmark).toBeInTheDocument();
  });

  it('should have at least one navigation landmark region', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify at least one navigation landmark exists (navbar and footer nav)
    const navLandmarks = screen.getAllByRole('navigation');
    expect(navLandmarks.length).toBeGreaterThanOrEqual(1);
  });

  it('should have a contentinfo (footer) landmark region', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify contentinfo (footer) landmark exists
    const footerLandmark = screen.getByRole('contentinfo');
    expect(footerLandmark).toBeInTheDocument();
  });

  it('should have all three required landmark regions on homepage', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify all landmarks are present
    expect(screen.getByRole('main')).toBeInTheDocument();
    // There may be multiple navigation elements (navbar and footer nav)
    expect(screen.getAllByRole('navigation').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByRole('contentinfo')).toBeInTheDocument();
  });

  it('should have proper section labeling with aria-labelledby', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Features section should have aria-labelledby pointing to its heading
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    // CTA section should have aria-labelledby pointing to its heading
    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-heading');
  });
});

describe('Accessibility Compliance - Alt Text for Images (TC6)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * TC6: Verify images have alt attributes
   * Expected: All img elements have meaningful alt text or empty alt for decorative
   */
  it('should have alt attributes on all img elements', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Get all images on the page
    const images = document.querySelectorAll('img');

    // Each image should have an alt attribute (can be empty for decorative images)
    images.forEach((img) => {
      expect(img).toHaveAttribute('alt');
    });
  });

  it('should have accessible icon containers in feature cards', () => {
    render(
      <TestWrapper>
        <FeaturesSection />
      </TestWrapper>
    );

    // Icons are SVGs, they should be decorative (in icon containers)
    // The text content provides the meaning, so icons don't need alt text
    const featureIcons = screen.getAllByTestId('feature-icon');
    expect(featureIcons.length).toBeGreaterThan(0);

    // Each icon container should exist and contain an SVG
    featureIcons.forEach((iconContainer) => {
      const svg = iconContainer.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });

  it('should not have images without alt attributes', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Query for any img without alt attribute
    const imagesWithoutAlt = document.querySelectorAll('img:not([alt])');
    expect(imagesWithoutAlt.length).toBe(0);
  });
});

describe('Accessibility Compliance - Heading Hierarchy (TC7)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * TC7: Check heading hierarchy
   * Expected: One h1 exists, headings don't skip levels
   */
  it('should have exactly one h1 element on the homepage', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // There should be exactly one h1 (the main headline)
    const h1Elements = screen.getAllByRole('heading', { level: 1 });
    expect(h1Elements).toHaveLength(1);
  });

  it('should have h1 as the main headline in HeroSection', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    );

    const h1 = screen.getByRole('heading', { level: 1 });
    expect(h1).toBeInTheDocument();
    expect(h1.textContent?.toLowerCase()).toContain('shorten');
  });

  it('should have h2 elements for section headings', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Should have h2 elements for Features and CTA sections
    const h2Elements = screen.getAllByRole('heading', { level: 2 });
    expect(h2Elements.length).toBeGreaterThanOrEqual(2);
  });

  it('should have h3 elements for feature card titles', () => {
    render(
      <TestWrapper>
        <FeaturesSection />
      </TestWrapper>
    );

    // Feature cards should use h3 for their titles
    const h3Elements = screen.getAllByRole('heading', { level: 3 });
    expect(h3Elements.length).toBeGreaterThanOrEqual(3);
  });

  it('should not skip heading levels (no h3 without h2, no h4 without h3)', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Get all headings and their levels
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels: number[] = [];
    headings.forEach((h) => {
      const level = parseInt(h.tagName.charAt(1), 10);
      levels.push(level);
    });

    // First heading should be h1
    expect(levels[0]).toBe(1);

    // Check that no level is skipped by more than 1
    for (let i = 1; i < levels.length; i++) {
      const diff = levels[i] - levels[i - 1];
      // Going deeper should not skip more than 1 level
      expect(diff).toBeLessThanOrEqual(1);
    }
  });

  it('should have meaningful text in all headings', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    const headings = screen.getAllByRole('heading');

    headings.forEach((heading) => {
      // Each heading should have non-empty text content
      expect(heading.textContent?.trim().length).toBeGreaterThan(0);
    });
  });
});

describe('Accessibility Compliance - ARIA Attributes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should have aria-label on footer navigation', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    );

    // Footer should have aria-labeled navigation
    const footerNav = screen.getByLabelText('Footer navigation');
    expect(footerNav).toBeInTheDocument();
  });

  it('should have properly labeled interactive elements', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Buttons should have accessible names
    const buttons = screen.getAllByRole('button');
    buttons.forEach((button) => {
      // Button should have accessible name (text content or aria-label)
      const accessibleName =
        button.textContent?.trim() || button.getAttribute('aria-label');
      expect(accessibleName?.length).toBeGreaterThan(0);
    });
  });

  it('should have links with descriptive text', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Get all links
    const links = screen.getAllByRole('link');

    links.forEach((link) => {
      // Link should have accessible name (text content or aria-label)
      const accessibleName =
        link.textContent?.trim() || link.getAttribute('aria-label');
      // Links should not be empty or just have generic text
      expect(accessibleName?.length).toBeGreaterThan(0);
    });
  });
});

describe('Accessibility Compliance - Semantic HTML Structure', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should use section elements with proper labeling', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Hero, Features, and CTA sections should be present
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(screen.getByTestId('cta-section')).toBeInTheDocument();
  });

  it('should use appropriate HTML elements for content', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Check for proper semantic elements
    const mainContent = screen.getByRole('main');
    expect(mainContent).toBeInTheDocument();

    // Navigation should be a nav element (may have multiple)
    const navs = screen.getAllByRole('navigation');
    expect(navs.length).toBeGreaterThanOrEqual(1);
    navs.forEach(nav => {
      expect(nav.tagName.toLowerCase()).toBe('nav');
    });

    // Footer should have role contentinfo
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('should have footer links as proper anchor elements', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    );

    // Footer links should be anchor elements
    const termsLink = screen.getByRole('link', { name: /terms of service/i });
    expect(termsLink.tagName.toLowerCase()).toBe('a');

    const privacyLink = screen.getByRole('link', { name: /privacy policy/i });
    expect(privacyLink.tagName.toLowerCase()).toBe('a');
  });
});

describe('Accessibility Compliance - Focus Management', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should have all interactive elements focusable', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Get all interactive elements
    const links = screen.getAllByRole('link');
    const buttons = screen.getAllByRole('button');

    // All links should be focusable (no tabindex=-1 unless intentional)
    links.forEach((link) => {
      const tabIndex = link.getAttribute('tabindex');
      // tabindex should be null (default) or >= 0
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    });

    // All buttons should be focusable
    buttons.forEach((button) => {
      const tabIndex = button.getAttribute('tabindex');
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    });
  });

  it('should have visible text or aria-label for all buttons', () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    const buttons = screen.getAllByRole('button');

    buttons.forEach((button) => {
      // Check that button has accessible name
      const hasVisibleText = button.textContent && button.textContent.trim().length > 0;
      const hasAriaLabel = button.hasAttribute('aria-label');

      expect(hasVisibleText || hasAriaLabel).toBe(true);
    });
  });
});
