/**
 * Accessibility Unit Tests for HomePage Components
 * Owner: Scenario 8 - Theme & Accessibility Compliance
 *
 * Test Cases:
 * 7. Inspect semantic HTML structure - Page uses semantic elements
 * 8. Inspect images and icons - All have alt text or aria-hidden
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import HomePage from '../../../src/pages/HomePage';
import HeroSection from '../../../src/components/homepage/HeroSection';
import FeaturesSection from '../../../src/components/homepage/FeaturesSection';
import DemoSection from '../../../src/components/homepage/DemoSection';
import SocialProofSection from '../../../src/components/homepage/SocialProofSection';
import CTASection from '../../../src/components/homepage/CTASection';
import Footer from '../../../src/components/homepage/Footer';
import PublicNavbar from '../../../src/components/homepage/PublicNavbar';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Mock matchMedia for theme detection
beforeEach(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });

  // Mock localStorage
  const localStorageMock = {
    getItem: vi.fn(),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
  };
  Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
    writable: true,
  });
});

describe('Semantic HTML Structure (Test Case 7)', () => {
  it('HomePage uses semantic nav element', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const navElements = document.querySelectorAll('nav');
    expect(navElements.length).toBeGreaterThanOrEqual(1);
  });

  it('HomePage uses semantic main element', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const mainElement = document.querySelector('main');
    expect(mainElement).toBeInTheDocument();
  });

  it('HomePage uses semantic section elements', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const sectionElements = document.querySelectorAll('section');
    // Should have multiple sections: hero, features, demo, social proof, cta
    expect(sectionElements.length).toBeGreaterThanOrEqual(5);
  });

  it('HomePage uses semantic footer element', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const footerElement = document.querySelector('footer');
    expect(footerElement).toBeInTheDocument();
  });

  it('HomePage has proper heading hierarchy with single h1', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const h1Elements = document.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);
  });

  it('HomePage headings follow logical hierarchy', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels = Array.from(headings).map((h) => parseInt(h.tagName[1]));

    // Check that heading levels don't skip
    for (let i = 1; i < levels.length; i++) {
      // Each heading should be same level, one level deeper, or back to a higher level
      expect(levels[i]).toBeLessThanOrEqual(levels[i - 1] + 1);
    }
  });

  it('HeroSection uses section element', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection.tagName.toLowerCase()).toBe('section');
  });

  it('FeaturesSection uses section element with id for navigation', () => {
    render(
      <BrowserRouter>
        <FeaturesSection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('features-section');
    expect(section.tagName.toLowerCase()).toBe('section');
    expect(section).toHaveAttribute('id', 'features');
  });

  it('DemoSection uses section element', () => {
    render(
      <BrowserRouter>
        <DemoSection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('demo-section');
    expect(section.tagName.toLowerCase()).toBe('section');
  });

  it('SocialProofSection uses section element', () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('social-proof-section');
    expect(section.tagName.toLowerCase()).toBe('section');
  });

  it('CTASection uses section element', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('cta-section');
    expect(section.tagName.toLowerCase()).toBe('section');
  });

  it('Footer uses footer element', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const footer = screen.getByTestId('footer');
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  it('Footer navigation has aria-label', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const footerNav = screen.getByTestId('footer-links');
    expect(footerNav).toHaveAttribute('aria-label');
  });
});

describe('Images and Icons Accessibility (Test Case 8)', () => {
  it('decorative icons have aria-hidden attribute in HeroSection', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    // Find all SVG icons within hero section
    const heroSection = screen.getByTestId('hero-section');
    const svgIcons = heroSection.querySelectorAll('svg');

    svgIcons.forEach((svg) => {
      // Decorative icons should have aria-hidden="true"
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('decorative icons have aria-hidden attribute in PublicNavbar', () => {
    render(
      <BrowserRouter>
        <PublicNavbar />
      </BrowserRouter>
    );

    const navbar = screen.getByTestId('public-navbar');
    const svgIcons = navbar.querySelectorAll('svg');

    svgIcons.forEach((svg) => {
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('decorative icons have aria-hidden attribute in DemoSection', () => {
    render(
      <BrowserRouter>
        <DemoSection />
      </BrowserRouter>
    );

    const demoSection = screen.getByTestId('demo-section');
    const svgIcons = demoSection.querySelectorAll('svg');

    svgIcons.forEach((svg) => {
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('decorative icons have aria-hidden attribute in SocialProofSection', () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('social-proof-section');
    const svgIcons = section.querySelectorAll('svg');

    svgIcons.forEach((svg) => {
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('decorative icons have aria-hidden attribute in FeaturesSection', () => {
    render(
      <BrowserRouter>
        <FeaturesSection />
      </BrowserRouter>
    );

    const section = screen.getByTestId('features-section');
    const svgIcons = section.querySelectorAll('svg');

    svgIcons.forEach((svg) => {
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('all images have alt attributes', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const images = document.querySelectorAll('img');

    images.forEach((img) => {
      // All images should have alt attribute (can be empty string for decorative)
      expect(img).toHaveAttribute('alt');
    });
  });

  it('buttons with only icons have accessible labels', () => {
    render(
      <BrowserRouter>
        <PublicNavbar />
      </BrowserRouter>
    );

    // The mobile menu button should have aria-label
    const mobileMenuBtn = screen.getByTestId('navbar-mobile-menu-btn');
    expect(mobileMenuBtn).toHaveAttribute('aria-label');
  });

  it('theme toggle button has accessible label', () => {
    render(
      <BrowserRouter>
        <PublicNavbar />
      </BrowserRouter>
    );

    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toHaveAttribute('aria-label');
  });
});

describe('Skip Link Accessibility', () => {
  it('HomePage has skip-to-main-content link', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const skipLink = screen.getByTestId('skip-to-main');
    expect(skipLink).toBeInTheDocument();
    expect(skipLink).toHaveAttribute('href', '#main-content');
  });

  it('main content has id for skip link target', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const mainContent = document.getElementById('main-content');
    expect(mainContent).toBeInTheDocument();
    expect(mainContent?.tagName.toLowerCase()).toBe('main');
  });

  it('skip link has sr-only class for screen readers', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const skipLink = screen.getByTestId('skip-to-main');
    expect(skipLink).toHaveClass('sr-only');
  });
});

describe('Focus Management', () => {
  it('main content is focusable for skip navigation', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const mainContent = document.getElementById('main-content');
    expect(mainContent).toHaveAttribute('tabindex', '-1');
  });

  it('all buttons are keyboard accessible', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const buttons = document.querySelectorAll('button');

    buttons.forEach((button) => {
      // Buttons should not have negative tabindex (except -1 for programmatic focus)
      const tabIndex = button.getAttribute('tabindex');
      if (tabIndex) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(-1);
      }
    });
  });

  it('all links are keyboard accessible', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const links = document.querySelectorAll('a');

    links.forEach((link) => {
      // Links should have href attribute
      expect(link).toHaveAttribute('href');
    });
  });
});
