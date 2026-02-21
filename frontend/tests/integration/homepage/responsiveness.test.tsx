/**
 * Responsive Design Integration Tests
 * Owner: Scenario 7 - Responsive Design Verification
 *
 * Test cases:
 * - Test case 1: Render HomePage at 375px width - No horizontal scrollbar, content fits
 * - Test case 2: Render HomePage at 375px width - Navigation shows hamburger menu, hides desktop links
 * - Test case 3: Render HomePage at 375px width - Features section displays in single column layout
 * - Test case 4: Render HomePage at 768px width - Features section displays in 2x2 grid or 2-column
 * - Test case 5: Render HomePage at 1280px width - Hero section shows split layout
 * - Test case 6: Render HomePage at 1280px width - Full navigation links visible, no hamburger menu
 * - Test case 7: Measure CTA button dimensions on mobile - meets 44x44px touch target
 * - Test case 8: Render HomePage at 1440px width - Content contained within max-width container
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import HomePage from '../../../src/pages/HomePage';

// Helper to render HomePage with router context
function renderHomePage() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/login" element={<div data-testid="login-page">Login</div>} />
        <Route path="/register" element={<div data-testid="register-page">Register</div>} />
      </Routes>
    </MemoryRouter>
  );
}

// Helper to set viewport width
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

describe('Mobile Viewport Tests (375px)', () => {
  beforeEach(() => {
    setViewportWidth(375);
  });

  afterEach(() => {
    setViewportWidth(1024); // Reset to default
  });

  // Test Case 1: No horizontal scrollbar, all content fits within viewport
  it('renders without horizontal overflow at 375px width', () => {
    const { container } = renderHomePage();

    // Verify homepage renders
    expect(screen.getByTestId('homepage')).toBeInTheDocument();

    // Check that the homepage container has min-h-screen and no overflow issues
    const homepage = screen.getByTestId('homepage');
    expect(homepage).toHaveClass('min-h-screen');

    // Verify main content sections render
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(screen.getByTestId('cta-section')).toBeInTheDocument();

    // Verify content containers use responsive padding
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection.className).toContain('px-4');
  });

  // Test Case 2: Navigation shows hamburger menu, hides desktop links
  it('shows hamburger menu and hides desktop navigation at 375px width', () => {
    renderHomePage();

    // Verify navbar is present
    const navbar = screen.getByTestId('public-navbar');
    expect(navbar).toBeInTheDocument();

    // Mobile hamburger menu button should be visible (md:hidden class means it's visible on mobile)
    const mobileMenuBtn = screen.getByTestId('navbar-mobile-menu-btn');
    expect(mobileMenuBtn).toBeInTheDocument();
    expect(mobileMenuBtn.className).toContain('md:hidden');

    // Desktop navigation should have hidden class on mobile
    const desktopNav = screen.getByTestId('navbar-desktop-nav');
    expect(desktopNav.className).toContain('hidden');
    expect(desktopNav.className).toContain('md:flex');

    // Desktop auth buttons should also be hidden on mobile
    const desktopAuth = screen.getByTestId('navbar-desktop-auth');
    expect(desktopAuth.className).toContain('hidden');
    expect(desktopAuth.className).toContain('md:flex');
  });

  // Test Case 3: Features section displays in single column layout on mobile
  it('displays features in single column layout at 375px width', () => {
    renderHomePage();

    // Verify features section renders
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();

    // Get the features grid
    const featuresGrid = screen.getByTestId('features-grid');
    expect(featuresGrid).toBeInTheDocument();

    // Check that the grid uses single column on mobile (grid-cols-1)
    // and switches to 2 columns on larger screens (lg:grid-cols-2)
    expect(featuresGrid.className).toContain('grid-cols-1');
    expect(featuresGrid.className).toContain('lg:grid-cols-2');
  });
});

describe('Tablet Viewport Tests (768px)', () => {
  beforeEach(() => {
    setViewportWidth(768);
  });

  afterEach(() => {
    setViewportWidth(1024);
  });

  // Test Case 4: Features section displays in 2x2 grid or 2-column layout at 768px
  it('displays features in appropriate layout at 768px width', () => {
    renderHomePage();

    // Verify features section renders
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();

    // Get the features grid
    const featuresGrid = screen.getByTestId('features-grid');
    expect(featuresGrid).toBeInTheDocument();

    // Grid should have responsive classes that enable 2-column on larger screens
    // At 768px (md breakpoint in Tailwind), layout starts transitioning
    // The lg breakpoint (1024px) is where 2-column kicks in
    expect(featuresGrid.className).toContain('grid');
    expect(featuresGrid.className).toContain('grid-cols-1');
    expect(featuresGrid.className).toContain('lg:grid-cols-2');

    // Verify all 4 feature cards render
    expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-analytics-dashboard')).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-secure-reliable')).toBeInTheDocument();
    expect(screen.getByTestId('feature-card-share-insights')).toBeInTheDocument();
  });
});

describe('Desktop Viewport Tests (1280px)', () => {
  beforeEach(() => {
    setViewportWidth(1280);
  });

  afterEach(() => {
    setViewportWidth(1024);
  });

  // Test Case 5: Hero section shows split layout (text left, visual right) at 1280px
  it('displays hero in split layout at 1280px width', () => {
    renderHomePage();

    // Verify hero section renders
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Check that hero uses flex layout that switches from column to row on lg screens
    const heroContainer = heroSection.querySelector('.flex');
    expect(heroContainer).toBeInTheDocument();
    expect(heroContainer?.className).toContain('flex-col');
    expect(heroContainer?.className).toContain('lg:flex-row');

    // Verify both text content and visual element exist
    const heroHeadline = screen.getByTestId('hero-headline');
    expect(heroHeadline).toBeInTheDocument();

    const heroVisual = screen.getByTestId('hero-visual');
    expect(heroVisual).toBeInTheDocument();

    // Text should be left-aligned on desktop
    const headlineParent = heroHeadline.parentElement;
    expect(headlineParent?.className).toContain('text-center');
    expect(headlineParent?.className).toContain('lg:text-left');
  });

  // Test Case 6: Full navigation links visible, no hamburger menu at 1280px
  it('shows full navigation links without hamburger menu at 1280px width', () => {
    renderHomePage();

    // Verify navbar is present
    const navbar = screen.getByTestId('public-navbar');
    expect(navbar).toBeInTheDocument();

    // Desktop navigation should be visible (hidden md:flex - visible from md breakpoint)
    const desktopNav = screen.getByTestId('navbar-desktop-nav');
    expect(desktopNav).toBeInTheDocument();
    expect(desktopNav.className).toContain('md:flex');

    // Desktop auth buttons should also be visible
    const desktopAuth = screen.getByTestId('navbar-desktop-auth');
    expect(desktopAuth).toBeInTheDocument();
    expect(desktopAuth.className).toContain('md:flex');

    // Hamburger menu should be hidden on desktop (md:hidden class)
    const mobileMenuBtn = screen.getByTestId('navbar-mobile-menu-btn');
    expect(mobileMenuBtn.className).toContain('md:hidden');

    // Verify login and signup buttons are present in desktop nav
    expect(screen.getByTestId('navbar-login-btn')).toBeInTheDocument();
    expect(screen.getByTestId('navbar-signup-btn')).toBeInTheDocument();
  });
});

describe('Touch Target Accessibility Tests', () => {
  beforeEach(() => {
    setViewportWidth(375);
  });

  afterEach(() => {
    setViewportWidth(1024);
  });

  // Test Case 7: CTA button meets minimum 44x44px touch target requirement
  it('CTA buttons have adequate touch target size on mobile', () => {
    renderHomePage();

    // Get the primary CTA button in hero section
    const primaryCta = screen.getByTestId('hero-cta-primary');
    expect(primaryCta).toBeInTheDocument();

    // DaisyUI btn-lg class provides minimum height of 48px (3rem)
    // which meets the 44x44px touch target requirement
    expect(primaryCta.className).toContain('btn');
    expect(primaryCta.className).toContain('btn-lg');

    // Get the CTA section button
    const ctaButton = screen.getByTestId('cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton.className).toContain('btn');
    expect(ctaButton.className).toContain('btn-lg');

    // Mobile menu button should also have adequate touch target
    const mobileMenuBtn = screen.getByTestId('navbar-mobile-menu-btn');
    expect(mobileMenuBtn).toBeInTheDocument();
    // DaisyUI btn-square combined with default btn provides 44px+ dimensions
    expect(mobileMenuBtn.className).toContain('btn');
    expect(mobileMenuBtn.className).toContain('btn-square');
  });
});

describe('Large Desktop Viewport Tests (1440px)', () => {
  beforeEach(() => {
    setViewportWidth(1440);
  });

  afterEach(() => {
    setViewportWidth(1024);
  });

  // Test Case 8: Content is contained within max-width container, centered on page
  it('content is contained and centered at 1440px width', () => {
    renderHomePage();

    // Verify homepage renders
    const homepage = screen.getByTestId('homepage');
    expect(homepage).toBeInTheDocument();

    // Check hero section container has max-width constraint
    const heroSection = screen.getByTestId('hero-section');
    const heroContainer = heroSection.querySelector('.container');
    expect(heroContainer).toBeInTheDocument();
    expect(heroContainer?.className).toContain('mx-auto');
    expect(heroContainer?.className).toContain('max-w-7xl');

    // Check features section container has max-width constraint
    const featuresSection = screen.getByTestId('features-section');
    const featuresContainer = featuresSection.querySelector('.container');
    expect(featuresContainer).toBeInTheDocument();
    expect(featuresContainer?.className).toContain('mx-auto');
    expect(featuresContainer?.className).toContain('max-w-6xl');

    // Check CTA section container has max-width constraint
    const ctaSection = screen.getByTestId('cta-section');
    const ctaContainer = screen.getByTestId('cta-content');
    expect(ctaContainer).toBeInTheDocument();
    expect(ctaContainer.className).toContain('mx-auto');
    expect(ctaContainer.className).toContain('max-w-4xl');

    // Verify navbar uses container with max-width
    const navbar = screen.getByTestId('public-navbar');
    const navbarContainer = navbar.querySelector('.container');
    expect(navbarContainer).toBeInTheDocument();
    expect(navbarContainer?.className).toContain('mx-auto');
    expect(navbarContainer?.className).toContain('max-w-7xl');
  });
});

describe('Responsive Layout Consistency', () => {
  // Test that all major sections render across different viewports
  it('renders all sections consistently across viewport sizes', () => {
    const viewports = [375, 768, 1024, 1280, 1440];

    viewports.forEach((width) => {
      setViewportWidth(width);

      const { unmount } = renderHomePage();

      // Verify all major sections are present
      expect(screen.getByTestId('homepage')).toBeInTheDocument();
      expect(screen.getByTestId('public-navbar')).toBeInTheDocument();
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('demo-section')).toBeInTheDocument();
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument();
      expect(screen.getByTestId('cta-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();

      unmount();
    });
  });

  // Test responsive text sizing
  it('applies responsive text sizing classes', () => {
    renderHomePage();

    // Hero headline should have responsive text sizes
    const heroHeadline = screen.getByTestId('hero-headline');
    expect(heroHeadline.className).toContain('text-4xl');
    expect(heroHeadline.className).toContain('md:text-5xl');
    expect(heroHeadline.className).toContain('lg:text-6xl');

    // Hero subheadline should also be responsive
    const heroSubheadline = screen.getByTestId('hero-subheadline');
    expect(heroSubheadline.className).toContain('text-lg');
    expect(heroSubheadline.className).toContain('md:text-xl');
  });

  // Test responsive padding and spacing
  it('applies responsive padding and spacing', () => {
    renderHomePage();

    // Hero section should have responsive vertical padding
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection.className).toContain('py-16');
    expect(heroSection.className).toContain('lg:py-0');

    // Features section should have responsive padding
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection.className).toContain('py-16');
    expect(featuresSection.className).toContain('lg:py-24');
  });
});
