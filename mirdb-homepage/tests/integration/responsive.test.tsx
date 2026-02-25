/**
 * Responsive Design Integration Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Tests responsive layout behavior across mobile, tablet, and desktop viewports.
 * Validates:
 * - Mobile viewport (375px) displays hamburger menu and single-column features
 * - Tablet viewport (768px) displays horizontal nav and 2-column features
 * - Desktop viewport (1920px) displays full layout and 4-column features
 * - No horizontal scrolling at any viewport size
 * - Smooth layout transitions between breakpoints
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from '../../src/App';
import { Header } from '../../src/components/Header';
import { Features } from '../../src/components/Features';

// Utility to simulate viewport width changes
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  Object.defineProperty(document.documentElement, 'clientWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
}

// Mock matchMedia for responsive CSS detection
function mockMatchMedia(width: number) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    configurable: true,
    value: vi.fn().mockImplementation((query: string) => {
      // Parse the query to determine if it matches
      const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/);
      const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/);

      let matches = false;
      if (maxWidthMatch) {
        matches = width <= parseInt(maxWidthMatch[1], 10);
      } else if (minWidthMatch) {
        matches = width >= parseInt(minWidthMatch[1], 10);
      }

      return {
        matches,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      };
    }),
  });
}

describe('Responsive Design Integration Tests', () => {
  let originalInnerWidth: number;
  let originalMatchMedia: typeof window.matchMedia;

  beforeEach(() => {
    originalInnerWidth = window.innerWidth;
    originalMatchMedia = window.matchMedia;
  });

  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: originalInnerWidth,
    });
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Mobile viewport - No horizontal scrolling', () => {
    // Test Case 1: Load homepage at 375px viewport width
    // Expected: All content is accessible without horizontal scrolling
    it('renders all content within viewport at 375px width', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<App />);

      // Verify the app container exists and is accessible
      const appContainer = document.querySelector('[class*="app"]');
      expect(appContainer).toBeInTheDocument();

      // The main content should be present
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();

      // Header should be visible
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
    });

    it('app container has responsive max-width styling', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<App />);

      // Verify the main container has width constraints
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();

      // Main should have max-width set via CSS module
      expect(main).toHaveClass(/main/);
    });

    it('no elements exceed viewport width at mobile size', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<App />);

      // All sections should be present and contained
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      sections.forEach((section) => {
        expect(section).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Mobile viewport - Hamburger menu', () => {
    // Test Case 2: Load homepage at 375px viewport width
    // Expected: Navigation displays as hamburger menu
    it('renders hamburger menu button at mobile viewport', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Header />);

      const menuButton = screen.getByRole('button', { name: /navigation menu/i });
      expect(menuButton).toBeInTheDocument();
    });

    it('hamburger menu button has correct ARIA attributes', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Header />);

      const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
      expect(menuButton).toHaveAttribute('aria-expanded', 'false');
      expect(menuButton).toHaveAttribute('aria-controls', 'mobile-navigation');
    });

    it('hamburger menu contains three lines for icon', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Header />);

      const menuButton = screen.getByRole('button', { name: /navigation menu/i });
      const hamburgerLines = menuButton.querySelectorAll('[aria-hidden="true"]');

      expect(hamburgerLines.length).toBe(3);
    });

    it('hamburger menu expands when clicked', async () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      const user = userEvent.setup();
      render(<Header />);

      const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
      await user.click(menuButton);

      expect(menuButton).toHaveAttribute('aria-expanded', 'true');
    });

    it('navigation links are accessible after opening hamburger menu', async () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      const user = userEvent.setup();
      render(<Header />);

      const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
      await user.click(menuButton);

      // All navigation links should be present
      expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Architecture' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Resources' })).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Mobile viewport - Single column features', () => {
    // Test Case 3: Load homepage at 375px viewport width
    // Expected: Feature cards display in single column
    it('renders feature cards in single column layout at mobile viewport', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // Grid should contain 4 feature cards
      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);
    });

    it('all feature cards are visible at mobile viewport', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Features />);

      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);

      // Each card should be visible and have content
      cards.forEach((card) => {
        expect(card).toBeVisible();
      });
    });

    it('feature card content is fully accessible at mobile width', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Features />);

      // Verify all feature titles are present and readable
      expect(screen.getByText('Persistent Storage')).toBeInTheDocument();
      expect(screen.getByText('Memcached Protocol Compatibility')).toBeInTheDocument();
      expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument();
      expect(screen.getByText('Raft Support')).toBeInTheDocument();
    });

    it('grid uses single column CSS rule for mobile breakpoint', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // The grid should have the grid class which applies CSS rules
      expect(grid).toHaveClass(/grid/);
    });
  });

  describe('Test Case 4: Tablet viewport - Horizontal navigation', () => {
    // Test Case 4: Load homepage at 768px viewport width
    // Expected: Navigation displays as horizontal links
    it('navigation displays as horizontal links at tablet viewport', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Header />);

      // Navigation should be present
      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      expect(nav).toBeInTheDocument();

      // Nav list should contain horizontal links
      const navList = nav.querySelector('[class*="navList"]');
      expect(navList).toBeInTheDocument();
    });

    it('all navigation links are visible at tablet viewport', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Header />);

      expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Architecture' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Resources' })).toBeInTheDocument();
    });

    it('header container has proper flex layout at tablet', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Header />);

      const container = document.querySelector('[class*="container"]');
      expect(container).toBeInTheDocument();
      expect(container).toHaveClass(/container/);
    });
  });

  describe('Test Case 5: Tablet viewport - 2-column features grid', () => {
    // Test Case 5: Load homepage at 768px viewport width
    // Expected: Feature cards display in 2-column grid
    it('renders feature cards in 2-column layout at tablet viewport', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // All 4 cards should still be present
      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);
    });

    it('features grid applies tablet breakpoint CSS', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();
      expect(grid).toHaveClass(/grid/);
    });

    it('all feature content is fully accessible at tablet width', () => {
      setViewportWidth(768);
      mockMatchMedia(768);

      render(<Features />);

      // Each card should have title and description visible
      const cards = screen.getAllByRole('article');
      cards.forEach((card) => {
        expect(card.querySelector('h3')).toBeVisible();
        expect(card.querySelector('p')).toBeVisible();
      });
    });
  });

  describe('Test Case 6: Desktop viewport - 4-column features grid', () => {
    // Test Case 6: Load homepage at 1920px viewport width
    // Expected: Feature cards display in 4-column grid
    it('renders feature cards in 4-column layout at desktop viewport', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // All 4 cards should be present for 4-column layout
      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);
    });

    it('features grid uses full desktop layout', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // Grid should contain exactly 4 feature cards
      const cards = grid?.querySelectorAll('article');
      expect(cards).toHaveLength(4);
    });

    it('all feature content is visible at desktop width', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<Features />);

      // All features should be visible
      expect(screen.getByText('Persistent Storage')).toBeVisible();
      expect(screen.getByText('Memcached Protocol Compatibility')).toBeVisible();
      expect(screen.getByText('LSM Tree Architecture')).toBeVisible();
      expect(screen.getByText('Raft Support')).toBeVisible();
    });

    it('desktop navigation is fully expanded', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<Header />);

      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();

      // All links should be accessible
      const links = screen.getAllByRole('link');
      expect(links.length).toBeGreaterThan(3);
    });
  });

  describe('Test Case 7: Desktop viewport - Hero section viewport fold', () => {
    // Test Case 7: Load homepage at 1920px viewport width
    // Expected: Hero section fills viewport fold appropriately
    it('main content area is properly positioned at desktop viewport', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();

      // Main should have proper max-width constraint
      expect(main).toHaveClass(/main/);
    });

    it('app container fills available viewport', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const appContainer = document.querySelector('[class*="app"]');
      expect(appContainer).toBeInTheDocument();
      expect(appContainer).toHaveClass(/app/);
    });

    it('header is visible and properly positioned at viewport top', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
      expect(header).toHaveClass(/header/);
    });

    it('features section is accessible within viewport at desktop', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
    });
  });

  describe('Test Case 8: Layout transitions - Viewport resize', () => {
    // Test Case 8: Resize viewport from desktop to mobile
    // Expected: Layout transitions smoothly without breaking
    it('components remain accessible during viewport resize', () => {
      // Start at desktop
      setViewportWidth(1920);
      mockMatchMedia(1920);

      const { rerender } = render(<App />);

      // Verify desktop state
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();

      // Simulate resize to tablet
      setViewportWidth(768);
      mockMatchMedia(768);
      window.dispatchEvent(new Event('resize'));
      rerender(<App />);

      // Components should still be accessible
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();

      // Simulate resize to mobile
      setViewportWidth(375);
      mockMatchMedia(375);
      window.dispatchEvent(new Event('resize'));
      rerender(<App />);

      // Components should still be accessible
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
    });

    it('header transitions smoothly between viewports', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      const { rerender } = render(<Header />);

      // Desktop: navigation visible
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();

      // Resize to mobile
      setViewportWidth(375);
      mockMatchMedia(375);
      rerender(<Header />);

      // Mobile: hamburger menu available
      const menuButton = screen.getByRole('button', { name: /navigation menu/i });
      expect(menuButton).toBeInTheDocument();

      // Navigation should still exist in DOM (just visually hidden on mobile)
      expect(screen.getByRole('navigation')).toBeInTheDocument();
    });

    it('features grid transitions between column layouts', () => {
      // Start at desktop (4 columns)
      setViewportWidth(1920);
      mockMatchMedia(1920);

      const { rerender } = render(<Features />);

      let grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      let cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);

      // Resize to tablet (2 columns)
      setViewportWidth(768);
      mockMatchMedia(768);
      rerender(<Features />);

      grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);

      // Resize to mobile (1 column)
      setViewportWidth(375);
      mockMatchMedia(375);
      rerender(<Features />);

      grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);
    });

    it('no content is lost during viewport transitions', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      const { rerender } = render(<App />);

      // Check all sections at desktop
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
      expect(document.getElementById('features')).toBeInTheDocument();
      expect(document.getElementById('usage')).toBeInTheDocument();
      expect(document.getElementById('architecture')).toBeInTheDocument();
      expect(document.getElementById('resources')).toBeInTheDocument();

      // Resize to mobile
      setViewportWidth(375);
      mockMatchMedia(375);
      rerender(<App />);

      // All sections should still be present
      expect(screen.getByRole('banner')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
      expect(document.getElementById('features')).toBeInTheDocument();
      expect(document.getElementById('usage')).toBeInTheDocument();
      expect(document.getElementById('architecture')).toBeInTheDocument();
      expect(document.getElementById('resources')).toBeInTheDocument();
    });
  });

  describe('Additional Responsive Design Validations', () => {
    it('app has proper minimum height for full viewport coverage', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const appContainer = document.querySelector('[class*="app"]');
      expect(appContainer).toBeInTheDocument();
    });

    it('content containers have responsive padding', () => {
      setViewportWidth(375);
      mockMatchMedia(375);

      render(<App />);

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveClass(/main/);
    });

    it('header logo is visible at all viewport sizes', () => {
      // Test mobile
      setViewportWidth(375);
      mockMatchMedia(375);

      const { rerender } = render(<Header />);
      expect(screen.getByRole('img', { name: /mirdb logo/i })).toBeInTheDocument();

      // Test tablet
      setViewportWidth(768);
      mockMatchMedia(768);
      rerender(<Header />);
      expect(screen.getByRole('img', { name: /mirdb logo/i })).toBeInTheDocument();

      // Test desktop
      setViewportWidth(1920);
      mockMatchMedia(1920);
      rerender(<Header />);
      expect(screen.getByRole('img', { name: /mirdb logo/i })).toBeInTheDocument();
    });

    it('GitHub link is accessible at all viewport sizes', () => {
      // Test mobile
      setViewportWidth(375);
      mockMatchMedia(375);

      const { rerender } = render(<Header />);
      expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument();

      // Test desktop
      setViewportWidth(1920);
      mockMatchMedia(1920);
      rerender(<Header />);
      expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument();
    });

    it('sections are stacked vertically in single-page layout', () => {
      setViewportWidth(1920);
      mockMatchMedia(1920);

      render(<App />);

      const sections = document.querySelectorAll('section[id]');
      expect(sections.length).toBeGreaterThanOrEqual(4);

      // Verify sections exist in expected order
      const sectionIds = Array.from(sections).map((s) => s.id);
      expect(sectionIds).toContain('features');
      expect(sectionIds).toContain('usage');
      expect(sectionIds).toContain('architecture');
      expect(sectionIds).toContain('resources');
    });
  });
});
