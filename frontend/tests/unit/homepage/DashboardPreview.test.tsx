/**
 * Unit tests for DashboardPreview component
 * Owner: Scenario 7 - Dashboard Preview Section
 *
 * Tests:
 * 1. Section contains an image or visual mockup element
 * 2. Preview section has a CTA button (e.g., 'Start Tracking Your Links')
 * 3. Dashboard preview image has descriptive alt attribute
 */

import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from './test-utils';
import { DashboardPreview } from '../../../src/components/homepage/DashboardPreview';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: any) => <div {...props}>{children}</div>,
    h2: ({ children, ...props }: any) => <h2 {...props}>{children}</h2>,
    p: ({ children, ...props }: any) => <p {...props}>{children}</p>,
  },
}));

describe('DashboardPreview', () => {
  describe('Test Case 1: Section contains an image or visual mockup element', () => {
    it('should render a dashboard preview image', () => {
      renderWithProviders(<DashboardPreview />);

      const previewImage = screen.getByTestId('dashboard-preview-image');
      expect(previewImage).toBeInTheDocument();
      expect(previewImage.tagName.toLowerCase()).toBe('img');
    });

    it('should render the dashboard preview section', () => {
      renderWithProviders(<DashboardPreview />);

      const section = screen.getByTestId('dashboard-preview-section');
      expect(section).toBeInTheDocument();
    });

    it('should have an image with a src attribute', () => {
      renderWithProviders(<DashboardPreview />);

      const previewImage = screen.getByTestId('dashboard-preview-image');
      expect(previewImage).toHaveAttribute('src');
      expect(previewImage.getAttribute('src')).toBeTruthy();
    });
  });

  describe('Test Case 2: Preview section has a CTA button', () => {
    it('should render a CTA button with "Start Tracking Your Links" text', () => {
      renderWithProviders(<DashboardPreview />);

      const ctaButton = screen.getByTestId('dashboard-preview-cta');
      expect(ctaButton).toBeInTheDocument();
      expect(ctaButton).toHaveTextContent('Start Tracking Your Links');
    });

    it('should have the CTA button link to /register', () => {
      renderWithProviders(<DashboardPreview />);

      const ctaButton = screen.getByTestId('dashboard-preview-cta');
      expect(ctaButton).toHaveAttribute('href', '/register');
    });

    it('should render the CTA as a link element', () => {
      renderWithProviders(<DashboardPreview />);

      const ctaButton = screen.getByTestId('dashboard-preview-cta');
      expect(ctaButton.tagName.toLowerCase()).toBe('a');
    });
  });

  describe('Test Case 3: Dashboard preview image has descriptive alt attribute', () => {
    it('should have a descriptive alt attribute on the preview image', () => {
      renderWithProviders(<DashboardPreview />);

      const previewImage = screen.getByTestId('dashboard-preview-image');
      expect(previewImage).toHaveAttribute('alt');
      const altText = previewImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText!.length).toBeGreaterThan(10);
    });

    it('should have alt text that describes analytics dashboard', () => {
      renderWithProviders(<DashboardPreview />);

      const previewImage = screen.getByTestId('dashboard-preview-image');
      const altText = previewImage.getAttribute('alt')!.toLowerCase();
      expect(altText).toMatch(/analytics|dashboard|statistics/);
    });

    it('should be accessible by role img with alt text', () => {
      renderWithProviders(<DashboardPreview />);

      const previewImage = screen.getByRole('img', {
        name: /analytics dashboard/i,
      });
      expect(previewImage).toBeInTheDocument();
    });
  });

  describe('Additional component structure tests', () => {
    it('should render a section heading', () => {
      renderWithProviders(<DashboardPreview />);

      const heading = screen.getByRole('heading', {
        name: /powerful analytics dashboard/i,
      });
      expect(heading).toBeInTheDocument();
    });

    it('should render feature callouts', () => {
      renderWithProviders(<DashboardPreview />);

      expect(screen.getByTestId('callout-click-tracking')).toBeInTheDocument();
      expect(screen.getByTestId('callout-geographic-data')).toBeInTheDocument();
      expect(screen.getByTestId('callout-referrer-insights')).toBeInTheDocument();
    });

    it('should have proper ARIA labelledby for accessibility', () => {
      renderWithProviders(<DashboardPreview />);

      const section = screen.getByTestId('dashboard-preview-section');
      expect(section).toHaveAttribute('aria-labelledby', 'dashboard-preview-title');
    });
  });
});
