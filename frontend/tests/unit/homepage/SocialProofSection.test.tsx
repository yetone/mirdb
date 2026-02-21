/**
 * SocialProofSection Unit Tests
 * Owner: Scenario 4 - Demo & Social Proof
 *
 * Test cases:
 * 1. At least 3 trust indicators (privacy, speed, reliability) are displayed
 * 2. Each trust indicator has an icon and supporting text
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import SocialProofSection from '../../../src/components/homepage/SocialProofSection';
import { SOCIAL_PROOF } from '../../../src/utils/constants';

describe('SocialProofSection', () => {
  // Test Case 5: At least 3 trust indicators (privacy, speed, reliability) are displayed
  it('displays at least 3 trust indicators for privacy, speed, and reliability', () => {
    render(<SocialProofSection />);

    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection).toBeInTheDocument();

    // Get all trust indicator elements
    const trustIndicators = screen.getAllByTestId(/trust-indicator-/);
    expect(trustIndicators.length).toBeGreaterThanOrEqual(3);

    // Verify trust indicators cover privacy, speed, and reliability
    const allIndicatorText = trustIndicators
      .map((el) => el.textContent?.toLowerCase() || '')
      .join(' ');

    // Check for privacy-related content
    expect(allIndicatorText).toMatch(/privacy|secure|encrypt|safe/);

    // Check for speed-related content
    expect(allIndicatorText).toMatch(/fast|speed|quick|millisecond/);

    // Check for reliability-related content
    expect(allIndicatorText).toMatch(/reliable|uptime|available|99/);
  });

  // Test Case 6: Each trust indicator has an icon and supporting text
  it('displays each trust indicator with an icon and supporting text', () => {
    render(<SocialProofSection />);

    // Verify each SOCIAL_PROOF item has both icon and text representation
    SOCIAL_PROOF.forEach((item, index) => {
      const indicator = screen.getByTestId(`trust-indicator-${index}`);
      expect(indicator).toBeInTheDocument();

      // Should have an icon element
      const icon = indicator.querySelector('[data-testid="trust-indicator-icon"]');
      expect(icon).toBeInTheDocument();

      // Should have title text
      const title = indicator.querySelector('[data-testid="trust-indicator-title"]');
      expect(title).toBeInTheDocument();
      expect(title?.textContent).toBe(item.title);

      // Should have description text
      const description = indicator.querySelector('[data-testid="trust-indicator-description"]');
      expect(description).toBeInTheDocument();
      expect(description?.textContent).toBe(item.description);
    });
  });

  // Additional test: Section has appropriate structure
  it('has appropriate section structure', () => {
    render(<SocialProofSection />);

    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection.tagName).toBe('SECTION');
  });

  // Additional test: Trust indicators are displayed in a grid or flex layout
  it('displays trust indicators in a responsive layout', () => {
    render(<SocialProofSection />);

    const indicatorsContainer = screen.getByTestId('trust-indicators-container');
    expect(indicatorsContainer).toBeInTheDocument();

    // Should have responsive grid or flex classes
    expect(indicatorsContainer).toHaveClass('grid');
  });

  // Additional test: Section has appropriate heading
  it('has a heading describing the section', () => {
    render(<SocialProofSection />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
  });

  // Additional test: Icons are accessible (have aria-hidden or role)
  it('has accessible icon elements', () => {
    render(<SocialProofSection />);

    const icons = screen.getAllByTestId('trust-indicator-icon');
    icons.forEach((icon) => {
      // Icons should have aria-hidden for decorative icons or proper role
      const ariaHidden = icon.getAttribute('aria-hidden');
      const role = icon.getAttribute('role');
      expect(ariaHidden === 'true' || role === 'img').toBe(true);
    });
  });
});
