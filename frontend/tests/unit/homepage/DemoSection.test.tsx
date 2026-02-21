/**
 * DemoSection Unit Tests
 * Owner: Scenario 4 - Demo & Social Proof
 *
 * Test cases:
 * 1. Visual mockup showing URL transformation flow is displayed
 * 2. Example long URL is displayed (realistic URL format)
 * 3. Example short URL is displayed (short format)
 * 4. Analytics preview or indicator is shown as part of the flow
 * 5. Demo visualization adapts to smaller screen with readable content (mobile)
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import DemoSection from '../../../src/components/homepage/DemoSection';

describe('DemoSection', () => {
  // Test Case 1: Visual mockup showing URL transformation flow is displayed
  it('displays visual mockup showing URL transformation flow', () => {
    render(<DemoSection />);

    const demoSection = screen.getByTestId('demo-section');
    expect(demoSection).toBeInTheDocument();

    // Verify the transformation flow is visible
    const transformationFlow = screen.getByTestId('demo-transformation-flow');
    expect(transformationFlow).toBeInTheDocument();

    // Should show the flow from long URL to short URL to analytics
    const longUrlStep = screen.getByTestId('demo-long-url');
    const shortUrlStep = screen.getByTestId('demo-short-url');
    const analyticsStep = screen.getByTestId('demo-analytics');

    expect(longUrlStep).toBeInTheDocument();
    expect(shortUrlStep).toBeInTheDocument();
    expect(analyticsStep).toBeInTheDocument();
  });

  // Test Case 2: Example long URL is displayed (realistic URL format)
  it('displays example long URL with realistic format', () => {
    render(<DemoSection />);

    const longUrlElement = screen.getByTestId('demo-long-url');
    expect(longUrlElement).toBeInTheDocument();

    // Should contain a realistic long URL format (https:// prefix and path structure)
    const urlText = longUrlElement.textContent || '';
    expect(urlText).toMatch(/https?:\/\//);
    expect(urlText.length).toBeGreaterThan(30); // Long URLs should be lengthy
  });

  // Test Case 3: Example short URL is displayed (short format)
  it('displays example short URL with short format', () => {
    render(<DemoSection />);

    const shortUrlElement = screen.getByTestId('demo-short-url');
    expect(shortUrlElement).toBeInTheDocument();

    // Should contain a short URL format
    const shortUrlText = shortUrlElement.textContent || '';
    // Short URLs should be concise (less than 30 characters typically)
    expect(shortUrlText.length).toBeLessThan(30);
    // Should contain a domain-like structure
    expect(shortUrlText).toMatch(/[a-z]+\.[a-z]+\/[a-z0-9]+/i);
  });

  // Test Case 4: Analytics preview or indicator is shown as part of the flow
  it('displays analytics preview or indicator as part of the flow', () => {
    render(<DemoSection />);

    const analyticsStep = screen.getByTestId('demo-analytics');
    expect(analyticsStep).toBeInTheDocument();

    // Should show analytics-related content (clicks, views, stats, etc.)
    const analyticsText = analyticsStep.textContent?.toLowerCase() || '';
    expect(analyticsText).toMatch(/click|view|stat|analytic|track|insight/);
  });

  // Test Case 5: Demo visualization adapts to smaller screen (mobile responsive)
  it('has responsive layout that adapts to mobile viewport', () => {
    render(<DemoSection />);

    const demoSection = screen.getByTestId('demo-section');
    expect(demoSection).toBeInTheDocument();

    // Check for responsive classes on the section
    expect(demoSection).toHaveClass('px-4');

    // The transformation flow container should have responsive behavior
    const transformationFlow = screen.getByTestId('demo-transformation-flow');

    // Should have flex column on mobile and row/better layout on larger screens
    expect(transformationFlow).toHaveClass('flex');
  });

  // Additional test: Section has appropriate accessibility attributes
  it('has appropriate section structure and accessibility', () => {
    render(<DemoSection />);

    const demoSection = screen.getByTestId('demo-section');
    expect(demoSection.tagName).toBe('SECTION');

    // Should have a heading to describe the section
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
  });

  // Additional test: Arrow indicators show flow direction
  it('displays visual flow indicators between steps', () => {
    render(<DemoSection />);

    // Check for arrow/indicator elements that show the flow direction
    const flowIndicators = screen.getAllByTestId(/demo-flow-indicator/);
    expect(flowIndicators.length).toBeGreaterThanOrEqual(2); // At least 2 arrows for 3-step flow
  });
});
