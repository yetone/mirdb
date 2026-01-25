/**
 * Unit tests for HowItWorksSection component
 * Owner: Scenario 6 - How It Works Section
 *
 * Test Cases:
 * 1. Section contains exactly 3 steps with numbers/icons
 * 2. Step 1 mentions pasting or entering a long URL
 * 3. Step 2 mentions sharing the short link
 * 4. Step 3 mentions tracking or viewing analytics
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { HowItWorksSection } from '../../../src/components/homepage/HowItWorksSection';
import { renderWithProviders } from './test-utils';

describe('HowItWorksSection', () => {
  /**
   * Test Case 1: Section contains exactly 3 steps with numbers/icons
   */
  it('renders exactly 3 steps with numbers and icons', () => {
    renderWithProviders(<HowItWorksSection />);

    // Verify section exists
    const section = screen.getByTestId('how-it-works-section');
    expect(section).toBeInTheDocument();

    // Verify section has heading
    const heading = screen.getByRole('heading', { name: /how it works/i });
    expect(heading).toBeInTheDocument();

    // Verify exactly 3 steps are rendered
    const step1 = screen.getByTestId('step-1');
    const step2 = screen.getByTestId('step-2');
    const step3 = screen.getByTestId('step-3');

    expect(step1).toBeInTheDocument();
    expect(step2).toBeInTheDocument();
    expect(step3).toBeInTheDocument();

    // Verify step numbers are displayed
    expect(step1).toHaveTextContent('1');
    expect(step2).toHaveTextContent('2');
    expect(step3).toHaveTextContent('3');

    // Verify each step has an SVG icon
    const icons = section.querySelectorAll('svg');
    expect(icons.length).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test Case 2: Step 1 mentions pasting or entering a long URL
   */
  it('step 1 mentions pasting or entering a long URL', () => {
    renderWithProviders(<HowItWorksSection />);

    const step1 = screen.getByTestId('step-1');

    // Verify step 1 title mentions pasting/entering URL
    expect(step1).toHaveTextContent(/paste/i);
    expect(step1).toHaveTextContent(/url/i);

    // Verify description mentions long URL
    expect(step1).toHaveTextContent(/long/i);
  });

  /**
   * Test Case 3: Step 2 mentions sharing the short link
   */
  it('step 2 mentions sharing the short link', () => {
    renderWithProviders(<HowItWorksSection />);

    const step2 = screen.getByTestId('step-2');

    // Verify step 2 title mentions sharing
    expect(step2).toHaveTextContent(/share/i);

    // Verify description mentions short link
    expect(step2).toHaveTextContent(/link/i);
  });

  /**
   * Test Case 4: Step 3 mentions tracking or viewing analytics
   */
  it('step 3 mentions tracking or viewing analytics', () => {
    renderWithProviders(<HowItWorksSection />);

    const step3 = screen.getByTestId('step-3');

    // Verify step 3 mentions tracking/analytics
    expect(step3).toHaveTextContent(/track/i);
    expect(step3).toHaveTextContent(/analytics/i);
  });

  /**
   * Additional test: Verify accessibility attributes
   */
  it('has proper accessibility attributes', () => {
    renderWithProviders(<HowItWorksSection />);

    // Verify section has aria-labelledby
    const section = screen.getByTestId('how-it-works-section');
    expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-title');

    // Verify heading has the correct id
    const heading = screen.getByRole('heading', { name: /how it works/i });
    expect(heading).toHaveAttribute('id', 'how-it-works-title');
  });

  /**
   * Additional test: Verify all step titles are rendered as headings
   */
  it('renders step titles as headings', () => {
    renderWithProviders(<HowItWorksSection />);

    // Each step should have an h3 heading
    const stepHeadings = screen.getAllByRole('heading', { level: 3 });
    expect(stepHeadings).toHaveLength(3);

    // Verify step titles
    expect(stepHeadings[0]).toHaveTextContent(/paste your url/i);
    expect(stepHeadings[1]).toHaveTextContent(/share your link/i);
    expect(stepHeadings[2]).toHaveTextContent(/track analytics/i);
  });
});
