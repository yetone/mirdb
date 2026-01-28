/**
 * HowItWorks Unit Tests
 * Owner: Scenario 3 - How It Works Section
 *
 * Test coverage:
 * - Component renders without errors
 * - Section heading contains 'How It Works'
 * - Exactly 3 steps are displayed
 * - Step 1: Paste Your URL
 * - Step 2: Get Your Short Link
 * - Step 3: Track Performance
 * - Step numbering (1, 2, 3)
 * - Semantic structure (ordered list)
 * - Each step has an icon
 * - Section has visual distinction (different background)
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { HowItWorks } from '@/components/home/HowItWorks';

describe('HowItWorks', () => {
  // Test Case 1: Component renders without throwing errors
  it('renders without throwing errors', () => {
    expect(() => render(<HowItWorks />)).not.toThrow();
  });

  // Test Case 2: Heading contains 'How It Works' text
  it('displays How It Works heading', () => {
    render(<HowItWorks />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).toMatch(/how it works/i);
  });

  // Test Case 3: Exactly 3 step items are present
  it('displays exactly 3 step items', () => {
    render(<HowItWorks />);

    const stepItems = screen.getAllByTestId('step-item');
    expect(stepItems).toHaveLength(3);
  });

  // Test Case 4: Step 1 contains 'Paste Your URL' or similar title with description
  it('displays Step 1 with Paste Your URL title and description', () => {
    render(<HowItWorks />);

    const step1 = screen.getByTestId('step-1');
    expect(step1).toBeInTheDocument();

    // Check for title
    const title = within(step1).getByRole('heading', { level: 3 });
    expect(title.textContent?.toLowerCase()).toMatch(/paste.*url|enter.*url/i);

    // Check for description
    const description = within(step1).getByTestId('step-description');
    expect(description).toBeInTheDocument();
  });

  // Test Case 5: Step 2 contains 'Get Your Short Link' or similar title with description
  it('displays Step 2 with Get Your Short Link title and description', () => {
    render(<HowItWorks />);

    const step2 = screen.getByTestId('step-2');
    expect(step2).toBeInTheDocument();

    // Check for title
    const title = within(step2).getByRole('heading', { level: 3 });
    expect(title.textContent?.toLowerCase()).toMatch(/get.*short.*link|receive.*link/i);

    // Check for description
    const description = within(step2).getByTestId('step-description');
    expect(description).toBeInTheDocument();
  });

  // Test Case 6: Step 3 contains 'Track Performance' or similar title with description
  it('displays Step 3 with Track Performance title and description', () => {
    render(<HowItWorks />);

    const step3 = screen.getByTestId('step-3');
    expect(step3).toBeInTheDocument();

    // Check for title
    const title = within(step3).getByRole('heading', { level: 3 });
    expect(title.textContent?.toLowerCase()).toMatch(/track.*performance|monitor.*clicks|analytics/i);

    // Check for description
    const description = within(step3).getByTestId('step-description');
    expect(description).toBeInTheDocument();
  });

  // Test Case 7: Visual step indicators show 1, 2, 3 in order
  it('displays step numbers 1, 2, 3 in order', () => {
    render(<HowItWorks />);

    const stepNumbers = screen.getAllByTestId('step-number');
    expect(stepNumbers).toHaveLength(3);

    expect(stepNumbers[0].textContent).toBe('1');
    expect(stepNumbers[1].textContent).toBe('2');
    expect(stepNumbers[2].textContent).toBe('3');
  });

  // Test Case 8: Steps use ordered list (ol/li) or numbered elements for semantics
  it('uses ordered list for semantic structure', () => {
    render(<HowItWorks />);

    const orderedList = screen.getByRole('list');
    expect(orderedList).toBeInTheDocument();
    expect(orderedList.tagName.toLowerCase()).toBe('ol');

    const listItems = within(orderedList).getAllByRole('listitem');
    expect(listItems).toHaveLength(3);
  });

  // Test Case 9: Each step has an associated icon element
  it('each step has an associated icon element', () => {
    render(<HowItWorks />);

    const stepItems = screen.getAllByTestId('step-item');
    expect(stepItems).toHaveLength(3);

    stepItems.forEach((step) => {
      const iconContainer = within(step).getByTestId('step-icon');
      expect(iconContainer).toBeInTheDocument();

      // Check for SVG inside the icon container
      const svg = iconContainer.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });

  // Test Case 10: Section has visual distinction from features section (different background)
  it('section has visual distinction with different background', () => {
    render(<HowItWorks />);

    const section = screen.getByRole('region', { name: /how it works/i });
    // Check for background color class that differs from the default
    expect(section.className).toMatch(/bg-base-200|bg-base-300|bg-secondary|bg-gradient/i);
  });

  // Additional tests for accessibility and semantic structure
  it('has a section with aria-labelledby', () => {
    render(<HowItWorks />);

    const section = screen.getByRole('region', { name: /how it works/i });
    expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
  });

  it('icons have aria-hidden for accessibility', () => {
    render(<HowItWorks />);

    const stepItems = screen.getAllByTestId('step-item');

    stepItems.forEach((step) => {
      const svg = step.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });

  it('steps are displayed in logical visual order', () => {
    render(<HowItWorks />);

    const stepItems = screen.getAllByTestId('step-item');
    const titles = stepItems.map((step) =>
      within(step).getByRole('heading', { level: 3 }).textContent?.toLowerCase()
    );

    // Verify order: Paste URL -> Get Short Link -> Track Performance
    expect(titles[0]).toMatch(/paste|enter/);
    expect(titles[1]).toMatch(/get|receive|short/);
    expect(titles[2]).toMatch(/track|monitor|analytics/);
  });
});
