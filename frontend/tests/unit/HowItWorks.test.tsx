/**
 * How It Works Section Unit Tests
 * Owner: Scenario 5
 *
 * Test coverage:
 * - Three-step workflow display
 * - Step content validation
 * - Step numbering
 *
 * Test suites:
 * - describe('How It Works Section')
 * - describe('Workflow Steps')
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import HowItWorks from '../../src/components/homepage/HowItWorks';

describe('How It Works Section (Scenario 5)', () => {
  describe('How It Works Section Heading', () => {
    it('should render a section with "How It Works" or similar heading text', () => {
      renderWithProviders(<HowItWorks />);

      // Find heading by role - should have h2 element
      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();

      // Verify the heading contains "How It Works" text
      const headingText = heading.textContent?.toLowerCase() || '';
      expect(
        headingText.includes('how it works') ||
        headingText.includes('how to') ||
        headingText.includes('steps')
      ).toBe(true);
    });

    it('should have proper semantic section structure', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Verify the component renders within a section element
      const section = container.querySelector('section');
      expect(section).toBeInTheDocument();
    });
  });

  describe('Workflow Steps Display', () => {
    it('should display exactly 3 workflow steps', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Find all step number indicators (the round badges with numbers)
      const stepNumbers = container.querySelectorAll('.rounded-full');

      // Filter to only those that contain numbers 1, 2, 3
      const numberBadges = Array.from(stepNumbers).filter(el => {
        const text = el.textContent?.trim();
        return text === '1' || text === '2' || text === '3';
      });

      expect(numberBadges.length).toBe(3);
    });

    it('should display step 1 content describing pasting a URL', () => {
      renderWithProviders(<HowItWorks />);

      // Look for step 1 title - paste URL (use getAllByText since both title and description match)
      const step1Elements = screen.getAllByText(/paste.*url/i);
      expect(step1Elements.length).toBeGreaterThanOrEqual(1);

      // Verify at least one is an h3 heading
      const hasTitle = step1Elements.some(el => el.tagName === 'H3');
      expect(hasTitle).toBe(true);
    });

    it('should display step 2 content describing getting a short link', () => {
      renderWithProviders(<HowItWorks />);

      // Look for step 2 title - get short link (use getAllByText since both title and description match)
      const step2Elements = screen.getAllByText(/short.*link/i);
      expect(step2Elements.length).toBeGreaterThanOrEqual(1);

      // Verify at least one is an h3 heading
      const hasTitle = step2Elements.some(el => el.tagName === 'H3');
      expect(hasTitle).toBe(true);
    });

    it('should display step 3 content describing tracking or insights', () => {
      renderWithProviders(<HowItWorks />);

      // Look for step 3 content - track clicks or gain insights
      const step3Title = screen.getByText(/track.*click|gain.*insight/i);
      expect(step3Title).toBeInTheDocument();
    });

    it('should display step numbers in sequential order (1, 2, 3)', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Get all the step number badges
      const stepNumbers = container.querySelectorAll('.rounded-full');
      const numbers = Array.from(stepNumbers)
        .map(el => el.textContent?.trim())
        .filter(text => text === '1' || text === '2' || text === '3');

      // Verify they appear in order
      expect(numbers).toEqual(['1', '2', '3']);
    });
  });

  describe('Step Content Structure', () => {
    it('should have a title for each step', () => {
      renderWithProviders(<HowItWorks />);

      // Find h3 elements (step titles)
      const stepTitles = screen.getAllByRole('heading', { level: 3 });
      expect(stepTitles.length).toBe(3);

      // Verify each title has meaningful content
      stepTitles.forEach(title => {
        expect(title.textContent).toBeTruthy();
        expect(title.textContent!.length).toBeGreaterThan(5);
      });
    });

    it('should have a description for each step', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Find paragraph elements with descriptions
      const descriptions = container.querySelectorAll('p');

      // Should have at least 3 descriptions (one per step)
      expect(descriptions.length).toBeGreaterThanOrEqual(3);

      // Each description should have meaningful content
      descriptions.forEach(desc => {
        expect(desc.textContent).toBeTruthy();
        expect(desc.textContent!.length).toBeGreaterThan(10);
      });
    });

    it('should display step number badge for each step', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Find all number badges
      const badges = container.querySelectorAll('.rounded-full');

      const numberBadges = Array.from(badges).filter(el => {
        const text = el.textContent?.trim();
        return ['1', '2', '3'].includes(text || '');
      });

      expect(numberBadges.length).toBe(3);

      // Verify each badge displays the correct number
      expect(numberBadges[0].textContent?.trim()).toBe('1');
      expect(numberBadges[1].textContent?.trim()).toBe('2');
      expect(numberBadges[2].textContent?.trim()).toBe('3');
    });
  });

  describe('Visual Hierarchy', () => {
    it('should have the main section heading be larger/more prominent', () => {
      renderWithProviders(<HowItWorks />);

      const mainHeading = screen.getByRole('heading', { level: 2 });
      const stepHeadings = screen.getAllByRole('heading', { level: 3 });

      // Main heading should be h2, step titles should be h3
      expect(mainHeading.tagName).toBe('H2');
      stepHeadings.forEach(heading => {
        expect(heading.tagName).toBe('H3');
      });
    });

    it('should render workflow steps in a container layout', () => {
      const { container } = renderWithProviders(<HowItWorks />);

      // Check for flex or grid layout container
      const flexContainer = container.querySelector('.flex');
      expect(flexContainer).toBeInTheDocument();
    });
  });
});
