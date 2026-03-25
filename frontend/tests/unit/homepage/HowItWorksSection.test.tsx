/**
 * HowItWorksSection Unit Tests
 * Owner: Scenario 5 - Usage Steps Guide
 *
 * Tests the 3-step usage guide (Paste URL → Shorten → Share):
 * - Section visibility with heading
 * - Exactly 3 steps rendered
 * - Each step content (title, description)
 * - Step numbers displayed
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { HowItWorksSection } from '../../../src/components/homepage/HowItWorksSection';
import { renderWithProviders } from './test-utils';

describe('HowItWorksSection', () => {
  describe('Test Case 1: How It Works section exists with heading', () => {
    it('renders How It Works section with heading', () => {
      renderWithProviders(<HowItWorksSection />);

      // Section should be visible
      const section = screen.getByTestId('how-it-works-section');
      expect(section).toBeInTheDocument();
      expect(section).toBeVisible();

      // Should have proper aria-label
      expect(section).toHaveAttribute('aria-label', 'How it works');
    });

    it('displays "How It Works" heading', () => {
      renderWithProviders(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2, name: /how it works/i });
      expect(heading).toBeInTheDocument();
      expect(heading).toBeVisible();
    });
  });

  describe('Test Case 2: Exactly 3 step elements are rendered', () => {
    it('renders exactly 3 step elements', () => {
      renderWithProviders(<HowItWorksSection />);

      const steps = screen.getAllByRole('listitem');
      expect(steps).toHaveLength(3);
    });

    it('steps are contained in a list structure', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsList = screen.getByRole('list', { name: /steps/i });
      expect(stepsList).toBeInTheDocument();

      const steps = within(stepsList).getAllByRole('listitem');
      expect(steps).toHaveLength(3);
    });

    it('each step has a unique data-testid', () => {
      renderWithProviders(<HowItWorksSection />);

      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Step 1 displays "Paste URL" with description', () => {
    it('step 1 displays "Paste URL" title', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Title = screen.getByTestId('step-1-title');
      expect(step1Title).toBeInTheDocument();
      expect(step1Title).toHaveTextContent('Paste URL');
    });

    it('step 1 has a description', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Description = screen.getByTestId('step-1-description');
      expect(step1Description).toBeInTheDocument();
      expect(step1Description.textContent!.length).toBeGreaterThan(10);
    });

    it('step 1 description mentions entering a URL', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Description = screen.getByTestId('step-1-description');
      expect(step1Description.textContent?.toLowerCase()).toMatch(/url|link/);
    });

    it('step 1 has an icon', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Icon = screen.getByTestId('step-1-icon');
      expect(step1Icon).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Step 2 displays "Shorten" with description', () => {
    it('step 2 displays "Shorten" title', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2Title = screen.getByTestId('step-2-title');
      expect(step2Title).toBeInTheDocument();
      expect(step2Title).toHaveTextContent('Shorten');
    });

    it('step 2 has a description', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2Description = screen.getByTestId('step-2-description');
      expect(step2Description).toBeInTheDocument();
      expect(step2Description.textContent!.length).toBeGreaterThan(10);
    });

    it('step 2 description mentions shortening or generating a link', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2Description = screen.getByTestId('step-2-description');
      expect(step2Description.textContent?.toLowerCase()).toMatch(/shorten|generate|compact/);
    });

    it('step 2 has an icon', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2Icon = screen.getByTestId('step-2-icon');
      expect(step2Icon).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Step 3 displays "Share" with description', () => {
    it('step 3 displays "Share" title', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3Title = screen.getByTestId('step-3-title');
      expect(step3Title).toBeInTheDocument();
      expect(step3Title).toHaveTextContent('Share');
    });

    it('step 3 has a description', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3Description = screen.getByTestId('step-3-description');
      expect(step3Description).toBeInTheDocument();
      expect(step3Description.textContent!.length).toBeGreaterThan(10);
    });

    it('step 3 description mentions sharing or tracking', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3Description = screen.getByTestId('step-3-description');
      expect(step3Description.textContent?.toLowerCase()).toMatch(/share|track/);
    });

    it('step 3 has an icon', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3Icon = screen.getByTestId('step-3-icon');
      expect(step3Icon).toBeInTheDocument();
    });
  });

  describe('Test Case 6: Numbers 1, 2, 3 are visible with each step', () => {
    it('step 1 displays number 1', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Number = screen.getByTestId('step-1-number');
      expect(step1Number).toBeInTheDocument();
      expect(step1Number).toHaveTextContent('1');
      expect(step1Number).toBeVisible();
    });

    it('step 2 displays number 2', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2Number = screen.getByTestId('step-2-number');
      expect(step2Number).toBeInTheDocument();
      expect(step2Number).toHaveTextContent('2');
      expect(step2Number).toBeVisible();
    });

    it('step 3 displays number 3', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3Number = screen.getByTestId('step-3-number');
      expect(step3Number).toBeInTheDocument();
      expect(step3Number).toHaveTextContent('3');
      expect(step3Number).toBeVisible();
    });

    it('all step numbers are styled consistently', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1Number = screen.getByTestId('step-1-number');
      const step2Number = screen.getByTestId('step-2-number');
      const step3Number = screen.getByTestId('step-3-number');

      // All numbers should have the same styling class
      expect(step1Number).toHaveClass('rounded-full');
      expect(step2Number).toHaveClass('rounded-full');
      expect(step3Number).toHaveClass('rounded-full');
    });
  });

  describe('Accessibility', () => {
    it('section has proper semantic structure', () => {
      renderWithProviders(<HowItWorksSection />);

      const section = screen.getByRole('region', { name: /how it works/i });
      expect(section).toBeInTheDocument();
    });

    it('steps are in a list for screen readers', () => {
      renderWithProviders(<HowItWorksSection />);

      const list = screen.getByRole('list', { name: /steps/i });
      expect(list).toBeInTheDocument();
    });

    it('each step is a list item', () => {
      renderWithProviders(<HowItWorksSection />);

      const listItems = screen.getAllByRole('listitem');
      expect(listItems).toHaveLength(3);
    });

    it('icon containers are hidden from screen readers', () => {
      renderWithProviders(<HowItWorksSection />);

      // The icon container div should have aria-hidden
      const step1 = screen.getByTestId('step-1');
      const iconContainer = step1.querySelector('[aria-hidden="true"]');
      expect(iconContainer).toBeInTheDocument();
    });
  });

  describe('Visual Layout', () => {
    it('section has responsive grid layout', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsList = screen.getByRole('list', { name: /steps/i });
      expect(stepsList).toHaveClass('grid');
      expect(stepsList).toHaveClass('md:grid-cols-3');
    });

    it('steps are centered horizontally', () => {
      renderWithProviders(<HowItWorksSection />);

      const steps = screen.getAllByRole('listitem');
      steps.forEach((step) => {
        expect(step).toHaveClass('items-center');
        expect(step).toHaveClass('text-center');
      });
    });
  });
});
