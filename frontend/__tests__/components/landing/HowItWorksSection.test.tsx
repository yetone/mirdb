/**
 * Tests for How It Works Section Component.
 * Owner: Scenario 3 - How It Works Section
 *
 * Test cases:
 * 1. How It Works section is present with section heading
 * 2. Step 1 'Create' is displayed with description
 * 3. Step 2 'Share' is displayed with description
 * 4. Step 3 'Track' is displayed with description
 * 5. Steps are displayed in a visual flow format
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '../../test-utils';
import { HowItWorksSection } from '../../../src/components/landing/HowItWorksSection';

describe('HowItWorksSection', () => {
  describe('Test Case 1: Section presence and heading', () => {
    it('should render the How It Works section with heading', () => {
      render(<HowItWorksSection />);

      // Check for section element
      const section = document.querySelector('#how-it-works');
      expect(section).toBeInTheDocument();

      // Check for heading
      const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 });
      expect(heading).toBeInTheDocument();
    });

    it('should have proper aria-labelledby attribute for accessibility', () => {
      render(<HowItWorksSection />);

      const section = document.querySelector('#how-it-works');
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
    });
  });

  describe('Test Case 2: Step 1 Create', () => {
    it('should display Step 1 with title "Create"', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-1-title');
      expect(stepTitle).toBeInTheDocument();
      expect(stepTitle).toHaveTextContent('Create');
    });

    it('should display Step 1 with description about pasting URL and getting short link', () => {
      render(<HowItWorksSection />);

      const stepDescription = screen.getByTestId('step-1-description');
      expect(stepDescription).toBeInTheDocument();
      expect(stepDescription.textContent?.toLowerCase()).toContain('url');
      expect(stepDescription.textContent?.toLowerCase()).toContain('short link');
    });

    it('should display Step 1 with step number badge showing "1"', () => {
      render(<HowItWorksSection />);

      const step = screen.getByTestId('step-1');
      expect(step).toBeInTheDocument();
      expect(step.textContent).toContain('1');
    });
  });

  describe('Test Case 3: Step 2 Share', () => {
    it('should display Step 2 with title "Share"', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-2-title');
      expect(stepTitle).toBeInTheDocument();
      expect(stepTitle).toHaveTextContent('Share');
    });

    it('should display Step 2 with description about sharing shortened link', () => {
      render(<HowItWorksSection />);

      const stepDescription = screen.getByTestId('step-2-description');
      expect(stepDescription).toBeInTheDocument();
      expect(stepDescription.textContent?.toLowerCase()).toContain('share');
      expect(stepDescription.textContent?.toLowerCase()).toContain('shortened link');
    });

    it('should display Step 2 with step number badge showing "2"', () => {
      render(<HowItWorksSection />);

      const step = screen.getByTestId('step-2');
      expect(step).toBeInTheDocument();
      expect(step.textContent).toContain('2');
    });
  });

  describe('Test Case 4: Step 3 Track', () => {
    it('should display Step 3 with title "Track"', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-3-title');
      expect(stepTitle).toBeInTheDocument();
      expect(stepTitle).toHaveTextContent('Track');
    });

    it('should display Step 3 with description about monitoring clicks and analytics', () => {
      render(<HowItWorksSection />);

      const stepDescription = screen.getByTestId('step-3-description');
      expect(stepDescription).toBeInTheDocument();
      expect(stepDescription.textContent?.toLowerCase()).toContain('monitor');
      expect(stepDescription.textContent?.toLowerCase()).toContain('click');
    });

    it('should display Step 3 with step number badge showing "3"', () => {
      render(<HowItWorksSection />);

      const step = screen.getByTestId('step-3');
      expect(step).toBeInTheDocument();
      expect(step.textContent).toContain('3');
    });
  });

  describe('Test Case 5: Visual flow format', () => {
    it('should display all three steps in a container', () => {
      render(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toBeInTheDocument();
    });

    it('should display steps in correct order (1, 2, 3)', () => {
      render(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');
      const step2 = screen.getByTestId('step-2');
      const step3 = screen.getByTestId('step-3');

      // All steps should be present
      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();

      // Check the order by comparing positions in the DOM
      const stepsContainer = screen.getByTestId('steps-container');
      // Use regex to match exactly step-1, step-2, step-3 (not step-1-title etc.)
      const steps = stepsContainer.querySelectorAll('[data-testid="step-1"], [data-testid="step-2"], [data-testid="step-3"]');
      expect(steps).toHaveLength(3);

      expect(steps[0]).toHaveAttribute('data-testid', 'step-1');
      expect(steps[1]).toHaveAttribute('data-testid', 'step-2');
      expect(steps[2]).toHaveAttribute('data-testid', 'step-3');
    });

    it('should use grid layout for visual flow', () => {
      render(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toHaveClass('grid');
    });

    it('should have responsive grid columns', () => {
      render(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('steps-container');
      // Check for responsive classes
      expect(stepsContainer.className).toContain('grid-cols-1');
      expect(stepsContainer.className).toContain('md:grid-cols-3');
    });
  });
});
