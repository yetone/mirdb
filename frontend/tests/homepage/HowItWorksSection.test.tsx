/**
 * How It Works Section Tests
 * Owner: Scenario 3 - How It Works Section
 *
 * Tests for:
 * - Section heading display
 * - 3-step process rendering
 * - Step content verification
 * - Visual indicators (numbers)
 * - Connectors between steps
 * - Responsive layout (horizontal/vertical)
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { HowItWorksSection } from '../../src/components/homepage/HowItWorksSection';
import { renderWithProviders, createMatchMedia, setViewportWidth } from './test-utils';

describe('HowItWorksSection', () => {
  // Test Case 1: Section exists with heading
  describe('Section Heading', () => {
    it('renders the How It Works section with proper heading', () => {
      renderWithProviders(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent(/how it works/i);
    });

    it('has the correct id for anchor navigation', () => {
      renderWithProviders(<HowItWorksSection />);

      const section = document.getElementById('how-it-works');
      expect(section).toBeInTheDocument();
    });

    it('has proper aria-labelledby for accessibility', () => {
      renderWithProviders(<HowItWorksSection />);

      const section = screen.getByRole('region', { name: /how it works/i });
      expect(section).toBeInTheDocument();
    });
  });

  // Test Case 2: Exactly 3 step elements
  describe('Step Elements Count', () => {
    it('renders exactly 3 step elements', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');
      const step2 = screen.getByTestId('step-2');
      const step3 = screen.getByTestId('step-3');

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();

      // Ensure no step 4
      expect(screen.queryByTestId('step-4')).not.toBeInTheDocument();
    });

    it('renders steps within a list container', () => {
      renderWithProviders(<HowItWorksSection />);

      const list = screen.getByRole('list', { name: /steps to use the service/i });
      expect(list).toBeInTheDocument();

      const listItems = within(list).getAllByRole('listitem');
      expect(listItems).toHaveLength(3);
    });
  });

  // Test Case 3: Step 1 content - Create short link
  describe('Step 1 Content', () => {
    it('first step mentions create, URL, or short link', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');
      const stepText = step1.textContent?.toLowerCase() || '';

      // Should contain at least one of these keywords
      const hasRelevantContent =
        stepText.includes('create') ||
        stepText.includes('paste') ||
        stepText.includes('url') ||
        stepText.includes('short link') ||
        stepText.includes('link');

      expect(hasRelevantContent).toBe(true);
    });

    it('step 1 has a title about creating links', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');
      const title = within(step1).getByRole('heading', { level: 3 });
      expect(title.textContent?.toLowerCase()).toContain('link');
    });
  });

  // Test Case 4: Step 2 content - Share anywhere
  describe('Step 2 Content', () => {
    it('second step mentions share, social, email, or anywhere', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2 = screen.getByTestId('step-2');
      const stepText = step2.textContent?.toLowerCase() || '';

      const hasRelevantContent =
        stepText.includes('share') ||
        stepText.includes('social') ||
        stepText.includes('email') ||
        stepText.includes('anywhere') ||
        stepText.includes('message');

      expect(hasRelevantContent).toBe(true);
    });

    it('step 2 has a title about sharing', () => {
      renderWithProviders(<HowItWorksSection />);

      const step2 = screen.getByTestId('step-2');
      const title = within(step2).getByRole('heading', { level: 3 });
      expect(title.textContent?.toLowerCase()).toContain('share');
    });
  });

  // Test Case 5: Step 3 content - Track analytics
  describe('Step 3 Content', () => {
    it('third step mentions track, analytics, clicks, or statistics', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3 = screen.getByTestId('step-3');
      const stepText = step3.textContent?.toLowerCase() || '';

      const hasRelevantContent =
        stepText.includes('track') ||
        stepText.includes('analytics') ||
        stepText.includes('clicks') ||
        stepText.includes('statistics') ||
        stepText.includes('monitor');

      expect(hasRelevantContent).toBe(true);
    });

    it('step 3 has a title about tracking or analytics', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3 = screen.getByTestId('step-3');
      const title = within(step3).getByRole('heading', { level: 3 });
      const titleText = title.textContent?.toLowerCase() || '';
      expect(titleText.includes('track') || titleText.includes('analytics')).toBe(true);
    });
  });

  // Test Case 6: Step number indicators
  describe('Step Number Indicators', () => {
    it('each step displays a number indicator (1, 2, 3)', () => {
      renderWithProviders(<HowItWorksSection />);

      const number1 = screen.getByTestId('step-number-1');
      const number2 = screen.getByTestId('step-number-2');
      const number3 = screen.getByTestId('step-number-3');

      expect(number1).toHaveTextContent('1');
      expect(number2).toHaveTextContent('2');
      expect(number3).toHaveTextContent('3');
    });

    it('number indicators have accessible labels', () => {
      renderWithProviders(<HowItWorksSection />);

      const number1 = screen.getByTestId('step-number-1');
      expect(number1).toHaveAttribute('aria-label', 'Step 1');

      const number2 = screen.getByTestId('step-number-2');
      expect(number2).toHaveAttribute('aria-label', 'Step 2');

      const number3 = screen.getByTestId('step-number-3');
      expect(number3).toHaveAttribute('aria-label', 'Step 3');
    });
  });

  // Test Case 7: Step connectors/dividers
  describe('Step Connectors', () => {
    it('renders visual connectors between steps', () => {
      renderWithProviders(<HowItWorksSection />);

      // There should be 2 connectors (between 3 steps)
      const connector1 = screen.getByTestId('connector-1');
      const connector2 = screen.getByTestId('connector-2');

      expect(connector1).toBeInTheDocument();
      expect(connector2).toBeInTheDocument();
    });

    it('connectors are marked as aria-hidden for accessibility', () => {
      renderWithProviders(<HowItWorksSection />);

      const connector1 = screen.getByTestId('connector-1');
      const connector2 = screen.getByTestId('connector-2');

      expect(connector1).toHaveAttribute('aria-hidden', 'true');
      expect(connector2).toHaveAttribute('aria-hidden', 'true');
    });

    it('renders vertical connectors for mobile layout', () => {
      renderWithProviders(<HowItWorksSection />);

      const verticalConnector1 = screen.getByTestId('connector-vertical-1');
      const verticalConnector2 = screen.getByTestId('connector-vertical-2');

      expect(verticalConnector1).toBeInTheDocument();
      expect(verticalConnector2).toBeInTheDocument();
    });
  });

  // Test Case 8: Horizontal layout at 1024px viewport
  describe('Horizontal Layout (Desktop)', () => {
    beforeEach(() => {
      setViewportWidth(1024);
    });

    afterEach(() => {
      // Reset viewport
      setViewportWidth(1024);
    });

    it('steps container has flex-row class for horizontal layout on desktop', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsContainer = screen.getByRole('list', { name: /steps to use the service/i });

      // Check for md:flex-row class (Tailwind responsive class)
      expect(stepsContainer).toHaveClass('md:flex-row');
    });

    it('horizontal connectors are visible on desktop', () => {
      renderWithProviders(<HowItWorksSection />);

      const connector1 = screen.getByTestId('connector-1');

      // Desktop connectors should have md:flex class (visible on md and up)
      expect(connector1).toHaveClass('md:flex');
    });

    it('vertical connectors are hidden on desktop', () => {
      renderWithProviders(<HowItWorksSection />);

      const verticalConnector1 = screen.getByTestId('connector-vertical-1');

      // Mobile connectors should have md:hidden class
      expect(verticalConnector1).toHaveClass('md:hidden');
    });
  });

  // Test Case 9: Vertical layout at 375px viewport
  describe('Vertical Layout (Mobile)', () => {
    beforeEach(() => {
      setViewportWidth(375);
    });

    afterEach(() => {
      // Reset viewport
      setViewportWidth(1024);
    });

    it('steps container has flex-col class for vertical layout on mobile', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsContainer = screen.getByRole('list', { name: /steps to use the service/i });

      // Check for flex-col class (default mobile)
      expect(stepsContainer).toHaveClass('flex-col');
    });

    it('horizontal connectors are hidden on mobile', () => {
      renderWithProviders(<HowItWorksSection />);

      const connector1 = screen.getByTestId('connector-1');

      // Desktop connectors should have hidden class by default
      expect(connector1).toHaveClass('hidden');
    });

    it('vertical connectors exist for mobile display', () => {
      renderWithProviders(<HowItWorksSection />);

      const verticalConnector1 = screen.getByTestId('connector-vertical-1');

      // Mobile connectors should NOT have hidden class (visible by default on mobile)
      expect(verticalConnector1).not.toHaveClass('hidden');
    });
  });

  // Additional accessibility tests
  describe('Accessibility', () => {
    it('section has proper semantic structure', () => {
      renderWithProviders(<HowItWorksSection />);

      const section = screen.getByRole('region');
      expect(section.tagName).toBe('SECTION');
    });

    it('each step has a heading for title', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepHeadings = screen.getAllByRole('heading', { level: 3 });
      expect(stepHeadings).toHaveLength(3);
    });

    it('icons are hidden from assistive technology', () => {
      renderWithProviders(<HowItWorksSection />);

      // The SVG icons should be aria-hidden
      const step1 = screen.getByTestId('step-1');
      const svgs = step1.querySelectorAll('svg');
      svgs.forEach((svg) => {
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });
});
