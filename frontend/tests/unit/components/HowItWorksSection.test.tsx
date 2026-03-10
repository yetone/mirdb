/**
 * Unit tests for HowItWorksSection component
 * Owner: Scenario 5 - How It Works Section
 *
 * Tests the 3-step visual guide explaining the URL shortening process
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { HowItWorksSection } from '../../../src/components/landing/HowItWorksSection';
import { StepCard } from '../../../src/components/landing/StepCard';
import { HOW_IT_WORKS_CONTENT } from '../../../src/constants/landingContent';

describe('HowItWorksSection', () => {
  describe('Test Case 1: Render HowItWorksSection component', () => {
    it('should display 3 numbered steps with icons and descriptions', () => {
      render(<HowItWorksSection />);

      // Verify section is rendered
      const section = screen.getByTestId('how-it-works-section');
      expect(section).toBeInTheDocument();

      // Verify title is rendered
      expect(screen.getByTestId('how-it-works-title')).toHaveTextContent(
        HOW_IT_WORKS_CONTENT.title
      );

      // Verify all 3 steps are rendered
      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();

      // Verify each step has number, icon, title and description
      for (let i = 1; i <= 3; i++) {
        expect(screen.getByTestId(`step-${i}-number`)).toBeInTheDocument();
        expect(screen.getByTestId(`step-${i}-icon`)).toBeInTheDocument();
        expect(screen.getByTestId(`step-${i}-title`)).toBeInTheDocument();
        expect(screen.getByTestId(`step-${i}-description`)).toBeInTheDocument();
      }
    });
  });

  describe('Test Case 2: Step 1 content', () => {
    it("should show '1', icon, 'Paste your URL' text", () => {
      render(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');
      expect(step1).toBeInTheDocument();

      // Check number
      const numberBadge = screen.getByTestId('step-1-number');
      expect(numberBadge).toHaveTextContent('1');

      // Check icon is present
      const iconContainer = screen.getByTestId('step-1-icon');
      expect(iconContainer).toBeInTheDocument();
      expect(iconContainer.querySelector('svg')).toBeInTheDocument();

      // Check title matches "Paste Your URL" (case insensitive partial match)
      const title = screen.getByTestId('step-1-title');
      expect(title.textContent?.toLowerCase()).toContain('paste');
      expect(title.textContent?.toLowerCase()).toContain('url');
    });
  });

  describe('Test Case 3: Step 2 content', () => {
    it("should show '2', icon, 'Get a short link' text", () => {
      render(<HowItWorksSection />);

      const step2 = screen.getByTestId('step-2');
      expect(step2).toBeInTheDocument();

      // Check number
      const numberBadge = screen.getByTestId('step-2-number');
      expect(numberBadge).toHaveTextContent('2');

      // Check icon is present
      const iconContainer = screen.getByTestId('step-2-icon');
      expect(iconContainer).toBeInTheDocument();
      expect(iconContainer.querySelector('svg')).toBeInTheDocument();

      // Check title matches "Get Short Link" (case insensitive partial match)
      const title = screen.getByTestId('step-2-title');
      expect(title.textContent?.toLowerCase()).toContain('short');
      expect(title.textContent?.toLowerCase()).toContain('link');
    });
  });

  describe('Test Case 4: Step 3 content', () => {
    it("should show '3', icon, 'Track and share' text", () => {
      render(<HowItWorksSection />);

      const step3 = screen.getByTestId('step-3');
      expect(step3).toBeInTheDocument();

      // Check number
      const numberBadge = screen.getByTestId('step-3-number');
      expect(numberBadge).toHaveTextContent('3');

      // Check icon is present
      const iconContainer = screen.getByTestId('step-3-icon');
      expect(iconContainer).toBeInTheDocument();
      expect(iconContainer.querySelector('svg')).toBeInTheDocument();

      // Check title matches "Track and Share" (case insensitive partial match)
      const title = screen.getByTestId('step-3-title');
      expect(title.textContent?.toLowerCase()).toContain('track');
      expect(title.textContent?.toLowerCase()).toContain('share');
    });
  });

  describe('Desktop and Mobile layouts', () => {
    it('should have responsive container with flex layout classes', () => {
      render(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toBeInTheDocument();

      // Check for responsive classes (vertical on mobile, horizontal on desktop)
      expect(stepsContainer).toHaveClass('flex-col');
      expect(stepsContainer).toHaveClass('lg:flex-row');
    });

    it('should have visual connectors between steps on desktop', () => {
      render(<HowItWorksSection />);

      // First two steps should have connectors, last step should not
      expect(screen.getByTestId('step-1-connector')).toBeInTheDocument();
      expect(screen.getByTestId('step-2-connector')).toBeInTheDocument();
      expect(screen.queryByTestId('step-3-connector')).not.toBeInTheDocument();

      // Connectors should be hidden on mobile (lg:flex class)
      const connector1 = screen.getByTestId('step-1-connector');
      expect(connector1).toHaveClass('hidden', 'lg:flex');
    });
  });

  describe('Accessibility', () => {
    it('should have proper section labeling', () => {
      render(<HowItWorksSection />);

      // Section should be labeled by the title
      const section = screen.getByRole('region', { name: /how it works/i });
      expect(section).toBeInTheDocument();
    });

    it('should have list semantics for steps', () => {
      render(<HowItWorksSection />);

      // Steps container should have list role
      const list = screen.getByRole('list', { name: /steps to shorten/i });
      expect(list).toBeInTheDocument();

      // Each step should be a list item
      const listItems = screen.getAllByRole('listitem');
      expect(listItems).toHaveLength(3);
    });

    it('should hide decorative elements from screen readers', () => {
      render(<HowItWorksSection />);

      // Icons and connectors should have aria-hidden
      const icon1 = screen.getByTestId('step-1-icon');
      expect(icon1).toHaveAttribute('aria-hidden', 'true');

      const connector1 = screen.getByTestId('step-1-connector');
      expect(connector1).toHaveAttribute('aria-hidden', 'true');

      const numberBadge = screen.getByTestId('step-1-number');
      expect(numberBadge).toHaveAttribute('aria-hidden', 'true');
    });
  });
});

describe('StepCard', () => {
  const mockIcon = <svg data-testid="mock-icon" />;

  it('should render step card with all props', () => {
    render(
      <StepCard
        number={1}
        icon={mockIcon}
        title="Test Title"
        description="Test Description"
      />
    );

    expect(screen.getByTestId('step-1')).toBeInTheDocument();
    expect(screen.getByTestId('step-1-number')).toHaveTextContent('1');
    expect(screen.getByTestId('step-1-title')).toHaveTextContent('Test Title');
    expect(screen.getByTestId('step-1-description')).toHaveTextContent('Test Description');
    expect(screen.getByTestId('mock-icon')).toBeInTheDocument();
  });

  it('should show connector when isLast is false', () => {
    render(
      <StepCard
        number={1}
        icon={mockIcon}
        title="Test"
        description="Test"
        isLast={false}
      />
    );

    expect(screen.getByTestId('step-1-connector')).toBeInTheDocument();
  });

  it('should hide connector when isLast is true', () => {
    render(
      <StepCard
        number={3}
        icon={mockIcon}
        title="Test"
        description="Test"
        isLast={true}
      />
    );

    expect(screen.queryByTestId('step-3-connector')).not.toBeInTheDocument();
  });
});
