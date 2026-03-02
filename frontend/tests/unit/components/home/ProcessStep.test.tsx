/**
 * ProcessStep Component Unit Tests.
 *
 * Tests for Scenario 5 - How It Works Section:
 * - Step number display
 * - Title rendering
 * - Description rendering
 * - Icon rendering
 * - Proper test IDs
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ProcessStep } from '../../../../src/components/home/ProcessStep';
import { Link } from 'lucide-react';

describe('ProcessStep', () => {
  const mockStepData = {
    stepNumber: 1,
    title: 'Test Step',
    description: 'Test description',
    icon: <Link data-testid="test-icon" />,
  };

  describe('Step Number Display', () => {
    it('renders the step number in a badge', () => {
      render(<ProcessStep {...mockStepData} />);

      const stepNumber = screen.getByText('1');
      expect(stepNumber).toBeInTheDocument();
      expect(stepNumber.closest('.step-number')).toHaveClass('rounded-full');
    });

    it('step number badge is aria-hidden', () => {
      render(<ProcessStep {...mockStepData} />);

      const stepNumber = screen.getByText('1');
      expect(stepNumber.closest('.step-number')).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Title Rendering', () => {
    it('renders the step title as h3', () => {
      render(<ProcessStep {...mockStepData} />);

      const title = screen.getByRole('heading', { level: 3 });
      expect(title).toBeInTheDocument();
      expect(title).toHaveTextContent('Test Step');
    });

    it('title has proper test ID', () => {
      render(<ProcessStep {...mockStepData} />);

      const title = screen.getByTestId('step-1-title');
      expect(title).toHaveTextContent('Test Step');
    });
  });

  describe('Description Rendering', () => {
    it('renders the description when provided', () => {
      render(<ProcessStep {...mockStepData} />);

      const description = screen.getByTestId('step-1-description');
      expect(description).toBeInTheDocument();
      expect(description).toHaveTextContent('Test description');
    });

    it('does not render description when not provided', () => {
      const { stepNumber, title, icon } = mockStepData;
      render(<ProcessStep stepNumber={stepNumber} title={title} description="" icon={icon} />);

      const description = screen.queryByTestId('step-1-description');
      expect(description).not.toBeInTheDocument();
    });
  });

  describe('Icon Rendering', () => {
    it('renders the icon in icon container', () => {
      render(<ProcessStep {...mockStepData} />);

      const iconContainer = screen.getByTestId('step-1-icon');
      expect(iconContainer).toBeInTheDocument();
      expect(iconContainer).toHaveClass('step-icon');
    });

    it('icon container is aria-hidden', () => {
      render(<ProcessStep {...mockStepData} />);

      const iconContainer = screen.getByTestId('step-1-icon');
      expect(iconContainer).toHaveAttribute('aria-hidden', 'true');
    });

    it('renders the actual icon element', () => {
      render(<ProcessStep {...mockStepData} />);

      const icon = screen.getByTestId('test-icon');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Component Structure', () => {
    it('has proper data-testid based on step number', () => {
      render(<ProcessStep {...mockStepData} />);

      const stepContainer = screen.getByTestId('process-step-1');
      expect(stepContainer).toBeInTheDocument();
    });

    it('applies custom className when provided', () => {
      render(<ProcessStep {...mockStepData} className="custom-class" />);

      const stepContainer = screen.getByTestId('process-step-1');
      expect(stepContainer).toHaveClass('custom-class');
    });

    it('has process-step base class', () => {
      render(<ProcessStep {...mockStepData} />);

      const stepContainer = screen.getByTestId('process-step-1');
      expect(stepContainer).toHaveClass('process-step');
    });
  });

  describe('Different Step Numbers', () => {
    it('renders step 2 with correct test IDs', () => {
      render(
        <ProcessStep
          stepNumber={2}
          title="Step Two"
          description="Second step"
          icon={<Link data-testid="icon-2" />}
        />
      );

      expect(screen.getByTestId('process-step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-2-title')).toHaveTextContent('Step Two');
      expect(screen.getByTestId('step-2-description')).toHaveTextContent('Second step');
      expect(screen.getByText('2')).toBeInTheDocument();
    });

    it('renders step 3 with correct test IDs', () => {
      render(
        <ProcessStep
          stepNumber={3}
          title="Step Three"
          description="Third step"
          icon={<Link data-testid="icon-3" />}
        />
      );

      expect(screen.getByTestId('process-step-3')).toBeInTheDocument();
      expect(screen.getByTestId('step-3-title')).toHaveTextContent('Step Three');
      expect(screen.getByText('3')).toBeInTheDocument();
    });
  });
});
