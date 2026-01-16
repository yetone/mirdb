import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import HowItWorksSection from './HowItWorksSection';

describe('HowItWorksSection', () => {
  describe('Test Case 1: Component renders 3 numbered steps with descriptions', () => {
    it('renders exactly 3 steps', () => {
      render(<HowItWorksSection />);

      // Check that exactly 3 steps are rendered
      const step1 = screen.getByTestId('step-1');
      const step2 = screen.getByTestId('step-2');
      const step3 = screen.getByTestId('step-3');

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();

      // Verify no 4th step exists
      expect(screen.queryByTestId('step-4')).not.toBeInTheDocument();
    });

    it('renders numbered indicators for each step', () => {
      render(<HowItWorksSection />);

      const number1 = screen.getByTestId('step-number-1');
      const number2 = screen.getByTestId('step-number-2');
      const number3 = screen.getByTestId('step-number-3');

      expect(number1).toHaveTextContent('1');
      expect(number2).toHaveTextContent('2');
      expect(number3).toHaveTextContent('3');
    });

    it('renders descriptions for each step', () => {
      render(<HowItWorksSection />);

      const desc1 = screen.getByTestId('step-description-1');
      const desc2 = screen.getByTestId('step-description-2');
      const desc3 = screen.getByTestId('step-description-3');

      expect(desc1).toBeInTheDocument();
      expect(desc2).toBeInTheDocument();
      expect(desc3).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Verify step content matches requirements', () => {
    it('Step 1 has correct title: "Paste your long URL"', () => {
      render(<HowItWorksSection />);

      const title1 = screen.getByTestId('step-title-1');
      expect(title1).toHaveTextContent('Paste your long URL');
    });

    it('Step 2 has correct title: "Get your short link instantly"', () => {
      render(<HowItWorksSection />);

      const title2 = screen.getByTestId('step-title-2');
      expect(title2).toHaveTextContent('Get your short link instantly');
    });

    it('Step 3 has correct title: "Track performance with analytics"', () => {
      render(<HowItWorksSection />);

      const title3 = screen.getByTestId('step-title-3');
      expect(title3).toHaveTextContent('Track performance with analytics');
    });

    it('all steps have informative descriptions', () => {
      render(<HowItWorksSection />);

      const desc1 = screen.getByTestId('step-description-1');
      const desc2 = screen.getByTestId('step-description-2');
      const desc3 = screen.getByTestId('step-description-3');

      // Descriptions should be non-empty and informative
      expect(desc1.textContent).toBeTruthy();
      expect(desc1.textContent!.length).toBeGreaterThan(10);

      expect(desc2.textContent).toBeTruthy();
      expect(desc2.textContent!.length).toBeGreaterThan(10);

      expect(desc3.textContent).toBeTruthy();
      expect(desc3.textContent!.length).toBeGreaterThan(10);
    });
  });

  describe('Test Case 3: Steps follow logical visual progression with numbered indicators', () => {
    it('renders section with proper heading', () => {
      render(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { name: /how it works/i });
      expect(heading).toBeInTheDocument();
    });

    it('step numbers appear in sequential order (1, 2, 3)', () => {
      render(<HowItWorksSection />);

      const number1 = screen.getByTestId('step-number-1');
      const number2 = screen.getByTestId('step-number-2');
      const number3 = screen.getByTestId('step-number-3');

      // Check that numbers are in correct sequence
      expect(number1).toHaveTextContent('1');
      expect(number2).toHaveTextContent('2');
      expect(number3).toHaveTextContent('3');
    });

    it('renders connector elements between steps for visual flow', () => {
      render(<HowItWorksSection />);

      // Check for connector elements (desktop)
      const connector1 = screen.getByTestId('connector-1');
      const connector2 = screen.getByTestId('connector-2');

      expect(connector1).toBeInTheDocument();
      expect(connector2).toBeInTheDocument();

      // Should not have a connector after the last step
      expect(screen.queryByTestId('connector-3')).not.toBeInTheDocument();
    });

    it('has accessible section landmark', () => {
      render(<HowItWorksSection />);

      const section = document.querySelector('section#how-it-works');
      expect(section).toBeInTheDocument();
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-title');
    });

    it('step numbers have proper aria labels', () => {
      render(<HowItWorksSection />);

      const number1 = screen.getByTestId('step-number-1');
      const number2 = screen.getByTestId('step-number-2');
      const number3 = screen.getByTestId('step-number-3');

      expect(number1).toHaveAttribute('aria-label', 'Step 1');
      expect(number2).toHaveAttribute('aria-label', 'Step 2');
      expect(number3).toHaveAttribute('aria-label', 'Step 3');
    });
  });
});
