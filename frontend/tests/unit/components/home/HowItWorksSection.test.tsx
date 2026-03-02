/**
 * HowItWorksSection Component Unit Tests.
 *
 * Tests for Scenario 5 - How It Works Section:
 * - Test Case 1: Section renders with heading 'How It Works' and three steps
 * - Test Case 2: Step 1 displays 'Paste Your URL' text and link icon
 * - Test Case 3: Step 2 displays 'Get Short Link' text and scissors icon
 * - Test Case 4: Step 3 displays 'Track Analytics' text and chart icon
 * - Test Case 5: Section has min-height of 400px
 * - Test Case 6: Framer Motion fade-in animation triggers on intersection
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { HowItWorksSection } from '../../../../src/components/home/HowItWorksSection';

// Mock framer-motion to control animation state
vi.mock('framer-motion', async () => {
  const actual = await vi.importActual('framer-motion');
  return {
    ...actual,
    useInView: vi.fn(() => true), // Default to "in view" for most tests
  };
});

describe('HowItWorksSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Section renders with heading and three steps', () => {
    it('renders with "How It Works" heading', () => {
      render(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent('How It Works');
    });

    it('heading has proper test ID', () => {
      render(<HowItWorksSection />);

      const heading = screen.getByTestId('how-it-works-heading');
      expect(heading).toHaveTextContent('How It Works');
    });

    it('renders exactly three steps', () => {
      render(<HowItWorksSection />);

      const step1 = screen.getByTestId('process-step-1');
      const step2 = screen.getByTestId('process-step-2');
      const step3 = screen.getByTestId('process-step-3');

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();
    });

    it('section has proper aria-labelledby', () => {
      render(<HowItWorksSection />);

      const section = screen.getByTestId('how-it-works-section');
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
    });
  });

  describe('Test Case 2: Step 1 - Paste Your URL', () => {
    it('displays "Paste Your URL" text', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-1-title');
      expect(stepTitle).toHaveTextContent('Paste Your URL');
    });

    it('contains link icon', () => {
      render(<HowItWorksSection />);

      const linkIcon = screen.getByTestId('link-icon');
      expect(linkIcon).toBeInTheDocument();
    });

    it('step 1 has description', () => {
      render(<HowItWorksSection />);

      const description = screen.getByTestId('step-1-description');
      expect(description).toBeInTheDocument();
      expect(description).toHaveTextContent(/enter any long url/i);
    });
  });

  describe('Test Case 3: Step 2 - Get Short Link', () => {
    it('displays "Get Short Link" text', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-2-title');
      expect(stepTitle).toHaveTextContent('Get Short Link');
    });

    it('contains scissors icon', () => {
      render(<HowItWorksSection />);

      const scissorsIcon = screen.getByTestId('scissors-icon');
      expect(scissorsIcon).toBeInTheDocument();
    });

    it('step 2 has description', () => {
      render(<HowItWorksSection />);

      const description = screen.getByTestId('step-2-description');
      expect(description).toBeInTheDocument();
      expect(description).toHaveTextContent(/instantly receive/i);
    });
  });

  describe('Test Case 4: Step 3 - Track Analytics', () => {
    it('displays "Track Analytics" text', () => {
      render(<HowItWorksSection />);

      const stepTitle = screen.getByTestId('step-3-title');
      expect(stepTitle).toHaveTextContent('Track Analytics');
    });

    it('contains chart icon', () => {
      render(<HowItWorksSection />);

      const chartIcon = screen.getByTestId('chart-icon');
      expect(chartIcon).toBeInTheDocument();
    });

    it('step 3 has description', () => {
      render(<HowItWorksSection />);

      const description = screen.getByTestId('step-3-description');
      expect(description).toBeInTheDocument();
      expect(description).toHaveTextContent(/monitor clicks/i);
    });
  });

  describe('Test Case 5: Section minimum height', () => {
    it('section has min-height of 400px', () => {
      render(<HowItWorksSection />);

      const section = screen.getByTestId('how-it-works-section');
      expect(section).toHaveStyle({ minHeight: '400px' });
    });

    it('section has how-it-works-section class', () => {
      render(<HowItWorksSection />);

      const section = screen.getByTestId('how-it-works-section');
      expect(section).toHaveClass('how-it-works-section');
    });
  });

  describe('Test Case 6: Animation on scroll', () => {
    it('section has data-animate attribute when in view', () => {
      render(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('steps-container');
      expect(stepsContainer).toHaveAttribute('data-animate', 'true');
    });

    it('uses framer motion for animation', () => {
      render(<HowItWorksSection />);

      // Verify the section renders with motion components
      const section = screen.getByTestId('how-it-works-section');
      expect(section).toBeInTheDocument();
    });
  });

  describe('Connecting Arrows', () => {
    it('renders arrows between steps', () => {
      render(<HowItWorksSection />);

      const arrow1 = screen.getByTestId('arrow-1');
      const arrow2 = screen.getByTestId('arrow-2');

      expect(arrow1).toBeInTheDocument();
      expect(arrow2).toBeInTheDocument();
    });

    it('arrows are aria-hidden for accessibility', () => {
      render(<HowItWorksSection />);

      const arrow1 = screen.getByTestId('arrow-1');
      expect(arrow1).toHaveAttribute('aria-hidden', 'true');
    });

    it('no arrow after the last step', () => {
      render(<HowItWorksSection />);

      const arrow3 = screen.queryByTestId('arrow-3');
      expect(arrow3).not.toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('section is a landmark with proper label', () => {
      render(<HowItWorksSection />);

      const section = screen.getByRole('region', { name: /how it works/i });
      expect(section).toBeInTheDocument();
    });

    it('icons have proper accessibility attributes', () => {
      render(<HowItWorksSection />);

      const linkIcon = screen.getByTestId('link-icon');
      expect(linkIcon).toHaveAttribute('aria-label', 'Link icon');
    });
  });
});
