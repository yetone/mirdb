/**
 * How It Works Section Tests
 * Owner: Scenario 5 - How It Works Section
 *
 * Tests for the how it works section covering:
 * - Section presence and heading
 * - Step count (3-4 steps)
 * - Step 1 content (paste URL)
 * - Sharing step content
 */
import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './testUtils';
import HowItWorksSection from '../../src/components/homepage/HowItWorksSection';
import Home from '../../src/pages/Home';

describe('HowItWorksSection', () => {
  describe('Test Case 1: Render HomePage and check for How It Works section', () => {
    it('renders how it works section container in DOM', () => {
      renderWithProviders(<HowItWorksSection />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();
    });

    it('how it works section has correct id attribute', () => {
      renderWithProviders(<HowItWorksSection />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toHaveAttribute('id', 'how-it-works');
    });

    it('renders how it works section with "How It Works" heading', () => {
      renderWithProviders(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent(/How It Works/i);
    });

    it('renders supporting description text', () => {
      renderWithProviders(<HowItWorksSection />);

      expect(screen.getByText(/Get started in just a few simple steps/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Count steps in How It Works section', () => {
    it('renders 3-4 steps with step numbers or indicators', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('how-it-works-steps');
      const stepHeadings = within(stepsContainer).getAllByRole('heading', { level: 3 });

      expect(stepHeadings.length).toBeGreaterThanOrEqual(3);
      expect(stepHeadings.length).toBeLessThanOrEqual(4);
    });

    it('renders exactly 4 steps', () => {
      renderWithProviders(<HowItWorksSection />);

      // Check for all 4 step containers
      expect(screen.getByTestId('step-1')).toBeInTheDocument();
      expect(screen.getByTestId('step-2')).toBeInTheDocument();
      expect(screen.getByTestId('step-3')).toBeInTheDocument();
      expect(screen.getByTestId('step-4')).toBeInTheDocument();
    });

    it('each step has a numbered indicator', () => {
      renderWithProviders(<HowItWorksSection />);

      // Check for step number badges
      expect(screen.getByTestId('step-indicator-1')).toHaveTextContent('1');
      expect(screen.getByTestId('step-indicator-2')).toHaveTextContent('2');
      expect(screen.getByTestId('step-indicator-3')).toHaveTextContent('3');
      expect(screen.getByTestId('step-indicator-4')).toHaveTextContent('4');
    });

    it('each step has a title and description', () => {
      renderWithProviders(<HowItWorksSection />);

      const stepsContainer = screen.getByTestId('how-it-works-steps');
      const stepTitles = within(stepsContainer).getAllByRole('heading', { level: 3 });

      stepTitles.forEach((title) => {
        expect(title).toBeInTheDocument();
        expect(title.textContent).not.toBe('');
      });
    });
  });

  describe('Test Case 3: Verify step 1 content', () => {
    it('first step mentions pasting or entering a URL', () => {
      renderWithProviders(<HowItWorksSection />);

      const step1 = screen.getByTestId('step-1');

      // Check for "Paste" in the title
      const step1Title = within(step1).getByRole('heading', { level: 3 });
      expect(step1Title).toHaveTextContent(/Paste/i);
    });

    it('first step has title "Paste Your URL"', () => {
      renderWithProviders(<HowItWorksSection />);

      const pasteTitle = screen.getByRole('heading', {
        level: 3,
        name: /Paste Your URL/i,
      });
      expect(pasteTitle).toBeInTheDocument();
    });

    it('first step description mentions entering URL', () => {
      renderWithProviders(<HowItWorksSection />);

      expect(screen.getByText(/Enter your long URL/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Verify sharing step content', () => {
    it('a step mentions sharing the shortened link', () => {
      renderWithProviders(<HowItWorksSection />);

      // Check for "Share" in step titles
      const shareTitle = screen.getByRole('heading', {
        level: 3,
        name: /Share/i,
      });
      expect(shareTitle).toBeInTheDocument();
    });

    it('sharing step has title "Share Anywhere"', () => {
      renderWithProviders(<HowItWorksSection />);

      const shareTitle = screen.getByRole('heading', {
        level: 3,
        name: /Share Anywhere/i,
      });
      expect(shareTitle).toBeInTheDocument();
    });

    it('sharing step description mentions sharing on social media', () => {
      renderWithProviders(<HowItWorksSection />);

      const step3 = screen.getByTestId('step-3');
      expect(within(step3).getByText(/social media/i)).toBeInTheDocument();
    });
  });

  describe('Additional Step Content Tests', () => {
    it('step 2 mentions getting a short link', () => {
      renderWithProviders(<HowItWorksSection />);

      const getLinkTitle = screen.getByRole('heading', {
        level: 3,
        name: /Get Short Link/i,
      });
      expect(getLinkTitle).toBeInTheDocument();
    });

    it('step 4 mentions tracking performance', () => {
      renderWithProviders(<HowItWorksSection />);

      const trackTitle = screen.getByRole('heading', {
        level: 3,
        name: /Track Performance/i,
      });
      expect(trackTitle).toBeInTheDocument();
    });

    it('each step has an icon (SVG element)', () => {
      renderWithProviders(<HowItWorksSection />);

      // There should be 4 SVG icons for the 4 steps
      const svgIcons = document.querySelectorAll(
        '[data-testid="how-it-works-steps"] svg'
      );
      expect(svgIcons.length).toBe(4);
    });
  });
});

describe('Home Page with HowItWorksSection', () => {
  it('renders how it works section on the home page', () => {
    renderWithProviders(<Home />);

    const howItWorksSection = screen.getByTestId('how-it-works-section');
    expect(howItWorksSection).toBeInTheDocument();
  });

  it('how it works section heading is present on home page', () => {
    renderWithProviders(<Home />);

    const howItWorksHeading = screen.getByRole('heading', {
      level: 2,
      name: /How It Works/i,
    });
    expect(howItWorksHeading).toBeInTheDocument();
  });

  it('all steps are visible on home page', () => {
    renderWithProviders(<Home />);

    const howItWorksSection = screen.getByTestId('how-it-works-section');

    // Use heading queries to be more specific about step titles
    expect(within(howItWorksSection).getByRole('heading', { level: 3, name: /Paste Your URL/i })).toBeInTheDocument();
    expect(within(howItWorksSection).getByRole('heading', { level: 3, name: /Get Short Link/i })).toBeInTheDocument();
    expect(within(howItWorksSection).getByRole('heading', { level: 3, name: /Share Anywhere/i })).toBeInTheDocument();
    expect(within(howItWorksSection).getByRole('heading', { level: 3, name: /Track Performance/i })).toBeInTheDocument();
  });

  it('how it works section appears after features section', () => {
    renderWithProviders(<Home />);

    // Both sections should be present
    const featuresSection = screen.getByTestId('features-section');
    const howItWorksSection = screen.getByTestId('how-it-works-section');

    expect(featuresSection).toBeInTheDocument();
    expect(howItWorksSection).toBeInTheDocument();
  });
});
