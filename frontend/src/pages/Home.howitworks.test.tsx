import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// How It Works Section Tests (Scenario: How It Works Section)
describe('Home - How It Works Section', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Section with 3 distinct steps is displayed
  describe('Test Case 1: Section with 3 distinct steps is displayed', () => {
    it('renders the How It Works section', () => {
      renderHome();

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();
    });

    it('renders the How It Works title', () => {
      renderHome();

      const title = screen.getByTestId('how-it-works-title');
      expect(title).toBeInTheDocument();
      expect(title.textContent).toBe('How It Works');
    });

    it('renders exactly 3 distinct steps', () => {
      renderHome();

      const step1 = screen.getByTestId('how-it-works-step-1');
      const step2 = screen.getByTestId('how-it-works-step-2');
      const step3 = screen.getByTestId('how-it-works-step-3');

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();
    });

    it('renders all steps within the steps container', () => {
      renderHome();

      const stepsContainer = screen.getByTestId('how-it-works-steps-container');
      expect(stepsContainer).toBeInTheDocument();

      // Verify all 3 steps are within the container
      const steps = stepsContainer.querySelectorAll('[data-testid^="how-it-works-step-"]');
      // Filter to only step containers (not indicators, titles, descriptions)
      const stepContainers = Array.from(steps).filter(
        (step) => step.getAttribute('data-testid')?.match(/^how-it-works-step-\d$/)
      );
      expect(stepContainers).toHaveLength(3);
    });
  });

  // Test Case 2: Step 1: 'Paste your long URL' content is displayed
  describe("Test Case 2: Step 1: 'Paste your long URL' content is displayed", () => {
    it('renders Step 1 with correct title', () => {
      renderHome();

      const step1Title = screen.getByTestId('how-it-works-step-1-title');
      expect(step1Title).toBeInTheDocument();
      expect(step1Title.textContent).toBe('Paste your long URL');
    });

    it('renders Step 1 with description', () => {
      renderHome();

      const step1Description = screen.getByTestId('how-it-works-step-1-description');
      expect(step1Description).toBeInTheDocument();
      expect(step1Description.textContent).toContain('long URL');
    });

    it('Step 1 description provides helpful context', () => {
      renderHome();

      const step1Description = screen.getByTestId('how-it-works-step-1-description');
      expect(step1Description.textContent).toContain('shorten');
    });
  });

  // Test Case 3: Step 2: 'Get your short link' content is displayed
  describe("Test Case 3: Step 2: 'Get your short link' content is displayed", () => {
    it('renders Step 2 with correct title', () => {
      renderHome();

      const step2Title = screen.getByTestId('how-it-works-step-2-title');
      expect(step2Title).toBeInTheDocument();
      expect(step2Title.textContent).toBe('Get your short link');
    });

    it('renders Step 2 with description', () => {
      renderHome();

      const step2Description = screen.getByTestId('how-it-works-step-2-description');
      expect(step2Description).toBeInTheDocument();
      expect(step2Description.textContent).toContain('URL');
    });

    it('Step 2 description mentions receiving the shortened URL', () => {
      renderHome();

      const step2Description = screen.getByTestId('how-it-works-step-2-description');
      expect(step2Description.textContent).toContain('Receive');
    });
  });

  // Test Case 4: Step 3: 'Track performance' content is displayed
  describe("Test Case 4: Step 3: 'Track performance' content is displayed", () => {
    it('renders Step 3 with correct title', () => {
      renderHome();

      const step3Title = screen.getByTestId('how-it-works-step-3-title');
      expect(step3Title).toBeInTheDocument();
      expect(step3Title.textContent).toBe('Track performance');
    });

    it('renders Step 3 with description', () => {
      renderHome();

      const step3Description = screen.getByTestId('how-it-works-step-3-description');
      expect(step3Description).toBeInTheDocument();
      expect(step3Description.textContent).toContain('analytics');
    });

    it('Step 3 description mentions monitoring or tracking', () => {
      renderHome();

      const step3Description = screen.getByTestId('how-it-works-step-3-description');
      expect(step3Description.textContent).toContain('Monitor');
    });
  });

  // Test Case 5: Steps have visual indicators (numbers, icons, or connectors)
  describe('Test Case 5: Steps have visual indicators (numbers, icons, or connectors)', () => {
    it('Step 1 has a visual number indicator showing "1"', () => {
      renderHome();

      const step1Indicator = screen.getByTestId('how-it-works-step-1-indicator');
      expect(step1Indicator).toBeInTheDocument();
      expect(step1Indicator.textContent).toBe('1');
    });

    it('Step 2 has a visual number indicator showing "2"', () => {
      renderHome();

      const step2Indicator = screen.getByTestId('how-it-works-step-2-indicator');
      expect(step2Indicator).toBeInTheDocument();
      expect(step2Indicator.textContent).toBe('2');
    });

    it('Step 3 has a visual number indicator showing "3"', () => {
      renderHome();

      const step3Indicator = screen.getByTestId('how-it-works-step-3-indicator');
      expect(step3Indicator).toBeInTheDocument();
      expect(step3Indicator.textContent).toBe('3');
    });

    it('all step indicators are styled as circular badges', () => {
      renderHome();

      const step1Indicator = screen.getByTestId('how-it-works-step-1-indicator');
      const step2Indicator = screen.getByTestId('how-it-works-step-2-indicator');
      const step3Indicator = screen.getByTestId('how-it-works-step-3-indicator');

      // Check that indicators have rounded-full class for circular appearance
      expect(step1Indicator.className).toContain('rounded-full');
      expect(step2Indicator.className).toContain('rounded-full');
      expect(step3Indicator.className).toContain('rounded-full');
    });

    it('step indicators have primary background color styling', () => {
      renderHome();

      const step1Indicator = screen.getByTestId('how-it-works-step-1-indicator');
      const step2Indicator = screen.getByTestId('how-it-works-step-2-indicator');
      const step3Indicator = screen.getByTestId('how-it-works-step-3-indicator');

      // Check that indicators have bg-primary class
      expect(step1Indicator.className).toContain('bg-primary');
      expect(step2Indicator.className).toContain('bg-primary');
      expect(step3Indicator.className).toContain('bg-primary');
    });

    it('step indicators have consistent dimensions', () => {
      renderHome();

      const step1Indicator = screen.getByTestId('how-it-works-step-1-indicator');
      const step2Indicator = screen.getByTestId('how-it-works-step-2-indicator');
      const step3Indicator = screen.getByTestId('how-it-works-step-3-indicator');

      // Check that all indicators have the same size classes
      expect(step1Indicator.className).toContain('w-16');
      expect(step1Indicator.className).toContain('h-16');
      expect(step2Indicator.className).toContain('w-16');
      expect(step2Indicator.className).toContain('h-16');
      expect(step3Indicator.className).toContain('w-16');
      expect(step3Indicator.className).toContain('h-16');
    });
  });

  // Additional structural tests
  describe('Section structure and accessibility', () => {
    it('section has proper id for anchor navigation', () => {
      renderHome();

      const section = screen.getByTestId('how-it-works-section');
      expect(section).toHaveAttribute('id', 'how-it-works');
    });

    it('section title is a proper heading element (h2)', () => {
      renderHome();

      const title = screen.getByTestId('how-it-works-title');
      expect(title.tagName.toLowerCase()).toBe('h2');
    });

    it('each step title is a proper heading element (h3)', () => {
      renderHome();

      const step1Title = screen.getByTestId('how-it-works-step-1-title');
      const step2Title = screen.getByTestId('how-it-works-step-2-title');
      const step3Title = screen.getByTestId('how-it-works-step-3-title');

      expect(step1Title.tagName.toLowerCase()).toBe('h3');
      expect(step2Title.tagName.toLowerCase()).toBe('h3');
      expect(step3Title.tagName.toLowerCase()).toBe('h3');
    });

    it('section has responsive layout classes', () => {
      renderHome();

      const stepsContainer = screen.getByTestId('how-it-works-steps-container');
      // Verify flex layout with responsive classes
      expect(stepsContainer.className).toContain('flex');
      expect(stepsContainer.className).toContain('flex-col');
      expect(stepsContainer.className).toContain('md:flex-row');
    });
  });
});
