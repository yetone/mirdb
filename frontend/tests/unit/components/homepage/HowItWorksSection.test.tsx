/**
 * HowItWorksSection Component Unit Tests
 *
 * Tests for Scenario 8: How It Works Section
 * Verifies that the How It Works section displays the three-step URL shortening
 * process as specified in REQ-7.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { HowItWorksSection } from '@/components/homepage/HowItWorksSection';

describe('HowItWorksSection', () => {
  describe('Test Case 1: Render HowItWorksSection component', () => {
    it('renders three steps with visual distinction', () => {
      render(<HowItWorksSection />);

      // Check that all three steps are rendered
      const step1 = screen.getByText(/Step 1/i);
      const step2 = screen.getByText(/Step 2/i);
      const step3 = screen.getByText(/Step 3/i);

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();
    });

    it('renders exactly three step items', () => {
      render(<HowItWorksSection />);

      // Each step has a heading role (h3) for the title
      const stepTitles = screen.getAllByRole('heading', { level: 3 });
      expect(stepTitles).toHaveLength(3);
    });

    it('renders the main section heading', () => {
      render(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent(/How It Works/i);
    });
  });

  describe('Test Case 2: Query for step 1 content', () => {
    it('displays step 1 with number indicator', () => {
      render(<HowItWorksSection />);

      const stepBadge = screen.getByText(/Step 1/i);
      expect(stepBadge).toBeInTheDocument();
    });

    it('shows icon for step 1', () => {
      render(<HowItWorksSection />);

      // Check for the paste/clipboard icon emoji
      const icon = screen.getByText('📋');
      expect(icon).toBeInTheDocument();
    });

    it('shows text about pasting long URL', () => {
      render(<HowItWorksSection />);

      // Check for title about pasting URL
      const title = screen.getByText(/Paste your long URL/i);
      expect(title).toBeInTheDocument();

      // Check for description about entering URL
      const description = screen.getByText(/Enter any long URL/i);
      expect(description).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Query for step 2 content', () => {
    it('displays step 2 with number indicator', () => {
      render(<HowItWorksSection />);

      const stepBadge = screen.getByText(/Step 2/i);
      expect(stepBadge).toBeInTheDocument();
    });

    it('shows icon for step 2', () => {
      render(<HowItWorksSection />);

      // Check for the sparkle/magic icon emoji
      const icon = screen.getByText('✨');
      expect(icon).toBeInTheDocument();
    });

    it('shows text about getting short link', () => {
      render(<HowItWorksSection />);

      // Check for title about getting short link
      const title = screen.getByText(/Get your short link/i);
      expect(title).toBeInTheDocument();

      // Check for description about receiving short link
      const description = screen.getByText(/Instantly receive a short.*memorable link/i);
      expect(description).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Query for step 3 content', () => {
    it('displays step 3 with number indicator', () => {
      render(<HowItWorksSection />);

      const stepBadge = screen.getByText(/Step 3/i);
      expect(stepBadge).toBeInTheDocument();
    });

    it('shows icon for step 3', () => {
      render(<HowItWorksSection />);

      // Check for the chart/analytics icon emoji
      const icon = screen.getByText('📈');
      expect(icon).toBeInTheDocument();
    });

    it('shows text about tracking performance', () => {
      render(<HowItWorksSection />);

      // Check for title about tracking performance
      const title = screen.getByText(/Track performance/i);
      expect(title).toBeInTheDocument();

      // Check for description about monitoring
      const description = screen.getByText(/Monitor clicks.*locations.*referrers/i);
      expect(description).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Verify visual flow between steps', () => {
    it('steps are displayed in a numbered sequence', () => {
      render(<HowItWorksSection />);

      // Verify all step badges are present in order
      const badges = screen.getAllByText(/Step \d/i);
      expect(badges).toHaveLength(3);

      // Verify the order
      expect(badges[0]).toHaveTextContent('Step 1');
      expect(badges[1]).toHaveTextContent('Step 2');
      expect(badges[2]).toHaveTextContent('Step 3');
    });

    it('steps are visually connected via flex layout', () => {
      render(<HowItWorksSection />);

      // Check that the container uses flex layout for visual flow
      const section = screen.getByRole('region', { name: /How It Works/i });
      expect(section).toBeInTheDocument();

      // Check for flex container
      const flexContainer = section.querySelector('[class*="flex"]');
      expect(flexContainer).toBeInTheDocument();
    });

    it('uses badge components to indicate step numbers', () => {
      render(<HowItWorksSection />);

      // Check that badges are styled with DaisyUI badge class
      const badges = screen.getAllByText(/Step \d/i);
      badges.forEach((badge) => {
        expect(badge.className).toMatch(/badge/);
      });
    });

    it('maintains correct step order in DOM', () => {
      render(<HowItWorksSection />);

      // Get all step headings
      const stepTitles = screen.getAllByRole('heading', { level: 3 });

      expect(stepTitles[0]).toHaveTextContent(/Paste your long URL/i);
      expect(stepTitles[1]).toHaveTextContent(/Get your short link/i);
      expect(stepTitles[2]).toHaveTextContent(/Track performance/i);
    });
  });

  describe('Accessibility', () => {
    it('uses semantic section element with aria-labelledby', () => {
      render(<HowItWorksSection />);

      const section = screen.getByRole('region', { name: /How It Works/i });
      expect(section).toBeInTheDocument();
      expect(section.tagName).toBe('SECTION');
    });

    it('heading has proper id for aria-labelledby reference', () => {
      render(<HowItWorksSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'how-it-works-title');
    });
  });

  describe('Custom steps prop', () => {
    it('accepts custom steps through props', () => {
      const customSteps = [
        { step: 1, icon: '🔗', title: 'Custom Step 1', description: 'Custom description 1' },
        { step: 2, icon: '🚀', title: 'Custom Step 2', description: 'Custom description 2' },
        { step: 3, icon: '📊', title: 'Custom Step 3', description: 'Custom description 3' },
      ];

      render(<HowItWorksSection steps={customSteps} />);

      expect(screen.getByText('Custom Step 1')).toBeInTheDocument();
      expect(screen.getByText('Custom Step 2')).toBeInTheDocument();
      expect(screen.getByText('Custom Step 3')).toBeInTheDocument();
      expect(screen.getByText('🔗')).toBeInTheDocument();
      expect(screen.getByText('🚀')).toBeInTheDocument();
      expect(screen.getByText('📊')).toBeInTheDocument();
    });
  });
});
