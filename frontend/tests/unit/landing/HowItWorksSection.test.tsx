/**
 * Unit Tests for HowItWorksSection Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Test cases:
 * 1. Section heading "How It Works" is present
 * 2. At least 3 step elements are rendered with step numbers
 * 3. Step about signing up or creating account is present
 * 4. Step about pasting/entering URL is present
 * 5. Step about getting short link is present
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from './setup.tsx';
import HowItWorksSection from '../../../src/components/landing/HowItWorksSection';

describe('HowItWorksSection', () => {
  // Test Case 1: Section with heading 'How It Works' is present
  it('renders the How It Works section with correct heading', () => {
    render(<HowItWorksSection />);

    const heading = screen.getByRole('heading', { name: /how it works/i });
    expect(heading).toBeInTheDocument();
    expect(heading.tagName).toBe('H2');
  });

  // Test Case 2: At least 3 step elements are rendered with step numbers
  it('renders at least 3 step elements with step numbers', () => {
    render(<HowItWorksSection />);

    // Check for step number indicators
    const step1 = screen.getByTestId('step-number-1');
    const step2 = screen.getByTestId('step-number-2');
    const step3 = screen.getByTestId('step-number-3');

    expect(step1).toBeInTheDocument();
    expect(step1).toHaveTextContent('1');
    expect(step2).toBeInTheDocument();
    expect(step2).toHaveTextContent('2');
    expect(step3).toBeInTheDocument();
    expect(step3).toHaveTextContent('3');

    // Verify at least 3 steps exist
    const allSteps = screen.getAllByTestId(/^step-\d+$/);
    expect(allSteps.length).toBeGreaterThanOrEqual(3);
  });

  // Test Case 3: Step about signing up or creating account is present
  it('contains a step about signing up or creating account', () => {
    render(<HowItWorksSection />);

    // Look for sign up related content
    const signUpText = screen.getByText(/sign up/i);
    expect(signUpText).toBeInTheDocument();

    // Also check for "account" or "create" keywords in descriptions
    const accountText = screen.getByText(/account/i);
    expect(accountText).toBeInTheDocument();
  });

  // Test Case 4: Step about pasting/entering URL is present
  it('contains a step about pasting or entering URL', () => {
    render(<HowItWorksSection />);

    // Look for the step title that mentions pasting URL
    const pasteUrlTitle = screen.getByRole('heading', { name: /paste your url/i });
    expect(pasteUrlTitle).toBeInTheDocument();

    // Also verify description mentions URL-related content
    const enterUrlText = screen.getByText(/enter any long url/i);
    expect(enterUrlText).toBeInTheDocument();
  });

  // Test Case 5: Step about getting short link is present
  it('contains a step about getting short link', () => {
    render(<HowItWorksSection />);

    // Look for short link related content
    const shortLinkText = screen.getByText(/short link/i);
    expect(shortLinkText).toBeInTheDocument();

    // Also verify "shortened" or "shorten" content
    const shortenedText = screen.getByText(/shortened/i);
    expect(shortenedText).toBeInTheDocument();
  });

  // Additional test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<HowItWorksSection />);

    const section = document.querySelector('section#how-it-works');
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
  });

  // Additional test: Custom steps can be provided
  it('accepts custom steps via props', () => {
    const customSteps = [
      { number: 1, title: 'Custom Step 1', description: 'Custom description 1' },
      { number: 2, title: 'Custom Step 2', description: 'Custom description 2' },
      { number: 3, title: 'Custom Step 3', description: 'Custom description 3' },
    ];

    render(<HowItWorksSection steps={customSteps} />);

    expect(screen.getByText('Custom Step 1')).toBeInTheDocument();
    expect(screen.getByText('Custom Step 2')).toBeInTheDocument();
    expect(screen.getByText('Custom Step 3')).toBeInTheDocument();
  });

  // Additional test: All default steps are rendered (4 steps)
  it('renders all 4 default steps', () => {
    render(<HowItWorksSection />);

    const allSteps = screen.getAllByTestId(/^step-\d+$/);
    expect(allSteps).toHaveLength(4);

    // Verify step 4 (Track Analytics) is also present
    const step4 = screen.getByTestId('step-number-4');
    expect(step4).toBeInTheDocument();
    expect(step4).toHaveTextContent('4');

    // Verify analytics content
    const analyticsText = screen.getByText(/analytics/i);
    expect(analyticsText).toBeInTheDocument();
  });
});
