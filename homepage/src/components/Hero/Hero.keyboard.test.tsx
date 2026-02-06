import { describe, it, expect } from 'vitest';
import userEvent from '@testing-library/user-event';
import { render, screen } from '../../../tests/setup/test-utils';
import { Hero } from './Hero';

describe('Hero Component - Keyboard Accessibility', () => {
  /**
   * Test Case 6: Buttons are activatable via keyboard with visible focus states
   */
  it('CTA buttons are keyboard focusable', async () => {
    const user = userEvent.setup();
    render(<Hero />);

    const primaryButton = screen.getByRole('link', { name: /get started/i });
    const secondaryButton = screen.getByRole('link', { name: /learn more/i });

    // Tab to first button
    await user.tab();
    expect(primaryButton).toHaveFocus();

    // Tab to second button
    await user.tab();
    expect(secondaryButton).toHaveFocus();
  });

  it('buttons can receive focus programmatically', () => {
    render(<Hero />);

    const primaryButton = screen.getByRole('link', { name: /get started/i });
    const secondaryButton = screen.getByRole('link', { name: /learn more/i });

    // Focus primary button
    primaryButton.focus();
    expect(document.activeElement).toBe(primaryButton);

    // Focus secondary button
    secondaryButton.focus();
    expect(document.activeElement).toBe(secondaryButton);
  });

  it('hero section is properly structured for screen readers', () => {
    render(<Hero />);

    // Check section is a landmark
    const heroSection = screen.getByRole('region', { name: /build something amazing/i });
    expect(heroSection).toBeInTheDocument();

    // Check heading structure
    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveAttribute('id', 'hero-headline');
  });
});
