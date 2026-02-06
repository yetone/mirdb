import { describe, it, expect } from 'vitest';
import { render, screen } from '../../../tests/setup/test-utils';
import { Hero } from './Hero';
import { HERO_CONTENT } from '../../utils/constants';

describe('Hero Component', () => {
  /**
   * Test Case 1: Hero section contains h1 headline and subheadline paragraph
   */
  it('renders hero section with h1 headline and subheadline paragraph', () => {
    render(<Hero />);

    // Verify hero section exists
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Verify h1 headline
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent(HERO_CONTENT.headline);

    // Verify subheadline paragraph
    const subheadline = screen.getByText(HERO_CONTENT.subheadline);
    expect(subheadline).toBeInTheDocument();
    expect(subheadline.tagName.toLowerCase()).toBe('p');
  });

  /**
   * Test Case 2: Primary CTA button with "Get Started" text is present and visible
   */
  it('renders primary CTA button with "Get Started" text', () => {
    render(<Hero />);

    const primaryButton = screen.getByRole('link', { name: /get started/i });
    expect(primaryButton).toBeInTheDocument();
    expect(primaryButton).toBeVisible();
    expect(primaryButton).toHaveTextContent(HERO_CONTENT.primaryCTA.text);
    expect(primaryButton).toHaveAttribute('href', HERO_CONTENT.primaryCTA.href);
  });

  /**
   * Test Case 3: Secondary CTA button with "Learn More" text is present
   */
  it('renders secondary CTA button with "Learn More" text', () => {
    render(<Hero />);

    const secondaryButton = screen.getByRole('link', { name: /learn more/i });
    expect(secondaryButton).toBeInTheDocument();
    expect(secondaryButton).toHaveTextContent(HERO_CONTENT.secondaryCTA.text);
    expect(secondaryButton).toHaveAttribute('href', HERO_CONTENT.secondaryCTA.href);
  });

  /**
   * Test Case 6: Buttons have proper ARIA labels
   */
  it('renders CTA buttons with proper ARIA labels', () => {
    render(<Hero />);

    const primaryButton = screen.getByRole('link', { name: /get started/i });
    expect(primaryButton).toHaveAttribute(
      'aria-label',
      `${HERO_CONTENT.primaryCTA.text} - Create your account`
    );

    const secondaryButton = screen.getByRole('link', { name: /learn more/i });
    expect(secondaryButton).toHaveAttribute(
      'aria-label',
      `${HERO_CONTENT.secondaryCTA.text} - Explore features`
    );
  });

  it('hero section has proper accessibility attributes', () => {
    render(<Hero />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');
  });
});
