import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import FeatureCard from '../../src/components/Home/FeatureCard';

describe('FeatureCard', () => {
  it('renders feature card with icon, title, and description', () => {
    render(
      <FeatureCard
        icon={<span data-testid="custom-icon">🔧</span>}
        title="Test Feature"
        description="This is a test feature description."
      />
    );

    expect(screen.getByTestId('feature-card')).toBeInTheDocument();
    expect(screen.getByTestId('feature-title')).toHaveTextContent('Test Feature');
    expect(screen.getByTestId('feature-description')).toHaveTextContent(
      'This is a test feature description.'
    );
    expect(screen.getByTestId('custom-icon')).toBeInTheDocument();
  });

  it('uses GlassMorphismCard for consistent styling', () => {
    render(
      <FeatureCard
        icon={<span>🚀</span>}
        title="Rocket Feature"
        description="Blast off with this feature."
      />
    );

    const card = screen.getByTestId('feature-card');
    expect(card).toBeInTheDocument();
    expect(card.className).toMatch(/glass-card/);
  });

  it('renders the icon inside the feature-icon container', () => {
    render(
      <FeatureCard
        icon={<span data-testid="icon-content">📊</span>}
        title="Analytics"
        description="Track your metrics."
      />
    );

    const iconContainer = screen.getByTestId('feature-icon');
    expect(iconContainer).toBeInTheDocument();
    expect(iconContainer).toContainElement(screen.getByTestId('icon-content'));
  });

  it('renders the title as a heading element', () => {
    render(
      <FeatureCard
        icon={<span>🔒</span>}
        title="Secure Links"
        description="Protected with encryption."
      />
    );

    const title = screen.getByTestId('feature-title');
    expect(title.tagName.toLowerCase()).toBe('h3');
    expect(title).toHaveTextContent('Secure Links');
  });

  it('renders the description as a paragraph element', () => {
    render(
      <FeatureCard
        icon={<span>📁</span>}
        title="Management"
        description="Organize all your links."
      />
    );

    const description = screen.getByTestId('feature-description');
    expect(description.tagName.toLowerCase()).toBe('p');
    expect(description).toHaveTextContent('Organize all your links.');
  });

  it('applies the feature-card CSS class for styling hooks', () => {
    render(
      <FeatureCard
        icon={<span>✨</span>}
        title="Sparkle"
        description="Shiny feature."
      />
    );

    const card = screen.getByTestId('feature-card');
    expect(card.className).toMatch(/feature-card/);
  });
});
