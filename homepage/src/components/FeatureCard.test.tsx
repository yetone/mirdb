import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeatureCard } from './FeatureCard';

describe('FeatureCard', () => {
  it('renders with title and description', () => {
    render(
      <FeatureCard
        title="Test Feature"
        description="Test description for the feature"
        testId="test-feature"
      />
    );

    expect(screen.getByText('Test Feature')).toBeInTheDocument();
    expect(screen.getByText('Test description for the feature')).toBeInTheDocument();
  });

  it('renders with an optional icon', () => {
    const TestIcon = () => <svg data-testid="test-icon" />;

    render(
      <FeatureCard
        title="Feature with Icon"
        description="Description"
        icon={<TestIcon />}
        testId="feature-with-icon"
      />
    );

    expect(screen.getByTestId('test-icon')).toBeInTheDocument();
    expect(screen.getByTestId('feature-with-icon-icon')).toBeInTheDocument();
  });

  it('renders without icon when not provided', () => {
    render(
      <FeatureCard
        title="Feature without Icon"
        description="Description"
        testId="feature-no-icon"
      />
    );

    expect(screen.queryByTestId('feature-no-icon-icon')).not.toBeInTheDocument();
  });

  it('applies correct test id to the card', () => {
    render(
      <FeatureCard
        title="Test Card"
        description="Description"
        testId="my-test-card"
      />
    );

    expect(screen.getByTestId('my-test-card')).toBeInTheDocument();
  });

  it('renders title in an h3 element', () => {
    render(
      <FeatureCard
        title="Heading Test"
        description="Description"
      />
    );

    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toHaveTextContent('Heading Test');
  });

  it('renders description in a paragraph element', () => {
    render(
      <FeatureCard
        title="Title"
        description="This is the paragraph description"
      />
    );

    const paragraph = screen.getByText('This is the paragraph description');
    expect(paragraph.tagName).toBe('P');
  });
});
