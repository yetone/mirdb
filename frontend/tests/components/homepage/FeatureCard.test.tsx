import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import FeatureCard from '../../../src/components/homepage/FeatureCard';
import { Feature } from '../../../src/types/homepage';

const baseFeature: Feature = {
  id: 'sample',
  title: 'Sample Title',
  description: 'A sample description that describes the feature in one sentence.',
  icon: 'link',
};

describe('FeatureCard', () => {
  it('renders the feature title and description', () => {
    render(<FeatureCard feature={baseFeature} />);

    expect(screen.getByText('Sample Title')).toBeInTheDocument();
    expect(
      screen.getByText(/sample description/i),
    ).toBeInTheDocument();
  });

  it('renders the title as an h3 heading', () => {
    render(<FeatureCard feature={baseFeature} />);

    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toHaveTextContent('Sample Title');
  });

  it('uses an article element as the card root with an accessible name', () => {
    render(<FeatureCard feature={baseFeature} />);

    const card = screen.getByRole('article');
    expect(card).toHaveAttribute('aria-labelledby');

    const labelId = card.getAttribute('aria-labelledby');
    const labelElement = document.getElementById(labelId as string);
    expect(labelElement).not.toBeNull();
    expect(labelElement?.textContent).toBe('Sample Title');
  });

  it('marks the decorative icon as aria-hidden', () => {
    render(<FeatureCard feature={baseFeature} />);

    const iconWrapper = screen.getByTestId(`feature-card-icon-${baseFeature.id}`);
    expect(iconWrapper).toHaveAttribute('aria-hidden', 'true');
  });

  it('renders an SVG icon for the link variant', () => {
    render(<FeatureCard feature={{ ...baseFeature, icon: 'link' }} />);
    expect(screen.getByTestId('feature-icon-link')).toBeInTheDocument();
  });

  it('renders an SVG icon for the chart variant', () => {
    render(<FeatureCard feature={{ ...baseFeature, icon: 'chart' }} />);
    expect(screen.getByTestId('feature-icon-chart')).toBeInTheDocument();
  });

  it('renders an SVG icon for the dashboard variant', () => {
    render(<FeatureCard feature={{ ...baseFeature, icon: 'dashboard' }} />);
    expect(screen.getByTestId('feature-icon-dashboard')).toBeInTheDocument();
  });

  it('renders an SVG icon for the share variant', () => {
    render(<FeatureCard feature={{ ...baseFeature, icon: 'share' }} />);
    expect(screen.getByTestId('feature-icon-share')).toBeInTheDocument();
  });

  it('falls back to a default icon for unknown variants', () => {
    render(<FeatureCard feature={{ ...baseFeature, icon: 'unknown-icon' }} />);
    expect(screen.getByTestId('feature-icon-default')).toBeInTheDocument();
  });

  it('the icon SVG is wrapped in an aria-hidden container', () => {
    const { container } = render(<FeatureCard feature={baseFeature} />);

    const svg = container.querySelector('svg');
    expect(svg).not.toBeNull();
    const hiddenAncestor = svg!.closest('[aria-hidden="true"]');
    expect(hiddenAncestor).not.toBeNull();
  });
});
