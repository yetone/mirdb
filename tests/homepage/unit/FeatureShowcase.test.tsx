import { render, screen } from '@testing-library/react';
import { FeatureShowcase } from '../../components/homepage/FeatureShowcase';

describe('FeatureShowcase', () => {
  test('displays at least 3 feature cards in initial viewport', () => {
    render(<FeatureShowcase />);
    const featureCards = screen.getAllByRole('article');
    expect(featureCards).toHaveLength(3);
  });

  test('feature cards include icons and visual representations', () => {
    render(<FeatureShowcase />);
    const icons = screen.getAllByRole('img');
    expect(icons).toHaveLength(3);

    const titles = screen.getAllByRole('heading', { level: 3 });
    expect(titles).toHaveLength(3);
    expect(titles[0]).toHaveTextContent('Instant URL Shortening');
    expect(titles[1]).toHaveTextContent('Real-time Analytics');
    expect(titles[2]).toHaveTextContent('Team Collaboration');
  });

  test('feature descriptions are present and correct', () => {
    render(<FeatureShowcase />);
    expect(screen.getByText(/Transform lengthy URLs into concise, shareable links/)).toBeInTheDocument();
    expect(screen.getByText(/Track clicks, geographic data, and device information/)).toBeInTheDocument();
    expect(screen.getByText(/Share analytics with share tokens to collaborate/)).toBeInTheDocument();
  });
});