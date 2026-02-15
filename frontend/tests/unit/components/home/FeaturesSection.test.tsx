import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import FeaturesSection from '../../../../src/components/home/FeaturesSection';

describe('FeaturesSection', () => {
  it('renders the section heading', () => {
    render(<FeaturesSection />);

    expect(screen.getByRole('heading', { name: /Powerful Features/i })).toBeInTheDocument();
  });

  it('renders 5 feature cards', () => {
    render(<FeaturesSection />);

    const featureCards = screen.getAllByTestId(/feature-card-/);
    expect(featureCards).toHaveLength(5);
  });

  it('displays URL shortening feature with title and description', () => {
    render(<FeaturesSection />);

    expect(screen.getByText(/Lightning Fast URL Shortening/i)).toBeInTheDocument();
    expect(screen.getByText(/Create short, memorable links in milliseconds/i)).toBeInTheDocument();
  });

  it('displays click analytics feature with title and description', () => {
    render(<FeaturesSection />);

    expect(screen.getByText(/Detailed Click Analytics/i)).toBeInTheDocument();
    expect(screen.getByText(/Track every click with comprehensive analytics/i)).toBeInTheDocument();
  });

  it('displays geo-location feature with title and description', () => {
    render(<FeaturesSection />);

    expect(screen.getByText(/Geo-Location Tracking/i)).toBeInTheDocument();
    expect(screen.getByText(/Understand your global audience/i)).toBeInTheDocument();
  });

  it('displays referrer analysis feature with title and description', () => {
    render(<FeaturesSection />);

    expect(screen.getByText(/Referrer Analysis/i)).toBeInTheDocument();
    expect(screen.getByText(/Know exactly where your traffic originates/i)).toBeInTheDocument();
  });

  it('displays security feature with title and description', () => {
    render(<FeaturesSection />);

    expect(screen.getByText(/Secure & Private/i)).toBeInTheDocument();
    expect(screen.getByText(/Your data is protected with enterprise-grade security/i)).toBeInTheDocument();
  });

  it('renders feature icons', () => {
    render(<FeaturesSection />);

    // Each feature card should have an SVG icon
    const featureCards = screen.getAllByTestId(/feature-card-/);
    featureCards.forEach((card) => {
      expect(card.querySelector('svg')).toBeInTheDocument();
    });
  });
});
