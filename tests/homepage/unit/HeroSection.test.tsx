import { render, screen } from '@testing-library/react';
import { HeroSection } from '../../components/homepage/HeroSection';

describe('HeroSection', () => {
  test('displays headline text', () => {
    render(<HeroSection />);
    expect(screen.getByText('Shorten Links, Track Insights')).toBeInTheDocument();
  });

  test('displays subheadline explaining value proposition', () => {
    render(<HeroSection />);
    expect(screen.getByText(/Transform lengthy URLs into trackable links/)).toBeInTheDocument();
  });

  test('shows primary CTA buttons', () => {
    render(<HeroSection />);
    expect(screen.getByText('Try Demo')).toBeInTheDocument();
    expect(screen.getByText('Get Started')).toBeInTheDocument();
  });
});