import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import HeroSection from '../../../../src/components/home/HeroSection';

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('HeroSection', () => {
  it('renders the hero section with headline containing product name', () => {
    renderWithRouter(<HeroSection />);

    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('LinkSnip');
  });

  it('displays tagline text', () => {
    renderWithRouter(<HeroSection />);

    expect(screen.getByText('Shorten. Share. Track.')).toBeInTheDocument();
  });

  it('renders value proposition description', () => {
    renderWithRouter(<HeroSection />);

    expect(
      screen.getByText(/Transform long URLs into short, shareable links/i)
    ).toBeInTheDocument();
  });

  it('renders Get Started Free button', () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByRole('link', { name: /Get Started Free/i });
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveAttribute('href', '/register');
  });

  it('renders Learn More button', () => {
    renderWithRouter(<HeroSection />);

    const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });
    expect(learnMoreButton).toBeInTheDocument();
  });

  it('calls onGetStarted callback when Get Started Free is clicked', () => {
    const onGetStarted = vi.fn();
    renderWithRouter(<HeroSection onGetStarted={onGetStarted} />);

    const getStartedButton = screen.getByRole('link', { name: /Get Started Free/i });
    fireEvent.click(getStartedButton);

    expect(onGetStarted).toHaveBeenCalledTimes(1);
  });

  it('calls onLearnMore callback when Learn More is clicked', () => {
    const onLearnMore = vi.fn();
    renderWithRouter(<HeroSection onLearnMore={onLearnMore} />);

    const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });
    fireEvent.click(learnMoreButton);

    expect(onLearnMore).toHaveBeenCalledTimes(1);
  });

  it('renders the logo icon', () => {
    renderWithRouter(<HeroSection />);

    // The Link2 icon should be rendered (check for SVG element)
    const heroContent = screen.getByRole('heading', { level: 1 }).parentElement;
    expect(heroContent?.querySelector('svg')).toBeInTheDocument();
  });
});
