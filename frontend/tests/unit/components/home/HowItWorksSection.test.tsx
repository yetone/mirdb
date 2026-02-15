import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import HowItWorksSection from '../../../../src/components/home/HowItWorksSection';

describe('HowItWorksSection', () => {
  it('renders the section heading', () => {
    render(<HowItWorksSection />);

    expect(screen.getByRole('heading', { name: /How It Works/i })).toBeInTheDocument();
  });

  it('displays three steps', () => {
    render(<HowItWorksSection />);

    const steps = screen.getAllByTestId(/step-/);
    expect(steps).toHaveLength(3);
  });

  it('displays step 1 - Paste Your URL with description', () => {
    render(<HowItWorksSection />);

    expect(screen.getByText('Paste Your URL')).toBeInTheDocument();
    expect(screen.getByText(/Simply paste your long URL into the input field/i)).toBeInTheDocument();
  });

  it('displays step 2 - Get Short Link with description', () => {
    render(<HowItWorksSection />);

    expect(screen.getByText('Get Short Link')).toBeInTheDocument();
    expect(screen.getByText(/Click the shorten button and instantly receive/i)).toBeInTheDocument();
  });

  it('displays step 3 - Track Clicks with description', () => {
    render(<HowItWorksSection />);

    expect(screen.getByText('Track Clicks')).toBeInTheDocument();
    expect(screen.getByText(/Monitor your link performance with detailed analytics/i)).toBeInTheDocument();
  });

  it('displays step numbers', () => {
    render(<HowItWorksSection />);

    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('renders step icons', () => {
    render(<HowItWorksSection />);

    const steps = screen.getAllByTestId(/step-/);
    steps.forEach((step) => {
      expect(step.querySelector('svg')).toBeInTheDocument();
    });
  });
});
