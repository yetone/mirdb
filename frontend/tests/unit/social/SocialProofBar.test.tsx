import { render, screen } from '@testing-library/react';
import SocialProofBar from '../../../src/components/social/SocialProofBar';
import SocialProofBar from '../../../src/components/social/SocialProofBar.tsx';

describe('SocialProofBar', () => {
  it('displays all required metrics', () => {
    render(<SocialProofBar />);

    expect(screen.getByText('1M+')).toBeInTheDocument();
    expect(screen.getByText('URLs shortened')).toBeInTheDocument();

    expect(screen.getByText('10M+')).toBeInTheDocument();
    expect(screen.getByText('clicks tracked')).toBeInTheDocument();

    expect(screen.getByText('50K+')).toBeInTheDocument();
    expect(screen.getByText('active users')).toBeInTheDocument();
  });

  it('displays metrics in a grid layout', () => {
    const { container } = render(<SocialProofBar />);
    const grid = container.querySelector('.grid');

    expect(grid).toHaveClass('grid-cols-1');
    expect(grid).toHaveClass('md:grid-cols-3');
  });
});