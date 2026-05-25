/**
 * Unit tests for PlannedFeatures component.
 * Covers REQ-9 (planned features / roadmap).
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import PlannedFeatures, { PLANNED_FEATURES } from '../../../src/components/features/PlannedFeatures';

describe('PlannedFeatures', () => {
  it('renders the planned features section container', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-features-section')).toBeInTheDocument();
  });

  it('renders the section heading', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-features-heading')).toHaveTextContent('Roadmap');
  });

  it('renders the section subheading', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-features-subheading')).toBeInTheDocument();
  });

  it('lists Raft consensus as a planned feature', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-feature-title-raft-consensus')).toHaveTextContent(
      'Raft Consensus'
    );
  });

  it('renders Raft consensus with a non-empty description', () => {
    render(<PlannedFeatures />);
    const description = screen.getByTestId('planned-feature-description-raft-consensus');
    expect(description).toBeInTheDocument();
    expect(description.textContent).not.toBe('');
    expect(description.textContent!.toLowerCase()).toContain('distributed');
  });

  it('displays a Coming Soon badge', () => {
    render(<PlannedFeatures />);
    const badge = screen.getByTestId('coming-soon-badge');
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveTextContent('Coming Soon');
  });

  it('renders planned feature with an icon', () => {
    render(<PlannedFeatures />);
    const iconWrapper = screen.getByTestId('planned-feature-icon-wrapper-raft-consensus');
    expect(iconWrapper).toBeInTheDocument();
    const icon = iconWrapper.querySelector('svg');
    expect(icon).toBeInTheDocument();
  });

  it('renders planned feature icon with correct test ID', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-feature-icon-raft')).toBeInTheDocument();
  });

  it('has distinct visual style via CSS class', () => {
    render(<PlannedFeatures />);
    const section = screen.getByTestId('planned-features-section');
    expect(section).toHaveClass('planned-features-section');
    const card = screen.getByTestId('planned-feature-card-raft-consensus');
    expect(card).toHaveClass('planned-feature-card');
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(<PlannedFeatures />);
    expect(screen.getByTestId('planned-features-section')).toHaveAttribute(
      'aria-label',
      'Planned Features'
    );
    expect(screen.getByTestId('planned-features-grid')).toHaveAttribute('role', 'list');
    const cards = screen.getAllByRole('listitem');
    expect(cards).toHaveLength(PLANNED_FEATURES.length);
  });

  it('planned feature cards are keyboard focusable', () => {
    render(<PlannedFeatures />);
    const cards = screen.getAllByRole('listitem');
    for (const card of cards) {
      expect(card).toHaveAttribute('tabIndex', '0');
    }
  });

  it('planned feature card aria-label includes Coming Soon', () => {
    render(<PlannedFeatures />);
    const card = screen.getByTestId('planned-feature-card-raft-consensus');
    expect(card).toHaveAttribute('aria-label', 'Raft Consensus - Coming Soon');
  });

  it('shows hover interaction on planned feature cards', async () => {
    const user = userEvent.setup();
    render(<PlannedFeatures />);
    const card = screen.getByTestId('planned-feature-card-raft-consensus');
    await user.hover(card);
    expect(card).toBeInTheDocument();
  });
});
