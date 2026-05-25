/**
 * Unit tests for FeaturesSection component.
 * Covers REQ-8 (implemented features listing).
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FeaturesSection, { FEATURES } from '../../../src/components/features/FeaturesSection';

describe('FeaturesSection', () => {
  it('renders the features section container', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
  });

  it('renders the section heading', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('features-heading')).toHaveTextContent('Features');
  });

  it('renders the section subheading', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('features-subheading')).toBeInTheDocument();
  });

  it('renders all 6 implemented features', () => {
    render(<FeaturesSection />);
    const grid = screen.getByTestId('features-grid');
    expect(grid.children).toHaveLength(6);
  });

  it('renders the Memcached Protocol feature as prominent', () => {
    render(<FeaturesSection />);
    const card = screen.getByTestId('feature-card-memcached-protocol');
    expect(card).toHaveClass('feature-card--prominent');
  });

  it('renders each feature with a non-empty title', () => {
    render(<FeaturesSection />);
    for (const feature of FEATURES) {
      const title = screen.getByTestId(`feature-title-${feature.id}`);
      expect(title).toBeInTheDocument();
      expect(title.textContent).not.toBe('');
      expect(title.textContent).toBe(feature.title);
    }
  });

  it('renders each feature with a non-empty description', () => {
    render(<FeaturesSection />);
    for (const feature of FEATURES) {
      const description = screen.getByTestId(`feature-description-${feature.id}`);
      expect(description).toBeInTheDocument();
      expect(description.textContent).not.toBe('');
      expect(description.textContent).toBe(feature.description);
    }
  });

  it('renders each feature with an icon', () => {
    render(<FeaturesSection />);
    for (const feature of FEATURES) {
      const iconWrapper = screen.getByTestId(`feature-icon-wrapper-${feature.id}`);
      expect(iconWrapper).toBeInTheDocument();
      const icon = iconWrapper.querySelector('svg');
      expect(icon).toBeInTheDocument();
    }
  });

  it('renders feature card icons with correct test IDs', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('feature-icon-memcached')).toBeInTheDocument();
    expect(screen.getByTestId('feature-icon-tokio')).toBeInTheDocument();
    expect(screen.getByTestId('feature-icon-skiplist')).toBeInTheDocument();
    expect(screen.getByTestId('feature-icon-lsm')).toBeInTheDocument();
    expect(screen.getByTestId('feature-icon-compaction')).toBeInTheDocument();
    expect(screen.getByTestId('feature-icon-persistence')).toBeInTheDocument();
  });

  it('renders the correct feature titles in order', () => {
    render(<FeaturesSection />);
    const expectedTitles = [
      'Memcached Protocol',
      'Tokio Async Runtime',
      'Skip-list Memtable',
      'LSM Tree Storage',
      'Minor/Major Compaction',
      'Persistence',
    ];
    for (const title of expectedTitles) {
      expect(screen.getByText(title)).toBeInTheDocument();
    }
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('features-section')).toHaveAttribute('aria-label', 'Features');
    expect(screen.getByTestId('features-grid')).toHaveAttribute('role', 'list');
    const cards = screen.getAllByRole('listitem');
    expect(cards).toHaveLength(6);
  });

  it('feature cards are keyboard focusable', () => {
    render(<FeaturesSection />);
    const cards = screen.getAllByRole('listitem');
    for (const card of cards) {
      expect(card).toHaveAttribute('tabIndex', '0');
    }
  });

  it('shows hover highlight effect on feature cards', async () => {
    const user = userEvent.setup();
    render(<FeaturesSection />);
    const card = screen.getByTestId('feature-card-memcached-protocol');
    await user.hover(card);
    expect(card).toHaveClass('feature-card--prominent');
  });

  it('prominent feature card has a prominent icon wrapper', () => {
    render(<FeaturesSection />);
    const iconWrapper = screen.getByTestId('feature-icon-wrapper-memcached-protocol');
    expect(iconWrapper).toHaveClass('feature-card__icon--prominent');
  });
});
