/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - FeatureCard renders title and description
 * - FeatureCard renders optional icon
 * - Features section renders all features
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeatureCard } from './FeatureCard';
import { Features } from './Features';
import type { Feature } from '../../types';
import { FEATURES } from '../../utils/constants';

describe('FeatureCard', () => {
  it('renders feature title and description', () => {
    const feature: Feature = {
      title: 'Test Feature',
      description: 'This is a test description',
    };

    render(<FeatureCard feature={feature} />);

    expect(screen.getByText('Test Feature')).toBeInTheDocument();
    expect(screen.getByText('This is a test description')).toBeInTheDocument();
  });

  it('renders feature with icon', () => {
    const feature: Feature = {
      title: 'Feature with Icon',
      description: 'Has an icon',
      icon: '🔌',
    };

    render(<FeatureCard feature={feature} />);

    expect(screen.getByText('🔌')).toBeInTheDocument();
    expect(screen.getByText('Feature with Icon')).toBeInTheDocument();
  });

  it('renders feature without icon when not provided', () => {
    const feature: Feature = {
      title: 'No Icon Feature',
      description: 'No icon here',
    };

    render(<FeatureCard feature={feature} />);

    const iconElement = document.querySelector('.feature-icon');
    expect(iconElement).toBeNull();
  });

  it('has correct semantic structure', () => {
    const feature: Feature = {
      title: 'Semantic Feature',
      description: 'Testing semantics',
      icon: '📋',
    };

    render(<FeatureCard feature={feature} />);

    const article = screen.getByTestId('feature-card');
    expect(article.tagName).toBe('ARTICLE');

    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toHaveTextContent('Semantic Feature');
  });
});

describe('Features', () => {
  it('renders the features section', () => {
    render(<Features />);

    expect(screen.getByRole('heading', { name: /features/i })).toBeInTheDocument();
  });

  it('renders all defined features', () => {
    render(<Features />);

    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards.length).toBe(FEATURES.length);
    expect(featureCards.length).toBeGreaterThanOrEqual(5);
  });

  it('displays Memcached protocol feature', () => {
    render(<Features />);

    // Check for title
    const titleElement = document.querySelector('.feature-title');
    expect(titleElement?.textContent).toMatch(/Memcached Protocol/i);

    // Check description mentions protocol
    const descriptionElements = document.querySelectorAll('.feature-description');
    const hasProtocolDescription = Array.from(descriptionElements).some(
      el => el.textContent?.toLowerCase().includes('memcached protocol')
    );
    expect(hasProtocolDescription).toBe(true);
  });

  it('displays disk persistence feature', () => {
    render(<Features />);

    expect(screen.getByText(/Disk Persistence/i)).toBeInTheDocument();
    expect(screen.getByText(/SSTable/i)).toBeInTheDocument();
  });

  it('displays LSM-tree feature', () => {
    render(<Features />);

    expect(screen.getByText(/LSM-Tree Architecture/i)).toBeInTheDocument();
    expect(screen.getByText(/Log-Structured Merge-Tree/i)).toBeInTheDocument();
  });

  it('displays skip list feature', () => {
    render(<Features />);

    expect(screen.getByText(/Skip List Memtables/i)).toBeInTheDocument();
    expect(screen.getByText(/skip list data structure/i)).toBeInTheDocument();
  });

  it('displays compaction feature', () => {
    render(<Features />);

    expect(screen.getByText(/Compaction/i, { selector: '.feature-title' })).toBeInTheDocument();
    expect(screen.getByText(/minor and major compaction/i)).toBeInTheDocument();
  });

  it('has proper accessibility attributes', () => {
    render(<Features />);

    const section = document.getElementById('features');
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading');

    const list = screen.getByRole('list');
    expect(list).toBeInTheDocument();

    const listItems = screen.getAllByRole('listitem');
    expect(listItems.length).toBeGreaterThanOrEqual(5);
  });
});
