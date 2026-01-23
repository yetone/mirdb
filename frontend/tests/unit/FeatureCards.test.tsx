/**
 * Feature Cards Unit Tests
 * Owner: Scenarios 4, 19
 *
 * Test coverage:
 * - Scenario 4: Feature cards display
 * - Scenario 19: Value proposition in features
 *
 * Test suites:
 * - describe('Feature Cards Rendering')
 * - describe('Feature Content')
 * - describe('GlassMorphismCard Usage')
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import FeatureCards from '../../src/components/homepage/FeatureCards';

describe('Feature Cards Rendering', () => {
  it('renders the feature section container', () => {
    renderWithProviders(<FeatureCards />);

    // Check for the section heading
    expect(screen.getByText('Powerful Features')).toBeInTheDocument();
  });

  it('renders 4-6 feature cards', () => {
    renderWithProviders(<FeatureCards />);

    // Find all feature card titles (h3 elements in cards)
    const featureTitles = [
      'URL Shortening',
      'Click Analytics',
      'Geographic Insights',
      'Browser & Device Data',
      'Shareable Stats',
      'Multi-Theme Support',
    ];

    const renderedTitles = featureTitles.filter(title =>
      screen.queryByText(title) !== null
    );

    // Should have between 4 and 6 feature cards
    expect(renderedTitles.length).toBeGreaterThanOrEqual(4);
    expect(renderedTitles.length).toBeLessThanOrEqual(6);
  });
});

describe('Feature Content', () => {
  it('renders URL shortening feature card with content about creating short links', () => {
    renderWithProviders(<FeatureCards />);

    // Check for URL Shortening card title
    expect(screen.getByText('URL Shortening')).toBeInTheDocument();

    // Check for description mentioning short links
    expect(screen.getByText(/short links/i)).toBeInTheDocument();
  });

  it('renders analytics feature card with content about click tracking and statistics', () => {
    renderWithProviders(<FeatureCards />);

    // Check for Click Analytics card title
    expect(screen.getByText('Click Analytics')).toBeInTheDocument();

    // Check for description mentioning tracking and statistics
    expect(screen.getByText(/Track every click with detailed statistics/i)).toBeInTheDocument();
  });

  it('renders geographic insights feature card with content about location tracking', () => {
    renderWithProviders(<FeatureCards />);

    // Check for Geographic Insights card title
    expect(screen.getByText('Geographic Insights')).toBeInTheDocument();

    // Check for description mentioning location or GeoIP
    expect(screen.getByText(/where your audience is located|GeoIP/i)).toBeInTheDocument();
  });

  it('renders browser and device data feature card', () => {
    renderWithProviders(<FeatureCards />);

    // Check for Browser & Device Data card title
    expect(screen.getByText('Browser & Device Data')).toBeInTheDocument();

    // Check for description about how users access links
    expect(screen.getByText(/how users access your links/i)).toBeInTheDocument();
  });

  it('renders shareable stats feature card with content about share tokens', () => {
    renderWithProviders(<FeatureCards />);

    // Check for Shareable Stats card title
    expect(screen.getByText('Shareable Stats')).toBeInTheDocument();

    // Check for description mentioning share tokens
    expect(screen.getByText(/share.*tokens?/i)).toBeInTheDocument();
  });

  it('renders multi-theme support feature card', () => {
    renderWithProviders(<FeatureCards />);

    // Check for Multi-Theme Support card title
    expect(screen.getByText('Multi-Theme Support')).toBeInTheDocument();

    // Check for description about theme options
    expect(screen.getByText(/theme options|customize/i)).toBeInTheDocument();
  });

  it('displays icons for each feature card', () => {
    renderWithProviders(<FeatureCards />);

    // Check that icon emojis are rendered
    expect(screen.getByText('🔗')).toBeInTheDocument();
    expect(screen.getByText('📊')).toBeInTheDocument();
    expect(screen.getByText('🌍')).toBeInTheDocument();
    expect(screen.getByText('💻')).toBeInTheDocument();
    expect(screen.getByText('📤')).toBeInTheDocument();
    expect(screen.getByText('🎨')).toBeInTheDocument();
  });
});

describe('GlassMorphismCard Usage', () => {
  it('renders feature cards with glassmorphism styling classes', () => {
    const { container } = renderWithProviders(<FeatureCards />);

    // GlassMorphismCard uses these classes: bg-base-100/70 backdrop-blur-md shadow-xl card
    const glassCards = container.querySelectorAll('.card.backdrop-blur-md');

    // Should have 6 glassmorphism cards
    expect(glassCards.length).toBe(6);
  });

  it('each feature card has card-body structure', () => {
    const { container } = renderWithProviders(<FeatureCards />);

    // GlassMorphismCard wraps children in card-body div
    const cardBodies = container.querySelectorAll('.card-body');

    // Should have 6 card bodies (one for each feature)
    expect(cardBodies.length).toBe(6);
  });

  it('feature cards are displayed in a responsive grid layout', () => {
    const { container } = renderWithProviders(<FeatureCards />);

    // Check for grid layout classes
    const gridContainer = container.querySelector('.grid');
    expect(gridContainer).toBeInTheDocument();

    // Check for responsive grid column classes
    expect(gridContainer).toHaveClass('grid-cols-1');
    expect(gridContainer).toHaveClass('md:grid-cols-2');
    expect(gridContainer).toHaveClass('lg:grid-cols-3');
  });
});
