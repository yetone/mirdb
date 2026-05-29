import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import FeaturesSection, { featuresData } from '../../src/components/Home/FeaturesSection';

describe('FeaturesSection', () => {
  it('renders the features section container', () => {
    render(<FeaturesSection />);
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
  });

  it('renders a section heading describing the features', () => {
    render(<FeaturesSection />);
    const heading = screen.getByTestId('features-heading');
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).toBeTruthy();
    expect(heading.textContent!.length).toBeGreaterThan(0);
  });

  it('renders exactly 4 feature cards', () => {
    render(<FeaturesSection />);
    const cards = screen.getAllByTestId('feature-card');
    expect(cards.length).toBe(4);
  });

  it('renders all feature cards inside the grid container', () => {
    render(<FeaturesSection />);
    const grid = screen.getByTestId('features-grid');
    const cards = within(grid).getAllByTestId('feature-card');
    expect(cards.length).toBe(4);
  });

  it('has a card for URL shortening', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title').map((el) => el.textContent);
    expect(titles.some((t) => t?.toLowerCase().includes('shortening'))).toBe(true);
  });

  it('has a card for analytics and click tracking', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title').map((el) => el.textContent);
    expect(titles.some((t) => t?.toLowerCase().includes('analytics'))).toBe(true);
  });

  it('has a card for security or privacy features', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title').map((el) => el.textContent);
    expect(
      titles.some(
        (t) =>
          t?.toLowerCase().includes('security') || t?.toLowerCase().includes('privacy')
      )
    ).toBe(true);
  });

  it('has a card for URL management', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title').map((el) => el.textContent);
    expect(titles.some((t) => t?.toLowerCase().includes('management'))).toBe(true);
  });

  it('each feature card has a descriptive title', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title');
    expect(titles.length).toBe(4);
    titles.forEach((titleEl) => {
      expect(titleEl.textContent).toBeTruthy();
      expect(titleEl.textContent!.length).toBeGreaterThan(3);
    });
  });

  it('each feature card has explanatory description text', () => {
    render(<FeaturesSection />);
    const descriptions = screen.getAllByTestId('feature-description');
    expect(descriptions.length).toBe(4);
    descriptions.forEach((descEl) => {
      expect(descEl.textContent).toBeTruthy();
      expect(descEl.textContent!.length).toBeGreaterThan(10);
    });
  });

  it('each feature card includes an icon element', () => {
    render(<FeaturesSection />);
    const icons = screen.getAllByTestId('feature-icon');
    expect(icons.length).toBe(4);
    icons.forEach((iconEl) => {
      expect(iconEl).toBeInTheDocument();
    });
  });

  it('exports featuresData with 4 feature definitions', () => {
    expect(featuresData).toHaveLength(4);
    featuresData.forEach((feature) => {
      expect(feature.title).toBeTruthy();
      expect(feature.description).toBeTruthy();
      expect(feature.icon).toBeTruthy();
    });
  });

  it('featuresData contains entries for URL shortening, analytics, security, and management', () => {
    const titles = featuresData.map((f) => f.title.toLowerCase());
    expect(titles.some((t) => t.includes('shortening'))).toBe(true);
    expect(titles.some((t) => t.includes('analytics'))).toBe(true);
    expect(titles.some((t) => t.includes('security') || t.includes('privacy'))).toBe(true);
    expect(titles.some((t) => t.includes('management'))).toBe(true);
  });
});
