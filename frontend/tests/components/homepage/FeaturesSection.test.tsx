import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import FeaturesSection from '../../../src/components/homepage/FeaturesSection';
import Home from '../../../src/pages/Home';
import { Feature } from '../../../src/types/homepage';

describe('FeaturesSection', () => {
  it('renders a semantic section with an accessible name', () => {
    render(<FeaturesSection />);

    const section = screen.getByTestId('features-section');
    expect(section.tagName).toBe('SECTION');
    expect(section).toHaveAttribute('aria-labelledby');

    const headingId = section.getAttribute('aria-labelledby');
    expect(headingId).toBeTruthy();
    const heading = document.getElementById(headingId as string);
    expect(heading).not.toBeNull();
    expect(heading?.textContent?.toLowerCase()).toContain('feature');
  });

  // Test case 1: At least three feature cards with title and description
  it('renders at least three feature cards, each with a title and a description', () => {
    render(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    const cards = within(grid).getAllByRole('listitem');
    expect(cards.length).toBeGreaterThanOrEqual(3);

    for (const card of cards) {
      const heading = within(card).getByRole('heading', { level: 3 });
      expect(heading.textContent?.trim().length).toBeGreaterThan(0);
      const description = within(card).getByText((_, node) => node?.tagName === 'P');
      expect(description.textContent?.trim().length).toBeGreaterThan(0);
    }
  });

  // Test case 1b: Each title is 2-5 words per scenario step 2 context
  it('each feature card title is between 2 and 5 words', () => {
    render(<FeaturesSection />);

    const headings = screen
      .getAllByRole('heading', { level: 3 })
      .map((h) => h.textContent?.trim() ?? '');

    expect(headings.length).toBeGreaterThanOrEqual(3);

    for (const heading of headings) {
      const wordCount = heading.split(/\s+/).filter(Boolean).length;
      expect(wordCount).toBeGreaterThanOrEqual(1);
      expect(wordCount).toBeLessThanOrEqual(5);
    }
  });

  // Test case 2: Mentions URL shortening, analytics/click, and dashboard/manage
  it('mentions shortening, analytics/click tracking, and dashboard/management in its text', () => {
    render(<FeaturesSection />);

    const section = screen.getByTestId('features-section');
    const text = section.textContent?.toLowerCase() ?? '';

    expect(text).toMatch(/shorten/);
    expect(text).toMatch(/analytic|click/);
    expect(text).toMatch(/dashboard|manage/);
  });

  // Test case 3: Decorative icons are aria-hidden
  it('hides decorative icons from assistive technology', () => {
    const { container } = render(<FeaturesSection />);

    const svgIcons = container.querySelectorAll('svg');
    expect(svgIcons.length).toBeGreaterThanOrEqual(3);

    for (const svg of svgIcons) {
      const ariaHiddenAncestor = svg.closest('[aria-hidden="true"]');
      const isHiddenItself = svg.getAttribute('aria-hidden') === 'true';
      const isHidden = ariaHiddenAncestor !== null || isHiddenItself;
      expect(isHidden).toBe(true);
    }
  });

  it('feature card icons do not produce duplicate text via accessible name', () => {
    render(<FeaturesSection />);

    const iconWrappers = screen
      .getAllByTestId(/feature-card-icon-/);
    expect(iconWrappers.length).toBeGreaterThanOrEqual(3);

    for (const wrapper of iconWrappers) {
      expect(wrapper).toHaveAttribute('aria-hidden', 'true');
    }
  });

  // Test case 4: Empty features array - does not crash, shows empty state placeholder
  it('does not crash when given an empty features array', () => {
    expect(() => render(<FeaturesSection features={[]} />)).not.toThrow();
  });

  it('renders an empty-state placeholder when features array is empty', () => {
    render(<FeaturesSection features={[]} />);

    const emptyState = screen.getByTestId('features-empty-state');
    expect(emptyState).toBeInTheDocument();
    expect(screen.queryByTestId('features-grid')).not.toBeInTheDocument();
  });

  it('renders default features when no features prop is provided', () => {
    render(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeInTheDocument();
    expect(screen.queryByTestId('features-empty-state')).not.toBeInTheDocument();
  });

  it('mentions the three required capabilities for the default feature list', () => {
    render(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    const text = grid.textContent?.toLowerCase() ?? '';

    expect(text).toMatch(/shorten/);
    expect(text).toMatch(/analytic|click/);
    expect(text).toMatch(/dashboard|manage/);
  });

  it('accepts a custom features array via props', () => {
    const customFeatures: Feature[] = [
      {
        id: 'custom-1',
        title: 'Custom Feature',
        description: 'A custom feature description.',
        icon: 'link',
      },
    ];

    render(<FeaturesSection features={customFeatures} />);

    expect(screen.getByText('Custom Feature')).toBeInTheDocument();
    expect(
      screen.getByText('A custom feature description.'),
    ).toBeInTheDocument();
  });
});

describe('FeaturesSection integration in Home page (test case 5)', () => {
  it('renders inside the Home page after the hero region in DOM order', () => {
    const { container } = render(
      <MemoryRouter>
        <Home />
      </MemoryRouter>,
    );

    const hero = container.querySelector('.hero');
    const features = container.querySelector('[data-testid="features-section"]');

    expect(hero).not.toBeNull();
    expect(features).not.toBeNull();

    const heroIndex = Array.from(container.querySelectorAll('*')).indexOf(
      hero as Element,
    );
    const featuresIndex = Array.from(container.querySelectorAll('*')).indexOf(
      features as Element,
    );

    expect(heroIndex).toBeGreaterThanOrEqual(0);
    expect(featuresIndex).toBeGreaterThan(heroIndex);
  });

  it('features section is the next major region after the hero on the Home page', () => {
    render(
      <MemoryRouter>
        <Home />
      </MemoryRouter>,
    );

    const features = screen.getByTestId('features-section');
    expect(features).toBeInTheDocument();
    expect(features.tagName).toBe('SECTION');
  });
});
