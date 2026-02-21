/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 3 - Features Section
 *
 * Test cases:
 * 1. FeaturesSection renders exactly 4 FeatureCard components
 * 2. Feature card for 'URL Shortening' is present with correct description
 * 3. Feature card for 'Analytics Dashboard' is present with correct description
 * 4. Feature card for 'Secure & Reliable' is present with correct description
 * 5. Feature card for 'Share Insights' is present with correct description
 * 6. FeatureCard displays icon, title, and description
 * 7. Desktop viewport (1280px) shows 2-column grid layout
 * 8. Mobile viewport (375px) shows single column layout
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import FeaturesSection from '../../../src/components/homepage/FeaturesSection';
import FeatureCard from '../../../src/components/shared/FeatureCard';
import { FEATURES } from '../../../src/utils/constants';

describe('FeaturesSection', () => {
  // Test Case 1: FeaturesSection renders exactly 4 FeatureCard components
  it('renders exactly 4 feature cards', () => {
    render(<FeaturesSection />);

    // Get all feature cards by their specific test ids (url-shortening, analytics-dashboard, etc.)
    const urlShortening = screen.getByTestId('feature-card-url-shortening');
    const analytics = screen.getByTestId('feature-card-analytics-dashboard');
    const secure = screen.getByTestId('feature-card-secure-reliable');
    const share = screen.getByTestId('feature-card-share-insights');

    expect(urlShortening).toBeInTheDocument();
    expect(analytics).toBeInTheDocument();
    expect(secure).toBeInTheDocument();
    expect(share).toBeInTheDocument();

    // Count the feature card wrappers
    const featureGrid = screen.getByTestId('features-grid');
    const cards = featureGrid.children;
    expect(cards).toHaveLength(4);
  });

  // Test Case 2: Feature card for 'URL Shortening' with description about creating short links
  it('displays URL Shortening feature card with description about creating short links', () => {
    render(<FeaturesSection />);

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
    expect(urlShorteningCard).toBeInTheDocument();

    const title = within(urlShorteningCard).getByTestId('feature-card-title');
    expect(title).toHaveTextContent('URL Shortening');

    const description = within(urlShorteningCard).getByTestId('feature-card-description');
    expect(description.textContent?.toLowerCase()).toMatch(/short|link|url/);
  });

  // Test Case 3: Feature card for 'Analytics Dashboard' with description about tracking clicks
  it('displays Analytics Dashboard feature card with description about tracking clicks', () => {
    render(<FeaturesSection />);

    const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
    expect(analyticsCard).toBeInTheDocument();

    const title = within(analyticsCard).getByTestId('feature-card-title');
    expect(title).toHaveTextContent('Analytics Dashboard');

    const description = within(analyticsCard).getByTestId('feature-card-description');
    expect(description.textContent?.toLowerCase()).toMatch(/click|statistics|insights/);
  });

  // Test Case 4: Feature card for 'Secure & Reliable' with description about safety and uptime
  it('displays Secure & Reliable feature card with description about safety and uptime', () => {
    render(<FeaturesSection />);

    const secureCard = screen.getByTestId('feature-card-secure-reliable');
    expect(secureCard).toBeInTheDocument();

    const title = within(secureCard).getByTestId('feature-card-title');
    expect(title).toHaveTextContent('Secure & Reliable');

    const description = within(secureCard).getByTestId('feature-card-description');
    expect(description.textContent?.toLowerCase()).toMatch(/secure|protected|reliable|uptime|encrypted/);
  });

  // Test Case 5: Feature card for 'Share Insights' with description about share tokens
  it('displays Share Insights feature card with description about share tokens', () => {
    render(<FeaturesSection />);

    const shareCard = screen.getByTestId('feature-card-share-insights');
    expect(shareCard).toBeInTheDocument();

    const title = within(shareCard).getByTestId('feature-card-title');
    expect(title).toHaveTextContent('Share Insights');

    const description = within(shareCard).getByTestId('feature-card-description');
    expect(description.textContent?.toLowerCase()).toMatch(/share|token|statistics/);
  });

  // Test Case 7: Desktop viewport (1280px) shows 2-column grid layout
  it('has 2-column grid layout for desktop viewport', () => {
    render(<FeaturesSection />);

    const featuresGrid = screen.getByTestId('features-grid');
    expect(featuresGrid).toBeInTheDocument();

    // Check for grid layout with responsive columns
    expect(featuresGrid).toHaveClass('grid');
    // lg:grid-cols-2 for desktop 2x2 layout
    expect(featuresGrid).toHaveClass('lg:grid-cols-2');
  });

  // Test Case 8: Mobile viewport (375px) shows single column layout
  it('has single column layout for mobile viewport', () => {
    render(<FeaturesSection />);

    const featuresGrid = screen.getByTestId('features-grid');
    expect(featuresGrid).toBeInTheDocument();

    // Check for grid-cols-1 (default) for mobile
    expect(featuresGrid).toHaveClass('grid-cols-1');
  });

  // Additional test: Section has correct id for navigation
  it('has id="features" for navigation anchor', () => {
    render(<FeaturesSection />);

    const section = screen.getByTestId('features-section');
    expect(section).toHaveAttribute('id', 'features');
  });

  // Additional test: Section has proper heading
  it('displays a section heading', () => {
    render(<FeaturesSection />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading.textContent?.toLowerCase()).toMatch(/feature|capabilities|what we offer/i);
  });

  // Additional test: All features from constants are rendered
  it('renders all features from constants', () => {
    render(<FeaturesSection />);

    FEATURES.forEach((feature) => {
      expect(screen.getByText(feature.title)).toBeInTheDocument();
      expect(screen.getByText(feature.description)).toBeInTheDocument();
    });
  });
});

describe('FeatureCard', () => {
  // Test Case 6: FeatureCard displays icon element, title text, and description text
  it('displays icon element, title text, and description text', () => {
    const testFeature = {
      icon: 'link',
      title: 'Test Feature',
      description: 'This is a test feature description.',
    };

    render(
      <FeatureCard
        icon={testFeature.icon}
        title={testFeature.title}
        description={testFeature.description}
      />
    );

    // Check icon is rendered
    const icon = screen.getByTestId('feature-card-icon');
    expect(icon).toBeInTheDocument();

    // Check title is rendered
    const title = screen.getByTestId('feature-card-title');
    expect(title).toBeInTheDocument();
    expect(title).toHaveTextContent(testFeature.title);

    // Check description is rendered
    const description = screen.getByTestId('feature-card-description');
    expect(description).toBeInTheDocument();
    expect(description).toHaveTextContent(testFeature.description);
  });

  // Additional test: FeatureCard accepts optional className prop
  it('accepts optional className prop for additional styling', () => {
    render(
      <FeatureCard
        icon="link"
        title="Test"
        description="Description"
        className="custom-class"
      />
    );

    const card = screen.getByTestId('feature-card');
    expect(card).toHaveClass('custom-class');
  });

  // Additional test: FeatureCard renders different icons
  it('renders different icon types correctly', () => {
    const { rerender } = render(
      <FeatureCard icon="link" title="Link" description="Desc" />
    );
    expect(screen.getByTestId('feature-card-icon')).toBeInTheDocument();

    rerender(<FeatureCard icon="chart" title="Chart" description="Desc" />);
    expect(screen.getByTestId('feature-card-icon')).toBeInTheDocument();

    rerender(<FeatureCard icon="shield" title="Shield" description="Desc" />);
    expect(screen.getByTestId('feature-card-icon')).toBeInTheDocument();

    rerender(<FeatureCard icon="share" title="Share" description="Desc" />);
    expect(screen.getByTestId('feature-card-icon')).toBeInTheDocument();
  });

  // Additional test: Card has proper accessibility structure
  it('has proper semantic structure with heading', () => {
    render(
      <FeatureCard icon="link" title="Feature Title" description="Description" />
    );

    const heading = screen.getByRole('heading', { level: 3 });
    expect(heading).toHaveTextContent('Feature Title');
  });
});
