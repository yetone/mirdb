/**
 * Unit tests for DemoSection Component
 * Owner: Scenario 6 - Demo Section Display
 *
 * Tests cover:
 * - Usage GIF rendering
 * - GIF source path verification
 * - Accessibility attributes (alt text)
 * - Contextual description display
 * - Section structure and semantic HTML
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { DemoSection } from '../../../src/components/sections/DemoSection';

describe('DemoSection', () => {
  // Test Case 1: Section contains usage demonstration GIF image
  it('contains usage demonstration GIF image', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('demo-gif');
    expect(gif).toBeInTheDocument();
    expect(gif.tagName.toLowerCase()).toBe('img');
  });

  // Test Case 2: GIF image loads from assets/usage.gif path
  it('GIF image loads from assets/usage.gif path', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('demo-gif');
    expect(gif).toHaveAttribute('src', '/assets/usage.gif');
  });

  // Test Case 3: Section has appropriate alt text for accessibility
  it('has appropriate alt text for accessibility', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('demo-gif');
    const altText = gif.getAttribute('alt');

    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(20); // Meaningful alt text
    expect(altText).toMatch(/mirdb/i);
    expect(altText).toMatch(/memcached/i);
    expect(altText).toMatch(/command/i);
  });

  // Test Case 5: Section includes contextual description of what demo shows
  it('includes contextual description of what demo shows', () => {
    render(<DemoSection />);

    const description = screen.getByTestId('demo-description');
    expect(description).toBeInTheDocument();

    const descriptionText = description.textContent;
    expect(descriptionText).toMatch(/memcached/i);
    expect(descriptionText).toMatch(/get|set/i);
    expect(descriptionText).toMatch(/command/i);
  });

  // Test: Demo section renders with correct section element and id
  it('renders with correct section element and id', () => {
    render(<DemoSection />);

    const section = screen.getByTestId('demo-section');
    expect(section).toBeInTheDocument();
    expect(section.tagName.toLowerCase()).toBe('section');
    expect(section).toHaveAttribute('id', 'demo');
  });

  // Test: Demo section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<DemoSection />);

    const section = screen.getByTestId('demo-section');
    expect(section).toHaveAttribute('aria-labelledby', 'demo-heading');

    const heading = screen.getByTestId('demo-heading');
    expect(heading).toHaveAttribute('id', 'demo-heading');
  });

  // Test: Heading displays appropriate title
  it('displays appropriate heading title', () => {
    render(<DemoSection />);

    const heading = screen.getByTestId('demo-heading');
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent(/mirdb|demo|action/i);
  });

  // Test: Demo container wraps the GIF
  it('renders demo container wrapper', () => {
    render(<DemoSection />);

    const container = screen.getByTestId('demo-container');
    expect(container).toBeInTheDocument();

    // GIF should be inside the container
    const gif = screen.getByTestId('demo-gif');
    expect(container).toContainElement(gif);
  });

  // Test: Demo has caption explaining the demonstration
  it('has caption explaining the demonstration', () => {
    render(<DemoSection />);

    const caption = screen.getByTestId('demo-caption');
    expect(caption).toBeInTheDocument();
    expect(caption.textContent).toMatch(/memcached/i);
    expect(caption.textContent).toMatch(/persist|durability/i);
  });

  // Test: GIF has lazy loading for performance
  it('has lazy loading attribute for performance', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('demo-gif');
    expect(gif).toHaveAttribute('loading', 'lazy');
  });

  // Test: Description mentions input/output operations
  it('description mentions input/output operations', () => {
    render(<DemoSection />);

    const description = screen.getByTestId('demo-description');
    const descriptionText = description.textContent?.toLowerCase() ?? '';

    // Should mention get/set operations which are input/output
    expect(descriptionText).toMatch(/get|set|operation/i);
  });

  // Test: Complete demo section renders with all elements
  it('renders complete demo section with all elements', () => {
    render(<DemoSection />);

    // Verify all main elements are present
    expect(screen.getByTestId('demo-section')).toBeInTheDocument();
    expect(screen.getByTestId('demo-heading')).toBeInTheDocument();
    expect(screen.getByTestId('demo-description')).toBeInTheDocument();
    expect(screen.getByTestId('demo-container')).toBeInTheDocument();
    expect(screen.getByTestId('demo-gif')).toBeInTheDocument();
    expect(screen.getByTestId('demo-caption')).toBeInTheDocument();
  });

  // Test: Alt text mentions command interactions
  it('alt text mentions command interactions', () => {
    render(<DemoSection />);

    const gif = screen.getByTestId('demo-gif');
    const altText = gif.getAttribute('alt');

    expect(altText).toMatch(/input|output|command/i);
    expect(altText).toMatch(/get|set|operation/i);
  });

  // Test: Section accepts custom className
  it('accepts custom className prop', () => {
    render(<DemoSection className="custom-class" />);

    const section = screen.getByTestId('demo-section');
    expect(section).toHaveClass('custom-class');
  });

  // Test: Section has role region through semantic HTML
  it('uses semantic HTML section element', () => {
    render(<DemoSection />);

    const section = screen.getByRole('region', { name: /mirdb|demo|action/i });
    expect(section).toBeInTheDocument();
  });
});
