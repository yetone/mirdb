/**
 * MapMockup Component Tests.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Tests the map mockup component:
 * - Renders as static SVG
 * - Has proper accessibility attributes
 * - Supports animate prop
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { MapMockup } from '../../../../src/components/charts/MapMockup';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    g: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <g {...props}>{children}</g>
    ),
    circle: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <circle {...props}>{children}</circle>
    ),
  },
}));

describe('MapMockup', () => {
  it('renders as an SVG element', () => {
    render(<MapMockup />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toBeInTheDocument();
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });

  it('has proper accessibility attributes', () => {
    render(<MapMockup />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toHaveAttribute('role', 'img');
    expect(svg).toHaveAttribute('aria-label');
  });

  it('applies custom className when provided', () => {
    render(<MapMockup className="custom-map-class" />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toHaveClass('custom-map-class');
  });

  it('renders with animate prop set to true by default', () => {
    render(<MapMockup />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('renders with animate prop set to false', () => {
    render(<MapMockup animate={false} />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toBeInTheDocument();
  });

  it('has proper viewBox attribute', () => {
    render(<MapMockup />);

    const svg = screen.getByTestId('map-mockup');
    expect(svg).toHaveAttribute('viewBox');
  });

  it('is a static SVG (not a react-simple-maps component)', () => {
    const { container } = render(<MapMockup />);

    // Verify no react-simple-maps wrapper elements
    const mapContainers = container.querySelectorAll('.rsm-svg');
    expect(mapContainers).toHaveLength(0);

    // Verify it's a native SVG
    const svg = screen.getByTestId('map-mockup');
    expect(svg.tagName.toLowerCase()).toBe('svg');
  });
});
