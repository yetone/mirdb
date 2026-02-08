/**
 * Usage Component Unit Tests
 * Owner: Scenario 4 - Usage Demonstration
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Usage } from './Usage';

describe('Usage', () => {
  // Test Case 1: Render Usage component and check for usage.gif image
  it('displays usage.gif image with correct src', () => {
    render(<Usage />);

    const usageImage = screen.getByTestId('usage-gif');
    expect(usageImage).toBeInTheDocument();
    expect(usageImage).toHaveAttribute('src', '/assets/usage.gif');
  });

  // Test Case 2: Verify usage.gif has appropriate alt text
  it('has descriptive alt text for accessibility', () => {
    render(<Usage />);

    const usageImage = screen.getByTestId('usage-gif');
    expect(usageImage).toHaveAttribute('alt');

    const altText = usageImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(20); // Ensure alt text is descriptive
    expect(altText).toMatch(/MirDB|usage|demonstration/i);
  });

  // Test Case 3: Verify documentation link in usage section
  it('has a link to documentation that is valid', () => {
    render(<Usage />);

    const docLink = screen.getByTestId('documentation-link');
    expect(docLink).toBeInTheDocument();
    expect(docLink).toHaveAttribute('href');

    const href = docLink.getAttribute('href');
    expect(href).toMatch(/github\.com.*readme/i);
  });

  // Test Case 5: Render Usage component and check for explanatory text
  it('includes text explaining the demonstration', () => {
    render(<Usage />);

    const explanation = screen.getByTestId('usage-explanation');
    expect(explanation).toBeInTheDocument();

    // Check that explanatory text is meaningful
    const text = explanation.textContent;
    expect(text).toBeTruthy();
    expect(text!.length).toBeGreaterThan(50); // Ensure substantial explanation
    expect(text).toMatch(/MirDB|key-value|Memcached/i);
  });

  it('renders the usage section with proper test id', () => {
    render(<Usage />);

    expect(screen.getByTestId('usage-section')).toBeInTheDocument();
  });

  it('renders section with proper id for navigation', () => {
    render(<Usage />);

    const section = screen.getByTestId('usage-section');
    expect(section).toHaveAttribute('id', 'usage');
  });

  it('displays section title', () => {
    render(<Usage />);

    expect(screen.getByText('See It in Action')).toBeInTheDocument();
  });

  it('has proper heading structure with aria-labelledby', () => {
    render(<Usage />);

    const section = screen.getByTestId('usage-section');
    expect(section).toHaveAttribute('aria-labelledby', 'usage-title');

    const heading = screen.getByRole('heading', { name: 'See It in Action' });
    expect(heading).toHaveAttribute('id', 'usage-title');
  });

  it('documentation link opens in new tab', () => {
    render(<Usage />);

    const docLink = screen.getByTestId('documentation-link');
    expect(docLink).toHaveAttribute('target', '_blank');
    expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('displays call-to-action text', () => {
    render(<Usage />);

    expect(screen.getByText(/Ready to dive deeper/i)).toBeInTheDocument();
  });

  it('image has lazy loading attribute for performance', () => {
    render(<Usage />);

    const usageImage = screen.getByTestId('usage-gif');
    expect(usageImage).toHaveAttribute('loading', 'lazy');
  });
});
