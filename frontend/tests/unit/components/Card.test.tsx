/**
 * Card Component Unit Tests
 * Owner: Scenario 15 - Theme Consistency with Existing App
 *
 * Tests for the Card component:
 * - DaisyUI card classes are applied
 * - GlassMorphismCard-consistent styling
 * - Hover effects
 * - Dark/light mode support
 *
 * Requirements: NFR-4 - Theme consistency with existing app
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { Card, CardBody, CardTitle } from '@/components/ui/Card';

describe('Card', () => {
  /**
   * Test Case 2: Feature cards use GlassMorphismCard component or consistent styling
   * Input: Render feature cards
   * Expected: Feature cards use GlassMorphismCard component or consistent styling
   */
  describe('DaisyUI Theme Consistency (NFR-4)', () => {
    it('should apply DaisyUI card class for theme consistency', () => {
      render(<Card>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('card');
    });

    it('should use bg-base-200 for DaisyUI theme variable background', () => {
      render(<Card>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('bg-base-200');
    });

    it('should have shadow-md for consistent depth', () => {
      render(<Card>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('shadow-md');
    });

    it('should apply glass variant with glassmorphism effect', () => {
      render(<Card variant="glass">Glass Card</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('backdrop-blur-md');
      expect(card).toHaveClass('border');
    });
  });

  describe('Hover Effects', () => {
    it('should apply hover effects when hover prop is true', () => {
      render(<Card hover>Hoverable</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('hover:shadow-lg');
      expect(card).toHaveClass('hover:border-primary');
      expect(card).toHaveClass('transition-all');
      expect(card).toHaveClass('duration-300');
    });

    it('should not apply hover effects by default', () => {
      render(<Card>Default</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).not.toHaveClass('hover:shadow-lg');
      expect(card).not.toHaveClass('hover:border-primary');
    });

    it('should have border-transparent for hover transition base', () => {
      render(<Card hover>Hoverable</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('border');
      expect(card).toHaveClass('border-transparent');
    });
  });

  describe('Variants', () => {
    it('should apply default variant styling', () => {
      render(<Card variant="default">Default</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('bg-base-200');
      expect(card).not.toHaveClass('backdrop-blur-md');
    });

    it('should apply bordered variant with border-base-300', () => {
      render(<Card variant="bordered">Bordered</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('border');
      expect(card).toHaveClass('border-base-300');
    });

    it('should apply glass variant with backdrop blur', () => {
      render(<Card variant="glass">Glass</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('backdrop-blur-md');
    });
  });

  describe('Padding', () => {
    it('should apply medium padding by default', () => {
      render(<Card>Padded</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('p-6');
    });

    it('should apply small padding when specified', () => {
      render(<Card padding="sm">Small Padding</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('p-4');
    });

    it('should apply large padding when specified', () => {
      render(<Card padding="lg">Large Padding</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('p-8');
    });

    it('should not apply padding when set to none', () => {
      render(<Card padding="none">No Padding</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).not.toHaveClass('p-4');
      expect(card).not.toHaveClass('p-6');
      expect(card).not.toHaveClass('p-8');
    });
  });

  describe('Custom Props', () => {
    it('should merge custom className', () => {
      render(<Card className="custom-class">Custom</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('card', 'custom-class');
    });

    it('should spread additional props to div element', () => {
      render(<Card data-custom="value">Custom Data</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveAttribute('data-custom', 'value');
    });
  });

  describe('Rendering', () => {
    it('should render children correctly', () => {
      render(
        <Card>
          <span data-testid="child-content">Child content</span>
        </Card>
      );

      expect(screen.getByTestId('child-content')).toBeInTheDocument();
    });

    it('should render as div element', () => {
      render(<Card>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card.tagName).toBe('DIV');
    });
  });
});

describe('CardBody', () => {
  it('should apply card-body class for DaisyUI consistency', () => {
    render(<CardBody>Body content</CardBody>);

    const body = screen.getByTestId('ui-card-body');
    expect(body).toHaveClass('card-body');
  });

  it('should render children correctly', () => {
    render(<CardBody>Test content</CardBody>);

    expect(screen.getByText('Test content')).toBeInTheDocument();
  });

  it('should merge custom className', () => {
    render(<CardBody className="custom-body">Custom</CardBody>);

    const body = screen.getByTestId('ui-card-body');
    expect(body).toHaveClass('card-body', 'custom-body');
  });
});

describe('CardTitle', () => {
  it('should apply card-title class for DaisyUI consistency', () => {
    render(<CardTitle>Title</CardTitle>);

    const title = screen.getByTestId('ui-card-title');
    expect(title).toHaveClass('card-title');
  });

  it('should use text-base-content for theme-aware text color', () => {
    render(<CardTitle>Title</CardTitle>);

    const title = screen.getByTestId('ui-card-title');
    expect(title).toHaveClass('text-base-content');
  });

  it('should render as h3 by default', () => {
    render(<CardTitle>Title</CardTitle>);

    const title = screen.getByTestId('ui-card-title');
    expect(title.tagName).toBe('H3');
  });

  it('should allow custom heading level', () => {
    render(<CardTitle as="h2">Title</CardTitle>);

    const title = screen.getByTestId('ui-card-title');
    expect(title.tagName).toBe('H2');
  });

  it('should merge custom className', () => {
    render(<CardTitle className="custom-title">Custom</CardTitle>);

    const title = screen.getByTestId('ui-card-title');
    expect(title).toHaveClass('card-title', 'custom-title');
  });
});

describe('Card Integration with CardBody and CardTitle', () => {
  it('should render complete card structure', () => {
    render(
      <Card>
        <CardBody>
          <CardTitle>Feature Title</CardTitle>
          <p>Feature description</p>
        </CardBody>
      </Card>
    );

    expect(screen.getByTestId('ui-card')).toBeInTheDocument();
    expect(screen.getByTestId('ui-card-body')).toBeInTheDocument();
    expect(screen.getByTestId('ui-card-title')).toBeInTheDocument();
    expect(screen.getByText('Feature description')).toBeInTheDocument();
  });

  it('should maintain proper card structure hierarchy', () => {
    render(
      <Card>
        <CardBody>
          <CardTitle>Title</CardTitle>
        </CardBody>
      </Card>
    );

    const card = screen.getByTestId('ui-card');
    const body = screen.getByTestId('ui-card-body');
    const title = screen.getByTestId('ui-card-title');

    expect(card).toContainElement(body);
    expect(body).toContainElement(title);
  });
});
