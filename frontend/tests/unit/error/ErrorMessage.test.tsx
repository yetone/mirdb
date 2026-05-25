/**
 * Unit tests for Error Message component.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import ErrorMessage from '../../../src/components/error/ErrorMessage';

describe('ErrorMessage', () => {
  it('renders with error variant by default', () => {
    render(<ErrorMessage message="Something went wrong" />);

    const message = screen.getByTestId('error-message');
    expect(message).toBeInTheDocument();
    expect(message).toHaveAttribute('role', 'alert');
    expect(screen.getByTestId('error-message-text')).toHaveTextContent('Something went wrong');
  });

  it('renders with title when provided', () => {
    render(<ErrorMessage title="Error Title" message="Error details" />);

    expect(screen.getByTestId('error-message-title')).toHaveTextContent('Error Title');
    expect(screen.getByTestId('error-message-text')).toHaveTextContent('Error details');
  });

  it('renders without title when not provided', () => {
    render(<ErrorMessage message="Just a message" />);

    expect(screen.queryByTestId('error-message-title')).not.toBeInTheDocument();
    expect(screen.getByTestId('error-message-text')).toHaveTextContent('Just a message');
  });

  it('applies error variant styles', () => {
    render(<ErrorMessage message="Error" variant="error" />);

    const container = screen.getByTestId('error-message');
    expect(container).toHaveClass('error-message--error');
  });

  it('applies warning variant styles', () => {
    render(<ErrorMessage message="Warning" variant="warning" />);

    const container = screen.getByTestId('error-message');
    expect(container).toHaveClass('error-message--warning');
  });

  it('applies info variant styles', () => {
    render(<ErrorMessage message="Info" variant="info" />);

    const container = screen.getByTestId('error-message');
    expect(container).toHaveClass('error-message--info');
  });

  it('applies critical variant styles', () => {
    render(<ErrorMessage message="Critical" variant="critical" />);

    const container = screen.getByTestId('error-message');
    expect(container).toHaveClass('error-message--critical');
  });

  it('uses custom test id when provided', () => {
    render(<ErrorMessage message="Test" data-testid="custom-error" />);

    expect(screen.getByTestId('custom-error')).toBeInTheDocument();
    expect(screen.getByTestId('custom-error-text')).toHaveTextContent('Test');
  });

  it('displays user-friendly server unreachable message', () => {
    render(
      <ErrorMessage
        title="Connection Lost"
        message="Server unreachable. Please check your connection."
        variant="error"
      />
    );

    expect(screen.getByTestId('error-message-title')).toHaveTextContent('Connection Lost');
    expect(screen.getByTestId('error-message-text')).toHaveTextContent(
      'Server unreachable. Please check your connection.'
    );
  });
});
