/**
 * Demo Section Unit Tests
 * Owner: Scenario 6 - Usage Demonstration
 *
 * Tests:
 * - Demo component renders media content
 * - Demo has proper accessibility attributes
 * - Demo displays set/get operations demonstration
 * - Demo has descriptive caption
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Demo } from './Demo';

describe('Demo', () => {
  it('renders the demo section', () => {
    render(<Demo />);

    expect(screen.getByRole('heading', { name: /see it in action/i })).toBeInTheDocument();
    expect(screen.getByTestId('demo-section')).toBeInTheDocument();
  });

  it('renders the demo figure with terminal display', () => {
    render(<Demo />);

    const figure = screen.getByTestId('demo-figure');
    expect(figure).toBeInTheDocument();

    const terminal = screen.getByTestId('demo-terminal');
    expect(terminal).toBeInTheDocument();
  });

  it('displays demonstration content showing set/get operations', () => {
    render(<Demo />);

    // Check for set operation
    expect(screen.getByText(/set mykey 0 0 5/i)).toBeInTheDocument();
    // 'hello' appears twice (as input and output), so we use getAllByText
    const helloElements = screen.getAllByText('hello');
    expect(helloElements.length).toBe(2); // once in SET, once in GET response
    expect(screen.getByText('STORED')).toBeInTheDocument();

    // Check for get operation
    expect(screen.getByText(/get mykey/i)).toBeInTheDocument();
    expect(screen.getByText(/VALUE mykey 0 5/i)).toBeInTheDocument();
    expect(screen.getByText('END')).toBeInTheDocument();
  });

  it('has descriptive alt text for the demonstration', () => {
    render(<Demo />);

    const terminal = screen.getByTestId('demo-terminal');
    expect(terminal).toHaveAttribute('role', 'img');
    expect(terminal).toHaveAttribute(
      'aria-label',
      expect.stringMatching(/terminal demonstration.*set.*get.*memcached/i)
    );
  });

  it('has a descriptive caption', () => {
    render(<Demo />);

    const caption = screen.getByTestId('demo-caption');
    expect(caption).toBeInTheDocument();
    expect(caption.textContent).toMatch(/memcached commands/i);
    expect(caption.textContent).toMatch(/set/i);
    expect(caption.textContent).toMatch(/get/i);
  });

  it('has proper accessibility attributes', () => {
    render(<Demo />);

    // Section has aria-labelledby
    const section = screen.getByTestId('demo-section');
    expect(section).toHaveAttribute('aria-labelledby', 'demo-heading');

    // Uses semantic figure element
    const figure = screen.getByTestId('demo-figure');
    expect(figure.tagName).toBe('FIGURE');
  });

  it('displays terminal header with window controls', () => {
    render(<Demo />);

    const terminal = screen.getByTestId('demo-terminal');
    expect(terminal).toBeInTheDocument();

    // Check for terminal title
    expect(screen.getByText('MirDB Terminal Session')).toBeInTheDocument();
  });

  it('renders demo subtitle explaining the demonstration', () => {
    render(<Demo />);

    expect(screen.getByText(/see how easy it is to use mirdb/i)).toBeInTheDocument();
  });

  it('includes comment lines explaining the commands', () => {
    render(<Demo />);

    expect(screen.getByText(/# Connect to MirDB/i)).toBeInTheDocument();
    expect(screen.getByText(/# Store a value/i)).toBeInTheDocument();
    expect(screen.getByText(/# Retrieve the value/i)).toBeInTheDocument();
  });
});
