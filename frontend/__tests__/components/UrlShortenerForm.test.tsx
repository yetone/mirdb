import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { UrlShortenerForm } from '../../src/components/UrlShortenerForm';

describe('UrlShortenerForm', () => {
  const mockOnShorten = vi.fn();

  beforeEach(() => {
    mockOnShorten.mockClear();
  });

  // Test Case 1: Input field with type='url' or type='text' and placeholder text is present
  it('renders input field with type url and placeholder text', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    expect(input).toBeInTheDocument();
    expect(input).toHaveAttribute('type', 'url');
    expect(input).toHaveAttribute('placeholder');
  });

  it('renders a shorten button', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const button = screen.getByRole('button', { name: /shorten/i });
    expect(button).toBeInTheDocument();
  });

  // Test Case 4: Press Enter key while input is focused with valid URL - form submits and API call is made
  it('submits form when Enter key is pressed with valid URL', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    expect(mockOnShorten).toHaveBeenCalledWith('https://example.com/test');
    expect(mockOnShorten).toHaveBeenCalledTimes(1);
  });

  it('submits form when Shorten button is clicked with valid URL', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });

    const button = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(button);

    expect(mockOnShorten).toHaveBeenCalledWith('https://example.com/test');
  });

  it('shows validation error for invalid URL', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'not-a-valid-url' } });

    const form = screen.getByRole('form');
    fireEvent.submit(form);

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(mockOnShorten).not.toHaveBeenCalled();
  });

  it('shows validation error for empty input', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const button = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(button);

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(screen.getByText(/please enter a url/i)).toBeInTheDocument();
    expect(mockOnShorten).not.toHaveBeenCalled();
  });

  it('shows validation error for URL without http/https protocol', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'ftp://example.com' } });

    const button = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(button);

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(screen.getByText(/must start with http/i)).toBeInTheDocument();
  });

  // Test Case 6: Submit form while loading state is active - submit button is disabled
  it('disables submit button when loading', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={true} error={null} />
    );

    const button = screen.getByRole('button');
    expect(button).toBeDisabled();
    expect(screen.getByText(/shortening/i)).toBeInTheDocument();
  });

  it('does not submit when loading', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={true} error={null} />
    );

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    expect(mockOnShorten).not.toHaveBeenCalled();
  });

  it('displays external error message', () => {
    render(
      <UrlShortenerForm
        onShorten={mockOnShorten}
        isLoading={false}
        error="Something went wrong"
      />
    );

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(screen.getByText(/something went wrong/i)).toBeInTheDocument();
  });

  it('has proper accessibility attributes', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const input = screen.getByLabelText(/enter url to shorten/i);
    expect(input).toBeInTheDocument();

    const form = screen.getByRole('form', { name: /url shortening form/i });
    expect(form).toBeInTheDocument();
  });

  it('clears validation error when user types', () => {
    render(
      <UrlShortenerForm onShorten={mockOnShorten} isLoading={false} error={null} />
    );

    const button = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(button);

    expect(screen.getByRole('alert')).toBeInTheDocument();

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'h' } });

    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
