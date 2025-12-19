import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { NotFound } from './NotFound';

describe('NotFound Component', () => {
  it('renders 404 error page with correct title', () => {
    render(<NotFound />);

    const title = screen.getByRole('heading', { level: 1 });
    expect(title).toHaveTextContent('404');
  });

  it('displays "Page Not Found" subtitle', () => {
    render(<NotFound />);

    const subtitle = screen.getByRole('heading', { level: 2 });
    expect(subtitle).toHaveTextContent('Page Not Found');
  });

  it('displays descriptive message about the error', () => {
    render(<NotFound />);

    const message = screen.getByText(/page you're looking for doesn't exist/i);
    expect(message).toBeInTheDocument();
  });

  it('contains a link to return to homepage', () => {
    render(<NotFound />);

    const homeLink = screen.getByRole('link', { name: /return to homepage/i });
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });

  it('has the correct data-testid attribute', () => {
    render(<NotFound />);

    const notFoundPage = screen.getByTestId('not-found-page');
    expect(notFoundPage).toBeInTheDocument();
  });

  it('home link has correct data-testid attribute', () => {
    render(<NotFound />);

    const homeLink = screen.getByTestId('home-link');
    expect(homeLink).toBeInTheDocument();
  });
});
