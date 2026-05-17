import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import HeroCTA from '../../../src/components/homepage/HeroCTA';
import App from '../../../src/App';

describe('HeroCTA', () => {
  it('renders exactly two CTA links', () => {
    render(
      <MemoryRouter>
        <HeroCTA />
      </MemoryRouter>
    );

    const links = screen.getAllByRole('link');
    expect(links).toHaveLength(2);
  });

  it('renders a primary Register CTA with btn-primary class', () => {
    render(
      <MemoryRouter>
        <HeroCTA />
      </MemoryRouter>
    );

    const registerLink = screen.getByTestId('hero-cta-primary');
    expect(registerLink).toHaveTextContent(/register/i);
    expect(registerLink).toHaveClass('btn-primary');
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('renders a secondary Login CTA with btn-outline class', () => {
    render(
      <MemoryRouter>
        <HeroCTA />
      </MemoryRouter>
    );

    const loginLink = screen.getByTestId('hero-cta-secondary');
    expect(loginLink).toHaveTextContent(/login/i);
    expect(loginLink).toHaveClass('btn-outline');
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('navigates to /register when Register CTA is clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <HeroCTA />
      </MemoryRouter>
    );

    const registerLink = screen.getByTestId('hero-cta-primary');
    await user.click(registerLink);

    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('navigates to /login when Login CTA is clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <HeroCTA />
      </MemoryRouter>
    );

    const loginLink = screen.getByTestId('hero-cta-secondary');
    await user.click(loginLink);

    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('has exactly one primary CTA and one secondary CTA', () => {
    render(
      <MemoryRouter>
        <HeroCTA />
      </MemoryRouter>
    );

    const primaryCta = screen.getByTestId('hero-cta-primary');
    const secondaryCta = screen.getByTestId('hero-cta-secondary');

    expect(primaryCta).toBeInTheDocument();
    expect(secondaryCta).toBeInTheDocument();
    expect(primaryCta).toHaveClass('btn-primary');
    expect(secondaryCta).toHaveClass('btn-outline');
  });

  it('does not link to any undefined route', () => {
    render(
      <MemoryRouter>
        <HeroCTA />
      </MemoryRouter>
    );

    const links = screen.getAllByRole('link');
    const allowedRoutes = ['/login', '/register'];

    for (const link of links) {
      const href = link.getAttribute('href');
      expect(href).toBeDefined();
      expect(allowedRoutes).toContain(href);
    }
  });

  it('integration: clicking the Register CTA inside <App /> navigates to /register', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>
    );

    const registerButton = screen.getByRole('link', {
      name: /register|get started|sign up/i,
    });
    await user.click(registerButton);

    expect(await screen.findByTestId('register-page')).toBeInTheDocument();
  });
});
