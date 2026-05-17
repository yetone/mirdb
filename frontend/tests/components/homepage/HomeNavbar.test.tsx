import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import HomeNavbar from '../../../src/components/homepage/HomeNavbar';
import App from '../../../src/App';

describe('HomeNavbar', () => {
  it('renders the MirDB brand link pointing to /', () => {
    render(
      <MemoryRouter>
        <HomeNavbar />
      </MemoryRouter>
    );

    const brandLink = screen.getByTestId('navbar-brand');
    expect(brandLink).toHaveTextContent('MirDB');
    expect(brandLink).toHaveAttribute('href', '/');
  });

  it('renders Login and Register navigation links', () => {
    render(
      <MemoryRouter>
        <HomeNavbar />
      </MemoryRouter>
    );

    expect(screen.getByTestId('navbar-link-login')).toHaveTextContent('Login');
    expect(screen.getByTestId('navbar-link-register')).toHaveTextContent('Register');
  });

  it('Login link points to /login', () => {
    render(
      <MemoryRouter>
        <HomeNavbar />
      </MemoryRouter>
    );

    const loginLink = screen.getByTestId('navbar-link-login');
    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('Register link points to /register', () => {
    render(
      <MemoryRouter>
        <HomeNavbar />
      </MemoryRouter>
    );

    const registerLink = screen.getByTestId('navbar-link-register');
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('brand link has accessible name MirDB', () => {
    render(
      <MemoryRouter>
        <HomeNavbar />
      </MemoryRouter>
    );

    const brandLink = screen.getByTestId('navbar-brand');
    expect(brandLink).toHaveAttribute('aria-label', 'MirDB');
  });

  it('navigates to /login when Login link is clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <HomeNavbar />
      </MemoryRouter>
    );

    const loginLink = screen.getByTestId('navbar-link-login');
    await user.click(loginLink);

    expect(loginLink).toHaveAttribute('href', '/login');
  });

  it('navigates to /register when Register link is clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <HomeNavbar />
      </MemoryRouter>
    );

    const registerLink = screen.getByTestId('navbar-link-register');
    await user.click(registerLink);

    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('navigates to / when brand link is clicked', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/login']}>
        <HomeNavbar />
      </MemoryRouter>
    );

    const brandLink = screen.getByTestId('navbar-brand');
    await user.click(brandLink);

    expect(brandLink).toHaveAttribute('href', '/');
  });

  it('integration: clicking the Login link in the navbar navigates to /login inside <App />', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>
    );

    const nav = screen.getByRole('navigation', { name: /main navigation/i });
    const loginLink = within(nav).getByRole('link', { name: /login|sign in/i });
    await user.click(loginLink);

    expect(await screen.findByTestId('login-page')).toBeInTheDocument();
  });
});
