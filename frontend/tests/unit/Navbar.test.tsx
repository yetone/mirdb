import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import Navbar from '../../src/components/Navbar';
import * as AuthContext from '../../src/contexts/AuthContext';

vi.mock('../../src/contexts/AuthContext', async () => {
  const actual = await vi.importActual<typeof AuthContext>('../../src/contexts/AuthContext');
  return {
    ...actual,
    useAuth: vi.fn(),
  };
});

const mockedUseAuth = vi.mocked(AuthContext.useAuth);

describe('Navbar', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const renderNavbar = () => {
    return render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    );
  };

  it('shows Login and Register links for unauthenticated users', () => {
    mockedUseAuth.mockReturnValue({
      user: null,
      loading: false,
      isAuthenticated: false,
      isAdmin: false,
      login: vi.fn(),
      logout: vi.fn(),
    });

    renderNavbar();

    expect(screen.getByTestId('nav-login')).toBeInTheDocument();
    expect(screen.getByTestId('nav-register')).toBeInTheDocument();
    expect(screen.queryByTestId('nav-dashboard')).not.toBeInTheDocument();
  });

  it('shows Dashboard link for authenticated users', () => {
    mockedUseAuth.mockReturnValue({
      user: { id: 1, username: 'testuser', email: 'test@test.com', is_admin: 0 },
      loading: false,
      isAuthenticated: true,
      isAdmin: false,
      login: vi.fn(),
      logout: vi.fn(),
    });

    renderNavbar();

    expect(screen.getByTestId('nav-dashboard')).toBeInTheDocument();
    expect(screen.queryByTestId('nav-login')).not.toBeInTheDocument();
    expect(screen.queryByTestId('nav-register')).not.toBeInTheDocument();
  });

  it('has a Home/Logo link that navigates to root', () => {
    mockedUseAuth.mockReturnValue({
      user: null,
      loading: false,
      isAuthenticated: false,
      isAdmin: false,
      login: vi.fn(),
      logout: vi.fn(),
    });

    renderNavbar();

    const homeLink = screen.getByTestId('nav-home-link');
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });

  it('renders the navbar element', () => {
    mockedUseAuth.mockReturnValue({
      user: null,
      loading: false,
      isAuthenticated: false,
      isAdmin: false,
      login: vi.fn(),
      logout: vi.fn(),
    });

    renderNavbar();

    expect(screen.getByTestId('navbar')).toBeInTheDocument();
  });
});
