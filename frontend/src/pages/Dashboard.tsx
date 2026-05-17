import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { ROUTES } from '../utils/constants';

export default function Dashboard() {
  const navigate = useNavigate();
  const { logout } = useAuth();

  const handleLogout = () => {
    logout();
    navigate(ROUTES.HOME);
  };

  return (
    <div data-testid="dashboard-page">
      <h1>Dashboard</h1>
      <button
        type="button"
        data-testid="logout-button"
        onClick={handleLogout}
      >
        Logout
      </button>
    </div>
  );
}
