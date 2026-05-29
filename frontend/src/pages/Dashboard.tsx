import { useAuth } from '../contexts/AuthContext';

export default function Dashboard() {
  const { user, logout } = useAuth();

  return (
    <div data-testid="dashboard-page">
      <h1>Dashboard</h1>
      <p>Welcome, {user?.username || 'User'}!</p>
      <button onClick={logout} data-testid="logout-button">
        Logout
      </button>
    </div>
  );
}
