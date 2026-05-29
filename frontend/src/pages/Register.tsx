import { useState } from 'react';

export default function Register() {
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!username || !email || !password) {
      setError('All fields are required');
      return;
    }

    try {
      const response = await fetch('/api/users/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, email, password }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Registration failed');
      }

      setSuccess(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Registration failed');
    }
  };

  if (success) {
    return (
      <div data-testid="register-page">
        <h1>Registration Successful</h1>
        <p>Your account has been created. You can now log in.</p>
      </div>
    );
  }

  return (
    <div data-testid="register-page">
      <h1>Register</h1>
      <form onSubmit={handleSubmit} data-testid="register-form">
        <div>
          <label htmlFor="reg-username">Username</label>
          <input
            id="reg-username"
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            data-testid="register-username"
            required
          />
        </div>
        <div>
          <label htmlFor="reg-email">Email</label>
          <input
            id="reg-email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            data-testid="register-email"
            required
          />
        </div>
        <div>
          <label htmlFor="reg-password">Password</label>
          <input
            id="reg-password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            data-testid="register-password"
            required
          />
        </div>
        {error && <p data-testid="register-error">{error}</p>}
        <button type="submit" data-testid="register-submit">
          Register
        </button>
      </form>
    </div>
  );
}
