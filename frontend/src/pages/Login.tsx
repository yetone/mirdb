import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { GlassMorphismCard } from '../components/GlassMorphismCard';
import { FuturisticButton } from '../components/FuturisticButton';

export function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);

    try {
      await login(email, password);
      navigate('/dashboard');
    } catch {
      setError('Invalid email or password');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen" data-testid="login-page">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-24 px-4 flex items-center justify-center min-h-screen">
        <GlassMorphismCard className="w-full max-w-md">
          <h1 className="text-3xl font-bold mb-6 text-center" data-testid="login-heading">Login</h1>

          {error && (
            <div className="alert alert-error mb-4">
              <span>{error}</span>
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4" data-testid="login-form">
            <div className="form-control">
              <label className="label">
                <span className="label-text">Email</span>
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="input input-bordered w-full"
                placeholder="you@example.com"
                required
                data-testid="login-email"
              />
            </div>

            <div className="form-control">
              <label className="label">
                <span className="label-text">Password</span>
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="input input-bordered w-full"
                placeholder="••••••••"
                required
                data-testid="login-password"
              />
            </div>

            <FuturisticButton
              variant="primary"
              size="lg"
              className="w-full"
              onClick={() => {}}
            >
              {isLoading ? 'Logging in...' : 'Login'}
            </FuturisticButton>
          </form>

          <p className="text-center mt-6 text-base-content/70">
            Don't have an account?{' '}
            <Link to="/register" className="text-primary hover:underline">
              Register
            </Link>
          </p>
        </GlassMorphismCard>
      </main>
    </div>
  );
}
