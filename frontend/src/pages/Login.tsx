import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { GlassMorphismCard } from '../components/GlassMorphismCard';
import { FuturisticButton } from '../components/FuturisticButton';
import { Link2, AlertCircle } from 'lucide-react';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      await login(username, password);
      navigate('/dashboard');
    } catch {
      setError('Invalid username or password');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <BackgroundEffect />

      <GlassMorphismCard className="w-full max-w-md p-8">
        <div className="text-center mb-8">
          <Link to="/" className="inline-flex items-center gap-2 text-2xl font-bold">
            <Link2 className="h-8 w-8 text-primary" />
            URL Shortener
          </Link>
          <p className="mt-2 text-base-content/70">Sign in to your account</p>
        </div>

        {error && (
          <div className="alert alert-error mb-4">
            <AlertCircle className="h-5 w-5" />
            <span>{error}</span>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="form-control">
            <label className="label">
              <span className="label-text">Username</span>
            </label>
            <input
              type="text"
              placeholder="Enter your username"
              className="input input-bordered w-full"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
            />
          </div>

          <div className="form-control">
            <label className="label">
              <span className="label-text">Password</span>
            </label>
            <input
              type="password"
              placeholder="Enter your password"
              className="input input-bordered w-full"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>

          <FuturisticButton
            type="submit"
            variant="primary"
            className="w-full"
            disabled={loading}
          >
            {loading ? (
              <span className="loading loading-spinner"></span>
            ) : (
              'Sign In'
            )}
          </FuturisticButton>
        </form>

        <div className="text-center mt-6">
          <span className="text-base-content/70">Don&apos;t have an account? </span>
          <Link to="/register" className="link link-primary">
            Sign Up
          </Link>
        </div>
      </GlassMorphismCard>
    </div>
  );
};

export default Login;
