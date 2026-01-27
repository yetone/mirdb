import { Link } from 'react-router-dom';
import GlassMorphismCard from '../components/GlassMorphismCard';
import FuturisticButton from '../components/FuturisticButton';
import BackgroundEffect from '../components/BackgroundEffect';

export default function Login() {
  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <BackgroundEffect />
      <GlassMorphismCard className="w-full max-w-md p-8">
        <h1 className="text-3xl font-bold text-center mb-6">Sign In</h1>
        <form className="space-y-4">
          <div className="form-control">
            <label className="label">
              <span className="label-text">Email</span>
            </label>
            <input type="email" className="input input-bordered" placeholder="your@email.com" />
          </div>
          <div className="form-control">
            <label className="label">
              <span className="label-text">Password</span>
            </label>
            <input type="password" className="input input-bordered" placeholder="••••••••" />
          </div>
          <FuturisticButton type="submit" className="w-full">
            Sign In
          </FuturisticButton>
        </form>
        <p className="text-center mt-4">
          Don't have an account?{' '}
          <Link to="/register" className="link link-primary">
            Sign Up
          </Link>
        </p>
      </GlassMorphismCard>
    </div>
  );
}
