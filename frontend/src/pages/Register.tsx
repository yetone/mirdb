import { Link } from 'react-router-dom';
import GlassMorphismCard from '../components/GlassMorphismCard';
import FuturisticButton from '../components/FuturisticButton';
import BackgroundEffect from '../components/BackgroundEffect';

export default function Register() {
  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <BackgroundEffect />
      <GlassMorphismCard className="w-full max-w-md p-8">
        <h1 className="text-3xl font-bold text-center mb-6">Create Account</h1>
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
          <div className="form-control">
            <label className="label">
              <span className="label-text">Confirm Password</span>
            </label>
            <input type="password" className="input input-bordered" placeholder="••••••••" />
          </div>
          <FuturisticButton type="submit" className="w-full">
            Create Account
          </FuturisticButton>
        </form>
        <p className="text-center mt-4">
          Already have an account?{' '}
          <Link to="/login" className="link link-primary">
            Sign In
          </Link>
        </p>
      </GlassMorphismCard>
    </div>
  );
}
