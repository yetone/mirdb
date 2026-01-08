import { Link } from 'react-router-dom';
import Navbar from '../components/Navbar';

export default function Login() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <div className="flex items-center justify-center min-h-[calc(100vh-4rem)]">
        <div className="card w-full max-w-md bg-base-100 shadow-xl">
          <div className="card-body">
            <h1 className="text-2xl font-bold text-center mb-6">Login</h1>
            <form>
              <div className="form-control mb-4">
                <label className="label">
                  <span className="label-text">Email</span>
                </label>
                <input
                  type="email"
                  placeholder="your@email.com"
                  className="input input-bordered"
                />
              </div>
              <div className="form-control mb-6">
                <label className="label">
                  <span className="label-text">Password</span>
                </label>
                <input
                  type="password"
                  placeholder="Enter your password"
                  className="input input-bordered"
                />
              </div>
              <button type="submit" className="btn btn-primary w-full">
                Login
              </button>
            </form>
            <div className="divider">OR</div>
            <p className="text-center">
              Don't have an account?{' '}
              <Link to="/register" className="link link-primary">
                Sign Up
              </Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
