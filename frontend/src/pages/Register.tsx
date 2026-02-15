/**
 * Register Page
 * Added for Scenario 3 - Navigation and Routing
 */

import { Link } from 'react-router-dom';
import Navbar from '../components/Navbar';

export default function Register() {
  return (
    <div className="min-h-screen flex flex-col" data-testid="register-page">
      <Navbar />
      <main className="flex-1 flex items-center justify-center p-4">
        <div className="card w-full max-w-md bg-base-200 shadow-xl">
          <div className="card-body">
            <h1 className="card-title text-2xl justify-center mb-4" data-testid="register-title">
              Create Account
            </h1>
            <form className="space-y-4">
              <div className="form-control">
                <label className="label">
                  <span className="label-text">Name</span>
                </label>
                <input
                  type="text"
                  placeholder="Enter your name"
                  className="input input-bordered"
                  data-testid="name-input"
                />
              </div>
              <div className="form-control">
                <label className="label">
                  <span className="label-text">Email</span>
                </label>
                <input
                  type="email"
                  placeholder="Enter your email"
                  className="input input-bordered"
                  data-testid="email-input"
                />
              </div>
              <div className="form-control">
                <label className="label">
                  <span className="label-text">Password</span>
                </label>
                <input
                  type="password"
                  placeholder="Create a password"
                  className="input input-bordered"
                  data-testid="password-input"
                />
              </div>
              <div className="form-control">
                <label className="label">
                  <span className="label-text">Confirm Password</span>
                </label>
                <input
                  type="password"
                  placeholder="Confirm your password"
                  className="input input-bordered"
                  data-testid="confirm-password-input"
                />
              </div>
              <button type="submit" className="btn btn-primary w-full" data-testid="register-button">
                Create Account
              </button>
            </form>
            <div className="divider">OR</div>
            <p className="text-center text-base-content/70">
              Already have an account?{' '}
              <Link to="/login" className="link link-primary" data-testid="page-login-link">
                Login here
              </Link>
            </p>
          </div>
        </div>
      </main>
    </div>
  );
}
