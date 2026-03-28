/**
 * Login page component.
 * Minimal stub for routing integration.
 *
 * This is a placeholder for the actual login page implementation.
 * Created to support Route Integration scenario testing.
 */

import React from 'react'
import { Link } from 'react-router-dom'

const Login: React.FC = () => {
  return (
    <div className="min-h-screen bg-base-100 flex items-center justify-center" data-testid="login-page">
      <div className="card bg-base-200 shadow-xl w-full max-w-md">
        <div className="card-body">
          <h1 className="card-title text-2xl font-bold">Login</h1>
          <p className="text-base-content/70">Sign in to your account</p>
          <div className="form-control mt-4">
            <label className="label" htmlFor="email">
              <span className="label-text">Email</span>
            </label>
            <input
              type="email"
              id="email"
              placeholder="email@example.com"
              className="input input-bordered"
              data-testid="login-email-input"
            />
          </div>
          <div className="form-control mt-2">
            <label className="label" htmlFor="password">
              <span className="label-text">Password</span>
            </label>
            <input
              type="password"
              id="password"
              placeholder="Enter your password"
              className="input input-bordered"
              data-testid="login-password-input"
            />
          </div>
          <div className="form-control mt-6">
            <button className="btn btn-primary" data-testid="login-submit-button">
              Sign In
            </button>
          </div>
          <div className="text-center mt-4">
            <Link to="/" className="link link-primary" data-testid="back-to-home-link">
              Back to Home
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}

export default Login
