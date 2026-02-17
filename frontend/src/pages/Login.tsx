/**
 * Login page component.
 * Provides user authentication form for returning users.
 */

import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import { Navbar, Footer } from '../components/layout'
import { BackgroundEffect, FuturisticButton } from '../components/common'

export function Login() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // Form submission logic would go here
  }

  return (
    <div className="min-h-screen flex flex-col" data-testid="login-page">
      <BackgroundEffect />
      <Navbar />
      <main className="flex-1 flex items-center justify-center py-8">
        <div className="card w-96 bg-base-100 shadow-xl">
          <div className="card-body">
            <h2 className="card-title text-2xl" data-testid="login-heading">Login</h2>
            <p className="text-base-content/70 mb-4">
              Welcome back! Please enter your credentials.
            </p>
            <form onSubmit={handleSubmit} data-testid="login-form">
              <div className="form-control w-full">
                <label className="label" htmlFor="email">
                  <span className="label-text">Email</span>
                </label>
                <input
                  id="email"
                  type="email"
                  placeholder="Enter your email"
                  className="input input-bordered w-full"
                  data-testid="login-email-input"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  autoComplete="email"
                />
              </div>
              <div className="form-control w-full mt-4">
                <label className="label" htmlFor="password">
                  <span className="label-text">Password</span>
                </label>
                <input
                  id="password"
                  type="password"
                  placeholder="Enter your password"
                  className="input input-bordered w-full"
                  data-testid="login-password-input"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  autoComplete="current-password"
                />
              </div>
              <div className="form-control mt-6">
                <FuturisticButton
                  type="submit"
                  variant="primary"
                  className="w-full"
                  data-testid="login-submit-button"
                >
                  Sign In
                </FuturisticButton>
              </div>
            </form>
            <div className="divider">OR</div>
            <div className="text-center">
              <p className="text-sm text-base-content/70">
                Don't have an account?{' '}
                <Link to="/register" className="link link-primary">
                  Create one
                </Link>
              </p>
            </div>
            <div className="card-actions justify-center mt-4">
              <Link to="/" className="btn btn-ghost btn-sm">Back to Home</Link>
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}

export default Login
