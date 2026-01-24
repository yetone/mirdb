import React from 'react'
import { Link } from 'react-router-dom'
import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'

export function Login() {
  return (
    <div className="min-h-screen bg-base-100">
      <Navbar />
      <BackgroundEffect />
      <main className="container mx-auto px-4 py-16">
        <div className="max-w-md mx-auto">
          <h1 className="text-3xl font-bold text-center mb-8">Sign In</h1>
          <div className="card bg-base-200 shadow-xl">
            <div className="card-body">
              <form>
                <div className="form-control mb-4">
                  <label className="label">
                    <span className="label-text">Email</span>
                  </label>
                  <input type="email" className="input input-bordered" placeholder="Enter your email" />
                </div>
                <div className="form-control mb-6">
                  <label className="label">
                    <span className="label-text">Password</span>
                  </label>
                  <input type="password" className="input input-bordered" placeholder="Enter your password" />
                </div>
                <button type="submit" className="btn btn-primary w-full">Sign In</button>
              </form>
              <div className="divider">OR</div>
              <p className="text-center">
                Don't have an account?{' '}
                <Link to="/register" className="link link-primary">Register</Link>
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}

export default Login
