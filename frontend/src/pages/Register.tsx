/**
 * Register Page Component
 *
 * Handles new user registration.
 */

import React from 'react'

export function Register() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-200">
      <div className="card w-96 bg-base-100 shadow-xl">
        <div className="card-body">
          <h1 className="card-title text-2xl justify-center">Sign Up</h1>
          <p className="text-center text-base-content/70">Create your account</p>
          <form className="form-control gap-4 mt-4">
            <input
              type="text"
              placeholder="Username"
              className="input input-bordered"
            />
            <input
              type="email"
              placeholder="Email"
              className="input input-bordered"
            />
            <input
              type="password"
              placeholder="Password"
              className="input input-bordered"
            />
            <button type="submit" className="btn btn-primary">
              Create Account
            </button>
          </form>
        </div>
      </div>
    </div>
  )
}
