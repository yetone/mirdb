/**
 * Registration page component.
 *
 * Stub implementation - full implementation by another scenario.
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { Navbar } from '../components/layout/Navbar'

export function Register() {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1 flex items-center justify-center">
        <div className="card w-96 bg-base-200 shadow-xl">
          <div className="card-body">
            <h1 className="card-title text-2xl">Create Account</h1>
            <p className="text-base-content/70">Registration form coming soon</p>
            <Link to="/" className="btn btn-ghost mt-4">
              Back to Home
            </Link>
          </div>
        </div>
      </main>
    </div>
  )
}

export default Register
