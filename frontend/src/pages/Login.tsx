/**
 * Login page component.
 * Stub implementation for routing.
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { Navbar, Footer } from '../components/layout'
import { BackgroundEffect } from '../components/common'

export function Login() {
  return (
    <div className="min-h-screen flex flex-col">
      <BackgroundEffect />
      <Navbar />
      <main className="flex-1 flex items-center justify-center">
        <div className="card w-96 bg-base-100 shadow-xl">
          <div className="card-body">
            <h2 className="card-title">Login</h2>
            <p>Login form would go here.</p>
            <div className="card-actions justify-end mt-4">
              <Link to="/" className="btn btn-ghost">Back to Home</Link>
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}

export default Login
