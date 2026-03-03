/**
 * Register Page Component
 * Placeholder for existing registration functionality
 */
import React from 'react';
import { Link } from 'react-router-dom';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';

export function Register() {
  return (
    <div className="min-h-screen bg-base-100" data-testid="register-page">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-24 flex items-center justify-center">
        <div className="card bg-base-200 w-full max-w-md shadow-xl">
          <div className="card-body">
            <h2 className="card-title text-2xl font-bold mb-4">Create Account</h2>
            <form data-testid="registration-form">
              <div className="form-control mb-4">
                <label className="label">
                  <span className="label-text">Username</span>
                </label>
                <input type="text" placeholder="johndoe" className="input input-bordered" />
              </div>
              <div className="form-control mb-4">
                <label className="label">
                  <span className="label-text">Email</span>
                </label>
                <input type="email" placeholder="email@example.com" className="input input-bordered" />
              </div>
              <div className="form-control mb-6">
                <label className="label">
                  <span className="label-text">Password</span>
                </label>
                <input type="password" placeholder="********" className="input input-bordered" />
              </div>
              <button type="submit" className="btn btn-primary w-full">Get Started</button>
            </form>
            <p className="text-center mt-4">
              Already have an account? <Link to="/login" className="link link-primary">Sign In</Link>
            </p>
          </div>
        </div>
      </main>
    </div>
  );
}
