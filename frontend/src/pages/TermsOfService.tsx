/**
 * Terms of Service Page
 * Added for Scenario 3 - Navigation and Routing
 */

import { Link } from 'react-router-dom';
import Navbar from '../components/Navbar';

export default function TermsOfService() {
  return (
    <div className="min-h-screen flex flex-col" data-testid="terms-of-service-page">
      <Navbar />
      <main className="flex-1 p-4 max-w-4xl mx-auto">
        <div className="card bg-base-200 shadow-xl my-8">
          <div className="card-body">
            <h1 className="card-title text-3xl justify-center mb-6" data-testid="terms-title">
              Terms of Service
            </h1>
            <div className="prose max-w-none">
              <p>Last updated: {new Date().toLocaleDateString()}</p>

              <h2>1. Acceptance of Terms</h2>
              <p>
                By accessing and using this service, you accept and agree to be bound by the terms
                and conditions of this agreement.
              </p>

              <h2>2. Use of Service</h2>
              <p>
                You agree to use our URL shortening service only for lawful purposes and in
                accordance with these Terms of Service.
              </p>

              <h2>3. Prohibited Uses</h2>
              <p>
                You may not use our service to create shortened URLs that link to malicious,
                illegal, or harmful content.
              </p>

              <h2>4. Intellectual Property</h2>
              <p>
                The service and its original content, features, and functionality are owned by
                LinkSnip and are protected by international copyright and trademark laws.
              </p>

              <h2>5. Limitation of Liability</h2>
              <p>
                In no event shall LinkSnip be liable for any indirect, incidental, special,
                consequential, or punitive damages.
              </p>

              <h2>6. Changes to Terms</h2>
              <p>
                We reserve the right to modify or replace these Terms at any time. If a revision
                is material, we will provide notice prior to any new terms taking effect.
              </p>
            </div>
            <div className="mt-6 text-center">
              <Link to="/" className="btn btn-primary" data-testid="back-to-home">
                Back to Home
              </Link>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
