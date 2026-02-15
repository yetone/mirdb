/**
 * Privacy Policy Page
 * Added for Scenario 3 - Navigation and Routing
 */

import { Link } from 'react-router-dom';
import Navbar from '../components/Navbar';

export default function PrivacyPolicy() {
  return (
    <div className="min-h-screen flex flex-col" data-testid="privacy-policy-page">
      <Navbar />
      <main className="flex-1 p-4 max-w-4xl mx-auto">
        <div className="card bg-base-200 shadow-xl my-8">
          <div className="card-body">
            <h1 className="card-title text-3xl justify-center mb-6" data-testid="privacy-title">
              Privacy Policy
            </h1>
            <div className="prose max-w-none">
              <p>Last updated: {new Date().toLocaleDateString()}</p>

              <h2>1. Information We Collect</h2>
              <p>
                We collect information you provide directly to us, such as when you create an
                account, use our URL shortening service, or contact us for support.
              </p>

              <h2>2. How We Use Your Information</h2>
              <p>
                We use the information we collect to provide, maintain, and improve our services,
                to process your requests, and to communicate with you.
              </p>

              <h2>3. Information Sharing</h2>
              <p>
                We do not sell or share your personal information with third parties except as
                described in this policy or with your consent.
              </p>

              <h2>4. Data Security</h2>
              <p>
                We implement appropriate security measures to protect your personal information
                against unauthorized access, alteration, disclosure, or destruction.
              </p>

              <h2>5. Contact Us</h2>
              <p>
                If you have any questions about this Privacy Policy, please contact us.
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
