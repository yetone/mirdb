import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Navbar from './components/shared/Navbar';
import SocialProofBar from './components/social/SocialProofBar';

const Home: React.FC = () => {
  return (
    <Router>
      <div className="min-h-screen flex flex-col">
        <Navbar />
        <main className="flex-grow">
          <SocialProofBar />
          {/* Other homepage sections would go here */}
        </main>
        {/* Footer would go here */}
      </div>
    </Router>
  );
};

export default Home;