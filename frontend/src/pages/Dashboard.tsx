import React from 'react';
import Navbar from '../components/Navbar';

const Dashboard: React.FC = () => {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <div className="flex-1 p-4">
        <div className="container mx-auto">
          <h1 className="text-3xl font-bold mb-6">Dashboard</h1>
          <p>Welcome to your dashboard!</p>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
