import React from 'react';
import { useParams } from 'react-router-dom';
import Navbar from '../components/Navbar';

const UrlStats: React.FC = () => {
  const { shortCode } = useParams<{ shortCode: string }>();

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <div className="flex-1 p-4">
        <div className="container mx-auto">
          <h1 className="text-3xl font-bold mb-6">URL Stats</h1>
          <p>Stats for: {shortCode}</p>
        </div>
      </div>
    </div>
  );
};

export default UrlStats;
