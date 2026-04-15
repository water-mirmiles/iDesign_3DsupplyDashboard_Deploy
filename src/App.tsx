/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useState } from 'react';
import { Layout } from '@/components/Layout';
import Dashboard from '@/views/Dashboard';
import DataCenter from '@/views/DataCenter';
import SchemaMapping from '@/views/SchemaMapping';
import InventoryList from '@/views/InventoryList';
import LastSoleRelationView from '@/views/LastSoleRelation';
import Login from '@/views/Login';

export default function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [currentView, setCurrentView] = useState('dashboard');

  if (!isAuthenticated) {
    return <Login onLogin={() => setIsAuthenticated(true)} />;
  }

  const renderView = () => {
    switch (currentView) {
      case 'dashboard':
        return <Dashboard />;
      case 'inventory':
        return <InventoryList />;
      case 'relation':
        return <LastSoleRelationView />;
      case 'data-center':
        return <DataCenter />;
      case 'schema-mapping':
        return <SchemaMapping />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <Layout currentView={currentView} onNavigate={setCurrentView} onLogout={() => setIsAuthenticated(false)}>
      {renderView()}
    </Layout>
  );
}
