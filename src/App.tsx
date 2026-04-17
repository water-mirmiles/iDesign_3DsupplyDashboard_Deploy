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
  const [isAuthenticated, setIsAuthenticated] = useState(() => localStorage.getItem('isLoggedIn') === 'true');
  const [currentView, setCurrentView] = useState(() => localStorage.getItem('currentView') || 'dashboard');

  if (!isAuthenticated) {
    return <Login onLogin={({ username, rememberMe }) => {
      setIsAuthenticated(true);
      localStorage.setItem('username', username);
      if (rememberMe) localStorage.setItem('isLoggedIn', 'true');
      else localStorage.removeItem('isLoggedIn');
    }} />;
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
        return (
          <SchemaMapping
            onAfterCertify={() => {
              setCurrentView('dashboard');
              localStorage.setItem('currentView', 'dashboard');
            }}
          />
        );
      default:
        return <Dashboard />;
    }
  };

  return (
    <Layout
      currentView={currentView}
      onNavigate={(view) => {
        setCurrentView(view);
        localStorage.setItem('currentView', view);
      }}
      onLogout={() => {
        setIsAuthenticated(false);
        localStorage.removeItem('isLoggedIn');
      }}
    >
      {renderView()}
    </Layout>
  );
}
