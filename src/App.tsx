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
import Settings from '@/views/Settings';

type CurrentUser = { username: string; role?: string };

function readCurrentUser(): CurrentUser | null {
  try {
    const parsed = JSON.parse(localStorage.getItem('currentUser') || 'null');
    if (parsed && typeof parsed.username === 'string' && parsed.username.trim()) {
      return { username: parsed.username.trim(), role: typeof parsed.role === 'string' ? parsed.role : undefined };
    }
  } catch {
    // ignore
  }
  const legacy = localStorage.getItem('username');
  return legacy ? { username: legacy } : null;
}

export default function App() {
  const [currentUser, setCurrentUser] = useState<CurrentUser | null>(() => readCurrentUser());
  const [isAuthenticated, setIsAuthenticated] = useState(() => Boolean(readCurrentUser()));
  const [currentView, setCurrentView] = useState(() => localStorage.getItem('currentView') || 'dashboard');

  if (!isAuthenticated) {
    return <Login onLogin={({ username, role, rememberMe }) => {
      const user = { username, role };
      setCurrentUser(user);
      setIsAuthenticated(true);
      localStorage.setItem('currentUser', JSON.stringify(user));
      localStorage.setItem('username', username);
      localStorage.setItem('isLoggedIn', rememberMe ? 'true' : 'session');
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
      case 'settings':
        return <Settings currentUser={currentUser} />;
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
        setCurrentView('dashboard');
        setIsAuthenticated(false);
        setCurrentUser(null);
        localStorage.removeItem('isLoggedIn');
        localStorage.removeItem('currentUser');
        localStorage.removeItem('username');
        localStorage.removeItem('currentView');
      }}
      currentUser={currentUser}
    >
      {renderView()}
    </Layout>
  );
}
