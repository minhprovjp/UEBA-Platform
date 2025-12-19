// uba-frontend/src/pages/SelfMonitoring.jsx
import React, { useState } from 'react';
import {
  Shield, AlertTriangle, Activity, Server, Eye, Lock,
  CheckCircle, XCircle, AlertCircle, TrendingUp, RefreshCw, Database
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  useSelfMonitoringStatus,
  useSelfMonitoringAlerts,
  useInfrastructureEvents,
  useAcknowledgeAlertMutation
} from '@/api/queries';

// Component for status cards
const StatusCard = ({ title, status, icon: Icon, description, lastUpdate }) => {
  const getStatusColor = (status) => {
    switch (status) {
      case 'healthy': return 'text-green-400 border-green-900/20 bg-green-900/10';
      case 'warning': return 'text-yellow-400 border-yellow-900/20 bg-yellow-900/10';
      case 'error': return 'text-red-400 border-red-900/20 bg-red-900/10';
      default: return 'text-gray-400 border-gray-900/20 bg-gray-900/10';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'healthy': return CheckCircle;
      case 'warning': return AlertTriangle;
      case 'error': return XCircle;
      default: return AlertCircle;
    }
  };

  const StatusIcon = getStatusIcon(status);

  return (
    <div className={`border rounded-lg p-4 ${getStatusColor(status)}`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center space-x-2">
          <Icon className="h-5 w-5" />
          <h3 className="font-medium">{title}</h3>
        </div>
        <StatusIcon className="h-4 w-4" />
      </div>
      <p className="text-sm text-gray-300 mb-2">{description}</p>
      <p className="text-xs text-gray-500">
        Last updated: {lastUpdate ? new Date(lastUpdate).toLocaleString() : 'Never'}
      </p>
    </div>
  );
};

// Alert component
const AlertItem = ({ alert, onAcknowledge }) => {
  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'critical': return 'text-red-400 bg-red-900/20';
      case 'high': return 'text-orange-400 bg-orange-900/20';
      case 'medium': return 'text-yellow-400 bg-yellow-900/20';
      case 'low': return 'text-blue-400 bg-blue-900/20';
      default: return 'text-gray-400 bg-gray-900/20';
    }
  };

  return (
    <div className="border border-gray-700 rounded-lg p-4 mb-3">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center space-x-2">
          <span className={`px-2 py-1 rounded text-xs font-medium ${getSeverityColor(alert.severity)}`}>
            {alert.severity ? alert.severity.toUpperCase() : 'UNKNOWN'}
          </span>
          <span className="text-sm font-medium">{alert.title}</span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-xs text-gray-500">
            {new Date(alert.timestamp).toLocaleString()}
          </span>
          {alert.status === 'new' && (
            <button
              onClick={() => onAcknowledge(alert.id)}
              className="px-3 py-1 bg-blue-600 hover:bg-blue-700 rounded text-xs transition-colors"
            >
              Acknowledge
            </button>
          )}
        </div>
      </div>
      <p className="text-sm text-gray-300 mb-2">{alert.description && typeof alert.description === 'string' && alert.description.startsWith('{') ? 'Details in log' : alert.description}</p>
      <div className="flex items-center space-x-4 text-xs text-gray-500">
        <span>Component: {alert.component}</span>
        {alert.risk_score && <span>Risk Score: {alert.risk_score}</span>}
        {alert.affected_users && <span>Affected Users: {alert.affected_users}</span>}
      </div>
    </div>
  );
};

// Infrastructure event component
const InfrastructureEvent = ({ event }) => {
  const getEventTypeColor = (eventType) => {
    switch (eventType) {
      case 'connection': return 'text-blue-400';
      case 'query': return 'text-green-400';
      case 'admin_action': return 'text-yellow-400';
      case 'security_event': return 'text-red-400';
      default: return 'text-gray-400';
    }
  };

  return (
    <div className="border border-gray-700 rounded-lg p-3 mb-2 bg-gray-800/50">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center space-x-2">
          <span className={`text-xs font-medium ${getEventTypeColor(event.event_type)}`}>
            {event.event_type ? event.event_type.toUpperCase() : 'EVENT'}
          </span>
          <span className="text-sm text-white">{event.target_component}</span>
        </div>
        <span className="text-xs text-gray-500">
          {new Date(event.timestamp).toLocaleString()}
        </span>
      </div>
      <div className="text-xs text-gray-400 space-y-1">
        <div className='flex gap-4'>
          <span>User: <span className="text-zinc-300">{event.user_account}</span></span>
          <span>Source: <span className="text-zinc-300">{event.source_ip}</span></span>
        </div>
        <div>
          ID: <span className="font-mono text-[10px]">{event.event_id}</span>
          {event.risk_score > 0 && <span className="ml-2 px-1.5 py-0.5 bg-zinc-800 rounded border border-zinc-700">Risk: {event.risk_score}</span>}
        </div>
      </div>
    </div>
  );
};

// Main SelfMonitoring component
const SelfMonitoring = () => {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState('overview');

  // Using Hooks
  const { data: status, isLoading: statusLoading, error: statusError, refetch: refetchStatus } = useSelfMonitoringStatus();
  const { data: alerts, isLoading: alertsLoading, refetch: refetchAlerts } = useSelfMonitoringAlerts();
  const { data: events, isLoading: eventsLoading, refetch: refetchEvents } = useInfrastructureEvents();

  const acknowledgeMutation = useAcknowledgeAlertMutation();

  const handleRefresh = () => {
    refetchStatus();
    refetchAlerts();
    refetchEvents();
  };

  // derived state
  const isLoading = statusLoading || alertsLoading || eventsLoading;

  // Handle alert acknowledgment
  const handleAcknowledgeAlert = (alertId) => {
    acknowledgeMutation.mutate(alertId);
  };

  if (isLoading && !status) { // Only show loading if we have no data
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  if (statusError) {
    return (
      <div className="bg-red-900/20 border border-red-900/50 rounded-lg p-4">
        <div className="flex items-center space-x-2">
          <XCircle className="h-5 w-5 text-red-400" />
          <span className="text-red-400">Error loading self-monitoring data: {statusError.message}</span>
        </div>
        <button onClick={handleRefresh} className="mt-2 text-sm text-red-300 underline">Try Retry</button>
      </div>
    );
  }

  // Safe navigation for status
  const safeStatus = status || {};

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center space-x-2">
            <Shield className="h-6 w-6" />
            <span>{t('Self-Monitoring Dashboard')}</span>
          </h1>
          <p className="text-gray-400 mt-1">
            Monitor UBA infrastructure security and detect attacks against the monitoring system itself
          </p>
        </div>
        <button
          onClick={handleRefresh}
          className="flex items-center space-x-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Navigation Tabs */}
      <div className="border-b border-gray-700">
        <nav className="flex space-x-8">
          {[
            { id: 'overview', label: 'Overview', icon: Activity },
            { id: 'alerts', label: 'Security Alerts', icon: AlertTriangle },
            { id: 'events', label: 'Infrastructure Events', icon: Database },
            { id: 'components', label: 'Component Status', icon: Server }
          ].map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`flex items-center space-x-2 py-2 px-1 border-b-2 font-medium text-sm transition-colors ${activeTab === id
                  ? 'border-blue-500 text-blue-400'
                  : 'border-transparent text-gray-400 hover:text-gray-300'
                }`}
            >
              <Icon className="h-4 w-4" />
              <span>{label}</span>
            </button>
          ))}
        </nav>
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && (
        <div className="space-y-6">
          {/* System Status Cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <StatusCard
              title="Infrastructure Monitor"
              status={safeStatus.infrastructure_monitor?.status || 'unknown'}
              icon={Server}
              description={safeStatus.infrastructure_monitor?.description || 'Monitoring UBA infrastructure'}
              lastUpdate={safeStatus.infrastructure_monitor?.last_update}
            />
            <StatusCard
              title="Shadow Monitor"
              status={safeStatus.shadow_monitor?.status || 'unknown'}
              icon={Eye}
              description={safeStatus.shadow_monitor?.description || 'Independent backup monitoring'}
              lastUpdate={safeStatus.shadow_monitor?.last_update}
            />
            <StatusCard
              title="Threat Detection"
              status={safeStatus.threat_detection?.status || 'unknown'}
              icon={Shield}
              description={safeStatus.threat_detection?.description || 'Attack pattern recognition'}
              lastUpdate={safeStatus.threat_detection?.last_update}
            />
            <StatusCard
              title="Data Integrity"
              status={safeStatus.data_integrity?.status || 'unknown'}
              icon={Lock}
              description={safeStatus.data_integrity?.description || 'Cryptographic validation'}
              lastUpdate={safeStatus.data_integrity?.last_update}
            />
          </div>

          {/* Security Metrics */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-gray-800 rounded-lg p-6">
              <h3 className="text-lg font-medium mb-4 flex items-center space-x-2">
                <TrendingUp className="h-5 w-5" />
                <span>Security Metrics</span>
              </h3>
              <div className="space-y-4">
                <div className="flex justify-between">
                  <span className="text-gray-400">Active Threats</span>
                  <span className="text-red-400 font-medium">
                    {safeStatus.metrics?.active_threats || 0}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Events Processed (24h)</span>
                  <span className="text-blue-400 font-medium">
                    {safeStatus.metrics?.events_24h || 0}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Risk Score</span>
                  <span className={`font-medium ${(safeStatus.metrics?.risk_score || 0) > 0.7 ? 'text-red-400' :
                      (safeStatus.metrics?.risk_score || 0) > 0.4 ? 'text-yellow-400' : 'text-green-400'
                    }`}>
                    {((safeStatus.metrics?.risk_score || 0) * 100).toFixed(1)}%
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Monitored Components</span>
                  <span className="text-green-400 font-medium">
                    {safeStatus.metrics?.monitored_components || 0}
                  </span>
                </div>
              </div>
            </div>

            <div className="bg-gray-800 rounded-lg p-6">
              <h3 className="text-lg font-medium mb-4 flex items-center space-x-2">
                <Activity className="h-5 w-5" />
                <span>System Health</span>
              </h3>
              <div className="space-y-4">
                <div className="flex justify-between">
                  <span className="text-gray-400">Uptime</span>
                  <span className="text-green-400 font-medium">
                    {safeStatus.health?.uptime || 'Unknown'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Memory Usage</span>
                  <span className="text-blue-400 font-medium">
                    {safeStatus.health?.memory_usage || 'Unknown'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">CPU Usage</span>
                  <span className="text-blue-400 font-medium">
                    {safeStatus.health?.cpu_usage || 'Unknown'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Database Connections</span>
                  <span className="text-green-400 font-medium">
                    {safeStatus.health?.db_connections || 0}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Alerts Tab */}
      {activeTab === 'alerts' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold">Security Alerts</h2>
            <div className="flex items-center space-x-2 text-sm text-gray-400">
              <AlertTriangle className="h-4 w-4" />
              <span>{alerts ? alerts.filter(a => a.status === 'new').length : 0} new alerts</span>
            </div>
          </div>

          {(!alerts || alerts.length === 0) ? (
            <div className="text-center py-8 text-gray-400">
              <CheckCircle className="h-12 w-12 mx-auto mb-4" />
              <p>No security alerts at this time</p>
            </div>
          ) : (
            <div className="space-y-3">
              {alerts.map(alert => (
                <AlertItem
                  key={alert.id}
                  alert={alert}
                  onAcknowledge={handleAcknowledgeAlert}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Events Tab */}
      {activeTab === 'events' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold">Infrastructure Events</h2>
            <div className="flex items-center space-x-2 text-sm text-gray-400">
              <Database className="h-4 w-4" />
              <span>{events ? events.length : 0} recent events</span>
            </div>
          </div>

          {(!events || events.length === 0) ? (
            <div className="text-center py-8 text-gray-400">
              <Activity className="h-12 w-12 mx-auto mb-4" />
              <p>No infrastructure events recorded</p>
            </div>
          ) : (
            <div className="space-y-2">
              {events.map(event => (
                <InfrastructureEvent key={event.event_id} event={event} />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Components Tab */}
      {activeTab === 'components' && (
        <div className="space-y-6">
          <h2 className="text-xl font-semibold">Component Status</h2>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Core Components */}
            <div className="bg-gray-800 rounded-lg p-6">
              <h3 className="text-lg font-medium mb-4 flex items-center space-x-2">
                <Server className="h-5 w-5" />
                <span>Core Components</span>
              </h3>
              <div className="space-y-3">
                {Object.entries(safeStatus.components || {}).map(([name, component]) => (
                  <div key={name} className="flex items-center justify-between p-3 bg-gray-700 rounded">
                    <div className="flex items-center space-x-3">
                      <div className={`w-3 h-3 rounded-full ${component.status === 'healthy' ? 'bg-green-400' :
                          component.status === 'warning' ? 'bg-yellow-400' : 'bg-red-400'
                        }`}></div>
                      <span className="font-medium">{name}</span>
                    </div>
                    <span className="text-sm text-gray-400">
                      {component.last_check ? new Date(component.last_check).toLocaleTimeString() : 'Never'}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Detection Engines */}
            <div className="bg-gray-800 rounded-lg p-6">
              <h3 className="text-lg font-medium mb-4 flex items-center space-x-2">
                <Shield className="h-5 w-5" />
                <span>Detection Engines</span>
              </h3>
              <div className="space-y-3">
                {Object.entries(safeStatus.detectors || {}).map(([name, detector]) => (
                  <div key={name} className="flex items-center justify-between p-3 bg-gray-700 rounded">
                    <div className="flex items-center space-x-3">
                      <div className={`w-3 h-3 rounded-full ${detector.status === 'active' ? 'bg-green-400' :
                          detector.status === 'warning' ? 'bg-yellow-400' : 'bg-red-400'
                        }`}></div>
                      <span className="font-medium">{name}</span>
                    </div>
                    <div className="text-right">
                      <div className="text-sm text-gray-400">
                        {detector.detections_24h || 0} detections
                      </div>
                      <div className="text-xs text-gray-500">
                        {detector.last_detection ? new Date(detector.last_detection).toLocaleTimeString() : 'None'}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default SelfMonitoring;