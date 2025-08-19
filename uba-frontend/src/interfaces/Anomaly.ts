// src/interfaces/Anomaly.ts
export interface Anomaly {
  id: number;
  timestamp: string;
  user: string;
  client_ip: string | null;
  database: string | null;
  query: string;
  anomaly_type: string;
  score: number | null;
  reason: string | null;
  status: string;
}