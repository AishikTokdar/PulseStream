import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 20 },  // Ramp-up to 20 users
    { duration: '30s', target: 50 },  // Sustained load at 50 users
    { duration: '10s', target: 0 },   // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests must complete within 500ms
    http_req_failed: ['rate<0.01'],   # Less than 1% failure rate
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export default function () {
  // 1. Login to retrieve access token
  const loginPayload = {
    username: 'ingest@pulsestream.com',
    password: 'ingest123password',
  };

  const loginRes = http.post(`${BASE_URL}/api/v1/auth/login`, loginPayload);
  check(loginRes, {
    'login status is 200': (r) => r.status === 200,
    'token is received': (r) => r.json('access_token') !== undefined,
  });

  if (loginRes.status !== 200) {
    return;
  }

  const token = loginRes.json('access_token');
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
  };

  // 2. Ingest Single Event
  const eventPayload = JSON.stringify({
    tenant_id: 'tenant_k6_load',
    event_type: 'k6_page_click',
    idempotency_key: `k6_${Date.now()}_${Math.random()}`,
    payload: {
      user_agent: 'k6-load-tester',
      latency: Math.floor(Math.random() * 100),
    },
  });

  const ingestRes = http.post(`${BASE_URL}/api/v1/events`, eventPayload, params);
  check(ingestRes, {
    'ingest status is 201': (r) => r.status === 201,
  });

  sleep(0.1);
}
