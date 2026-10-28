import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    flash_sale_100k_spike: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 5000,
      stages: [
        { target: 10000, duration: '10s' }, // 100,000 requests burst window
        { target: 0, duration: '2s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<50'],
  },
};

const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://127.0.0.1:8000';

export default function () {
  const userId = `k6_buyer_${__VU}_${__ITER}`;
  const idempotencyKey = uuidv4();

  const payload = JSON.stringify({
    user_id: userId,
    product_id: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': idempotencyKey,
    },
  };

  const res = http.post(`${GATEWAY_URL}/api/v1/checkout`, payload, params);

  check(res, {
    'valid flash-sale status (202, 409, or 429)': (r) =>
      r.status === 202 || r.status === 409 || r.status === 429,
  });
}