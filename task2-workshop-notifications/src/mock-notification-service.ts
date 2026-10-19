import * as http from 'http';
import { ChaosConfig, DeliveredNotificationRecord } from './types';

export interface MockNotificationServerHandle {
  server: http.Server;
  port: number;
  getStats: () => {
    totalRequestsReceived: number;
    simulated503Errors: number;
    simulatedTimeouts: number;
    uniqueDeliveries: number;
    idempotentDuplicateReplays: number;
    chaosConfig: ChaosConfig;
    deliveries: DeliveredNotificationRecord[];
  };
  updateChaos: (patch: Partial<ChaosConfig>) => ChaosConfig;
  close: () => Promise<void>;
}

function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      try {
        const text = Buffer.concat(chunks).toString('utf8');
        resolve(text ? JSON.parse(text) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: http.ServerResponse, status: number, data: unknown): void {
  if (res.writableEnded) return;
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(JSON.stringify(data));
}

/**
 * Chaos-Injecting Mock HTTP Notification Service (`mock-notification-service.ts`)
 *
 * Requirements satisfied:
 * - "Build a mock notification service that sometimes returns an error or times out.
 *    A successful response represents successful delivery."
 * - Tracks `Idempotency-Key` headers so if a retry occurs after a dropped ACK or crash,
 *   we can empirically verify that duplicate deliveries are absorbed idempotently.
 */
export function startMockNotificationService(
  port = 4002,
  initialChaos?: Partial<ChaosConfig>
): Promise<MockNotificationServerHandle> {
  const chaos: ChaosConfig = {
    errorRate: 0.25,
    timeoutRate: 0.15,
    baseLatencyMs: 15,
    outageMode: false,
    failCountByMember: {},
    timeoutHoldMs: 800,
    ...initialChaos,
  };

  let totalRequestsReceived = 0;
  let simulated503Errors = 0;
  let simulatedTimeouts = 0;
  let idempotentDuplicateReplays = 0;
  const deliveredByKey = new Map<string, DeliveredNotificationRecord>();
  const pendingTimers = new Set<NodeJS.Timeout>();

  const server = http.createServer(async (req, res) => {
    const method = req.method || 'GET';
    const parsedUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const pathname = parsedUrl.pathname;

    if (method === 'GET' && pathname === '/api/mock/stats') {
      sendJson(res, 200, {
        totalRequestsReceived,
        simulated503Errors,
        simulatedTimeouts,
        uniqueDeliveries: deliveredByKey.size,
        idempotentDuplicateReplays,
        chaosConfig: chaos,
        deliveries: Array.from(deliveredByKey.values()),
      });
      return;
    }

    if (method === 'POST' && pathname === '/api/mock/chaos') {
      const body = await readJsonBody(req);
      Object.assign(chaos, body);
      sendJson(res, 200, { chaosConfig: chaos });
      return;
    }

    if (method === 'POST' && pathname === '/api/notify') {
      totalRequestsReceived++;
      const body = await readJsonBody(req);
      const idempotencyKey =
        (req.headers['idempotency-key'] as string) || body.idempotencyKey || `anon_${Date.now()}`;
      const memberId = body.memberId as string;

      // 1. Check complete outage mode
      if (chaos.outageMode) {
        simulated503Errors++;
        sendJson(res, 503, {
          error: 'Simulated complete notification service outage (503 Service Unavailable)',
        });
        return;
      }

      // 2. Check deterministic per-member failure injection (proves failure isolation)
      if (memberId && chaos.failCountByMember[memberId] && chaos.failCountByMember[memberId] > 0) {
        chaos.failCountByMember[memberId]--;
        simulated503Errors++;
        sendJson(res, 502, {
          error: `Simulated member-specific gateway failure for ${memberId}`,
          remainingForcedFailures: chaos.failCountByMember[memberId],
        });
        return;
      }

      // 3. Random timeout injection (holds connection past client AbortController deadline)
      const roll = Math.random();
      if (roll < chaos.timeoutRate) {
        simulatedTimeouts++;
        const t = setTimeout(() => {
          pendingTimers.delete(t);
          sendJson(res, 504, { error: 'Simulated upstream gateway timeout' });
        }, chaos.timeoutHoldMs);
        pendingTimers.add(t);
        req.on('close', () => {
          clearTimeout(t);
          pendingTimers.delete(t);
        });
        return;
      }

      // 4. Random transient HTTP 500/503 error injection
      if (roll < chaos.timeoutRate + chaos.errorRate) {
        simulated503Errors++;
        sendJson(res, 503, { error: 'Simulated transient notification provider error' });
        return;
      }

      // 5. Simulate base network latency then succeed (with Idempotency-Key deduplication)
      const t = setTimeout(() => {
        pendingTimers.delete(t);
        const existing = deliveredByKey.get(idempotencyKey);
        if (existing) {
          existing.duplicateReplaysAbsorbed++;
          idempotentDuplicateReplays++;
          sendJson(res, 200, {
            status: 'DELIVERED',
            idempotentReplay: true,
            record: existing,
          });
          return;
        }

        const record: DeliveredNotificationRecord = {
          idempotencyKey,
          outboxId: body.outboxId || idempotencyKey,
          kind: body.kind,
          workshopId: body.workshopId,
          memberId: body.memberId,
          memberEmail: body.memberEmail,
          subject: body.subject || '',
          firstDeliveredAt: new Date().toISOString(),
          duplicateReplaysAbsorbed: 0,
        };
        deliveredByKey.set(idempotencyKey, record);
        sendJson(res, 200, {
          status: 'DELIVERED',
          idempotentReplay: false,
          record,
        });
      }, chaos.baseLatencyMs);
      pendingTimers.add(t);
      return;
    }

    sendJson(res, 404, { error: 'Not found' });
  });

  return new Promise((resolve) => {
    server.listen(port, () => {
      const addr = server.address();
      const actualPort = typeof addr === 'object' && addr ? addr.port : port;
      resolve({
        server,
        port: actualPort,
        getStats: () => ({
          totalRequestsReceived,
          simulated503Errors,
          simulatedTimeouts,
          uniqueDeliveries: deliveredByKey.size,
          idempotentDuplicateReplays,
          chaosConfig: { ...chaos },
          deliveries: Array.from(deliveredByKey.values()),
        }),
        updateChaos: (patch) => {
          Object.assign(chaos, patch);
          return { ...chaos };
        },
        close: () =>
          new Promise<void>((resClose) => {
            for (const timer of pendingTimers) {
              clearTimeout(timer);
            }
            pendingTimers.clear();
            server.close(() => resClose());
          }),
      });
    });
  });
}

if (require.main === module) {
  const port = Number(process.env.MOCK_PORT || 4002);
  startMockNotificationService(port).then((handle) => {
    console.log(`[Task 2 Mock Notification Service] listening on http://localhost:${handle.port}`);
  });
}
