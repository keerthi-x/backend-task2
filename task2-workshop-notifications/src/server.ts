import * as http from 'http';
import * as path from 'path';
import { WorkshopOutboxEngine, OutboxEngineOptions } from './transactional-outbox-engine';

export interface Task2ServerHandle {
  server: http.Server;
  engine: WorkshopOutboxEngine;
  port: number;
  close: () => Promise<void>;
  simulateCrash: () => Promise<void>;
}

function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(new Error('Invalid JSON payload'));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key',
  });
  res.end(JSON.stringify(payload));
}

export function startTask2Server(
  port = 4003,
  options?: Partial<OutboxEngineOptions>
): Promise<Task2ServerHandle> {
  const walFilePath =
    options?.walFilePath ||
    path.resolve(__dirname, '../data/task2-workshop-outbox.wal.ndjson');
  const notificationEndpointUrl =
    options?.notificationEndpointUrl || 'http://127.0.0.1:4002/api/notify';

  const engine = new WorkshopOutboxEngine({
    walFilePath,
    notificationEndpointUrl,
    ...options,
  });

  const server = http.createServer(async (req, res) => {
    try {
      const method = req.method || 'GET';
      const parsedUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
      const pathname = parsedUrl.pathname;

      if (method === 'OPTIONS') {
        res.writeHead(204, {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key',
        });
        res.end();
        return;
      }

      if (method === 'GET' && pathname === '/api/health') {
        sendJson(res, 200, { status: 'ok', service: 'task2-workshop-notifications', stats: engine.getStats() });
        return;
      }

      if (method === 'GET' && pathname === '/api/workshops') {
        sendJson(res, 200, { workshops: engine.listWorkshops() });
        return;
      }

      if (method === 'POST' && pathname === '/api/workshops') {
        const body = await readJsonBody(req);
        const workshop = engine.createWorkshop(body);
        sendJson(res, 201, { workshop });
        return;
      }

      const registerMatch = pathname.match(/^\/api\/workshops\/([^/]+)\/register$/);
      if (method === 'POST' && registerMatch) {
        const workshopId = decodeURIComponent(registerMatch[1]);
        const body = await readJsonBody(req);
        const result = engine.registerMember(workshopId, body);
        sendJson(res, 201, {
          status: 'REGISTERED',
          registration: result.registration,
          notificationStatus: result.outboxMessage.status,
          outboxId: result.outboxMessage.id,
          idempotencyKey: result.outboxMessage.idempotencyKey,
        });
        return;
      }

      const cancelMatch = pathname.match(/^\/api\/workshops\/([^/]+)\/cancel$/);
      if (method === 'POST' && cancelMatch) {
        const workshopId = decodeURIComponent(cancelMatch[1]);
        const body = await readJsonBody(req);
        const result = engine.cancelWorkshop(workshopId, body.reason);
        sendJson(res, 200, {
          status: 'CANCELLED',
          workshop: result.workshop,
          cancellationNotificationsQueued: result.cancellationOutboxMessages.length,
        });
        return;
      }

      if (method === 'GET' && pathname === '/api/registrations') {
        const workshopId = parsedUrl.searchParams.get('workshopId') || undefined;
        sendJson(res, 200, { registrations: engine.listRegistrations(workshopId) });
        return;
      }

      if (method === 'GET' && pathname === '/api/outbox') {
        const status = parsedUrl.searchParams.get('status') || undefined;
        const workshopId = parsedUrl.searchParams.get('workshopId') || undefined;
        sendJson(res, 200, { outbox: engine.listOutboxMessages({ status, workshopId }) });
        return;
      }

      sendJson(res, 404, { error: `Route not found: ${method} ${pathname}` });
    } catch (err: any) {
      const msg = err?.message || 'Internal error';
      let status = 400;
      if (msg.includes('Duplicate registration')) status = 409;
      else if (msg.includes('not found')) status = 404;
      else if (msg.includes('full capacity') || msg.includes('CANCELLED')) status = 422;
      sendJson(res, status, { error: msg });
    }
  });

  return new Promise((resolve) => {
    server.listen(port, () => {
      const addr = server.address();
      const actualPort = typeof addr === 'object' && addr ? addr.port : port;
      resolve({
        server,
        engine,
        port: actualPort,
        close: () =>
          new Promise<void>((resClose) => {
            engine.close();
            server.close(() => resClose());
          }),
        simulateCrash: () =>
          new Promise<void>((resCrash) => {
            engine.simulateUngracefulCrash();
            server.close(() => resCrash());
          }),
      });
    });
  });
}

if (require.main === module) {
  const port = Number(process.env.PORT || 4003);
  startTask2Server(port).then((handle) => {
    console.log(`[Task 2 Server] Workshop Registration & Outbox Relay listening on http://localhost:${handle.port}`);
  });
}
