/**
 * Task 2: Workshop Registration and Notifications — Domain Types
 */

export type WorkshopStatus = 'OPEN' | 'CANCELLED';

export interface Workshop {
  id: string;
  title: string;
  organizer: string;
  capacity: number;
  startsAt: string;
  status: WorkshopStatus;
  registeredCount: number;
  createdAt: string;
  cancelledAt?: string;
  cancellationReason?: string;
}

export interface Registration {
  id: string;
  workshopId: string;
  memberId: string;
  memberName: string;
  memberEmail: string;
  registeredAt: string;
}

export type NotificationKind = 'REGISTRATION_CONFIRMATION' | 'WORKSHOP_CANCELLATION';

export type OutboxStatus = 'PENDING' | 'IN_FLIGHT' | 'DELIVERED' | 'DEAD_LETTER';

export interface OutboxMessage {
  id: string;
  idempotencyKey: string;
  kind: NotificationKind;
  workshopId: string;
  workshopTitle: string;
  registrationId: string;
  memberId: string;
  memberEmail: string;
  payload: {
    subject: string;
    body: string;
    eventTimestamp: string;
  };
  status: OutboxStatus;
  attempts: number;
  maxAttempts: number;
  nextAttemptAtMs: number;
  lockedUntilMs: number;
  lastError?: string;
  createdAt: string;
  deliveredAt?: string;
}

export interface ChaosConfig {
  /** Probability [0..1] of returning HTTP 503 Internal/Transient Error */
  errorRate: number;
  /** Probability [0..1] of hanging/timing out past the client's deadline */
  timeoutRate: number;
  /** Base artificial network latency in ms */
  baseLatencyMs: number;
  /** Complete outage mode (100% 503 Service Unavailable) */
  outageMode: boolean;
  /** Deterministic failure counter per memberId to prove per-member isolation */
  failCountByMember: Record<string, number>;
  /** Simulated timeout duration in ms when timeout is triggered */
  timeoutHoldMs: number;
}

export interface DeliveredNotificationRecord {
  idempotencyKey: string;
  outboxId: string;
  kind: NotificationKind;
  workshopId: string;
  memberId: string;
  memberEmail: string;
  subject: string;
  firstDeliveredAt: string;
  duplicateReplaysAbsorbed: number;
}
