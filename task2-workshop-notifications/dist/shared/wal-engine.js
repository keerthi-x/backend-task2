"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DurableWalEngine = void 0;
exports.crc32 = crc32;
var fs = _interopRequireWildcard(require("fs"));
var path = _interopRequireWildcard(require("path"));
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function (nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || typeof obj !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
/**
 * Custom CRC32 Checksum Implementation (IEEE 802.3 polynomial 0xEDB88320).
 * Used to detect torn writes or corrupted tail frames during WAL crash recovery.
 */
const CRC32_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) {
    c = c & 1 ? 0xEDB88320 ^ c >>> 1 : c >>> 1;
  }
  CRC32_TABLE[i] = c >>> 0;
}
function crc32(str) {
  const buf = Buffer.from(str, 'utf8');
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    const byte = buf[i];
    crc = CRC32_TABLE[(crc ^ byte) & 0xFF] ^ crc >>> 8;
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}
/**
 * Dual-Mode Write-Ahead Log (WAL) Persistence Engine:
 * 1. Synchronous ACID Mode (`appendSyncFsync`): Guarantees durability before HTTP response
 *    returns (`fs.writeSync` + `fs.fsyncSync`), used by Task 2's Transactional Outbox so
 *    a process crash (`SIGKILL`) immediately after registration never loses a confirmation.
 * 2. Group-Commit Batch Mode (`enqueueWriteBehind`): Buffers high-frequency telemetry frames
 *    in memory and flushes coalesced batches asynchronously, used by Task 1 to sustain
 *    >10,000 sensor readings/sec without disk I/O bottlenecks.
 */
class DurableWalEngine {
  filePath;
  fd = null;
  fsyncSupported = true;
  seq = 0;
  writeBehindQueue = [];
  flushTimer = null;
  isFlushing = false;
  totalFramesPersisted = 0;
  totalGroupCommits = 0;
  corruptedFramesSkipped = 0;
  constructor(filePath) {
    this.filePath = filePath;
    fs.mkdirSync(path.dirname(filePath), {
      recursive: true
    });
    this.fd = fs.openSync(this.filePath, 'a+');
  }

  /**
   * Replays all valid CRC32-verified frames from disk upon startup or recovery.
   * Torn writes at the end of the file (e.g. from a mid-write power loss) are
   * detected via CRC32 mismatch and safely skipped.
   */
  replay(onFrame) {
    if (!fs.existsSync(this.filePath)) {
      return {
        replayedCount: 0,
        corruptedCount: 0
      };
    }
    const content = fs.readFileSync(this.filePath, 'utf8');
    const lines = content.split('\n');
    let replayedCount = 0;
    let corruptedCount = 0;
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const parsed = JSON.parse(trimmed);
        const canonical = JSON.stringify({
          seq: parsed.seq,
          ts: parsed.ts,
          op: parsed.op,
          payload: parsed.payload
        });
        const expectedCrc = crc32(canonical);
        if (expectedCrc !== parsed.crc) {
          corruptedCount++;
          this.corruptedFramesSkipped++;
          continue;
        }
        if (parsed.seq > this.seq) {
          this.seq = parsed.seq;
        }
        onFrame(parsed);
        replayedCount++;
      } catch (_) {
        corruptedCount++;
        this.corruptedFramesSkipped++;
      }
    }
    this.totalFramesPersisted += replayedCount;
    return {
      replayedCount,
      corruptedCount
    };
  }

  /**
   * Synchronous durable write onto persistent file descriptor with hardware `fsync`.
   * Used for ACID transactions in Task 2 (Workshop Registrations + Outbox).
   */
  appendSyncFsync(op, payload) {
    if (this.fd === null) {
      this.fd = fs.openSync(this.filePath, 'a+');
    }
    const frame = this.buildFrame(op, payload);
    const serialized = JSON.stringify(frame) + '\n';
    fs.writeSync(this.fd, serialized, undefined, 'utf8');
    if (this.fsyncSupported) {
      try {
        fs.fsyncSync(this.fd);
      } catch (err) {
        if (err?.code === 'ENOTSUP' || err?.code === 'EINVAL' || err?.code === 'EPERM') {
          this.fsyncSupported = false;
        } else {
          throw err;
        }
      }
    }
    this.totalFramesPersisted++;
    this.totalGroupCommits++;
    return frame;
  }

  /**
   * Enqueues a frame for high-throughput group-commit write-behind flushing.
   * Used in Task 1 to absorb bursts of 10,000+ sensor readings/sec without blocking the event loop.
   */
  enqueueWriteBehind(op, payload, maxBatchSize = 2500, flushIntervalMs = 20) {
    const frame = this.buildFrame(op, payload);
    this.writeBehindQueue.push(JSON.stringify(frame));
    if (this.writeBehindQueue.length >= maxBatchSize) {
      this.flushWriteBehindSync();
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.flushWriteBehindSync();
      }, flushIntervalMs);
    }
    return frame.seq;
  }
  flushWriteBehindSync() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.writeBehindQueue.length === 0 || this.isFlushing) {
      return 0;
    }
    this.isFlushing = true;
    try {
      if (this.fd === null) {
        this.fd = fs.openSync(this.filePath, 'a+');
      }
      const count = this.writeBehindQueue.length;
      const chunk = this.writeBehindQueue.join('\n') + '\n';
      this.writeBehindQueue = [];
      fs.writeSync(this.fd, chunk, undefined, 'utf8');
      this.totalFramesPersisted += count;
      this.totalGroupCommits++;
      return count;
    } finally {
      this.isFlushing = false;
    }
  }
  buildFrame(op, payload) {
    const seq = ++this.seq;
    const ts = Date.now();
    const canonical = JSON.stringify({
      seq,
      ts,
      op,
      payload
    });
    const crc = crc32(canonical);
    return {
      seq,
      ts,
      op,
      payload,
      crc
    };
  }
  getStats() {
    return {
      currentSeq: this.seq,
      pendingWriteBehindFrames: this.writeBehindQueue.length,
      totalFramesPersisted: this.totalFramesPersisted,
      totalGroupCommits: this.totalGroupCommits,
      corruptedFramesSkipped: this.corruptedFramesSkipped,
      filePath: this.filePath
    };
  }
  close() {
    this.flushWriteBehindSync();
    if (this.fd !== null) {
      try {
        fs.closeSync(this.fd);
      } catch (_) {}
      this.fd = null;
    }
  }
}
exports.DurableWalEngine = DurableWalEngine;