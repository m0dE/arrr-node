/**
 * Reliability Tracker - NOT WIRED UP. Nothing imports this file.
 *
 * Verified by reference count rather than by reading: no import of this module
 * exists anywhere in node, sdk, central or the tests. Every function here is
 * unreachable, and the scores it describes are never computed for any client.
 *
 * That matters more here than "dead code" usually does. This codebase has
 * already been bitten twice by machinery that existed and was never called: the
 * desync repair path, which the server and SDK both implemented and no client
 * ever invoked, so a diverged client stayed diverged for the rest of its
 * session; and a peer message case present in one dispatcher and not the other,
 * so whether a player was announced to the room depended on which node opened
 * the link. Both read as working code until somebody measured.
 *
 * So if partition assignment ever looks like it is weighting by reliability,
 * it is not: it cannot be, because none of this runs. Wiring it up means
 * calling it from the STATE_HASH path in client-handler, where the responses it
 * wants to score actually arrive.
 *
 * Tracks client reliability scores for partition assignment.
 * More reliable clients are more likely to be assigned as partition senders.
 *
 * Score factors:
 * - Response rate: Did they send STATE_HASH this frame?
 * - Hash correctness: Did their hash match the majority?
 * - Latency: How quickly did they respond?
 *
 * Scores range from 0-100.
 */

export interface ClientReliabilityInfo {
  /** Current reliability score (0-100) */
  score: number;

  /** Number of frames responded to */
  responsesReceived: number;

  /** Number of frames where hash matched majority */
  hashMatches: number;

  /** Average latency in ms */
  avgLatency: number;

  /** Last frame they responded to */
  lastResponseFrame: number;

  /** When this client was added */
  addedAt: number;
}

export class ReliabilityTracker {
  /** Client reliability info */
  private clients: Map<string, ClientReliabilityInfo> = new Map();

  /** Version number (increments on any change) */
  private _version: number = 0;

  /** Default score for new clients */
  private defaultScore: number = 70;

  /** Score decay per missed frame */
  private missedFramePenalty: number = 5;

  /** Score boost for responding */
  private responseBonus: number = 2;

  /** Score penalty for hash mismatch */
  private mismatchPenalty: number = 20;

  /** Latency threshold (ms) - responses above this are penalized */
  private latencyThreshold: number = 100;

  /**
   * Add a new client to tracking.
   */
  addClient(clientId: string): void {
    if (!this.clients.has(clientId)) {
      this.clients.set(clientId, {
        score: this.defaultScore,
        responsesReceived: 0,
        hashMatches: 0,
        avgLatency: 0,
        lastResponseFrame: 0,
        addedAt: Date.now()
      });
      this._version++;
    }
  }

  /**
   * Remove a client from tracking.
   */
  removeClient(clientId: string): void {
    if (this.clients.delete(clientId)) {
      this._version++;
    }
  }

  /**
   * Update client reliability based on their response (or lack thereof).
   *
   * @param clientId Client identifier
   * @param frame Current frame number
   * @param responded Did they send STATE_HASH this frame?
   * @param hashMatched Did their hash match the majority?
   * @param latencyMs Response latency in milliseconds
   */
  update(
    clientId: string,
    frame: number,
    responded: boolean,
    hashMatched: boolean = true,
    latencyMs: number = 0
  ): void {
    let info = this.clients.get(clientId);

    if (!info) {
      // Auto-add client if not tracked
      info = {
        score: this.defaultScore,
        responsesReceived: 0,
        hashMatches: 0,
        avgLatency: 0,
        lastResponseFrame: 0,
        addedAt: Date.now()
      };
      this.clients.set(clientId, info);
    }

    if (responded) {
      // Update response stats
      info.responsesReceived++;
      info.lastResponseFrame = frame;

      // Update average latency (exponential moving average)
      if (info.avgLatency === 0) {
        info.avgLatency = latencyMs;
      } else {
        info.avgLatency = info.avgLatency * 0.8 + latencyMs * 0.2;
      }

      // Score adjustments for response
      let scoreAdjust = this.responseBonus;

      // Penalize high latency
      if (latencyMs > this.latencyThreshold) {
        const latencyPenalty = Math.min(10, Math.floor((latencyMs - this.latencyThreshold) / 50));
        scoreAdjust -= latencyPenalty;
      }

      // Hash match/mismatch
      if (hashMatched) {
        info.hashMatches++;
        scoreAdjust += 1;
      } else {
        scoreAdjust -= this.mismatchPenalty;
      }

      info.score = Math.max(0, Math.min(100, info.score + scoreAdjust));
    } else {
      // Penalize for not responding
      info.score = Math.max(0, info.score - this.missedFramePenalty);
    }

    this._version++;
  }

  /**
   * Get reliability score for a client.
   * Returns defaultScore for unknown clients.
   */
  getScore(clientId: string): number {
    return this.clients.get(clientId)?.score ?? this.defaultScore;
  }

  /**
   * Get all reliability scores.
   */
  getScores(): Record<string, number> {
    const scores: Record<string, number> = {};
    for (const [clientId, info] of this.clients) {
      scores[clientId] = Math.round(info.score);
    }
    return scores;
  }

  /**
   * Get current version number.
   * Increments on any change.
   */
  getVersion(): number {
    return this._version;
  }

  /**
   * Get detailed info for a client.
   */
  getInfo(clientId: string): ClientReliabilityInfo | undefined {
    return this.clients.get(clientId);
  }

  /**
   * Get all tracked client IDs.
   */
  getClientIds(): string[] {
    return Array.from(this.clients.keys());
  }

  /**
   * Get client count.
   */
  getClientCount(): number {
    return this.clients.size;
  }

  /**
   * Get clients sorted by reliability (highest first).
   */
  getSortedClients(): string[] {
    return Array.from(this.clients.entries())
      .sort((a, b) => b[1].score - a[1].score)
      .map(([clientId]) => clientId);
  }

  /**
   * Apply decay to all clients who haven't responded recently.
   * Call this periodically (e.g., once per second).
   *
   * @param currentFrame Current frame number
   * @param maxFramesBehind Frames behind before decay applies
   */
  applyDecay(currentFrame: number, maxFramesBehind: number = 60): void {
    let changed = false;
    for (const [, info] of this.clients) {
      const framesBehind = currentFrame - info.lastResponseFrame;
      if (framesBehind > maxFramesBehind) {
        const decayAmount = Math.min(5, Math.floor((framesBehind - maxFramesBehind) / 30));
        if (decayAmount > 0) {
          info.score = Math.max(0, info.score - decayAmount);
          changed = true;
        }
      }
    }
    if (changed) {
      this._version++;
    }
  }

  /**
   * Clear all tracking data.
   */
  clear(): void {
    this.clients.clear();
    this._version++;
  }

  /**
   * Get statistics about reliability distribution.
   */
  getStats(): {
    clientCount: number;
    avgScore: number;
    minScore: number;
    maxScore: number;
  } {
    if (this.clients.size === 0) {
      return { clientCount: 0, avgScore: 0, minScore: 0, maxScore: 0 };
    }

    let sum = 0;
    let min = 100;
    let max = 0;

    for (const [, info] of this.clients) {
      sum += info.score;
      min = Math.min(min, info.score);
      max = Math.max(max, info.score);
    }

    return {
      clientCount: this.clients.size,
      avgScore: sum / this.clients.size,
      minScore: min,
      maxScore: max
    };
  }
}
