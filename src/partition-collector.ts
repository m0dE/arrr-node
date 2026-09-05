/**
 * Partition Collector
 *
 * Collects STATE_HASH and PARTITION_DATA messages from clients for each frame.
 * Determines majority hash and assembles delta from trusted partition data.
 *
 * Flow:
 * 1. Clients send STATE_HASH after each tick
 * 2. Assigned senders send PARTITION_DATA
 * 3. Server collects, determines majority hash
 * 4. Server accepts partition data only from clients with matching hash
 * 5. Server broadcasts assembled delta (or just majority hash in TICK)
 *
 * Only step 1 and the majority actually run. Traced 2026-08-17, because this
 * has been described as "half dead" several times and a decision about deleting
 * it deserves better than an impression:
 *
 *   STATE_HASH            live. Clients send it every tick, the majority
 *                         computed from it rides in TICK, and desync detection
 *                         and snapshot acceptance both depend on it.
 *   PARTITION_DATA        accepted and stored, never consulted.
 *                         getTrustedPartitionData is called only by
 *                         getAssembledDelta - which nothing outside this file
 *                         calls - and by getFrameStatus, which only
 *                         roomManager.getPartitionStatus calls, which nothing
 *                         calls at all.
 *   MAJORITY_HASH (0x32)  encodeMajorityHash exists and is never called. The
 *                         majority travels inside TICK instead.
 *   DELTA_REQUEST (0x34)  no sender anywhere in the SDK.
 *   DELTA_RESPONSE (0x35) encoder exists, never called.
 *   reliability-tracker   288 lines, imported by nothing; the only mention of
 *                         it outside itself is a comment in binary-protocol.ts.
 *
 * So the partition half is inert: it costs the node the memory to hold what
 * clients send, and it cost two security fixes - a frame-window check and a
 * read-only check - to make surface that nothing reads safe to expose. Whether
 * to finish it or remove it is a decision about intent rather than about code,
 * which is why this only records the facts.
 */

export interface FrameCollection {
  /** Frame number */
  frame: number;

  /** Client ID -> state hash */
  stateHashes: Map<string, number>;

  /** Client ID -> partition ID -> partition data */
  partitionData: Map<string, Map<number, Buffer>>;

  /** Timestamp when collection started */
  startedAt: number;

  /** Majority hash (computed lazily) */
  _majorityHash: number | null;
  _majorityComputed: boolean;
  /**
   * How many voters the cached verdict was judged against.
   *
   * The cache used to key on the votes alone, which quietly reintroduced the
   * bug this class exists to avoid. A verdict is "more than half of the room",
   * so it depends on the room's size as much as on the votes - and several
   * callers here ask without knowing it. One of those computing first would
   * cache a majority of whoever had answered, and the caller that did know the
   * room size would then be handed that cached answer instead of a correct one.
   * Same false desyncs, arriving through the cache rather than the arithmetic.
   */
  _majorityVoters: number;
}

export class PartitionCollector {
  /** Frame number -> collection data */
  private frames: Map<number, FrameCollection> = new Map();

  /** Maximum frames to keep in memory */
  private maxFrames: number = 10;

  /** Number of partitions per frame (set based on entity count) */
  private numPartitions: number = 3;

  /**
   * Set the number of partitions for future frames.
   */
  setPartitionCount(count: number): void {
    this.numPartitions = count;
  }

  /**
   * Get the current partition count.
   */
  getPartitionCount(): number {
    return this.numPartitions;
  }

  /**
   * Add a STATE_HASH from a client.
   */
  addStateHash(clientId: string, frame: number, hash: number): void {
    const collection = this.getOrCreateFrame(frame);
    collection.stateHashes.set(clientId, hash);
    // Invalidate cached majority
    collection._majorityComputed = false;
    collection._majorityHash = null;
    collection._majorityVoters = 0;
  }

  /**
   * Remove a disconnected client's data from all frames.
   * This prevents disconnected clients from polluting the majority hash.
   */
  removeClient(clientId: string): void {
    for (const [frame, collection] of this.frames) {
      if (collection.stateHashes.delete(clientId)) {
        // Invalidate cached majority since we removed a voter
        collection._majorityComputed = false;
        collection._majorityHash = null;
      }
      collection.partitionData.delete(clientId);
    }
  }

  /**
   * Add PARTITION_DATA from a client.
   */
  addPartitionData(clientId: string, frame: number, partitionId: number, data: Buffer): void {
    const collection = this.getOrCreateFrame(frame);

    let clientPartitions = collection.partitionData.get(clientId);
    if (!clientPartitions) {
      clientPartitions = new Map();
      collection.partitionData.set(clientId, clientPartitions);
    }

    clientPartitions.set(partitionId, data);
  }

  /**
   * Is this client out of step with the room, judged on frames already decided?
   *
   * Asked when a client offers a snapshot. Comparing that snapshot against the
   * consensus for its own frame does not work: a client publishes the frame it
   * has just simulated, and the votes that decide that frame are still arriving,
   * so the check is undecidable exactly when it is needed. Measured - the
   * corrupt snapshot was accepted every time, because "no consensus yet" was
   * being read as "nothing wrong".
   *
   * Recent decided frames answer it instead. A client that has diverged
   * disagrees on every frame, not just the one it happens to be publishing, so
   * a few settled frames are enough to tell - and they are settled precisely
   * because they are behind.
   *
   * Wants more than one disagreement, so that a single frame where this client
   * was momentarily ahead of the votes it is compared against does not cost it
   * the right to publish.
   */
  clientOutOfStep(clientId: string, lookback = 12, needed = 2): boolean {
    const decided: number[] = [];
    for (const frame of this.frames.keys()) decided.push(frame);
    decided.sort((a, b) => b - a);

    let seen = 0;
    let disagreed = 0;
    for (const frame of decided) {
      if (seen >= lookback) break;
      const collection = this.frames.get(frame);
      if (!collection) continue;
      const theirs = collection.stateHashes.get(clientId);
      if (theirs === undefined) continue;
      const majority = this.getMajorityHash(frame);
      if (majority === null) continue;
      seen++;
      if ((theirs >>> 0) !== (majority >>> 0)) disagreed++;
      if (disagreed >= needed) return true;
    }
    return false;
  }

  /**
   * Get the majority hash for a frame.
   * Returns null if no majority exists (tie or insufficient data).
   */
  getMajorityHash(frame: number, expectedVoters?: number): number | null {
    const collection = this.frames.get(frame);
    if (!collection) return null;

    // Cached, but only for the same room size it was judged against.
    const voterCount = Math.max(expectedVoters || 0, collection.stateHashes.size);
    if (collection._majorityComputed && collection._majorityVoters === voterCount) {
      return collection._majorityHash;
    }

    // Compute majority
    const hashCounts = new Map<number, number>();

    for (const hash of collection.stateHashes.values()) {
      hashCounts.set(hash, (hashCounts.get(hash) || 0) + 1);
    }

    // Find hash with most votes
    let maxCount = 0;
    let maxHash: number | null = null;
    let tieCount = 0;

    for (const [hash, count] of hashCounts) {
      if (count > maxCount) {
        maxCount = count;
        maxHash = hash;
        tieCount = 1;
      } else if (count === maxCount) {
        tieCount++;
      }
    }

    // A majority of the room, not of whoever happened to answer first.
    //
    // This used to divide by the number of votes received, so the first hash to
    // arrive for a frame was a majority of one and became the consensus - and
    // every other client, on comparing against it, reported itself desynced.
    // Observed live: four clients holding an identical hash all flagged against
    // a "consensus" that was the fifth client's, at a frame where only that one
    // had reported yet. Four agreeing clients are not four desyncs.
    //
    // Judging against the room's size instead means no verdict is issued until
    // enough clients have actually spoken. No verdict is safe: clients skip the
    // comparison entirely when the server sends none.
    const voters = voterCount;
    if (maxCount > voters / 2 && tieCount === 1) {
      collection._majorityHash = maxHash;
    } else {
      collection._majorityHash = null;
    }

    // Behind a flag, like the other traces. This fired on every frame where any
    // two clients disagreed - twenty lines a second for as long as a desync
    // lasted, i.e. loudest exactly when the log is most worth reading. The
    // desync reports carry the same information, once per client, already
    // bounded.
    if (process.env.MAJORITY_TRACE) {
      const contributors: string[] = [];
      for (const [cid, h] of collection.stateHashes) { contributors.push(`${cid.slice(0, 8)}=${h.toString(16)}`); }
      console.log(`[MAJORITY-HASH] frame=${frame} result=${collection._majorityHash?.toString(16) ?? 'null'} votes=${collection.stateHashes.size}/${voterCount} contributors=[${contributors.join(',')}]`);
    }
    collection._majorityComputed = true;
    collection._majorityVoters = voterCount;
    return collection._majorityHash;
  }

  /**
   * Check if a client's hash matches the majority.
   */
  isClientTrusted(clientId: string, frame: number): boolean {
    const collection = this.frames.get(frame);
    if (!collection) return false;

    const clientHash = collection.stateHashes.get(clientId);
    if (clientHash === undefined) return false;

    const majorityHash = this.getMajorityHash(frame);
    if (majorityHash === null) return false;

    return clientHash === majorityHash;
  }

  /**
   * Get all partition data from trusted clients for a frame.
   * Returns map of partitionId -> data.
   */
  getTrustedPartitionData(frame: number): Map<number, Buffer> | null {
    const collection = this.frames.get(frame);
    if (!collection) return null;

    const majorityHash = this.getMajorityHash(frame);
    if (majorityHash === null) return null;

    const result = new Map<number, Buffer>();

    // Collect partition data from trusted clients only
    for (const [clientId, clientPartitions] of collection.partitionData) {
      // Only accept data from clients whose hash matches majority
      if (!this.isClientTrusted(clientId, frame)) {
        continue;
      }

      for (const [partitionId, data] of clientPartitions) {
        // First trusted data for each partition wins
        if (!result.has(partitionId)) {
          result.set(partitionId, data);
        }
      }
    }

    return result;
  }

  /**
   * Get assembled delta from all partitions.
   * Returns null if not all partitions are available.
   */
  getAssembledDelta(frame: number): Buffer | null {
    const partitions = this.getTrustedPartitionData(frame);
    if (!partitions) return null;

    // Check if we have all partitions
    if (partitions.size < this.numPartitions) {
      return null;
    }

    // Assemble partitions into single buffer
    // Format: [numPartitions:1][partitionId:1][len:2][data]...
    let totalSize = 1;
    for (const data of partitions.values()) {
      totalSize += 1 + 2 + data.length; // partitionId + len + data
    }

    const buffer = Buffer.alloc(totalSize);
    let offset = 0;

    buffer[offset++] = this.numPartitions;

    // Write partitions in order
    for (let i = 0; i < this.numPartitions; i++) {
      const data = partitions.get(i);
      if (!data) {
        // Missing partition
        return null;
      }

      buffer[offset++] = i;
      buffer.writeUInt16LE(data.length, offset); offset += 2;
      data.copy(buffer, offset); offset += data.length;
    }

    return buffer;
  }

  /**
   * Get collection status for a frame.
   */
  getFrameStatus(frame: number): {
    exists: boolean;
    hashCount: number;
    partitionCount: number;
    trustedClientCount: number;
    majorityHash: number | null;
    isComplete: boolean;
  } {
    const collection = this.frames.get(frame);
    if (!collection) {
      return {
        exists: false,
        hashCount: 0,
        partitionCount: 0,
        trustedClientCount: 0,
        majorityHash: null,
        isComplete: false
      };
    }

    const majorityHash = this.getMajorityHash(frame);
    let trustedClientCount = 0;
    if (majorityHash !== null) {
      for (const [clientId] of collection.stateHashes) {
        if (this.isClientTrusted(clientId, frame)) {
          trustedClientCount++;
        }
      }
    }

    const partitions = this.getTrustedPartitionData(frame);
    const partitionCount = partitions?.size ?? 0;

    return {
      exists: true,
      hashCount: collection.stateHashes.size,
      partitionCount,
      trustedClientCount,
      majorityHash,
      isComplete: partitionCount >= this.numPartitions
    };
  }

  /**
   * Get clients who submitted hashes for a frame.
   */
  getSubmittedClients(frame: number): string[] {
    const collection = this.frames.get(frame);
    if (!collection) return [];
    return Array.from(collection.stateHashes.keys());
  }

  /**
   * Get clients whose hash doesn't match majority (potential cheaters or desynced).
   */
  getUntrustedClients(frame: number): string[] {
    const collection = this.frames.get(frame);
    if (!collection) return [];

    const majorityHash = this.getMajorityHash(frame);
    if (majorityHash === null) return [];

    const untrusted: string[] = [];
    for (const [clientId, hash] of collection.stateHashes) {
      if (hash !== majorityHash) {
        untrusted.push(clientId);
      }
    }

    return untrusted;
  }

  /** Frame count and total votes held, for leak hunting. */
  debugSizes(): { frames: number; hashVotes: number } {
    let hashVotes = 0;
    for (const c of this.frames.values()) hashVotes += c.stateHashes.size + c.partitionData.size;
    return { frames: this.frames.size, hashVotes };
  }

  /**
   * Clean up old frames.
   */
  pruneOldFrames(currentFrame: number): void {
    const minFrame = currentFrame - this.maxFrames;
    for (const frame of this.frames.keys()) {
      if (frame < minFrame) {
        this.frames.delete(frame);
      }
    }
  }

  /**
   * Clear all collection data.
   */
  clear(): void {
    this.frames.clear();
  }

  /**
   * Get or create frame collection.
   */
  private getOrCreateFrame(frame: number): FrameCollection {
    let collection = this.frames.get(frame);
    if (!collection) {
      collection = {
        frame,
        stateHashes: new Map(),
        partitionData: new Map(),
        startedAt: Date.now(),
        _majorityHash: null,
        _majorityComputed: false,
        _majorityVoters: 0
      };
      this.frames.set(frame, collection);

      // Prune old frames
      this.pruneOldFrames(frame);
    }
    return collection;
  }
}
