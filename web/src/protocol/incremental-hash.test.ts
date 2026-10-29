import { describe, expect, it, vi } from "vitest";
import { XXH64 } from "./xxh64";

// An in-process engine whose hash calls use croc's XXH64.
const engine = vi.hoisted(() => {
  const hashers = new Map<number, { update(bytes: Uint8Array): void; digest(): Uint8Array }>();
  let next = 1;
  return {
    hashers,
    updates: 0,
    create: undefined as undefined | (() => { update(bytes: Uint8Array): void; digest(): Uint8Array }),
    async hashInit() {
      const handle = next++;
      hashers.set(handle, engine.create!());
      return handle;
    },
    async hashUpdate(handle: number, bytes: Uint8Array) {
      engine.updates += 1;
      hashers.get(handle)!.update(bytes.slice());
    },
    async hashFinal(handle: number) {
      return hashers.get(handle)!.digest();
    },
    async hashRelease(handle: number) {
      hashers.delete(handle);
    },
  };
});
engine.create = () => new XXH64();
vi.mock("./engine", () => ({ protocolEngine: () => engine }));

import { hashWhileReceiving } from "./storage";
import type { ReceiveSink } from "./types";

class MemorySink implements ReceiveSink {
  written = 0;
  hashed = 0;
  async writeAt(_position: number, bytes: Uint8Array) {
    this.written += bytes.byteLength;
  }
  async finalize() {}
  async hash() {
    this.hashed += 1;
    return new Uint8Array([9]);
  }
  async commit() {}
  async abort() {}
}

const CHUNK = 32 * 1024;

function fixture(size: number) {
  return new Uint8Array(size).map((_, index) => (index * 31 + 7) % 256);
}

function chunks(data: Uint8Array) {
  const result: [number, Uint8Array][] = [];
  for (let position = 0; position < data.byteLength; position += CHUNK) {
    result.push([position, data.subarray(position, position + CHUNK)]);
  }
  return result;
}

function reference(data: Uint8Array) {
  const hasher = new XXH64();
  hasher.update(data);
  return hasher.digest();
}

describe("hashWhileReceiving", () => {
  it("hashes chunks that arrive out of order without rereading the file", async () => {
    const data = fixture(3 * 1024 * 1024 + 123);
    const inner = new MemorySink();
    const sink = hashWhileReceiving(inner, data.byteLength);
    // Interleave like four data sockets: each socket's chunks arrive in
    // order, but the sockets race each other.
    const parts = chunks(data);
    const order = [...parts.filter((_, i) => i % 4 === 1), ...parts.filter((_, i) => i % 4 !== 1)];
    for (const [position, bytes] of order) await sink.writeAt(position, bytes);
    expect(await sink.hash("xxhash")).toEqual(reference(data));
    expect(inner.hashed).toBe(0);
    expect(inner.written).toBe(data.byteLength);
    // About one worker update per MiB, not one per chunk.
    expect(engine.updates).toBeLessThanOrEqual(5);
  });

  it("falls back to the sink's hash when bytes are missing", async () => {
    const data = fixture(4 * CHUNK);
    const inner = new MemorySink();
    const sink = hashWhileReceiving(inner, data.byteLength);
    for (const [position, bytes] of chunks(data).slice(0, 3)) await sink.writeAt(position, bytes);
    expect(await sink.hash("xxhash")).toEqual(new Uint8Array([9]));
    expect(inner.hashed).toBe(1);
  });

  it("stops buffering and falls back to the sink's hash when one socket stalls", async () => {
    const size = 24 * 1024 * 1024;
    const inner = new MemorySink();
    const sink = hashWhileReceiving(inner, size);
    const retained = () =>
      [...(sink as unknown as { ahead: Map<number, Uint8Array> }).ahead.values()].reduce(
        (total, bytes) => total + bytes.byteLength,
        0,
      );
    // The socket carrying the first chunk falls behind while the others
    // deliver the rest of the file.
    const bytes = new Uint8Array(CHUNK);
    for (let position = CHUNK; position < size; position += CHUNK) {
      await sink.writeAt(position, bytes);
      expect(retained()).toBeLessThanOrEqual(16 * 1024 * 1024);
    }
    await sink.writeAt(0, bytes);
    expect(await sink.hash("xxhash")).toEqual(new Uint8Array([9]));
    expect(inner.hashed).toBe(1);
    expect(inner.written).toBe(size);
  });

  it("leaves SHA-256 verification to the sink", async () => {
    const data = fixture(CHUNK);
    const inner = new MemorySink();
    const sink = hashWhileReceiving(inner, data.byteLength);
    await sink.writeAt(0, data);
    expect(await sink.hash("sha256")).toEqual(new Uint8Array([9]));
    expect(inner.hashed).toBe(1);
  });

  it("releases its hash handle on abort", async () => {
    const inner = new MemorySink();
    const sink = hashWhileReceiving(inner, CHUNK);
    await sink.abort();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(engine.hashers.size).toBe(0);
  });
});
