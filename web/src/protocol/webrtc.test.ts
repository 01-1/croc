import { describe, expect, it } from "vitest";
import { DataChannelSocket } from "./webrtc";

// FakeChannel is a pair of linked data channels: send() delivers each message
// to the peer asynchronously, like RTCDataChannel.
class FakeChannel extends EventTarget {
  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  readyState: RTCDataChannelState = "open";
  peer?: FakeChannel;
  sizes: number[] = [];

  send(data: Uint8Array) {
    this.sizes.push(data.byteLength);
    const message = data.slice().buffer;
    queueMicrotask(() =>
      this.peer?.dispatchEvent(new MessageEvent("message", { data: message })),
    );
  }

  close() {
    if (this.readyState === "closed") return;
    this.readyState = "closed";
    this.dispatchEvent(new Event("close"));
    this.peer?.close();
  }
}

function linkedSockets() {
  const a = new FakeChannel();
  const b = new FakeChannel();
  a.peer = b;
  b.peer = a;
  return {
    a,
    b,
    left: new DataChannelSocket(a as unknown as RTCDataChannel),
    right: new DataChannelSocket(b as unknown as RTCDataChannel),
  };
}

describe("DataChannelSocket", () => {
  it("carries framed messages in order and in both directions", async () => {
    const { left, right } = linkedSockets();
    await left.send(new Uint8Array([1, 2, 3]));
    await left.send(new Uint8Array([4]));
    expect([...(await right.receive(false))]).toEqual([1, 2, 3]);
    expect([...(await right.receive(false))]).toEqual([4]);
    await right.send(new Uint8Array([9, 9]));
    expect([...(await left.receive())]).toEqual([9, 9]);
  });

  it("splits large frames into 16 KiB messages and reassembles them", async () => {
    const { a, left, right } = linkedSockets();
    const payload = new Uint8Array(100 * 1024).map((_, index) => index % 251);
    await left.send(payload);
    const received = await right.receive();
    expect(received).toEqual(payload);
    expect(Math.max(...a.sizes)).toBeLessThanOrEqual(16 * 1024);
    expect(a.sizes.reduce((total, size) => total + size, 0)).toBe(payload.byteLength + 8);
  });

  it("waits for the buffer to drain before sending more", async () => {
    const { a, left } = linkedSockets();
    a.bufferedAmount = 8 * 1024 * 1024;
    let sent = false;
    const sending = left.send(new Uint8Array([1])).then(() => {
      sent = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sent).toBe(false);
    a.bufferedAmount = 0;
    a.dispatchEvent(new Event("bufferedamountlow"));
    await sending;
    expect(sent).toBe(true);
  });

  it("skips relay pings only when asked to", async () => {
    const { left, right } = linkedSockets();
    await left.send(new Uint8Array([1]));
    await left.send(new Uint8Array([7]));
    expect([...(await right.receive())]).toEqual([7]);
  });

  it("rejects pending and later reads once the channel closes", async () => {
    const { left, right } = linkedSockets();
    const pending = right.receive();
    left.close();
    await expect(pending).rejects.toThrow(/closed/);
    await expect(right.receive()).rejects.toThrow(/closed/);
    await expect(left.send(new Uint8Array([1]))).rejects.toThrow();
  });

  it("calls its close hook once per socket", () => {
    const a = new FakeChannel();
    let closed = 0;
    const socket = new DataChannelSocket(a as unknown as RTCDataChannel, () => {
      closed += 1;
    });
    socket.close();
    socket.close();
    expect(closed).toBe(1);
  });
});
