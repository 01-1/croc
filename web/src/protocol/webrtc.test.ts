import { afterEach, describe, expect, it, vi } from "vitest";
import type { CrocSocket } from "./transport";
import { answerWebRTC, DataChannelSocket, offerWebRTC } from "./webrtc";

const control = vi.hoisted(() => ({
  sent: [] as { t: string }[],
  sendError: undefined as Error | undefined,
  receive: (): Promise<unknown> => new Promise(() => {}),
}));
vi.mock("./relay", () => ({
  sendControl: async (_socket: unknown, message: { t: string }) => {
    control.sent.push(message);
    if (control.sendError) throw control.sendError;
  },
  receiveControl: () => control.receive(),
}));

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

describe("DataChannelSocket cancellation", () => {
  it("closes the channel and rejects pending reads when the transfer is cancelled", async () => {
    const channel = new FakeChannel();
    const abort = new AbortController();
    const socket = new DataChannelSocket(channel as unknown as RTCDataChannel, () => {}, abort.signal);
    const pending = socket.receive();
    abort.abort();
    await expect(pending).rejects.toThrow(/closed/);
    expect(channel.readyState).toBe("closed");
  });
});

// FakePeerConnection completes ICE gathering immediately and records close().
class FakePeerConnection extends EventTarget {
  static last?: FakePeerConnection;
  iceGatheringState: RTCIceGatheringState = "complete";
  localDescription: RTCSessionDescriptionInit | null = null;
  closed = false;
  constructor() {
    super();
    FakePeerConnection.last = this;
  }
  createDataChannel(label: string) {
    const channel = new FakeChannel() as FakeChannel & { label: string };
    channel.readyState = "connecting";
    channel.label = label;
    return channel;
  }
  async createOffer() {
    return { type: "offer", sdp: "v=0 offer" } as RTCSessionDescriptionInit;
  }
  async createAnswer() {
    return { type: "answer", sdp: "v=0 answer" } as RTCSessionDescriptionInit;
  }
  async setLocalDescription(description: RTCSessionDescriptionInit) {
    this.localDescription = description;
  }
  async setRemoteDescription() {}
  close() {
    this.closed = true;
  }
}

describe("WebRTC negotiation", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    control.sent = [];
    control.sendError = undefined;
    control.receive = () => new Promise(() => {});
  });

  it("falls back to the relay when the answer never arrives", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("RTCPeerConnection", FakePeerConnection);
    const result = offerWebRTC({} as CrocSocket, new Uint8Array(32), []);
    await vi.advanceTimersByTimeAsync(13_000);
    await expect(result).resolves.toBeUndefined();
    expect(control.sent.map((message) => message.t)).toEqual(["webrtc-offer"]);
    expect(FakePeerConnection.last?.closed).toBe(true);
  });

  it("falls back to the relay when the peer connection cannot be created", async () => {
    vi.stubGlobal(
      "RTCPeerConnection",
      class {
        constructor() {
          throw new DOMException("WebRTC is disabled", "NotSupportedError");
        }
      },
    );
    await expect(offerWebRTC({} as CrocSocket, new Uint8Array(32), [])).resolves.toBeUndefined();
    expect(control.sent).toEqual([]);
  });

  it("falls back to the relay when a data channel cannot be created", async () => {
    vi.stubGlobal(
      "RTCPeerConnection",
      class extends FakePeerConnection {
        createDataChannel(): FakeChannel & { label: string } {
          throw new DOMException("Too many channels", "OperationError");
        }
      },
    );
    await expect(offerWebRTC({} as CrocSocket, new Uint8Array(32), [])).resolves.toBeUndefined();
    expect(control.sent).toEqual([]);
    expect(FakePeerConnection.last?.closed).toBe(true);
  });

  it("closes the peer connection when sending the answer fails", async () => {
    vi.stubGlobal("RTCPeerConnection", FakePeerConnection);
    const error = new Error("Relay closed while sending the answer");
    control.sendError = error;
    await expect(
      answerWebRTC(
        {} as CrocSocket,
        new Uint8Array(32),
        { t: "webrtc-offer", m: "v=0 offer", n: 2 },
        [],
      ),
    ).rejects.toBe(error);
    expect(control.sent).toEqual([{ t: "webrtc-answer", m: "v=0 answer" }]);
    expect(FakePeerConnection.last?.closed).toBe(true);
  });

  it("closes an answered peer connection when the transfer is cancelled", async () => {
    vi.stubGlobal("RTCPeerConnection", FakePeerConnection);
    const abort = new AbortController();
    const answered = await answerWebRTC(
      {} as CrocSocket,
      new Uint8Array(32),
      { t: "webrtc-offer", m: "v=0 offer", n: 2 },
      [],
      abort.signal,
    );
    expect(answered).toBeDefined();
    expect(control.sent.map((message) => message.t)).toEqual(["webrtc-answer"]);
    abort.abort();
    expect(FakePeerConnection.last?.closed).toBe(true);
  });
});
