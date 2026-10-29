import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CrocMessage } from "./types";

const control = vi.hoisted(() => ({
  messages: [] as CrocMessage[],
  sent: [] as CrocMessage[],
  failAt: "" as string,
  error: new Error("Relay signaling write failed"),
  close: vi.fn(),
}));

vi.mock("./compression", () => ({ checkProtocolCapabilities: () => undefined }));
vi.mock("./engine", () => ({
  protocolEngine: () => ({
    codeComponents: async () => ({ room: "room", passphrase: "phrase" }),
    relayIndex: async () => 0,
    pakeInitWithIdentities: async () => ({ handle: 1, bytes: new Uint8Array([1]) }),
    pakeUpdate: async () => ({ key: new Uint8Array([2]), bytes: new Uint8Array([3]) }),
    derivePeerKeys: async () => ({
      key: new Uint8Array(32),
      confirmationA: new Uint8Array([1]),
      confirmationB: new Uint8Array([2]),
    }),
    confirmPeerKey: async () => true,
    cipherInit: async () => 1,
    pakeRelease: async () => {},
    cipherRelease: async () => {},
  }),
}));
vi.mock("./relay", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./relay")>()),
  connectRelay: async () => ({
    socket: {
      receive: async () => new TextEncoder().encode("handshake"),
      send: async () => {},
      close: control.close,
    },
    banner: "9010",
    externalIP: "127.0.0.1",
  }),
  receiveControl: async () => {
    const message = control.messages.shift();
    if (!message) throw new Error("Unexpected control read");
    return message;
  },
  sendControl: async (_socket: unknown, message: CrocMessage) => {
    control.sent.push(message);
    if (message.t === control.failAt) throw control.error;
  },
}));

import { sendFiles } from "./client";
import { WEBRTC_FEATURE } from "./webrtc";

class FakeChannel extends EventTarget {
  readyState: RTCDataChannelState = "open";
  binaryType: BinaryType = "arraybuffer";
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;

  close() {
    if (this.readyState === "closed") return;
    this.readyState = "closed";
    this.dispatchEvent(new Event("close"));
  }
}

class FakePeerConnection extends EventTarget {
  static all: FakePeerConnection[] = [];
  iceGatheringState: RTCIceGatheringState = "complete";
  localDescription: RTCSessionDescriptionInit | null = null;
  channels: FakeChannel[] = [];
  closed = false;

  constructor() {
    super();
    FakePeerConnection.all.push(this);
  }
  createDataChannel() {
    const channel = new FakeChannel();
    this.channels.push(channel);
    return channel;
  }
  async createOffer() {
    return { type: "offer", sdp: "v=0 offer" } as RTCSessionDescriptionInit;
  }
  async setLocalDescription(description: RTCSessionDescriptionInit) {
    this.localDescription = description;
  }
  async setRemoteDescription() {}
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const channel of this.channels) channel.close();
  }
}

describe("sendFiles WebRTC signaling failures", () => {
  beforeEach(() => {
    vi.stubGlobal("RTCPeerConnection", FakePeerConnection);
    FakePeerConnection.all = [];
    control.close.mockReset();
    control.sent = [];
    control.messages = [
      {
        t: "pake",
        v: 2,
        b: new Uint8Array([1]),
        b2: new TextEncoder().encode("p256"),
        f: [WEBRTC_FEATURE],
      },
      { t: "pake-confirm", v: 2, b: new Uint8Array([1]) },
      { t: "webrtc-answer", m: "v=0 answer" },
    ];
  });

  afterEach(() => {
    for (const pc of FakePeerConnection.all) pc.close();
    vi.unstubAllGlobals();
  });

  it.each(["webrtc-offer", "transport-select"] as const)(
    "closes the peer and its channels when sending %s fails",
    async (failAt) => {
      control.failAt = failAt;
      const controller = new AbortController();
      await expect(
        sendFiles({
          files: [{
            file: new File(["data"], "test.txt"),
            name: "test.txt",
            size: 4,
            hash: new Uint8Array(),
            modified: new Date(0).toISOString(),
          }],
          secret: "sample-code",
          settings: {
            gatewayURL: "/ws",
            relayAddresses: ["relay:9009"],
            relayPassword: "pass",
            storeAPI: "/store",
            stunServers: [],
          },
          signal: controller.signal,
        }),
      ).rejects.toBe(control.error);

      expect(controller.signal.aborted).toBe(false);
      expect(control.close).toHaveBeenCalled();
      expect(control.sent).toContainEqual(expect.objectContaining({ t: failAt }));
      expect(FakePeerConnection.all).toHaveLength(1);
      const pc = FakePeerConnection.all[0];
      expect(pc.channels).toHaveLength(4);
      expect(pc.closed).toBe(true);
      expect(pc.channels.every((channel) => channel.readyState === "closed")).toBe(true);
    },
  );
});
