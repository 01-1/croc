import { beforeEach, describe, expect, it, vi } from "vitest";

const control = vi.hoisted(() => ({ messages: [] as { t: string; m?: string; n?: number }[] }));
const answered = vi.hoisted(() => ({ close: vi.fn(), wait: vi.fn() }));

vi.mock("./compression", () => ({ checkProtocolCapabilities: () => undefined }));
vi.mock("./engine", () => ({ protocolEngine: () => ({}) }));
vi.mock("./relay", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./relay")>()),
  receiveControl: async () => {
    const next = control.messages.shift();
    if (!next) throw new Error("no more control messages");
    return next;
  },
  sendControl: async () => {},
}));
vi.mock("./webrtc", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./webrtc")>()),
  webrtcSupported: () => true,
  answerWebRTC: async () => answered,
}));

import { openReceiverData } from "./client";
import type { CrocSocket } from "./transport";
import { WEBRTC_FEATURE } from "./webrtc";

function negotiate() {
  return openReceiverData(
    {} as CrocSocket,
    new Uint8Array(32),
    [WEBRTC_FEATURE],
    {} as Parameters<typeof openReceiverData>[3],
    "room",
    "",
    0,
    undefined,
  );
}

const offer = { t: "webrtc-offer", m: "v=0 offer", n: 2 };

describe("openReceiverData", () => {
  beforeEach(() => {
    answered.close.mockReset();
    answered.wait.mockReset();
  });

  it("closes an answered connection when the sender cancels before selecting", async () => {
    control.messages = [offer, { t: "error", m: "Sender cancelled" }];
    await expect(negotiate()).rejects.toThrow("Sender cancelled");
    expect(answered.close).toHaveBeenCalled();
  });

  it("closes an answered connection when the selected channels never open", async () => {
    control.messages = [offer, { t: "transport-select", m: "webrtc" }];
    answered.wait.mockRejectedValue(new Error("Timed out waiting for WebRTC data channels"));
    await expect(negotiate()).rejects.toThrow("WebRTC data channels");
    expect(answered.close).toHaveBeenCalled();
  });

  it("closes an answered connection after an unexpected message", async () => {
    control.messages = [offer, { t: "files" }];
    await expect(negotiate()).rejects.toThrow("Expected a transport selection");
    expect(answered.close).toHaveBeenCalled();
  });
});
