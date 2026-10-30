import { writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { artifactOccurrenceAt } from "../../fixtures/artifactEntryOrder.js";
import {
  inventoryArtifact,
  materializeArtifactInventory,
} from "../../fixtures/artifactInventory.js";
import { createTestTempDirectory } from "../../fixtures/temporaryDirectory.js";
import { buildUnityFs } from "../../fixtures/unityfs.js";

const writeMixedBundle = async (): Promise<{
  readonly root: string;
  readonly source: string;
}> => {
  const root = await createTestTempDirectory("rea-unityfs-inventory-");
  const source = join(root, "content.bundle");
  await writeFile(
    source,
    buildUnityFs(
      [
        { path: "first.resS", data: Buffer.alloc(16, 0x61) },
        { path: "second.resS", data: Buffer.alloc(16, 0x62) },
      ],
      { blockSize: 16, blockCompression: 2, blockFlags: [2, 1] },
    ),
  );
  return { root, source };
};

describe("UnityFS inventory with unsupported members", () => {
  it("retains an undecodable member while hashing its readable sibling", async () => {
    const { source } = await writeMixedBundle();
    const inventory = await inventoryArtifact(source);

    expect(artifactOccurrenceAt(inventory, "first.resS")).toMatchObject({
      hash_status: "verified",
    });
    const unsupported = artifactOccurrenceAt(inventory, "second.resS");
    expect(unsupported).toMatchObject({
      hash_status: "unavailable",
      artifact_id: null,
    });
    expect(unsupported.limitations.join(" ")).toContain("unsupported");
  });

  it("fails extraction of an unsupported member with a typed unavailable", async () => {
    const { root, source } = await writeMixedBundle();
    const inventory = await inventoryArtifact(source);

    await expect(
      materializeArtifactInventory(
        {
          inputPath: source,
          inputFormat: "unityfs",
          outputRoot: join(root, "out"),
          environment: {},
          integrityPolicy: "fail",
        },
        source,
        inventory,
      ),
    ).rejects.toMatchObject({ reason: "unavailable" });
  });
});
