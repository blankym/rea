import { artifactExpansionBudgetBytes } from "../domain/artifactExpansionBudget.js";
import type { FileHandle } from "node:fs/promises";
import { setImmediate } from "node:timers/promises";
import { Readable } from "node:stream";
import {
  ArtifactReaderFailure,
  copyArtifactEntry,
  sameArtifactEntry,
  type ArtifactEntry,
  type ArtifactReader,
  type ArtifactTraversalOptions,
} from "./ArtifactReader.js";
import { ArtifactResourceScope } from "./ArtifactResourceScope.js";
import { assertArtifactReadActive } from "./ArtifactFileOperation.js";
import { readPckBytes } from "./GodotPckLocation.js";
import type { UnityFsMetadata } from "../domain/artifactFormatMetadata.js";
import { jsonValueByteLength } from "../domain/jsonValueByteLength.js";
import type { StableRegularFileDescriptor } from "../filesystem/RegularFile.js";
import { ArtifactFileSource } from "./ArtifactFileSource.js";
import { decodeLz4Block } from "../gpu/Lz4Block.js";
import {
  UNITYFS_COMPRESSION_LZ4,
  UNITYFS_COMPRESSION_LZ4HC,
  UNITYFS_COMPRESSION_LZMA,
  UNITYFS_COMPRESSION_NONE,
  UNITYFS_FLAG_COMPRESSION_MASK,
  UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION,
} from "../domain/unityfsFormat.js";
import {
  readUnityFsBlocksInfo,
  readUnityFsHeader,
  unityFsFormatFailure,
  type UnityFsBlock,
  type UnityFsNode,
} from "./UnityFsStructures.js";

/** Encoded retained entry metadata, including escaped paths and source facts. */
const MAX_DIRECTORY_METADATA_BYTES = 64 * 1024 * 1024;

const directoryStatus = (flags: number): UnityFsMetadata["directory_status"] =>
  (flags & UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION) !== 0
    ? "encrypted"
    : (flags & UNITYFS_FLAG_COMPRESSION_MASK) === UNITYFS_COMPRESSION_LZMA
      ? "unsupported-compression"
      : "available";

const metadataLimit = (): ArtifactReaderFailure =>
  new ArtifactReaderFailure(
    "limit",
    "UnityFS directory exceeds the 64 MiB encoded metadata budget",
    { boundary: "memory" },
  );

/** One descriptor, one validated directory, and bounded per-block member reads. */
export class UnityFsArtifactReader implements ArtifactReader {
  readonly format = "unityfs" as const;
  readonly #scope = new ArtifactResourceScope();
  readonly #entries = new Map<string, ArtifactEntry>();
  readonly #nodes = new Map<string, UnityFsNode>();
  readonly #streams = new Set<Readable>();
  readonly #source: ArtifactFileSource;
  #prepared: Promise<void> | undefined;
  #header: UnityFsMetadata | undefined;
  #blocks: readonly UnityFsBlock[] = [];
  #dataStart = 0;
  #closing = false;

  constructor(path: string, admitted?: StableRegularFileDescriptor) {
    this.#source = new ArtifactFileSource(path, "UnityFS bundle", admitted);
  }

  formatMetadata(): UnityFsMetadata | undefined {
    return this.#header === undefined ? undefined : { ...this.#header };
  }

  async prepareContainer(
    expectedSha256: string,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.#scope.run(async () => {
      this.#assertOpen(signal);
      await this.#source.bindSha256(expectedSha256, signal);
    });
  }

  async *entries(
    signal?: AbortSignal,
    options?: ArtifactTraversalOptions,
  ): AsyncIterable<ArtifactEntry> {
    await this.#scope.run(async () => {
      this.#assertOpen(signal);
      this.#prepared ??= this.#prepare(signal);
      await this.#prepared;
      await this.#source.read(signal);
    });
    if (this.#header?.directory_status !== "available") {
      options?.onCoverage?.({
        status: "unknown",
        limitations: [
          this.#header?.directory_status === "encrypted"
            ? "UnityFS bundle declares asset-bundle encryption; member identities and contents remain unknown."
            : "UnityFS blocks-and-directory table uses an unsupported compression; member identities remain unknown.",
        ],
      });
      return;
    }
    let index = 0;
    for (const entry of this.#entries.values()) {
      if (index++ % 256 === 0) await setImmediate();
      this.#assertOpen(signal);
      yield copyArtifactEntry(entry);
    }
  }

  async open(entry: ArtifactEntry, signal?: AbortSignal): Promise<Readable> {
    return this.#scope.run(async () => {
      this.#assertOpen(signal);
      const stored = this.#entries.get(entry.adapterKey);
      const node = this.#nodes.get(entry.adapterKey);
      if (
        stored === undefined ||
        node === undefined ||
        !sameArtifactEntry(entry, stored)
      )
        throw new ArtifactReaderFailure(
          "integrity",
          "UnityFS entry does not match its owning inventory",
        );
      const { handle } = await this.#source.read(signal);
      this.#assertOpen(signal);
      const output = this.#memberStream(handle, stored, node, signal);
      output.on("error", () => undefined);
      this.#streams.add(output);
      output.on("close", () => this.#streams.delete(output));
      return output;
    });
  }

  async close(): Promise<void> {
    this.#closing = true;
    for (const stream of this.#streams)
      stream.destroy(
        new ArtifactReaderFailure("unavailable", "UnityFS reader closed"),
      );
    await this.#scope.close();
    await this.#source.close();
    this.#entries.clear();
    this.#nodes.clear();
  }

  provenance(): readonly [] {
    return [];
  }

  #assertOpen(signal?: AbortSignal): void {
    assertArtifactReadActive(signal);
    if (this.#closing)
      throw new ArtifactReaderFailure(
        "unavailable",
        "UnityFS reader is closed",
      );
  }

  async #prepare(signal?: AbortSignal): Promise<void> {
    const { handle, initial } = await this.#source.read(signal);
    const header = await readUnityFsHeader(handle, initial.size, signal);
    if (header === undefined)
      throw unityFsFormatFailure("no UnityFS signature or header");
    const status = directoryStatus(header.flags);
    this.#header = {
      kind: "unityfs",
      signature: "UnityFS",
      format_version: header.formatVersion,
      player_version: header.playerVersion,
      engine_version: header.engineVersion,
      declared_size: header.declaredSize,
      flags: header.flags,
      blocks_info_compressed_size: header.compressedBlocksInfoSize,
      blocks_info_uncompressed_size: header.uncompressedBlocksInfoSize,
      blocks_info_at_end: header.blocksInfoAtEnd,
      data_start: 0,
      block_count: 0,
      data_hash: null,
      directory_status: status,
    };
    if (status !== "available") return;
    const { dataHash, blocks, nodes, dataStart } = await readUnityFsBlocksInfo(
      handle,
      header,
      initial.size,
      signal,
    );
    this.#assertBlocksWithinBudget(blocks, initial.size);
    this.#header = {
      ...this.#header,
      data_hash: dataHash,
      block_count: blocks.length,
      data_start: dataStart,
    };
    this.#dataStart = dataStart;
    this.#blocks = blocks;
    await this.#registerNodes(nodes, signal);
    await this.#source.read(signal);
  }

  #assertBlocksWithinBudget(
    blocks: readonly UnityFsBlock[],
    fileSize: number,
  ): void {
    const expansionBudget = BigInt(artifactExpansionBudgetBytes(fileSize));
    let declaredStreamBytes = 0n;
    for (const block of blocks) {
      declaredStreamBytes += BigInt(block.uncompressedSize);
      if (declaredStreamBytes > expansionBudget)
        throw new ArtifactReaderFailure(
          "limit",
          `UnityFS blocks declare ${String(declaredStreamBytes)} stream bytes, exceeding this bundle's ${String(expansionBudget)}-byte expansion budget`,
        );
    }
  }

  async #registerNodes(
    nodes: readonly UnityFsNode[],
    signal?: AbortSignal,
  ): Promise<void> {
    let retainedBytes = 0;
    for (let index = 0; index < nodes.length; index++) {
      if (index % 256 === 0) await setImmediate();
      assertArtifactReadActive(signal);
      const node = nodes[index]!;
      const adapterKey = `unityfs-entry:${String(index)}`;
      const metadata = {
        path: node.path,
        sourcePath: node.path,
        kind: "file" as const,
        declaredSize: node.size,
        compressedSize: null,
        executable: false,
        encrypted: (node.flags & 1) !== 0,
        byteOffset: node.offset,
        declaredIntegrity: null,
        unpacked: false,
        limitations: [],
        adapterKey,
        formatMetadata: {
          kind: "unityfs-entry" as const,
          index,
          source_path: node.path,
          declared_offset: node.offset,
          flags: node.flags,
        },
      } satisfies ArtifactEntry;
      retainedBytes += jsonValueByteLength(
        metadata,
        MAX_DIRECTORY_METADATA_BYTES - retainedBytes,
      );
      if (retainedBytes > MAX_DIRECTORY_METADATA_BYTES) throw metadataLimit();
      this.#entries.set(adapterKey, metadata);
      this.#nodes.set(adapterKey, node);
    }
  }

  /** Decompress only the blocks covering the member range, one at a time. */
  #memberStream(
    handle: FileHandle,
    entry: ArtifactEntry,
    node: UnityFsNode,
    signal?: AbortSignal,
  ): Readable {
    const blocks = this.#blocks;
    const dataStart = this.#dataStart;
    const scope = this.#scope;
    const source = this.#source;
    const end = node.offset + node.size;
    const first = blocks.findIndex(
      (block) => block.streamStart + block.uncompressedSize > node.offset,
    );
    const last =
      node.size === 0
        ? first
        : blocks.findIndex(
            (block) => block.streamStart + block.uncompressedSize >= end,
          );
    if (node.size !== 0 && (first === -1 || last === -1))
      throw unityFsFormatFailure(
        `member range is beyond the stream: ${entry.path}`,
      );
    return Readable.from(
      (async function* () {
        try {
          if (node.size === 0) return;
          let delivered = 0;
          for (let index = first; index <= last; index++) {
            assertArtifactReadActive(signal);
            const block = blocks[index]!;
            const compressed = await readPckBytes(
              handle,
              dataStart + block.compressedStart,
              block.compressedSize,
              dataStart +
                (blocks[index + 1]?.compressedStart ??
                  block.compressedStart + block.compressedSize),
              signal,
            );
            const plain = await decodeUnityFsBlock(block, compressed, signal);
            const sliceStart = Math.max(node.offset - block.streamStart, 0);
            const sliceEnd = Math.min(
              end - block.streamStart,
              block.uncompressedSize,
            );
            if (sliceEnd > sliceStart) {
              const chunk = plain.subarray(sliceStart, sliceEnd);
              delivered += chunk.length;
              yield chunk;
            }
            if (index % 64 === 63) await setImmediate();
          }
          if (delivered !== node.size)
            throw unityFsFormatFailure(`member ended early: ${entry.path}`);
          await scope.run(async () => {
            await source.read(signal);
          });
        } catch (cause: unknown) {
          if (cause instanceof ArtifactReaderFailure) throw cause;
          throw new ArtifactReaderFailure(
            "format",
            `UnityFS member read failed: ${entry.path}`,
            { cause },
          );
        }
      })(),
    );
  }
}

const decodeUnityFsBlock = async (
  block: UnityFsBlock,
  compressed: Buffer,
  signal?: AbortSignal,
): Promise<Buffer> => {
  switch (block.flags & UNITYFS_FLAG_COMPRESSION_MASK) {
    case UNITYFS_COMPRESSION_NONE:
      if (compressed.length !== block.uncompressedSize)
        throw unityFsFormatFailure(
          "plain block size disagrees with its declaration",
        );
      return compressed;
    case UNITYFS_COMPRESSION_LZ4:
    case UNITYFS_COMPRESSION_LZ4HC:
      try {
        return await decodeLz4Block(compressed, block.uncompressedSize, signal);
      } catch (cause: unknown) {
        throw new ArtifactReaderFailure(
          "format",
          "UnityFS is malformed: LZ4 block does not decode to its declared size",
          { cause },
        );
      }
    case UNITYFS_COMPRESSION_LZMA:
      throw new ArtifactReaderFailure(
        "unavailable",
        "UnityFS LZMA-compressed blocks are unsupported",
      );
    default:
      throw new ArtifactReaderFailure(
        "unavailable",
        `UnityFS block compression 0x${String(block.flags & UNITYFS_FLAG_COMPRESSION_MASK)} is unsupported`,
      );
  }
};
